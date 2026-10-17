"""Parse SrvSurvey bio-criteria JSON files into a Python species prediction engine.

Reads the hierarchical JSON criteria trees from SrvSurvey's bio-criteria directory,
flattens them into per-species match branches, and evaluates them against the same
body_row dicts that MGST's score_body() uses.
"""

import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from mgst.bio.species_rules import (
    ATMOSPHERE_MAP,
    HIGH_VALUE_SPECIES,
    matches_atmosphere,
)
from mgst.bio.region import find_region

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SRVSURV_CRITERIA_DIR = "/mnt/z/SrvSurvey/SrvSurvey/bio-criteria"

BODY_MAP: Dict[str, str] = {
    "HMC": "High metal content world",
    "Rocky": "Rocky body",
    "RockyIce": "Rocky Ice world",
    "Rocky Ice": "Rocky Ice world",
    "Icy": "Icy body",
    "MRB": "Metal-rich body",
}

REGION_GROUPS: Dict[str, Set[int]] = {
    "Sagittarius-CarinaArm": {9, 18, 19, 20, 21, 22, 23, 40},
    "Scutum-CentaurusArm": {9, 10, 11, 12, 24, 25, 26, 42, 28},
    "CentreLeft": {1, 4},
    "CentreTop": {1, 3, 7},
    "CentreRight": {1, 2},
    "Orion-CygnusArm": {7, 8, 16, 17, 18, 35},
    "OuterArm": {5, 6, 13, 14, 27, 29, 31, 41, 37},
    "PerseusArm": {15, 30, 32, 33, 34, 36, 38, 39},
}

# SrvSurvey gas name (lowered) → Parquet column
GAS_COLUMN_MAP: Dict[str, str] = {
    "carbondioxide": "atm_Carbon_dioxide",
    "sulphurdioxide": "atm_Sulphur_dioxide",
    "water": "atm_Water",
    "ammonia": "atm_Ammonia",
    "methane": "atm_Methane",
    "argon": "atm_Argon",
    "oxygen": "atm_Oxygen",
    "nitrogen": "atm_Nitrogen",
    "neon": "atm_Neon",
    "hydrogen": "atm_Hydrogen",
    "helium": "atm_Helium",
    "iron": "atm_Iron",
    "silicates": "atm_Silicates",
}

# Property name → body_row key for numeric range checks
PROP_MAP: Dict[str, str] = {
    "gravity": "gravity",
    "temp": "surfaceTemperature",
    "pressure": "surfacePressure",
}

# Build value lookup from MGST species definitions
_SPECIES_VALUE: Dict[str, int] = {
    sp["name"]: sp["min_value"] for sp in HIGH_VALUE_SPECIES
}
_TARGET_NAMES: Set[str] = set(_SPECIES_VALUE.keys())


# ---------------------------------------------------------------------------
# Clause parsing
# ---------------------------------------------------------------------------

_CLAUSE_RE = re.compile(r"(\w+)\s+(!?)\[(.+)\]")
_COMP_RE = re.compile(r"(\w+)\s*>=\s*([\d.]+)")


def parse_clause(text: str) -> Optional[dict]:
    """Parse one SrvSurvey query clause string into a structured dict.

    Returns None for comments, star/mats clauses, and unparseable lines.
    """
    text = text.strip()
    if not text or text.startswith("#"):
        return None

    m = _CLAUSE_RE.match(text)
    if not m:
        return None

    prop = m.group(1)
    negated = m.group(2) == "!"
    content = m.group(3).strip()

    if prop in ("star", "mats"):
        return None

    if prop == "atmosComp":
        return _parse_atmos_comp(content)
    if prop == "regions":
        return _parse_regions(content, negated)
    if "~" in content:
        return _parse_range(prop, content)

    clause_type = "not" if negated else "is"
    values = [v.strip() for v in content.split(",")]
    return {"type": clause_type, "prop": prop, "values": values}


def _parse_range(prop: str, content: str) -> dict:
    parts = content.split("~", 1)
    lo = parts[0].strip()
    hi = parts[1].strip() if len(parts) > 1 else ""
    return {
        "type": "range",
        "prop": prop,
        "min": float(lo) if lo else None,
        "max": float(hi) if hi else None,
    }


def _parse_atmos_comp(content: str) -> dict:
    conditions = []
    for part in content.split("|"):
        m = _COMP_RE.search(part.strip())
        if m:
            col = GAS_COLUMN_MAP.get(m.group(1).lower())
            if col:
                conditions.append({"column": col, "min": float(m.group(2))})
    return {"type": "atmosComp", "conditions": conditions}


def _parse_regions(content: str, negated: bool) -> dict:
    ids: Set[int] = set()
    for part in content.split(","):
        part = part.strip()
        if part in REGION_GROUPS:
            ids.update(REGION_GROUPS[part])
        else:
            try:
                ids.add(int(part))
            except ValueError:
                pass
    return {"type": "regions", "ids": ids, "negated": negated}


# ---------------------------------------------------------------------------
# Tree flattening
# ---------------------------------------------------------------------------


def extract_species_branches(genus_data: dict) -> Dict[str, List[List[dict]]]:
    """Extract match branches per species from a genus criteria tree.

    Returns {\"Genus Species\": [[clause, ...], ...]} where each inner list
    is one AND-branch.  A species matches if ANY branch matches.
    """
    genus = genus_data.get("genus", "")
    genus_clauses = _parse_query(genus_data.get("query", []))
    result: Dict[str, List[List[dict]]] = {}
    for child in genus_data.get("children", []):
        _walk(child, genus_clauses[:], genus, result, species=None)
    return result


def _parse_query(query_list: list) -> List[dict]:
    out = []
    for q in query_list:
        c = parse_clause(q)
        if c is not None:
            out.append(c)
    return out


def _walk(
    node: dict,
    accumulated: List[dict],
    genus: str,
    result: Dict[str, List[List[dict]]],
    species: Optional[str],
):
    node_clauses = _parse_query(node.get("query", []))
    current = accumulated + node_clauses

    if "species" in node:
        species = node["species"]

    is_terminal = (
        node.get("useCommonChildren", False)
        or ("variant" in node and "children" not in node)
        or (species is not None and "children" not in node)
    )

    if is_terminal:
        if species:
            key = f"{genus} {species}"
            result.setdefault(key, []).append(current)
        return

    for child in node.get("children", []):
        _walk(child, current[:], genus, result, species)


# ---------------------------------------------------------------------------
# Branch evaluation
# ---------------------------------------------------------------------------


def evaluate_branch(
    body_row: dict, clauses: List[dict], region_id: Optional[int]
) -> bool:
    for clause in clauses:
        if not _eval_clause(body_row, clause, region_id):
            return False
    return True


def _eval_clause(
    body_row: dict, clause: dict, region_id: Optional[int]
) -> bool:
    t = clause["type"]
    if t == "range":
        return _eval_range(body_row, clause)
    if t == "is":
        return _eval_is(body_row, clause)
    if t == "not":
        return _eval_not(body_row, clause)
    if t == "atmosComp":
        return _eval_atmos_comp(body_row, clause)
    if t == "regions":
        return _eval_regions(region_id, clause)
    return True


def _eval_range(body_row: dict, clause: dict) -> bool:
    prop = clause["prop"]
    if prop == "dist":
        # SrvSurvey readme: dist = DistanceFromArrivalLS
        val = body_row.get("distanceToArrival")
    else:
        col = PROP_MAP.get(prop)
        val = body_row.get(col) if col else None
    if val is None:
        return False
    lo, hi = clause["min"], clause["max"]
    if lo is not None and val < lo:
        return False
    if hi is not None and val > hi:
        return False
    return True


def _eval_is(body_row: dict, clause: dict) -> bool:
    prop = clause["prop"]
    values = clause["values"]
    if prop == "body":
        return _match_body_type(body_row.get("subType"), values)
    if prop == "atmosType":
        return matches_atmosphere(body_row.get("atmosphereType"), values)
    if prop == "volcanism":
        return _match_volcanism_is(body_row.get("volcanismType"), values)
    return True


def _eval_not(body_row: dict, clause: dict) -> bool:
    prop = clause["prop"]
    values = clause["values"]
    if prop == "volcanism":
        return _match_volcanism_not(body_row.get("volcanismType"), values)
    if prop == "regions":
        # Already handled by regions type during parsing
        return True
    return True


def _eval_atmos_comp(body_row: dict, clause: dict) -> bool:
    # OR logic: any condition passing is sufficient
    for cond in clause["conditions"]:
        val = body_row.get(cond["column"])
        if val is not None and val >= cond["min"]:
            return True
    return len(clause["conditions"]) == 0


def _eval_regions(region_id: Optional[int], clause: dict) -> bool:
    if region_id is None:
        return False
    if clause["negated"]:
        return region_id not in clause["ids"]
    return region_id in clause["ids"]


def _match_body_type(sub_type: Optional[str], values: List[str]) -> bool:
    if sub_type is None:
        return False
    for v in values:
        mapped = BODY_MAP.get(v, v)
        if sub_type == mapped or sub_type.startswith(mapped):
            return True
    return False


def _match_volcanism_is(volc_type: Optional[str], values: List[str]) -> bool:
    has_none = "None" in values
    specific = [v for v in values if v != "None"]
    if not volc_type or volc_type in ("", "No volcanism", "None"):
        return has_none
    volc_lower = volc_type.lower()
    for v in specific:
        if v.lower() in volc_lower:
            return True
    return False


def _match_volcanism_not(volc_type: Optional[str], values: List[str]) -> bool:
    has_none = "None" in values
    if has_none:
        if not volc_type or volc_type in ("", "No volcanism", "None"):
            return False
    specific = [v for v in values if v != "None"]
    if specific and volc_type:
        volc_lower = volc_type.lower()
        for v in specific:
            if v.lower() in volc_lower:
                return False
    return True


# ---------------------------------------------------------------------------
# Main API
# ---------------------------------------------------------------------------

_SPECIES_BRANCHES: Dict[str, List[List[dict]]] = {}
_LOADED = False


def _load_criteria(criteria_dir: str = SRVSURV_CRITERIA_DIR):
    global _SPECIES_BRANCHES, _LOADED
    if _LOADED:
        return
    for fn in os.listdir(criteria_dir):
        if not fn.endswith(".json"):
            continue
        with open(os.path.join(criteria_dir, fn)) as f:
            data = json.load(f)
        branches = extract_species_branches(data)
        _SPECIES_BRANCHES.update(branches)
    _LOADED = True


def score_body_srvsurv(body_row: dict) -> List[Tuple[str, int]]:
    """Score a body using SrvSurvey criteria.

    Returns list of (species_name, value) for matching high-value species.
    Uses the same body_row dict format as MGST's score_body().
    """
    _load_criteria()

    sys_x = body_row.get("system_x")
    sys_y = body_row.get("system_y")
    sys_z = body_row.get("system_z")
    region_id = None
    if sys_x is not None and sys_y is not None and sys_z is not None:
        result = find_region(sys_x, sys_y, sys_z)
        region_id = result[0] if result else None

    matches: List[Tuple[str, int]] = []
    for name, branches in _SPECIES_BRANCHES.items():
        if name not in _TARGET_NAMES:
            continue
        for branch in branches:
            if evaluate_branch(body_row, branch, region_id):
                matches.append((name, _SPECIES_VALUE[name]))
                break
    return matches


def get_all_branches() -> Dict[str, List[List[dict]]]:
    """Return parsed branches for all species (not just high-value targets)."""
    _load_criteria()
    return _SPECIES_BRANCHES


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _load_criteria()

    print(f"Parsed {len(_SPECIES_BRANCHES)} species from SrvSurvey criteria\n")

    # Check coverage of our 18 targets
    found = []
    missing = []
    for name in sorted(_TARGET_NAMES):
        branches = _SPECIES_BRANCHES.get(name)
        if branches:
            found.append((name, len(branches)))
        else:
            missing.append(name)

    print(f"Target species found: {len(found)}/{len(_TARGET_NAMES)}")
    for name, n in found:
        print(f"  {name:30s}  {n} branches")

    if missing:
        print(f"\nMISSING targets:")
        for name in missing:
            print(f"  {name}")
        # Show all parsed species for debugging
        print(f"\nAll parsed species:")
        for name in sorted(_SPECIES_BRANCHES.keys()):
            print(f"  {name}")

    # Quick smoke test with a synthetic body
    print("\n--- Smoke test ---")
    test_body = {
        "subType": "Rocky body",
        "atmosphereType": "Thin Carbon dioxide",
        "volcanismType": "No volcanism",
        "gravity": 0.15,
        "surfaceTemperature": 193.0,
        "surfacePressure": 0.06,
        "star_class": "M",
        "system_x": -468.0,
        "system_y": -92.0,
        "system_z": 4474.0,
        "atm_Carbon_dioxide": 99.98,
        "atm_Sulphur_dioxide": 1.2,
    }
    results = score_body_srvsurv(test_body)
    if results:
        print(f"Matches for test body:")
        for name, val in results:
            print(f"  {name} ({val:,} cr)")
    else:
        print("No matches for test body")

    # Same body through MGST for comparison
    from mgst.bio.species_rules import score_body
    mgst_results = score_body(test_body)
    if mgst_results:
        print(f"MGST matches:")
        for name, val in mgst_results:
            print(f"  {name} ({val:,} cr)")
    else:
        print("MGST: No matches")
