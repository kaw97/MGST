"""Full port of SrvSurvey's bio predictor, evaluated against Parquet/Spansh body data.

Unlike srvsurv_parser.py (species-level, high-value species only, star/mats/
nebulae/guardian clauses ignored), this module mirrors SrvSurvey's
BioPredictor.predict() tree walk and predicts every genus, species and colour
variant in the criteria files.

Faithful to SrvSurvey (BioPredictor.cs, BioCriteria.cs, SystemData.cs):
- `star` is the flattened type of the single brightest parent star, ranked by
  SystemBody.getRelativeBrightness() (R^2 T^4 / sum of squared semi-major axes
  up both orbit chains to the common parent). Bodies with no parent star get
  no predictions at all.
- `primaryStar` is the flattened type of the system's main star.
- `mats` IS clauses need the material above 0.25%; ALL/NOT only test presence.
- Clauses on properties the body lacks behave as in C#: a missing atmosphere
  composition or material list passes, a missing numeric value fails.
- atmosComp keys are matched case-sensitively, as the C# dictionary does.

Deliberate deviation: Herbig stars are reported as `AeBe` by the journal but
the criteria test `star [Ae]`, so SrvSurvey never matches them. Star types are
passed through STAR_CLAUSE_ALIASES so `AeBe` matches `Ae`.

Differences from SrvSurvey, on by default (see PREDICTION_ALGORITHM.md):
- criteria corrections: TUSSOCK_MISSING_REGIONS, TUSSOCK_EXCLUDED_REGIONS and
  SPECIES_VETOES and TEMP_GRAVITY_LIMITS; `corrections=False` turns them off
- the star comes from the orbit-averaged model in star_brightness.py;
  `star_model="srvsurvey"` gives SrvSurvey's brightest star

Spansh/Parquet values are converted to what the in-game journal reports, since
SrvSurvey's criteria were fitted on journal data:
- gravity (g) -> m/s^2 / 10, pressure (atm) -> Pa / 100,000
- subType -> journal PlanetClass / StarType codes
- atmosphereType -> journal AtmosphereType (no Thin/Thick/Hot prefix)
- volcanismType -> journal Volcanism ("major rocky magma volcanism", "" for none)
"""

import copy
import json
import math
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import numpy as np
from scipy.spatial import cKDTree

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from mgst.bio.region import find_region
from mgst.bio.star_model import ORBIT_SAMPLES, STAR_CLAUSE_ALIASES, STAR_TYPE_MAP, OrbitModel, SystemStars, flatten_star_type, journal_star_type

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SRVSURV_ROOT = "/mnt/z/SrvSurvey"
SRVSURV_CRITERIA_DIR = os.path.join(SRVSURV_ROOT, "SrvSurvey", "bio-criteria")
SRVSURV_NEBULAE = os.path.join(SRVSURV_ROOT, "docs", "nebulae.json")

MATS_MINIMAL_THRESHOLD = 0.25  # BioPredictor.matsMinimalThreshold

# BioCriteria.Map.values: clause shorthand -> journal string
VALUE_MAP: Dict[str, str] = {
    "Icy": "Icy body",
    "Rocky": "Rocky body",
    "RockyIce": "Rocky ice ",
    "HMC": "High metal content ",
    "MRB": "Metal rich body",
}

# GalacticRegions.mapArmRegions (bio-criteria/readme.md)
REGION_GROUPS: Dict[str, Set[int]] = {
    "Orion-CygnusArm": {7, 8, 16, 17, 18, 35},
    "OuterArm": {5, 6, 13, 14, 27, 29, 31, 41, 37},
    "Scutum-CentaurusArm": {9, 10, 11, 12, 24, 25, 26, 42, 28},
    "PerseusArm": {15, 30, 32, 33, 34, 36, 38, 39},
    "Sagittarius-CarinaArm": {9, 18, 19, 20, 21, 22, 23, 40},
    "CentreLeft": {1, 4},
    "CentreTop": {1, 3, 7},
    "CentreRight": {1, 2},
    "AmphoraBatch": {10, 19, 20, 21, 22},
    "AnemoneBatch": {7, 8, 9, 13, 14, 15, 16, 17, 18, 27, 31},
    "BarkMoundBatch": {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 25, 32, 33, 34},
    "BrainTreeBatch": {2, 9, 10, 17, 18, 35},
    "TubersBatch": {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 18, 19},
    "ShardBatch": {14, 21, 22, 23, 24, 25, 26, 27, 28, 29, 31, 34, 36, 37, 38, 39, 40, 41, 42},
}

# CodexRef.isWithinGuardianBubble: (centre, radius ly)
GUARDIAN_BUBBLES: List[Tuple[Tuple[float, float, float], float]] = [
    ((1099.21875, -146.6875, -133.59375), 750),  # Gamma Velorum
    ((-840.65625, -561.15625, 13361.8125), 750),  # HEN 2-333
    ((-9298.6875, -419.40625, 7911.15625), 100),  # Prai Hypoo OK-I b0
    ((-5479.28125, -574.84375, 10468.96875), 100),  # Prua Phoe US-B d58
    ((1228.1875, -694.5625, 12341.65625), 100),  # Blaa Hypai EK-C c14-1
    ((4961.1875, 158.09375, 20642.65625), 100),  # Eorl Auwsy SY-Z d13-3643
    ((14602.75, -237.90625, 3561.875), 100),  # NGC 3199 Sector JH-V c2-0
    ((8649.125, -154.71875, 2686.03125), 100),  # Eta Carina Sector EL-Y d1
]

# signals_genuses codex keys -> criteria genus names
CODEX_GENUS: Dict[str, str] = {
    "$Codex_Ent_Aleoids_Genus_Name;": "Aleoida",
    "$Codex_Ent_Bacterial_Genus_Name;": "Bacterium",
    "$Codex_Ent_Cactoid_Genus_Name;": "Cactoida",
    "$Codex_Ent_Clypeus_Genus_Name;": "Clypeus",
    "$Codex_Ent_Conchas_Genus_Name;": "Concha",
    "$Codex_Ent_Electricae_Genus_Name;": "Electricae",
    "$Codex_Ent_Fonticulus_Genus_Name;": "Fonticulua",
    "$Codex_Ent_Fumerolas_Genus_Name;": "Fumerola",
    "$Codex_Ent_Fungoids_Genus_Name;": "Fungoida",
    "$Codex_Ent_Osseus_Genus_Name;": "Osseus",
    "$Codex_Ent_Recepta_Genus_Name;": "Recepta",
    "$Codex_Ent_Shrubs_Genus_Name;": "Frutexa",
    "$Codex_Ent_Stratum_Genus_Name;": "Stratum",
    "$Codex_Ent_Tubus_Genus_Name;": "Tubus",
    "$Codex_Ent_Tussocks_Genus_Name;": "Tussock",
    "$Codex_Ent_Brancae_Name;": "Brain Trees",
    "$Codex_Ent_Cone_Name;": "Bark Mounds",
    "$Codex_Ent_Ground_Struct_Ice_Name;": "Crystalline Shards",
    "$Codex_Ent_Sphere_Name;": "Luteolum Anemone",
    "$Codex_Ent_Tube_Name;": "Roseum Sinuous Tubers",
    "$Codex_Ent_Vents_Name;": "Amphora Plant",
}

# Spansh planet subType -> journal PlanetClass, where they differ
PLANET_CLASS_MAP: Dict[str, str] = {
    "High metal content world": "High metal content body",
    "Rocky Ice world": "Rocky ice body",
    "Metal-rich body": "Metal rich body",
}

G_TO_MS2 = 9.80665
LS_PER_AU = 499.005
ATM_TO_PA = 101_325.0

# Corrections measured against Spansh landmarks (output/rule_corrections_score_final_20261031).
# These Tussock species are found across the Orion-Cygnus arm, but the criteria only allow regions 7 and 18 of it,
# and never in region 9, which the criteria allow through the Sagittarius-Carina arm group
TUSSOCK_MISSING_REGIONS: Dict[str, Set[int]] = {
    s: {8, 16, 17, 35} for s in ("Albata", "Caputus", "Ignis", "Pennata", "Serrati", "Triticum", "Ventusa")
}
TUSSOCK_EXCLUDED_REGIONS: Set[int] = {9}


def _over_g(limit_g: float):
    """Test for surface gravity above a limit given in Spansh g (props hold the journal value)."""
    return lambda p: p["SurfaceGravity"] is not None and p["SurfaceGravity"] > limit_g * G_TO_MS2 / 10.0


# (genus, species) -> test on body props: drop the species when true
SPECIES_VETOES: Dict[Tuple[str, str], Any] = {
    ("Clypeus", "Speculumi"): lambda p: p["ChainDistanceLS"] is not None and p["ChainDistanceLS"] < 2500,
    ("Electricae", "Radialem"): lambda p: p["AtmosphereType"] == "Methane",
    ("Fonticulua", "Lapida"): _over_g(0.2753),
    ("Fonticulua", "Campestris"): lambda p: (p["PlanetClass"] == "Rocky ice body" and p["AtmosphereType"] == "Argon"
                                             and p["StarAge"] is not None and p["StarAge"] > 10_000),
    ("Osseus", "Discus"): lambda p: (p["PlanetClass"] == "High metal content body" and p["AtmosphereType"] == "Water"
                                     and p["StarAge"] is not None and p["StarAge"] < 8_500),
    ("Frutexa", "Flabellum"): lambda p: p["AtmosphereType"] != "Ammonia",
    ("Frutexa", "Metallicum"): _over_g(0.2753),
    ("Fumerola", "Nitris"): _over_g(0.2743),
    ("Stratum", "Araneamus"): lambda p: p["StarTemperature"] is not None and p["StarTemperature"] <= 7500,
    ("Stratum", "Laminamus"): lambda p: p["SurfaceTemperature"] is not None and p["SurfaceTemperature"] <= 165,
    ("Tubus", "Cavas"): _over_g(0.1515),
    ("Tubus", "Compagibus"): _over_g(0.1515),
    ("Tubus", "Rosarium"): _over_g(0.1515),
    ("Tubus", "Sororibus"): _over_g(0.1515),
}

# Sharp temperature and gravity edges, one set per species (output/temp_gravity_rules_v3_20261031):
# (genus, species, body subType or None, atmosphere or None, min K, max K, min g, max g), with g in Spansh units
TEMP_GRAVITY_LIMITS: List[Tuple] = [
    ("Aleoida", "Spica", 'High metal content world', None, None, 175.81, 0.05175, 0.27439),
    ("Bacterium", "Tela", None, 'CarbonDioxide', 274.64, None, 0.4658, 0.60975),
    ("Bacterium", "Tela", 'High metal content world', 'CarbonDioxide', 300.06, None, 0.50091, 0.60975),
    ("Cactoida", "Lapis", None, None, None, 176.64, None, 0.27372),
    ("Cactoida", "Peperatis", None, None, None, None, None, 0.27394),
    ("Cactoida", "Vermis", 'Rocky body', None, None, 450.03, None, 0.27359),
    ("Concha", "Labiata", None, None, None, 191.52, None, 0.20919),
    ("Concha", "Labiata", 'Rocky body', None, None, 191.51, None, 0.20673),
    ("Concha", "Renibus", None, None, 79.64, None, None, 0.25364),
    ("Concha", "Renibus", None, 'Methane', 79.64, 103.05, None, 0.11161),
    ("Fonticulua", "Campestris", None, None, None, None, None, 0.27424),
    ("Frutexa", "Collum", None, None, None, 167.03, None, 0.2747),
    ("Frutexa", "Flabellum", None, None, None, None, None, 0.27451),
    ("Frutexa", "Flammasis", None, None, None, None, None, 0.27438),
    ("Frutexa", "Metallicum", None, None, None, 399.97, None, 0.27488),
    ("Fumerola", "Extremus", None, None, None, None, None, 0.27453),
    ("Fungoida", "Bullarum", None, None, None, 132.91, None, 0.27462),
    ("Fungoida", "Setisis", None, None, None, 174.76, None, 0.27357),
    ("Fungoida", "Setisis", 'High metal content world', None, None, 174.67, 0.14743, 0.27431),
    ("Osseus", "Pumice", None, None, None, 133.35, None, 0.27459),
    ("Osseus", "Spiralis", None, None, None, 176.63, None, 0.27408),
    ("Recepta", "Conditivus", None, None, 132.12, None, 0.05098, 0.2746),
    ("Recepta", "Deltahedronix", None, None, None, None, 0.04488, 0.27483),
    ("Recepta", "Umbrux", None, 'SulphurDioxide', None, 272.84, 0.04071, 0.27464),
    ("Tubus", "Cavas", None, None, None, None, None, 0.14719),
    ("Tubus", "Compagibus", None, None, None, None, None, 0.14704),
    ("Tubus", "Conifer", None, None, None, 195.3, 0.04103, 0.14707),
    ("Tubus", "Rosarium", None, None, None, None, None, 0.14996),
    ("Tubus", "Sororibus", None, None, None, None, None, 0.14971),
    ("Tubus", "Sororibus", None, 'CarbonDioxide', None, None, None, 0.14811),
    ("Tussock", "Capillum", None, None, None, None, None, 0.27474),
    ("Tussock", "Catena", None, None, None, None, None, 0.27438),
    ("Tussock", "Cultro", None, None, None, None, None, 0.27428),
    ("Tussock", "Divisa", None, None, None, None, None, 0.27443),
    ("Tussock", "Stigmasis", None, None, None, None, None, 0.27481),
    ("Tussock", "Virgam", None, None, None, 449.99, None, None),
]
_LIMITS_BY_SPECIES: Dict[Tuple[str, str], List[Tuple]] = {}
for _limit in TEMP_GRAVITY_LIMITS:
    _LIMITS_BY_SPECIES.setdefault(_limit[:2], []).append(_limit[2:])


def outside_limits(genus: str, species: str, props: Dict[str, Any]) -> bool:
    """True when a body falls outside a species' temperature or gravity limits."""
    T, gravity = props.get("SurfaceTemperature"), props.get("SurfaceGravity")
    g = None if gravity is None else gravity * 10.0 / G_TO_MS2
    for body, atmosphere, t_min, t_max, g_min, g_max in _LIMITS_BY_SPECIES.get((genus, species), ()):
        if body is not None and props["PlanetClass"] != PLANET_CLASS_MAP.get(body, body):
            continue
        if atmosphere is not None and props["AtmosphereType"] != atmosphere:
            continue
        if T is not None and ((t_min is not None and T < t_min) or (t_max is not None and T > t_max)):
            return True
        if g is not None and ((g_min is not None and g < g_min) or (g_max is not None and g > g_max)):
            return True
    return False


# ---------------------------------------------------------------------------
# Value conversion (Spansh/Parquet -> journal)
# ---------------------------------------------------------------------------



def journal_atmosphere_type(atm: Optional[str]) -> str:
    """Journal AtmosphereType for a Spansh atmosphereType, e.g. "Thin Carbon dioxide-rich" -> "CarbonDioxideRich"."""
    if not atm or atm == "No atmosphere":
        return "None"
    if atm == "Suitable for water-based life":
        return "EarthLike"
    words = re.sub(r"^(hot |thin |thick )+", "", atm, flags=re.IGNORECASE)
    words = words.replace("-", " ").replace(" atmosphere", "").split()
    return "".join(w[0].upper() + w[1:] for w in words)


def journal_volcanism(volc: Optional[str]) -> str:
    """Journal Volcanism string for a Spansh volcanismType ("" for none)."""
    if not volc or volc == "No volcanism":
        return ""
    return f"{volc.lower()} volcanism"


def _camel(col_suffix: str) -> str:
    # atm_Carbon_dioxide -> CarbonDioxide (Util.compositionToCamel)
    return "".join(p[0].upper() + p[1:] for p in col_suffix.split("_") if p)


# ---------------------------------------------------------------------------
# Clause parsing (BioCriteria.Clause.parse)
# ---------------------------------------------------------------------------

PROPERTY_MAP: Dict[str, str] = {
    "body": "PlanetClass",
    "gravity": "SurfaceGravity",
    "temp": "SurfaceTemperature",
    "pressure": "SurfacePressure",
    "atmosphere": "Atmosphere",
    "atmosType": "AtmosphereType",
    "atmosComp": "AtmosphereComposition",
    "matsComp": "Materials",
    "dist": "DistanceFromArrivalLS",
    "volcanism": "Volcanism",
    "mats": "Materials",
    "regions": "Region",
    "star": "Star",
    "parentStar": "ParentStar",
    "primaryStar": "PrimaryStar",
    "nebulae": "Nebulae",
    "guardian": "Guardian",
}

_CLAUSE_RE = re.compile(r"\s*(\w+)\s*[&!]?\[(.+)\]")
_COMP_RE = re.compile(r"([\w\s]+)(>=)\s*([\.\d]+)")


def parse_clause(text: str) -> Optional[dict]:
    """Parse one query string into {prop, op, values|min/max|compositions}; None for comments."""
    text = text.strip()
    if not text or text.startswith("#"):
        return None
    m = _CLAUSE_RE.match(text)
    if not m:
        raise ValueError(f"Bad criteria: {text}")
    prop, val_txt = m.group(1), m.group(2)
    clause: Dict[str, Any] = {"prop": prop, "key": PROPERTY_MAP.get(prop, prop), "raw": text}

    if "~" in val_txt:
        lo, hi = (p.strip() for p in val_txt.split("~", 1))
        clause.update(op="range", min=float(lo) if lo else None, max=float(hi) if hi else None)
    elif ">=" in val_txt:
        comps = {}
        for part in val_txt.split("|"):
            cm = _COMP_RE.search(part.strip())
            comps[cm.group(1).strip()] = float(cm.group(3))
        clause.update(op="comp", compositions=comps)
    else:
        op = "not" if "![" in text else "all" if "&[" in text else "is"
        values = [v.strip() for v in val_txt.split(",") if v.strip()]
        values = [VALUE_MAP.get(v, v) for v in values]
        clause.update(op=op, values=values)
        if prop == "regions":
            ids: Set[int] = set()
            for v in values:
                ids |= REGION_GROUPS[v] if v in REGION_GROUPS else {int(v)}
            clause["region_ids"] = ids
    return clause


def _parse_node(node: dict) -> dict:
    out = {
        "genus": node.get("genus"),
        "species": node.get("species"),
        "variant": node.get("variant"),
        "useCommonChildren": node.get("useCommonChildren", False),
        "query": [c for c in (parse_clause(q) for q in node.get("query") or []) if c],
    }
    out["children"] = [_parse_node(c) for c in node.get("children") or []]
    common = node.get("commonChildren")
    out["commonChildren"] = [_parse_node(c) for c in common] if common is not None else None
    return out


_CRITERIA: List[dict] = []


def load_criteria(criteria_dir: str = SRVSURV_CRITERIA_DIR) -> List[dict]:
    """Parse every genus file once (BioCriteria.readCriteria)."""
    if not _CRITERIA:
        for fn in sorted(os.listdir(criteria_dir)):
            if fn.endswith(".json"):
                with open(os.path.join(criteria_dir, fn), encoding="utf-8-sig") as f:
                    _CRITERIA.append(_parse_node(json.load(f)))
    return _CRITERIA


_CORRECTED: List[dict] = []


def corrected_criteria() -> List[dict]:
    """Criteria with the Tussock region corrections applied to the species' region clauses."""
    if not _CORRECTED:
        _CORRECTED.extend(copy.deepcopy(load_criteria()))
        for root in _CORRECTED:
            if root["genus"] != "Tussock":
                continue
            for node in root["children"]:
                for clause in node["query"]:
                    if clause["prop"] == "regions" and node["species"] in TUSSOCK_MISSING_REGIONS:
                        clause["region_ids"] |= TUSSOCK_MISSING_REGIONS[node["species"]]
                        clause["region_ids"] -= TUSSOCK_EXCLUDED_REGIONS
    return _CORRECTED


# ---------------------------------------------------------------------------
# Clause evaluation (BioPredictor.test*Query)
# ---------------------------------------------------------------------------


def _test_is(clause: dict, value: Any) -> bool:
    values = clause["values"]
    prop = clause["prop"]
    if prop == "mats" and isinstance(value, dict):
        return any(
            k.lower() == v.lower() and pct > MATS_MINIMAL_THRESHOLD
            for v in values for k, pct in value.items()
        )
    if prop == "regions" and isinstance(value, str):
        return not value.isdigit() or int(value) in clause["region_ids"]
    if isinstance(value, dict):
        value = list(value.keys())
    if isinstance(value, list):
        return any(v.lower() == bv.lower() for v in values for bv in value)
    if isinstance(value, str):
        low = value.lower()
        if prop == "body":
            return any(low.startswith(v.lower()) for v in values)
        if prop == "volcanism":
            if values[0] == "Any":
                return value != "None"
            return any(v.lower() in low for v in values)
        return any(low == v.lower() for v in values)
    return True  # C# logs an unexpected type and records no failure


def _as_list(value: Any) -> Optional[List[str]]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return list(value.keys())
    return value if isinstance(value, list) else None


def _test_all(clause: dict, value: Any) -> bool:
    body_values = _as_list(value)
    if body_values is None:
        return True
    return all(any(v.lower() == bv.lower() for bv in body_values) for v in clause["values"])


def _test_not(clause: dict, value: Any) -> bool:
    if clause["prop"] == "regions" and isinstance(value, str):
        return not value.isdigit() or int(value) not in clause["region_ids"]
    body_values = _as_list(value)
    if body_values is None:
        return True
    return not any(v.lower() == bv.lower() for v in clause["values"] for bv in body_values)


def _test_range(clause: dict, value: Any) -> bool:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return False  # C# throws on a non-numeric value; treat as no match
    if clause["min"] is not None and value < clause["min"]:
        return False
    if clause["max"] is not None and value > clause["max"]:
        return False
    return True


def _test_comp(clause: dict, value: Any) -> bool:
    if not isinstance(value, dict):
        return True  # no composition: C# records no failure
    return any(k in value and value[k] >= pct for k, pct in clause["compositions"].items())


_TESTS = {"is": _test_is, "all": _test_all, "not": _test_not, "range": _test_range, "comp": _test_comp}


def test_query(query: List[dict], props: Dict[str, Any]) -> bool:
    for clause in query:
        if clause["key"] not in props:
            raise KeyError(f"Unexpected property: {clause['key']} ({clause['raw']})")
        if not _TESTS[clause["op"]](clause, props[clause["key"]]):
            return False
    return True


# ---------------------------------------------------------------------------
# Tree walk (BioPredictor.predict)
# ---------------------------------------------------------------------------


def _predict(
    node: dict,
    props: Dict[str, Any],
    known_genera: Optional[Set[str]],
    out: Set[Tuple[str, str, str]],
    genus: Optional[str] = None,
    species: Optional[str] = None,
    variant: Optional[str] = None,
    common: Optional[List[dict]] = None,
) -> None:
    common = node["commonChildren"] if node["commonChildren"] is not None else common
    genus = node["genus"] or genus
    species = node["species"] or species
    variant = node["variant"] if node["variant"] is not None else variant  # "" is a real value: the colour is in the species name (Roseum Brain Tree)

    if genus and known_genera and genus not in known_genera:
        return
    if not test_query(node["query"], props):
        return
    if genus is not None and species is not None and variant is not None:
        out.add((genus, species, variant))

    for child in (common if node["useCommonChildren"] else node["children"]) or []:
        _predict(child, props, known_genera, out, genus, species, variant, common)


def predict_props(
    props: Dict[str, Any],
    known_genera: Optional[Set[str]] = None,
    corrections: bool = True,
) -> Set[Tuple[str, str, str]]:
    """(genus, species, variant) predictions for prepared body properties.

    known_genera mirrors SrvSurvey's allGenusKnown filter: once every genus on
    the body is known, only those genera are predicted. A caller that swaps
    props["Star"] for another star must set props["StarTemperature"] to match.
    """
    out: Set[Tuple[str, str, str]] = set()
    for root in corrected_criteria() if corrections else load_criteria():
        _predict(root, props, known_genera, out)
    if corrections:
        out = {p for p in out if not ((p[0], p[1]) in SPECIES_VETOES and SPECIES_VETOES[(p[0], p[1])](props))
               and not outside_limits(p[0], p[1], props)}
    return out


# ---------------------------------------------------------------------------
# System context: parent stars and brightness (SystemData.cs)
# ---------------------------------------------------------------------------


class SystemContext(SystemStars):
    """SystemStars with SrvSurvey's brightness helpers."""

    def __init__(self, bodies: Iterable[dict]):
        super().__init__(bodies)
        self.orbit_model = None  # OrbitModel, built on first use

    def parent_bodies(self, body: dict) -> List[dict]:
        return [self.bodies[pid] for _, pid in self._chain[int(body["bodyId"])] if pid in self.bodies]

    def _euclidian(self, body: dict, target: Optional[dict]) -> float:
        if target is body:
            return 0.0
        dist = _num(body.get("semiMajorAxis")) ** 2
        for parent in self.parent_bodies(body):
            if target is not None and parent["bodyId"] == target["bodyId"]:
                return dist
            dist += _num(parent.get("semiMajorAxis")) ** 2
        return dist

    def relative_brightness(self, body: dict, star: dict) -> float:
        """SystemBody.getRelativeBrightness: R^2 T^4 / (d1 + d2)."""
        if not star.get("subType"):
            return 0.0
        star_parents = self.parent_bodies(star)
        common = next(
            (p for p in self.parent_bodies(body) if p is star or any(p is sp for sp in star_parents)),
            None,
        )
        d2 = self._euclidian(body, common) + self._euclidian(star, common)
        if d2 == 0:
            return math.inf
        return (_num(star.get("solarRadius")) * _num(star.get("surfaceTemperature")) ** 2) ** 2 / d2

    def brightest_star(self, body: dict) -> Optional[dict]:
        ranked = [(self.relative_brightness(body, s), s) for s in self.parent_stars(body)]
        ranked = [r for r in ranked if r[0] > 0]
        if not ranked:
            return None
        return max(ranked, key=lambda r: r[0])[1]

    def main_star(self) -> Optional[dict]:
        return next((s for s in self.stars if s.get("mainStar")), None)


def _num(v: Any) -> float:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return 0.0
    return float(v)


# ---------------------------------------------------------------------------
# Location context
# ---------------------------------------------------------------------------

_NEBULA_TREE: Optional[cKDTree] = None


def nebula_distance(x: float, y: float, z: float) -> float:
    """CodexRef.getDistToClosestNebula over SrvSurvey's nebulae.json."""
    global _NEBULA_TREE
    if _NEBULA_TREE is None:
        with open(SRVSURV_NEBULAE, encoding="utf-8-sig") as f:
            _NEBULA_TREE = cKDTree(np.array(json.load(f), dtype=float))
    return float(_NEBULA_TREE.query([x, y, z])[0])


def within_guardian_bubble(x: float, y: float, z: float) -> bool:
    return any(math.dist((x, y, z), c) < r for c, r in GUARDIAN_BUBBLES)


# ---------------------------------------------------------------------------
# Body properties and system prediction
# ---------------------------------------------------------------------------


def chain_distance(system: SystemContext, body: dict) -> Tuple[float, bool]:
    """(light-seconds, complete): orbit sizes added up to the first star or star-bearing barycentre above the body."""
    sma = body.get("semiMajorAxis")
    total, complete = _num(sma), bool(sma)
    for ptype, pid in system._chain[int(body["bodyId"])]:
        if ptype == "Star" or (ptype == "Null" and any(s.get("subType") and system._has_parent(s, pid) for s in system.stars)):
            return total * LS_PER_AU, complete
        parent = system.bodies.get(pid)
        sma = parent.get("semiMajorAxis") if parent else None
        total += _num(sma)
        complete = complete and bool(sma)
    return total * LS_PER_AU, False


def orbit_star(body: dict, system: SystemContext, fallback: dict) -> dict:
    """The star in the body's chain, or a barycentre on it, giving the most orbit-averaged light; fallback if there is no choice to make."""
    stars = [s for s in system.parent_stars(body) if s.get("subType")]
    if len(stars) < 2:
        return stars[0] if stars else fallback
    if system.orbit_model is None:
        system.orbit_model = OrbitModel(system, ORBIT_SAMPLES)
    flux = [system.orbit_model.flux(body, s) for s in stars]
    return stars[int(np.argmax(flux))] if sum(flux) > 0 else fallback


def body_props(
    body: dict,
    system: SystemContext,
    coords: Tuple[float, float, float],
    star_model: str = "orbit",
) -> Optional[Dict[str, Any]]:
    """BioPredictor bodyProps for a Parquet body row; None when SrvSurvey would skip it."""
    brightest = system.brightest_star(body)
    if brightest is None:
        return None  # "Why no parent stars?" -> no predictions
    if star_model == "orbit":
        brightest = orbit_star(body, system, brightest)
    elif star_model != "srvsurvey":
        raise ValueError(f"Unknown star model: {star_model}")
    main = system.main_star()

    comp = {
        _camel(k[4:]): float(v) for k, v in body.items()
        if k.startswith("atm_") and v is not None and not (isinstance(v, float) and math.isnan(v))
    } or None
    if comp and len(comp) == 1:
        comp = {k: 100.0 for k in comp}
    mats = {
        k[4:].lower(): float(v) for k, v in body.items()
        if k.startswith("mat_") and v is not None and not (isinstance(v, float) and math.isnan(v))
    } or None

    gravity = body.get("gravity")
    pressure = body.get("surfacePressure")
    atm = body.get("atmosphereType")
    region = find_region(*coords)
    volcanism = journal_volcanism(body.get("volcanismType"))
    chain_ls, chain_complete = chain_distance(system, body)

    return {
        "PlanetClass": PLANET_CLASS_MAP.get(body.get("subType"), body.get("subType") or ""),
        "SurfaceGravity": None if gravity is None else gravity * G_TO_MS2 / 10.0,
        "SurfaceTemperature": body.get("surfaceTemperature"),
        "SurfacePressure": None if pressure is None else pressure * ATM_TO_PA / 100_000.0,
        "Atmosphere": (atm or "").lower().replace(" atmosphere", "") if atm != "No atmosphere" else "",
        "AtmosphereType": journal_atmosphere_type(atm),
        "AtmosphereComposition": comp,
        "DistanceFromArrivalLS": body.get("distanceToArrival"),
        "ChainDistanceLS": chain_ls if chain_complete else body.get("distanceToArrival"),
        "Volcanism": volcanism or "None",
        "Materials": mats,
        "Region": str(region[0]) if region else "",
        "Star": [flatten_star_type(journal_star_type(brightest.get("subType")))],
        "StarTemperature": brightest.get("surfaceTemperature"),
        "StarAge": main.get("age") if main else None,
        "PrimaryStar": flatten_star_type(journal_star_type(main.get("subType"))) if main else "",
        "Nebulae": nebula_distance(*coords),
        "Guardian": str(within_guardian_bubble(*coords)),
        "_star_body": brightest.get("name"),
    }


def known_genera_from_codex(signals_genuses: Optional[str]) -> Optional[Set[str]]:
    """Criteria genus names for a Parquet signals_genuses value; unknown keys are kept as-is."""
    if not signals_genuses:
        return None
    keys = json.loads(signals_genuses)
    return {CODEX_GENUS.get(k, k) for k in keys} or None


def predict_system(
    bodies: List[dict],
    coords: Tuple[float, float, float],
    use_known_genera: bool = True,
    star_model: str = "orbit",
    corrections: bool = True,
) -> Dict[int, Dict[str, Any]]:
    """Predict every landable planet in one system.

    Returns {body id64: {"predictions": {(genus, species, variant)}, "star": brightest
    star class, "star_body": its name, "primary_star": main star class}}.
    """
    system = SystemContext(bodies)
    out: Dict[int, Dict[str, Any]] = {}
    for body in bodies:
        if body.get("type") != "Planet" or not body.get("isLandable"):
            continue
        props = body_props(body, system, coords, star_model)
        if props is None:
            continue
        known = known_genera_from_codex(body.get("signals_genuses")) if use_known_genera else None
        out[int(body["id64"])] = {
            "predictions": predict_props(props, known, corrections),
            "star": props["Star"][0],
            "star_body": props["_star_body"],
            "primary_star": props["PrimaryStar"],
        }
    return out


if __name__ == "__main__":
    crit = load_criteria()
    names: Set[Tuple[str, str]] = set()
    variants = 0

    def _count(node, genus=None, species=None):
        global variants
        genus = node["genus"] or genus
        species = node["species"] or species
        if species:
            names.add((genus, species))
        if node["variant"]:
            variants += 1
        for c in node["children"] + (node["commonChildren"] or []):
            _count(c, genus, species)

    for root in crit:
        _count(root)
    print(f"{len(crit)} genus files, {len(names)} species, {variants} variant nodes")
