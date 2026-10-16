#!/usr/bin/env python3
"""Systematic empirical parameter sweep for all high-value bio species.

Uses galaxy-wide Spansh landmark data for species-level ground truth.
Scans all ~12K sectors in parallel, then sweeps every numeric parameter
for every species+ruleset to find precision-improving tightenings.

Usage:
    cd /mnt/z/MGST
    python -I experiments/bio_rule_optimization/sweep_all_species.py [--workers 12]
"""
import argparse
import json
import multiprocessing
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import numpy as np
import pyarrow.compute as pc

from mgst.bio.species_rules import (
    BODY_COLUMNS,
    RELEVANT_BODY_TYPES,
    ATMOSPHERE_MAP,
    BODY_TYPE_MAP,
    HIGH_VALUE_SPECIES,
    matches_atmosphere,
    matches_body_type,
    matches_volcanism,
    matches_atmosphere_component,
    _check_range,
)
from mgst.bio.region import find_region, BIOSCAN_REGION_GROUPS
from mgst.data.parquet_reader import ParquetSectorReader

RESULTS_DIR = Path(__file__).resolve().parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

BASELINE_PATH = Path(__file__).resolve().parent / "baseline_rules.json"

DB = Path("/data/Databases_v2/galaxy_sectors_parquet")

EXTRA_COLS = [
    "earthMasses", "distanceToArrival",
    "type", "spectralClass", "mainStar", "solarRadius",
    "surfacePressure",
    "landmark_species", "landmark_count",
]
ALL_COLS = list(dict.fromkeys(BODY_COLUMNS + EXTRA_COLS))

# Sweepable numeric parameters and the body-row column they correspond to
PARAM_TO_COLUMN = {
    "min_gravity": "gravity",
    "max_gravity": "gravity",
    "min_temperature": "surfaceTemperature",
    "max_temperature": "surfaceTemperature",
    "min_pressure": "surfacePressure",
    "max_pressure": "surfacePressure",
}

TIGHTEN_PERCENTILES = [5, 10, 15, 20, 25]

STAR_CLASSES = ["O", "B", "A", "F", "G", "K", "M", "L", "T", "Y"]

# Species name -> genus name (first word)
_SPECIES_GENUS = {sp["name"]: sp["name"].split()[0] for sp in HIGH_VALUE_SPECIES}
# All target species names for FN counting
_TARGET_SPECIES_NAMES = {sp["name"] for sp in HIGH_VALUE_SPECIES}


def check_ruleset(row, ruleset):
    """Check if a body row matches a single ruleset (ignoring star class)."""
    if not matches_body_type(row.get("subType"), ruleset.get("body_type", [])):
        if "body_type" in ruleset:
            return False
    if not matches_atmosphere(row.get("atmosphereType"), ruleset["atmosphere"]):
        return False
    if not matches_volcanism(row.get("volcanismType"), ruleset.get("volcanism")):
        return False
    if not _check_range(
        row.get("gravity"),
        ruleset.get("min_gravity"),
        ruleset.get("max_gravity"),
    ):
        return False
    if not _check_range(
        row.get("surfaceTemperature"),
        ruleset.get("min_temperature"),
        ruleset.get("max_temperature"),
    ):
        return False
    if "min_pressure" in ruleset or "max_pressure" in ruleset:
        if not _check_range(
            row.get("surfacePressure"),
            ruleset.get("min_pressure"),
            ruleset.get("max_pressure"),
        ):
            return False
    atm_comp = ruleset.get("atmosphere_component")
    if atm_comp is not None:
        if not matches_atmosphere_component(row, atm_comp):
            return False
    constraints = ruleset.get("constraints")
    if constraints:
        for c in constraints:
            val = row.get(c["field"])
            if val is None:
                continue
            if "min" in c and val < c["min"]:
                return False
            if "max" in c and val > c["max"]:
                return False
    return True


def save_baseline_snapshot():
    """Save current HIGH_VALUE_SPECIES as a frozen baseline for future reference."""
    with open(BASELINE_PATH, "w") as f:
        json.dump(HIGH_VALUE_SPECIES, f, indent=2)
    print(f"  Updated baseline snapshot: {BASELINE_PATH}", flush=True)


# =========================================================================
# Phase 1: Parallel Galaxy-Wide Data Collection Using Landmarks
# =========================================================================

def _parse_landmark_species(lm_json):
    """Parse landmark_species JSON -> set of confirmed species names."""
    if not lm_json:
        return set(), set()
    try:
        landmarks = json.loads(lm_json)
        species = {lm["species"] for lm in landmarks}
        genuses = {lm["genus"] for lm in landmarks}
        return species, genuses
    except (json.JSONDecodeError, TypeError, KeyError):
        return set(), set()


def scan_sector_for_sweep(sector_dir):
    """Process one sector: for bodies with landmarks, classify TP/FP per species+ruleset.

    Returns a dict:
        {species_name: {ruleset_idx: {"tp_bodies": [...], "fp_bodies": [...],
                                       "tp_stars": [...], "fp_stars": [...]}},
         "fn_counts": {species_name: count},
         "landmark_counts": {species_name: count}}
    """
    reader = ParquetSectorReader(Path(sector_dir))
    if not reader.has_bodies:
        return {}

    # Star lookup
    star_lookup = {}
    try:
        star_data = reader.read_bodies(
            columns=["system_id64", "type", "spectralClass", "mainStar", "solarRadius"],
        )
        for srow in star_data.filter(
            pc.equal(star_data.column("type"), "Star")
        ).to_pylist():
            sid = srow.get("system_id64")
            if sid is None:
                continue
            sc = srow.get("spectralClass", "")
            is_main = srow.get("mainStar", False)
            if is_main and sc:
                star_lookup[sid] = srow
            elif sid not in star_lookup and sc:
                star_lookup[sid] = srow
        del star_data
    except Exception:
        pass

    # System coords
    sys_coords = {}
    try:
        sys_table = reader.read_systems(columns=["id64", "x", "y", "z"])
        for srow in sys_table.to_pylist():
            sys_coords[srow["id64"]] = (
                srow.get("x", 0.0) or 0.0,
                srow.get("y", 0.0) or 0.0,
                srow.get("z", 0.0) or 0.0,
            )
        del sys_table
    except Exception:
        pass

    # Read bodies
    try:
        body_filter = pc.field("subType").isin(RELEVANT_BODY_TYPES)
        bodies_table = reader.read_bodies(columns=ALL_COLS, filters=body_filter)
    except Exception:
        return {}

    if len(bodies_table) == 0:
        return {}

    # Initialize per-species, per-ruleset data
    sp_data = {}
    for sp in HIGH_VALUE_SPECIES:
        sp_data[sp["name"]] = {
            i: {"tp_bodies": [], "fp_bodies": [], "tp_stars": [], "fp_stars": []}
            for i in range(len(sp["rulesets"]))
        }

    fn_counts = Counter()
    landmark_counts = Counter()

    for row in bodies_table.to_pylist():
        # Parse landmarks
        lm_json = row.get("landmark_species")
        confirmed_species, confirmed_genuses = _parse_landmark_species(lm_json)
        has_landmark = bool(confirmed_species)

        # Skip bodies with no landmark data — we can't classify TP/FP
        if not has_landmark:
            continue

        # Count landmark species for FN denominator
        for sp_name in confirmed_species:
            if sp_name in _TARGET_SPECIES_NAMES:
                landmark_counts[sp_name] += 1

        # Enrich body row
        sid = row.get("system_id64")
        star_row = star_lookup.get(sid)
        if star_row:
            sc = star_row.get("spectralClass", "")
            row["star_class"] = sc[0] if sc else None
            row["star_solar_radius"] = star_row.get("solarRadius")
        else:
            row["star_class"] = None

        coords = sys_coords.get(sid)
        if coords:
            row["system_x"], row["system_y"], row["system_z"] = coords

        star_class = row.get("star_class", "?") or "?"

        # For each species, check if our rules predict it, and which ruleset matches
        predicted_species = set()

        for sp in HIGH_VALUE_SPECIES:
            sp_name = sp["name"]
            our_genus = _SPECIES_GENUS[sp_name]

            # Check species-level exclusions (star class, region, distance)
            excluded = sp.get("exclude_star_classes")
            if excluded and row.get("star_class") and row["star_class"] in excluded:
                continue

            region_group = sp.get("region")
            if region_group:
                if not coords:
                    continue
                allowed = set(BIOSCAN_REGION_GROUPS.get(region_group, []))
                if not allowed:
                    continue
                result = find_region(coords[0], coords[1], coords[2])
                rid = result[0] if result else None
                if rid is None or rid not in allowed:
                    continue

            min_dist = sp.get("min_distance_from_sol")
            if min_dist is not None:
                if not coords:
                    continue
                dist_sq = coords[0]**2 + coords[1]**2 + coords[2]**2
                if dist_sq < min_dist * min_dist:
                    continue

            # Try each ruleset
            for i, rs in enumerate(sp["rulesets"]):
                rs_excluded = rs.get("exclude_star_classes")
                if rs_excluded and row.get("star_class") and row["star_class"] in rs_excluded:
                    continue

                if check_ruleset(row, rs):
                    predicted_species.add(sp_name)

                    # Classify: TP or FP?
                    if sp_name in confirmed_species:
                        sp_data[sp_name][i]["tp_bodies"].append(row)
                        sp_data[sp_name][i]["tp_stars"].append(star_class)
                    else:
                        sp_data[sp_name][i]["fp_bodies"].append(row)
                        sp_data[sp_name][i]["fp_stars"].append(star_class)
                    break  # first matching ruleset wins

        # FN: landmark confirms species but we didn't predict it
        for sp_name in confirmed_species:
            if sp_name in _TARGET_SPECIES_NAMES and sp_name not in predicted_species:
                fn_counts[sp_name] += 1

    # Trim bodies to only the columns we need for sweeping (save memory)
    sweep_cols = {"gravity", "surfaceTemperature", "surfacePressure", "subType",
                  "atmosphereType", "volcanismType", "star_class", "atm_Sulphur_dioxide",
                  "distanceToArrival", "earthMasses", "star_solar_radius"}
    for sp_name, rs_dict in sp_data.items():
        for i, rd in rs_dict.items():
            rd["tp_bodies"] = [{k: b.get(k) for k in sweep_cols} for b in rd["tp_bodies"]]
            rd["fp_bodies"] = [{k: b.get(k) for k in sweep_cols} for b in rd["fp_bodies"]]

    return {
        "species_data": {
            sp_name: {
                str(i): rd for i, rd in rs_dict.items()
                if rd["tp_bodies"] or rd["fp_bodies"]
            }
            for sp_name, rs_dict in sp_data.items()
        },
        "fn_counts": dict(fn_counts),
        "landmark_counts": dict(landmark_counts),
    }


def collect_body_data_parallel(species_list, workers=12):
    """Scan all sectors in parallel, collecting TP/FP bodies per species+ruleset."""

    sectors = sorted([
        d for d in DB.iterdir()
        if d.is_dir() and (d / "bodies.parquet").exists()
    ])
    print(f"  Found {len(sectors)} sectors to scan with {workers} workers", flush=True)

    # Initialize merged structure
    data = {}
    for sp in species_list:
        name = sp["name"]
        data[name] = {
            "rulesets": sp["rulesets"],
            "exclude_star_classes": sp.get("exclude_star_classes", []),
            "ruleset_data": [],
        }
        for i, rs in enumerate(sp["rulesets"]):
            atm_label = "/".join(rs.get("atmosphere", []))
            data[name]["ruleset_data"].append({
                "label": atm_label,
                "tp_bodies": [],
                "fp_bodies": [],
                "tp_star_classes": [],
                "fp_star_classes": [],
            })

    fn_totals = Counter()
    landmark_totals = Counter()

    t0 = time.time()
    ctx = multiprocessing.get_context("fork")

    try:
        from tqdm import tqdm
        progress = tqdm(total=len(sectors), desc="Sectors", unit="sector")
    except ImportError:
        progress = None

    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as executor:
        futures = {executor.submit(scan_sector_for_sweep, str(s)): s.name for s in sectors}
        for future in as_completed(futures):
            try:
                result = future.result()
                if not result:
                    if progress:
                        progress.update(1)
                    continue

                # Merge species data
                for sp_name, rs_dict in result.get("species_data", {}).items():
                    if sp_name not in data:
                        continue
                    for i_str, rd in rs_dict.items():
                        i = int(i_str)
                        if i < len(data[sp_name]["ruleset_data"]):
                            target = data[sp_name]["ruleset_data"][i]
                            target["tp_bodies"].extend(rd["tp_bodies"])
                            target["fp_bodies"].extend(rd["fp_bodies"])
                            target["tp_star_classes"].extend(rd["tp_stars"])
                            target["fp_star_classes"].extend(rd["fp_stars"])

                for sp, c in result.get("fn_counts", {}).items():
                    fn_totals[sp] += c
                for sp, c in result.get("landmark_counts", {}).items():
                    landmark_totals[sp] += c

            except Exception as e:
                sector_name = futures[future]
                print(f"  Error in {sector_name}: {e}", flush=True)

            if progress:
                progress.update(1)

    if progress:
        progress.close()

    elapsed = time.time() - t0
    print(f"  Data collection complete in {elapsed:.1f}s", flush=True)

    return data, dict(fn_totals), dict(landmark_totals)


# =========================================================================
# Phase 2: Parameter Sweeps
# =========================================================================

def filter_by_star_exclusions(bodies, star_classes_list, exclude_classes):
    """Filter parallel body and star class lists by existing star exclusions."""
    if not exclude_classes:
        return bodies, star_classes_list
    excl_set = set(exclude_classes)
    filtered_bodies = []
    filtered_stars = []
    for b, sc in zip(bodies, star_classes_list):
        if sc not in excl_set:
            filtered_bodies.append(b)
            filtered_stars.append(sc)
    return filtered_bodies, filtered_stars


def extract_values(bodies, column):
    """Extract non-None values of a column from a list of body dicts."""
    vals = [b.get(column) for b in bodies]
    return np.array([v for v in vals if v is not None], dtype=float)


def count_in_range(values, lo, hi):
    """Count values in [lo, hi]."""
    mask = np.ones(len(values), dtype=bool)
    if lo is not None:
        mask &= values >= lo
    if hi is not None:
        mask &= values <= hi
    return int(np.sum(mask))


def sweep_single_param(tp_vals, fp_vals, param_name, current_value, direction):
    """Sweep a single min or max parameter and return candidate results."""
    if len(tp_vals) < 5:
        return []

    results = []
    for pct in TIGHTEN_PERCENTILES:
        if direction == "min":
            candidate = float(np.percentile(tp_vals, pct))
            if current_value is not None and candidate <= current_value:
                continue
            tp_remaining = int(np.sum(tp_vals >= candidate))
            fp_remaining = int(np.sum(fp_vals >= candidate))
        else:
            candidate = float(np.percentile(tp_vals, 100 - pct))
            if current_value is not None and candidate >= current_value:
                continue
            tp_remaining = int(np.sum(tp_vals <= candidate))
            fp_remaining = int(np.sum(fp_vals <= candidate))

        tp_lost = len(tp_vals) - tp_remaining
        fp_removed = len(fp_vals) - fp_remaining

        results.append({
            "param": param_name,
            "percentile": f"p{pct}" if direction == "min" else f"p{100-pct}",
            "current": current_value,
            "candidate": round(candidate, 6),
            "tp_remaining": tp_remaining,
            "fp_remaining": fp_remaining,
            "tp_lost": tp_lost,
            "fp_removed": fp_removed,
            "tp_lost_pct": round(tp_lost / len(tp_vals) * 100, 1) if len(tp_vals) > 0 else 0,
        })

    return results


def sweep_add_constraint(tp_vals, fp_vals, param_name):
    """Try adding a new min or max constraint where none currently exists."""
    if len(tp_vals) < 10:
        return []

    results = []
    direction = "min" if param_name.startswith("min_") else "max"

    for pct in [5, 10, 25]:
        if direction == "min":
            candidate = float(np.percentile(tp_vals, pct))
            tp_remaining = int(np.sum(tp_vals >= candidate))
            fp_remaining = int(np.sum(fp_vals >= candidate))
        else:
            candidate = float(np.percentile(tp_vals, 100 - pct))
            tp_remaining = int(np.sum(tp_vals <= candidate))
            fp_remaining = int(np.sum(fp_vals <= candidate))

        tp_lost = len(tp_vals) - tp_remaining
        fp_removed = len(fp_vals) - fp_remaining

        results.append({
            "param": param_name,
            "percentile": f"add_p{pct}" if direction == "min" else f"add_p{100-pct}",
            "current": None,
            "candidate": round(candidate, 6),
            "tp_remaining": tp_remaining,
            "fp_remaining": fp_remaining,
            "tp_lost": tp_lost,
            "fp_removed": fp_removed,
            "tp_lost_pct": round(tp_lost / len(tp_vals) * 100, 1) if len(tp_vals) > 0 else 0,
        })

    return results


def sweep_star_classes(tp_stars, fp_stars):
    """Evaluate excluding each star class individually."""
    results = []
    tp_counter = Counter(tp_stars)
    fp_counter = Counter(fp_stars)
    total_tp = len(tp_stars)
    total_fp = len(fp_stars)

    for sc in STAR_CLASSES:
        tp_sc = tp_counter.get(sc, 0)
        fp_sc = fp_counter.get(sc, 0)
        total_sc = tp_sc + fp_sc
        if total_sc < 10:
            continue
        prec_sc = tp_sc / total_sc * 100 if total_sc > 0 else 0
        tp_pct = tp_sc / total_tp * 100 if total_tp > 0 else 0

        results.append({
            "star_class": sc,
            "tp_in_class": tp_sc,
            "fp_in_class": fp_sc,
            "precision_in_class": round(prec_sc, 1),
            "tp_pct_of_total": round(tp_pct, 1),
            "tp_remaining": total_tp - tp_sc,
            "fp_remaining": total_fp - fp_sc,
            "tp_lost": tp_sc,
            "fp_removed": fp_sc,
        })

    return results


def compute_combined_variants(ruleset_data, ruleset_def, single_results):
    """Stack the top single-param improvements greedily."""
    tp_bodies = ruleset_data["tp_bodies"]
    fp_bodies = ruleset_data["fp_bodies"]

    if not tp_bodies or not fp_bodies:
        return []

    ranked = []
    for r in single_results:
        if r.get("tp_lost", 0) == 0 and r.get("fp_removed", 0) == 0:
            continue
        if r.get("tp_lost_pct", 0) > 25:
            continue
        efficiency = r["fp_removed"] / max(r["tp_lost"], 0.5)
        ranked.append((efficiency, r))

    ranked.sort(key=lambda x: -x[0])

    if len(ranked) < 2:
        return []

    combined_results = []
    for n_stack in [2, 3]:
        top_n = ranked[:n_stack]
        changes = {}
        for _, r in top_n:
            param = r["param"]
            if param in changes:
                continue
            changes[param] = r["candidate"]

        if len(changes) < 2:
            continue

        tp_remaining = 0
        fp_remaining = 0

        for bodies, counter_name in [(tp_bodies, "tp"), (fp_bodies, "fp")]:
            for body in bodies:
                passes = True
                for param, threshold in changes.items():
                    col = PARAM_TO_COLUMN.get(param)
                    if col is None:
                        continue
                    val = body.get(col)
                    if val is None:
                        passes = False
                        break
                    if param.startswith("min_") and val < threshold:
                        passes = False
                        break
                    if param.startswith("max_") and val > threshold:
                        passes = False
                        break
                if passes:
                    if counter_name == "tp":
                        tp_remaining += 1
                    else:
                        fp_remaining += 1

        tp_lost = len(tp_bodies) - tp_remaining
        fp_removed = len(fp_bodies) - fp_remaining

        combined_results.append({
            "type": f"combined_top{n_stack}",
            "changes": changes,
            "tp_remaining": tp_remaining,
            "fp_remaining": fp_remaining,
            "tp_lost": tp_lost,
            "fp_removed": fp_removed,
            "tp_lost_pct": round(tp_lost / len(tp_bodies) * 100, 1) if tp_bodies else 0,
        })

    return combined_results


def sweep_species(species_def, species_data, fn_totals, landmark_totals):
    """Run full parameter sweep for one species. Returns list of variant results."""
    sp_name = species_def["name"]
    exclude_stars = set(species_def.get("exclude_star_classes", []))

    # Total confirmed landmarks for this species (for recall)
    total_landmarks = landmark_totals.get(sp_name, 0)

    all_variants = []

    # Apply existing star exclusions to get true baseline counts per ruleset
    baseline_rs = []
    for rd in species_data["ruleset_data"]:
        tp_filt, tp_stars_filt = filter_by_star_exclusions(
            rd["tp_bodies"], rd["tp_star_classes"], exclude_stars)
        fp_filt, fp_stars_filt = filter_by_star_exclusions(
            rd["fp_bodies"], rd["fp_star_classes"], exclude_stars)
        baseline_rs.append({
            "tp_bodies": tp_filt,
            "fp_bodies": fp_filt,
            "tp_stars": tp_stars_filt,
            "fp_stars": fp_stars_filt,
            "tp": len(tp_filt),
            "fp": len(fp_filt),
        })

    baseline_tp = sum(r["tp"] for r in baseline_rs)
    baseline_fp = sum(r["fp"] for r in baseline_rs)
    baseline_fn = fn_totals.get(sp_name, 0)
    baseline_prec = baseline_tp / (baseline_tp + baseline_fp) * 100 if (baseline_tp + baseline_fp) > 0 else 0
    baseline_recall = baseline_tp / total_landmarks * 100 if total_landmarks > 0 else 0

    all_variants.append({
        "species": sp_name,
        "variant": "baseline",
        "ruleset": "all",
        "changes": {},
        "tp": baseline_tp,
        "fp": baseline_fp,
        "fn": baseline_fn,
        "precision": round(baseline_prec, 2),
        "recall": round(baseline_recall, 2),
        "tp_lost": 0,
        "fp_removed": 0,
        "tp_lost_pct": 0.0,
    })

    for i, rs in enumerate(species_def["rulesets"]):
        brs = baseline_rs[i]
        tp_bodies = brs["tp_bodies"]
        fp_bodies = brs["fp_bodies"]
        rs_label = species_data["ruleset_data"][i]["label"]

        if len(tp_bodies) + len(fp_bodies) < 10:
            continue

        rs_tp = brs["tp"]
        rs_fp = brs["fp"]

        single_results = []

        # --- Sweep existing numeric params ---
        for param, col in PARAM_TO_COLUMN.items():
            current = rs.get(param)

            tp_vals = extract_values(tp_bodies, col)
            fp_vals = extract_values(fp_bodies, col)

            if len(tp_vals) < 5 or len(fp_vals) < 5:
                continue

            if current is not None:
                direction = "min" if param.startswith("min_") else "max"
                results = sweep_single_param(tp_vals, fp_vals, param, current, direction)
                single_results.extend(results)
            else:
                results = sweep_add_constraint(tp_vals, fp_vals, param)
                single_results.extend(results)

        # Convert single-param results to variant format
        for r in single_results:
            other_tp = baseline_tp - rs_tp
            other_fp = baseline_fp - rs_fp
            new_tp = other_tp + r["tp_remaining"]
            new_fp = other_fp + r["fp_remaining"]
            new_prec = new_tp / (new_tp + new_fp) * 100 if (new_tp + new_fp) > 0 else 0
            new_recall = new_tp / total_landmarks * 100 if total_landmarks > 0 else 0

            all_variants.append({
                "species": sp_name,
                "variant": f"rs{i}_{r['param']}_{r['percentile']}",
                "ruleset": f"{i}:{rs_label}",
                "changes": {r["param"]: r["candidate"]},
                "current": {r["param"]: r["current"]},
                "tp": new_tp,
                "fp": new_fp,
                "precision": round(new_prec, 2),
                "recall": round(new_recall, 2),
                "tp_lost": baseline_tp - new_tp,
                "fp_removed": baseline_fp - new_fp,
                "tp_lost_pct": round((baseline_tp - new_tp) / baseline_tp * 100, 1) if baseline_tp > 0 else 0,
                "rs_tp": r["tp_remaining"],
                "rs_fp": r["fp_remaining"],
                "rs_tp_orig": rs_tp,
                "rs_fp_orig": rs_fp,
            })

        # --- Star class sweeps ---
        star_results = sweep_star_classes(brs["tp_stars"], brs["fp_stars"])
        for r in star_results:
            if r["star_class"] in exclude_stars:
                continue
            other_tp = baseline_tp - rs_tp
            other_fp = baseline_fp - rs_fp
            new_tp = other_tp + r["tp_remaining"]
            new_fp = other_fp + r["fp_remaining"]
            new_prec = new_tp / (new_tp + new_fp) * 100 if (new_tp + new_fp) > 0 else 0
            new_recall = new_tp / total_landmarks * 100 if total_landmarks > 0 else 0

            all_variants.append({
                "species": sp_name,
                "variant": f"rs{i}_excl_star_{r['star_class']}",
                "ruleset": f"{i}:{rs_label}",
                "changes": {"exclude_star": r["star_class"]},
                "star_precision": r["precision_in_class"],
                "star_tp_pct": r["tp_pct_of_total"],
                "tp": new_tp,
                "fp": new_fp,
                "precision": round(new_prec, 2),
                "recall": round(new_recall, 2),
                "tp_lost": baseline_tp - new_tp,
                "fp_removed": baseline_fp - new_fp,
                "tp_lost_pct": round((baseline_tp - new_tp) / baseline_tp * 100, 1) if baseline_tp > 0 else 0,
            })

        # --- Combined sweeps ---
        combined = compute_combined_variants(
            {"tp_bodies": tp_bodies, "fp_bodies": fp_bodies}, rs, single_results)
        for r in combined:
            other_tp = baseline_tp - rs_tp
            other_fp = baseline_fp - rs_fp
            new_tp = other_tp + r["tp_remaining"]
            new_fp = other_fp + r["fp_remaining"]
            new_prec = new_tp / (new_tp + new_fp) * 100 if (new_tp + new_fp) > 0 else 0
            new_recall = new_tp / total_landmarks * 100 if total_landmarks > 0 else 0

            all_variants.append({
                "species": sp_name,
                "variant": f"rs{i}_{r['type']}",
                "ruleset": f"{i}:{rs_label}",
                "changes": r["changes"],
                "tp": new_tp,
                "fp": new_fp,
                "precision": round(new_prec, 2),
                "recall": round(new_recall, 2),
                "tp_lost": baseline_tp - new_tp,
                "fp_removed": baseline_fp - new_fp,
                "tp_lost_pct": round((baseline_tp - new_tp) / baseline_tp * 100, 1) if baseline_tp > 0 else 0,
            })

    return all_variants


# =========================================================================
# Phase 3 & 4: Results Output and Pareto Analysis
# =========================================================================

def find_pareto_optimal(variants):
    """Find Pareto-optimal variants (no other has both higher precision AND recall)."""
    pareto = []
    for v in variants:
        dominated = False
        for other in variants:
            if other is v:
                continue
            if other["precision"] >= v["precision"] and other["recall"] >= v["recall"]:
                if other["precision"] > v["precision"] or other["recall"] > v["recall"]:
                    dominated = True
                    break
        if not dominated:
            pareto.append(v)
    return pareto


def write_results(all_variants):
    """Write sweep results to JSON and TSV."""
    with open(RESULTS_DIR / "sweep_results.json", "w") as f:
        json.dump(all_variants, f, indent=2, default=str)

    with open(RESULTS_DIR / "summary.tsv", "w") as f:
        headers = [
            "species", "variant", "ruleset", "changes",
            "tp", "fp", "precision", "recall",
            "tp_lost", "fp_removed", "tp_lost_pct", "pareto",
        ]
        f.write("\t".join(headers) + "\n")

        by_species = defaultdict(list)
        for v in all_variants:
            by_species[v["species"]].append(v)

        for sp_name, variants in by_species.items():
            pareto_set = find_pareto_optimal(variants)
            pareto_ids = {id(v) for v in pareto_set}

            variants.sort(key=lambda v: (v["variant"] != "baseline", -v["precision"]))

            for v in variants:
                is_pareto = "Y" if id(v) in pareto_ids else ""
                changes_str = json.dumps(v.get("changes", {}))
                row = [
                    v["species"],
                    v["variant"],
                    v.get("ruleset", ""),
                    changes_str,
                    str(v["tp"]),
                    str(v["fp"]),
                    f"{v['precision']:.2f}",
                    f"{v['recall']:.2f}",
                    str(v.get("tp_lost", 0)),
                    str(v.get("fp_removed", 0)),
                    f"{v.get('tp_lost_pct', 0):.1f}",
                    is_pareto,
                ]
                f.write("\t".join(row) + "\n")

    with open(RESULTS_DIR / "pareto_summary.txt", "w") as f:
        by_species = defaultdict(list)
        for v in all_variants:
            by_species[v["species"]].append(v)

        for sp_name, variants in by_species.items():
            baseline = next((v for v in variants if v["variant"] == "baseline"), None)
            if baseline is None:
                continue

            pareto = find_pareto_optimal(variants)
            best_balanced = None
            for v in sorted(pareto, key=lambda x: -x["precision"]):
                if v.get("tp_lost_pct", 0) <= 10:
                    best_balanced = v
                    break

            f.write(f"\n{'='*90}\n")
            f.write(f"  {sp_name}\n")
            f.write(f"  Baseline: {baseline['tp']} TP, {baseline['fp']} FP, "
                    f"{baseline['precision']:.1f}% precision, {baseline['recall']:.1f}% recall\n")
            f.write(f"{'='*90}\n\n")

            candidates = [v for v in variants
                          if v["variant"] != "baseline" and v.get("tp_lost_pct", 0) <= 15]
            candidates.sort(key=lambda v: -v["precision"])

            f.write(f"  {'Variant':<45s} {'Prec':>6s} {'Recall':>7s} {'TP Lost':>8s} {'FP Rem':>8s} {'Pareto':>6s}\n")
            f.write(f"  {'-'*45} {'-'*6} {'-'*7} {'-'*8} {'-'*8} {'-'*6}\n")

            pareto_ids = {id(v) for v in pareto}
            shown = set()
            for v in candidates[:15]:
                vid = v["variant"]
                if vid in shown:
                    continue
                shown.add(vid)
                is_pareto = "*" if id(v) in pareto_ids else ""
                f.write(f"  {vid:<45s} {v['precision']:>5.1f}% {v['recall']:>6.1f}% "
                        f"{v.get('tp_lost', 0):>8d} {v.get('fp_removed', 0):>8d} {is_pareto:>6s}\n")

            if best_balanced and best_balanced["variant"] != "baseline":
                f.write(f"\n  >> RECOMMENDED: {best_balanced['variant']}\n")
                f.write(f"     Precision: {baseline['precision']:.1f}% -> {best_balanced['precision']:.1f}% "
                        f"(+{best_balanced['precision'] - baseline['precision']:.1f}pp)\n")
                f.write(f"     TP Lost: {best_balanced.get('tp_lost', 0)} "
                        f"({best_balanced.get('tp_lost_pct', 0):.1f}%)\n")
                f.write(f"     FP Removed: {best_balanced.get('fp_removed', 0)}\n")
                f.write(f"     Changes: {json.dumps(best_balanced.get('changes', {}))}\n")
            f.write("\n")


def print_summary(all_variants):
    """Print a concise summary to stdout."""
    by_species = defaultdict(list)
    for v in all_variants:
        by_species[v["species"]].append(v)

    print(f"\n{'='*110}")
    print(f"  SWEEP SUMMARY — Best variants per species (<=10% TP loss)")
    print(f"  Ground truth: galaxy-wide Spansh landmark data (species-level)")
    print(f"{'='*110}\n")

    print(f"  {'Species':<30s} {'Baseline':>10s} {'Best':>10s} {'Delta':>8s} "
          f"{'TP Lost':>8s} {'FP Rem':>8s} {'Recall':>8s} {'Best Variant':<40s}")
    print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*40}")

    for sp_name, variants in by_species.items():
        baseline = next((v for v in variants if v["variant"] == "baseline"), None)
        if baseline is None:
            continue

        candidates = [v for v in variants
                      if v["variant"] != "baseline" and v.get("tp_lost_pct", 0) <= 10]
        candidates.sort(key=lambda v: -v["precision"])

        if candidates:
            best = candidates[0]
            delta = best["precision"] - baseline["precision"]
            print(f"  {sp_name:<30s} {baseline['precision']:>9.1f}% {best['precision']:>9.1f}% "
                  f"{delta:>+7.1f}% {best.get('tp_lost', 0):>8d} {best.get('fp_removed', 0):>8d} "
                  f"{best['recall']:>7.1f}% {best['variant']:<40s}")
        else:
            print(f"  {sp_name:<30s} {baseline['precision']:>9.1f}%  {'N/A':>9s}"
                  f"{'':>8s} {'':>8s} {'':>8s} {baseline['recall']:>7.1f}%")


# =========================================================================
# Main
# =========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Systematic species rule parameter sweep using landmark ground truth")
    parser.add_argument("--workers", type=int, default=12,
                        help="Parallel workers for sector scanning (default: 12)")
    args = parser.parse_args()

    print("=" * 80)
    print("  Bio Species Rule Optimization Sweep (Galaxy-Wide Landmark Data)")
    print("=" * 80)

    print("\nPhase 1: Using live rules from species_rules.py...")
    species_list = HIGH_VALUE_SPECIES
    print(f"  {len(species_list)} species, {sum(len(s['rulesets']) for s in species_list)} rulesets")
    save_baseline_snapshot()

    print(f"\nPhase 1: Collecting body data from all sectors (landmark ground truth)...")
    data, fn_totals, landmark_totals = collect_body_data_parallel(species_list, args.workers)

    # Print data collection summary
    print(f"\n  Per-species body counts (confirmed via landmarks):")
    print(f"  {'Species':<30s} {'TP':>7s} {'FP':>7s} {'FN':>7s} {'Landmarks':>10s} {'Prec%':>7s} {'Recall%':>8s}")
    print(f"  {'-'*30} {'-'*7} {'-'*7} {'-'*7} {'-'*10} {'-'*7} {'-'*8}")
    for sp in species_list:
        sd = data[sp["name"]]
        excl = set(sp.get("exclude_star_classes", []))
        total_tp = 0
        total_fp = 0
        for rd in sd["ruleset_data"]:
            for b, sc in zip(rd["tp_bodies"], rd["tp_star_classes"]):
                if sc not in excl:
                    total_tp += 1
            for b, sc in zip(rd["fp_bodies"], rd["fp_star_classes"]):
                if sc not in excl:
                    total_fp += 1
        fn = fn_totals.get(sp["name"], 0)
        lm = landmark_totals.get(sp["name"], 0)
        prec = total_tp / (total_tp + total_fp) * 100 if (total_tp + total_fp) > 0 else 0
        recall = total_tp / lm * 100 if lm > 0 else 0
        print(f"  {sp['name']:<30s} {total_tp:>7d} {total_fp:>7d} {fn:>7d} {lm:>10d} {prec:>6.1f}% {recall:>7.1f}%")

    print(f"\nPhase 2: Running parameter sweeps...")
    all_variants = []
    for sp in species_list:
        sp_name = sp["name"]
        sd = data[sp_name]
        print(f"  Sweeping {sp_name}...")
        variants = sweep_species(sp, sd, fn_totals, landmark_totals)
        all_variants.extend(variants)
        n_non_baseline = len([v for v in variants if v["variant"] != "baseline"])
        print(f"    -> {n_non_baseline} variants generated")

    print(f"\n  Total variants: {len(all_variants)}")

    print("\nPhase 3: Writing results...")
    write_results(all_variants)
    print(f"  -> {RESULTS_DIR / 'sweep_results.json'}")
    print(f"  -> {RESULTS_DIR / 'summary.tsv'}")
    print(f"  -> {RESULTS_DIR / 'pareto_summary.txt'}")

    print_summary(all_variants)
    print("\nDone.")


if __name__ == "__main__":
    main()
