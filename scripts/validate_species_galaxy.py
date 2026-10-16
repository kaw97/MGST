"""Galaxy-wide species rule validation using landmark ground truth.

Runs all species prediction rules against the full galaxy Parquet database,
comparing predictions against confirmed Spansh landmark data.

Requires: landmark columns added to bodies.parquet by merge_landmarks_to_parquet.py
  - landmark_species: JSON list of {genus, species, value}
  - landmark_genuses: JSON list of genus names
  - landmark_count: number of confirmed bio species

Outputs per species:
  - TP: predicted AND landmark confirms the species
  - FP_wrong: predicted but landmark shows DIFFERENT species in same genus
  - FP_no_landmark: predicted but body has no landmark data (unknown)
  - FP_other_genus: predicted but landmark shows only other genera
  - FN: NOT predicted but landmark confirms the species exists
  - Precision (among bodies with landmark data)
  - Recall (among bodies with landmark for this species)
"""

import argparse
import json
import multiprocessing
import sys
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pyarrow.compute as pc

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mgst.bio.species_rules import (
    BODY_COLUMNS, HIGH_VALUE_SPECIES, RELEVANT_BODY_TYPES, score_body,
)
from mgst.data.parquet_reader import ParquetSectorReader

DATABASE = Path("/data/Databases_v2/galaxy_sectors_parquet")

def scan_sector(sector_dir):
    """Process one sector: predict species, compare with landmarks."""
    reader = ParquetSectorReader(Path(sector_dir))
    if not reader.has_bodies:
        return {}

    # Star lookup
    star_lookup = {}
    try:
        star_data = reader.read_bodies(
            columns=["system_id64", "type", "spectralClass", "mainStar",
                     "solarRadius"],
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

    # Read bodies with landmark columns
    try:
        extra_cols = ["landmark_species", "landmark_genuses", "landmark_count"]
        cols = list(dict.fromkeys(
            BODY_COLUMNS + extra_cols +
            ["star_solar_radius", "earthMasses"]
        ))
        body_filter = pc.field("subType").isin(RELEVANT_BODY_TYPES)
        bodies_table = reader.read_bodies(columns=cols, filters=body_filter)
    except Exception:
        return {}

    if len(bodies_table) == 0:
        return {}

    # Per-species stats
    stats = defaultdict(lambda: {
        "tp": 0,
        "fp_wrong_species": 0,
        "fp_no_landmark": 0,
        "fp_other_genus": 0,
        "fn": 0,
        "wrong_species_detail": Counter(),
    })

    # Track all landmark species on relevant body types (for FN calculation)
    landmark_bodies_by_species = defaultdict(int)

    for row in bodies_table.to_pylist():
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

        # Parse landmarks on this body
        lm_species_json = row.get("landmark_species")
        body_species = set()
        body_genuses_from_lm = set()
        has_landmark = False

        if lm_species_json:
            try:
                landmarks = json.loads(lm_species_json)
                has_landmark = True
                for lm in landmarks:
                    body_species.add(lm["species"])
                    body_genuses_from_lm.add(lm["genus"])
            except (json.JSONDecodeError, TypeError):
                pass

        # Count landmark species (for FN denominator)
        for sp_name in body_species:
            landmark_bodies_by_species[sp_name] += 1

        # Run prediction
        predictions = score_body(row)
        predicted_species = {name for name, _ in predictions}

        # Compare predictions vs ground truth
        for pred_name, _ in predictions:
            species_def = next(
                (s for s in HIGH_VALUE_SPECIES if s["name"] == pred_name), None
            )
            if species_def is None:
                continue

            s = stats[pred_name]

            if pred_name in body_species:
                s["tp"] += 1
            elif has_landmark:
                our_genus = pred_name.split()[0]

                if our_genus in body_genuses_from_lm:
                    s["fp_wrong_species"] += 1
                    for sp in body_species:
                        if our_genus.lower() in sp.lower():
                            s["wrong_species_detail"][sp] += 1
                elif body_genuses_from_lm:
                    s["fp_other_genus"] += 1
                else:
                    s["fp_no_landmark"] += 1
            else:
                s["fp_no_landmark"] += 1

        # FN: species confirmed on body but not predicted
        for sp_name in body_species:
            matching_hvs = next(
                (s for s in HIGH_VALUE_SPECIES if s["name"] == sp_name), None
            )
            if matching_hvs and sp_name not in predicted_species:
                stats[sp_name]["fn"] += 1

    result = {}
    for sp_name, s in stats.items():
        result[sp_name] = {
            "tp": s["tp"],
            "fp_wrong_species": s["fp_wrong_species"],
            "fp_no_landmark": s["fp_no_landmark"],
            "fp_other_genus": s["fp_other_genus"],
            "fn": s["fn"],
            "wrong_species_detail": dict(s["wrong_species_detail"]),
        }

    return {
        "stats": result,
        "landmark_counts": dict(landmark_bodies_by_species),
    }


def main():
    parser = argparse.ArgumentParser(description="Galaxy-wide species validation")
    parser.add_argument("--database", default=str(DATABASE))
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--output", default="output/validation_galaxy/results.json")
    args = parser.parse_args()

    database = Path(args.database)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sectors = sorted([
        d for d in database.iterdir()
        if d.is_dir() and (d / "bodies.parquet").exists()
    ])
    print(f"Scanning {len(sectors)} sectors with {args.workers} workers...", flush=True)

    totals = defaultdict(lambda: {
        "tp": 0, "fp_wrong_species": 0, "fp_no_landmark": 0,
        "fp_other_genus": 0, "fn": 0,
        "wrong_species_detail": Counter(),
    })
    landmark_totals = Counter()

    ctx = multiprocessing.get_context("fork")
    try:
        from tqdm import tqdm
        progress = tqdm(total=len(sectors), desc="Sectors", unit="sector")
    except ImportError:
        progress = None

    with ProcessPoolExecutor(max_workers=args.workers, mp_context=ctx) as executor:
        futures = {executor.submit(scan_sector, str(s)): s.name for s in sectors}
        for future in as_completed(futures):
            try:
                result = future.result()
                for sp_name, s in result.get("stats", {}).items():
                    t = totals[sp_name]
                    for key in ["tp", "fp_wrong_species", "fp_no_landmark",
                                "fp_other_genus", "fn"]:
                        t[key] += s[key]
                    for ws, c in s.get("wrong_species_detail", {}).items():
                        t["wrong_species_detail"][ws] += c
                for sp, c in result.get("landmark_counts", {}).items():
                    landmark_totals[sp] += c
            except Exception as e:
                print(f"Error in {futures[future]}: {e}", flush=True)
            if progress:
                progress.update(1)

    if progress:
        progress.close()

    # Print results
    print(f"\n{'='*110}", flush=True)
    print("GALAXY-WIDE SPECIES VALIDATION", flush=True)
    print(f"{'='*110}", flush=True)

    header = (f"{'Species':<28} {'TP':>7} {'FP_Wrong':>8} {'FP_NoLM':>8} "
              f"{'FP_OthG':>8} {'FN':>7} {'Prec%':>7} {'Recall%':>8}")
    print(header, flush=True)
    print("-" * 110, flush=True)

    results_json = {}

    for sp_def in HIGH_VALUE_SPECIES:
        sp_name = sp_def["name"]
        t = totals[sp_name]
        tp = t["tp"]
        fp_ws = t["fp_wrong_species"]
        fp_nl = t["fp_no_landmark"]
        fp_og = t["fp_other_genus"]
        fn = t["fn"]

        # Precision: among predictions where we have landmark data
        total_with_data = tp + fp_ws + fp_og
        precision = tp / total_with_data * 100 if total_with_data > 0 else 0

        # Recall: among all landmark confirmations
        total_landmarks = landmark_totals.get(sp_name, 0)
        recall = tp / total_landmarks * 100 if total_landmarks > 0 else 0

        total_pred = tp + fp_ws + fp_nl + fp_og
        print(
            f"{sp_name:<28} {tp:>7} {fp_ws:>8} {fp_nl:>8} {fp_og:>8} "
            f"{fn:>7} {precision:>6.1f}% {recall:>7.1f}%",
            flush=True,
        )

        if t["wrong_species_detail"]:
            top_ws = t["wrong_species_detail"].most_common(3)
            ws_str = ", ".join(f"{s}:{c}" for s, c in top_ws)
            print(f"  -> Wrong species: {ws_str}", flush=True)

        results_json[sp_name] = {
            "tp": tp,
            "fp_wrong_species": fp_ws,
            "fp_no_landmark": fp_nl,
            "fp_other_genus": fp_og,
            "fn": fn,
            "total_predictions": total_pred,
            "total_landmarks": total_landmarks,
            "precision_with_data": round(precision, 2),
            "recall": round(recall, 2),
            "wrong_species": dict(t["wrong_species_detail"]),
            "value": sp_def["min_value"],
        }

    # Save JSON results
    with open(output_path, "w") as f:
        json.dump(results_json, f, indent=2)
    print(f"\nResults saved to {output_path}", flush=True)

    # Also write TSV summary
    tsv_path = output_path.with_suffix(".tsv")
    with open(tsv_path, "w") as f:
        f.write("species\tvalue\ttp\tfp_wrong_species\tfp_no_landmark\t"
                "fp_other_genus\tfn\tprecision_pct\trecall_pct\n")
        for sp_name, r in results_json.items():
            f.write(f"{sp_name}\t{r['value']}\t{r['tp']}\t"
                    f"{r['fp_wrong_species']}\t{r['fp_no_landmark']}\t"
                    f"{r['fp_other_genus']}\t{r['fn']}\t"
                    f"{r['precision_with_data']}\t{r['recall']}\n")
    print(f"TSV summary saved to {tsv_path}", flush=True)


if __name__ == "__main__":
    main()
