"""Galaxy-wide side-by-side comparison of MGST vs SrvSurvey species predictions.

Runs both prediction engines on every landmark-bearing body across all Parquet
sectors. Classifies each (body, species) pair into 7 categories and outputs
per-species metrics + disagreement samples for analysis.
"""

import argparse
import json
import multiprocessing
import os
import sys
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from mgst.bio.species_rules import (
    BODY_COLUMNS,
    HIGH_VALUE_SPECIES,
    RELEVANT_BODY_TYPES,
    parent_star_class,
    score_body,
)
from mgst.data.parquet_reader import ParquetSectorReader

# Import SrvSurvey parser (from same directory)
sys.path.insert(0, str(Path(__file__).resolve().parent))
from srvsurv_parser import score_body_srvsurv, _load_criteria

DATABASE = Path("/data/Databases_v2/galaxy_sectors_parquet")
BATCH_ROWS = 50_000

# Additional atmosphere composition columns SrvSurvey needs
ATM_COLUMNS = [
    "atm_Carbon_dioxide",
    "atm_Sulphur_dioxide",
    "atm_Water",
    "atm_Ammonia",
    "atm_Methane",
    "atm_Argon",
    "atm_Oxygen",
    "atm_Nitrogen",
    "atm_Neon",
]



def scan_sector(sector_dir):
    """Process one sector: run both engines, classify outcomes.

    Returns only counts (no samples) to minimize memory.
    """
    import gc

    reader = ParquetSectorReader(Path(sector_dir))
    if not reader.has_bodies:
        return {}

    # Both engines reject non-thin atmospheres, so only thin-atmosphere bodies
    # can be predicted; landmark bodies are kept regardless so FN stays exact.
    try:
        cols = list(dict.fromkeys(BODY_COLUMNS + ["landmark_species"] + ATM_COLUMNS))
        body_filter = pc.field("subType").isin(RELEVANT_BODY_TYPES) & (
            pc.starts_with(pc.field("atmosphereType"), "Thin")
            | pc.field("landmark_species").is_valid()
        )
        bodies_table = reader.read_bodies(columns=cols, filters=body_filter)
    except Exception:
        return {}

    if len(bodies_table) == 0:
        return {}

    candidate_sids = pc.unique(bodies_table.column("system_id64"))

    # Star class letter per candidate system (main star wins)
    star_classes = {}
    star_by_body = {}
    try:
        star_data = reader.read_bodies(
            columns=["system_id64", "bodyId", "spectralClass", "mainStar"],
            filters=(pc.field("type") == "Star")
            & pc.field("system_id64").isin(candidate_sids),
        )
        for srow in star_data.to_pylist():
            sid = srow.get("system_id64")
            sc = srow.get("spectralClass")
            if sid is None or not sc:
                continue
            star_by_body[(sid, srow.get("bodyId"))] = sc[0]
            if srow.get("mainStar") or sid not in star_classes:
                star_classes[sid] = sc[0]
        del star_data
    except Exception:
        pass

    sys_coords = {}
    try:
        sys_table = reader.read_systems(
            columns=["id64", "x", "y", "z"],
            filters=pc.field("id64").isin(candidate_sids),
        )
        for srow in sys_table.to_pylist():
            sys_coords[srow["id64"]] = (
                srow.get("x", 0.0) or 0.0,
                srow.get("y", 0.0) or 0.0,
                srow.get("z", 0.0) or 0.0,
            )
        del sys_table
    except Exception:
        pass

    counts = defaultdict(lambda: Counter())
    landmark_counts = Counter()

    for row in (
        r
        for batch in bodies_table.to_batches(max_chunksize=BATCH_ROWS)
        for r in batch.to_pylist()
    ):
        sid = row.get("system_id64")
        row["star_class"] = star_classes.get(sid)
        row["parent_star_class"] = parent_star_class(row, star_by_body)

        coords = sys_coords.get(sid)
        if coords:
            row["system_x"], row["system_y"], row["system_z"] = coords

        # Parse landmarks
        lm_json = row.get("landmark_species")
        body_species = set()
        has_landmark = False
        if lm_json:
            try:
                for lm in json.loads(lm_json):
                    body_species.add(lm["species"])
                has_landmark = True
            except (json.JSONDecodeError, TypeError):
                pass

        for sp_name in body_species:
            landmark_counts[sp_name] += 1

        # Run both engines
        mgst_preds = {name for name, _ in score_body(row)}
        srv_preds = {name for name, _ in score_body_srvsurv(row)}

        for sp in HIGH_VALUE_SPECIES:
            name = sp["name"]
            mgst_yes = name in mgst_preds
            srv_yes = name in srv_preds
            lm_yes = name in body_species

            if not mgst_yes and not srv_yes and not lm_yes:
                continue

            # Absence of a landmark on an unscanned body is not evidence of absence
            if not has_landmark:
                if mgst_yes:
                    counts[name]["MGST_NO_LANDMARK"] += 1
                if srv_yes:
                    counts[name]["SRV_NO_LANDMARK"] += 1
                continue

            if mgst_yes and srv_yes and lm_yes:
                cat = "BOTH_TP"
            elif mgst_yes and srv_yes and not lm_yes:
                cat = "BOTH_FP"
            elif mgst_yes and not srv_yes and lm_yes:
                cat = "MGST_ONLY_TP"
            elif not mgst_yes and srv_yes and lm_yes:
                cat = "SRV_ONLY_TP"
            elif mgst_yes and not srv_yes and not lm_yes:
                cat = "MGST_ONLY_FP"
            elif not mgst_yes and srv_yes and not lm_yes:
                cat = "SRV_ONLY_FP"
            elif not mgst_yes and not srv_yes and lm_yes:
                cat = "BOTH_FN"
            else:
                continue

            counts[name][cat] += 1

    del bodies_table, star_classes, sys_coords
    gc.collect()
    # Arrow's allocator keeps freed buffers reserved unless asked to return them
    pa.default_memory_pool().release_unused()

    return {
        "counts": {sp: dict(c) for sp, c in counts.items()},
        "landmark_counts": dict(landmark_counts),
    }


def main():
    parser = argparse.ArgumentParser(description="Compare MGST vs SrvSurvey predictions galaxy-wide")
    parser.add_argument("--database", default=str(DATABASE))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="output/engine_comparison/")
    args = parser.parse_args()

    database = Path(args.database)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Pre-load SrvSurvey criteria (will be forked to workers)
    _load_criteria()

    sectors = sorted([
        d for d in database.iterdir()
        if d.is_dir() and (d / "bodies.parquet").exists()
    ])
    print(f"Comparing engines on {len(sectors)} sectors with {args.workers} workers...", flush=True)

    totals = defaultdict(lambda: Counter())
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
                for sp, cat_counts in result.get("counts", {}).items():
                    for cat, n in cat_counts.items():
                        totals[sp][cat] += n
                for sp, c in result.get("landmark_counts", {}).items():
                    landmark_totals[sp] += c
            except Exception as e:
                print(f"Error in {futures[future]}: {e}", flush=True)
            if progress:
                progress.update(1)

    if progress:
        progress.close()

    # Print results
    print(f"\n{'='*140}", flush=True)
    print("ENGINE COMPARISON: MGST vs SrvSurvey", flush=True)
    print(f"{'='*140}", flush=True)

    header = (
        f"{'Species':<28} {'BOTH_TP':>8} {'BOTH_FP':>8} {'M_TP':>6} {'S_TP':>6} "
        f"{'M_FP':>7} {'S_FP':>7} {'B_FN':>6} "
        f"{'M_Prec%':>8} {'S_Prec%':>8} {'M_Rec%':>7} {'S_Rec%':>7}"
    )
    print(header, flush=True)
    print("-" * 140, flush=True)

    results = {}
    for sp_def in HIGH_VALUE_SPECIES:
        sp = sp_def["name"]
        c = totals[sp]

        both_tp = c.get("BOTH_TP", 0)
        both_fp = c.get("BOTH_FP", 0)
        m_tp = c.get("MGST_ONLY_TP", 0)
        s_tp = c.get("SRV_ONLY_TP", 0)
        m_fp = c.get("MGST_ONLY_FP", 0)
        s_fp = c.get("SRV_ONLY_FP", 0)
        b_fn = c.get("BOTH_FN", 0)

        # MGST metrics
        mgst_tp = both_tp + m_tp
        mgst_fp = both_fp + m_fp
        mgst_fn = s_tp + b_fn
        mgst_pred_with_lm = mgst_tp + both_fp + m_fp  # predictions with landmark data
        mgst_prec = mgst_tp / (mgst_tp + both_fp + m_fp) * 100 if (mgst_tp + both_fp + m_fp) > 0 else 0

        # SrvSurvey metrics
        srv_tp = both_tp + s_tp
        srv_fp = both_fp + s_fp
        srv_fn = m_tp + b_fn
        srv_prec = srv_tp / (srv_tp + both_fp + s_fp) * 100 if (srv_tp + both_fp + s_fp) > 0 else 0

        total_lm = landmark_totals.get(sp, 0)
        mgst_rec = mgst_tp / total_lm * 100 if total_lm > 0 else 0
        srv_rec = srv_tp / total_lm * 100 if total_lm > 0 else 0

        print(
            f"{sp:<28} {both_tp:>8} {both_fp:>8} {m_tp:>6} {s_tp:>6} "
            f"{m_fp:>7} {s_fp:>7} {b_fn:>6} "
            f"{mgst_prec:>7.1f}% {srv_prec:>7.1f}% {mgst_rec:>6.1f}% {srv_rec:>6.1f}%",
            flush=True,
        )

        results[sp] = {
            "BOTH_TP": both_tp,
            "BOTH_FP": both_fp,
            "MGST_ONLY_TP": m_tp,
            "SRV_ONLY_TP": s_tp,
            "MGST_ONLY_FP": m_fp,
            "SRV_ONLY_FP": s_fp,
            "BOTH_FN": b_fn,
            "MGST_NO_LANDMARK": c.get("MGST_NO_LANDMARK", 0),
            "SRV_NO_LANDMARK": c.get("SRV_NO_LANDMARK", 0),
            "mgst_precision": round(mgst_prec, 2),
            "srv_precision": round(srv_prec, 2),
            "mgst_recall": round(mgst_rec, 2),
            "srv_recall": round(srv_rec, 2),
            "total_landmarks": total_lm,
            "value": sp_def["min_value"],
        }

    # Totals
    print("-" * 140, flush=True)
    sum_both_tp = sum(c.get("BOTH_TP", 0) for c in totals.values())
    sum_m_tp = sum(c.get("MGST_ONLY_TP", 0) for c in totals.values())
    sum_s_tp = sum(c.get("SRV_ONLY_TP", 0) for c in totals.values())
    sum_both_fp = sum(c.get("BOTH_FP", 0) for c in totals.values())
    sum_m_fp = sum(c.get("MGST_ONLY_FP", 0) for c in totals.values())
    sum_s_fp = sum(c.get("SRV_ONLY_FP", 0) for c in totals.values())
    sum_b_fn = sum(c.get("BOTH_FN", 0) for c in totals.values())
    print(
        f"{'TOTAL':<28} {sum_both_tp:>8} {sum_both_fp:>8} {sum_m_tp:>6} {sum_s_tp:>6} "
        f"{sum_m_fp:>7} {sum_s_fp:>7} {sum_b_fn:>6}",
        flush=True,
    )

    # Save results
    with open(output_dir / "comparison_summary.json", "w") as f:
        json.dump(results, f, indent=2)

    # Save TSV
    with open(output_dir / "comparison_summary.tsv", "w") as f:
        f.write("species\tvalue\tBOTH_TP\tBOTH_FP\tMGST_ONLY_TP\tSRV_ONLY_TP\t"
                "MGST_ONLY_FP\tSRV_ONLY_FP\tBOTH_FN\t"
                "mgst_precision\tsrv_precision\tmgst_recall\tsrv_recall\n")
        for sp, r in results.items():
            f.write(f"{sp}\t{r['value']}\t"
                    f"{r['BOTH_TP']}\t{r['BOTH_FP']}\t"
                    f"{r['MGST_ONLY_TP']}\t{r['SRV_ONLY_TP']}\t"
                    f"{r['MGST_ONLY_FP']}\t{r['SRV_ONLY_FP']}\t{r['BOTH_FN']}\t"
                    f"{r['mgst_precision']}\t{r['srv_precision']}\t"
                    f"{r['mgst_recall']}\t{r['srv_recall']}\n")

    print(f"\nResults saved to {output_dir}/", flush=True)


if __name__ == "__main__":
    main()
