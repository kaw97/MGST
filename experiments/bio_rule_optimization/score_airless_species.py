"""Score the life forms SrvSurvey files with an empty variant (Brain Trees, Anemones, Tubers, Bark Mounds, Shards, Amphora).

Their colour, where they have forms, is in the name (Roseum Brain Tree). These are absent from the predictions stored in the analysis database, because the engine dropped empty
variants until 2026-10-31, so they are scored here on their own. They test the main star but not the
chosen star, so the star model doesn't matter. Spansh has few landmarks for them, because they grow on airless
bodies the walk barely reached, so genus lists give a fairer test than species records. Writes
per_species.tsv (same columns as score_rule_corrections.py) and results.txt.
"""

import argparse
import glob
import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

import species_colour_trees as trees
import srvsurv_engine as engine
from build_signal_features import COLUMNS
from sector_pool import run_sectors

DATABASE = "/data/Databases_v2/galaxy_sectors_parquet"
WANT = {}  # sector -> body id64s
GENUS = {"Brain Trees": "Brain Tree", "Luteolum Anemone": "Anemone", "Roseum Sinuous Tubers": "Tubers",
         "Amphora Plant": "Amphora", "Bark Mounds": "Bark Mounds", "Crystalline Shards": "Crystalline Shards"}


def work(sector):
    path = f"{DATABASE}/{sector}/bodies.parquet"
    names = pq.read_schema(path).names
    want = WANT[sector]
    sids = list(set(pq.read_table(path, columns=["id64", "system_id64"], filters=pc.field("id64").isin(list(want)))
                    .column("system_id64").to_pylist()))
    cols = [c for c in names if c in COLUMNS or c.startswith(("atm_", "mat_", "solid_", "parent_"))]
    rows = pq.read_table(path, columns=cols, filters=pc.field("system_id64").isin(sids)).to_pylist()
    systems = {s["id64"]: s for s in pq.read_table(f"{DATABASE}/{sector}/systems.parquet", columns=["id64", "x", "y", "z"],
                                                   filters=pc.field("id64").isin(sids)).to_pylist()}
    by_system = defaultdict(list)
    for r in rows:
        by_system[r["system_id64"]].append(r)
    out = []
    for sid, bodies in by_system.items():
        s = systems.get(sid)
        if s is None:
            continue
        context = engine.SystemContext(bodies)
        for b in bodies:
            if b["id64"] not in want or b.get("type") != "Planet":
                continue
            props = engine.body_props(b, context, (s["x"], s["y"], s["z"]), "srvsurvey")
            if props is None:
                continue
            found = [(g, sp) for g, sp, v in engine.predict_props(props, corrections=False) if v == ""]
            if found:
                out.append((b["id64"], found))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--workers", type=int, default=12)
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)

    df = pd.concat([pd.read_parquet(p, columns=["body_id64", "sector", "orbit_predictions", "db_genera"])
                    for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet")))], ignore_index=True)
    df = df[df.orbit_predictions.notna()]
    for sector, g in df.groupby("sector"):
        WANT[sector] = set(g.body_id64)
    predicted = defaultdict(set)  # species -> bodies
    genus_of = {}
    for rows in run_sectors(work, sorted(WANT), args.workers):
        for b, found in rows:
            for g, sp in found:
                predicted[sp].add(b)
                genus_of[sp] = GENUS[g]

    lm = pd.read_parquet(args.root / "landmarks/landmarks.parquet", columns=["body_id64", "genus", "species"])
    lm = lm[lm.body_id64.isin(set(df.body_id64))]
    found_genera, present = defaultdict(set), defaultdict(set)
    for b, g, s in zip(lm.body_id64, lm.genus, lm.species):
        found_genera[b].add(g)
        present[s].add(b)
    walk = pd.read_parquet(args.root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "api_genera"])
    api = dict(zip(walk.body_id64, walk.api_genera))
    lists = {}
    for b, db in zip(df.body_id64, df.db_genera):
        a = json.loads(api[b]) if api.get(b) else []
        lists[b] = set(a or ([trees.CODE_GENUS.get(c, c) for c in json.loads(db)] if isinstance(db, str) and db else []))
    complete = {b for b, l in lists.items() if l and l <= found_genera[b]}

    rows, known_rows = [], []
    for sp in sorted(set(predicted) | {s for s, g in zip(lm.species, lm.genus) if g in GENUS.values()}):
        p = predicted.get(sp, set())
        t = present.get(sp, set())
        g = genus_of.get(sp) or lm.genus[lm.species == sp].iloc[0]
        rows.append([sp, len(p & t & complete), len((p - t) & complete), len((t - p) & complete), 0, 0])
        wrong = sum(1 for b in p - t if g in found_genera[b] or (lists.get(b) and g not in lists[b]))
        known_rows.append((sp, len(p), len(p & t), wrong, len(p - t) - wrong, len(t - p), len(t)))
    pd.DataFrame(rows, columns=["species", "tp", "fp", "fn", "colour_right", "colour_missed"]).to_csv(
        args.output / "per_species.tsv", sep="\t", index=False)
    lines = [f"{len(df):,} bodies, {len(complete):,} complete", "",
             f"{'species':36} {'predicted':>9} {'right':>6} {'wrong':>7} {'unknown':>8} {'missed':>6} {'records':>7}"]
    lines += [f"{r[0]:36} {r[1]:>9,} {r[2]:>6,} {r[3]:>7,} {r[4]:>8,} {r[5]:>6,} {r[6]:>7,}" for r in known_rows]
    tp, fp, fn = (sum(r[i] for r in rows) for i in (1, 2, 3))
    lines += ["", f"Complete bodies: right {tp:,} wrong {fp:,} missed {fn:,}"]
    lines += ["", "Genus level, on bodies with a genus list (the list says whether the genus is there):",
              f"{'genus':20} {'listed':>7} {'right':>7} {'wrong':>8} {'missed':>7} {'precision':>9} {'recall':>7}"]
    listed_all = {b for b, l in lists.items() if l}
    for g in sorted(GENUS.values()):
        p = {b for sp, bodies in predicted.items() if genus_of[sp] == g for b in bodies} & listed_all
        t = {b for b in listed_all if g in lists[b]}
        tp_g, fp_g, fn_g = len(p & t), len(p - t), len(t - p)
        lines.append(f"{g:20} {len(t):>7,} {tp_g:>7,} {fp_g:>8,} {fn_g:>7,} {tp_g / max(tp_g + fp_g, 1):>9.1%} {tp_g / max(len(t), 1):>7.1%}")
    (args.output / "results.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
