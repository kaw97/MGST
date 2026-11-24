"""Test what predicts life on high-value candidate bodies, using only what is known before a visit.

Reads high_value_life_audit.py. Bodies are limited to well-scanned ones: last updated in the given year or later, in a
system where another body has signals. A blank biological signal then counts as no life. Reports share with life by
trait, and how well a tree model separates life from blanks on held-out sectors, overall and per main species.
"""

import argparse
import json
import sys
import zlib
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

NUMERIC = ["gravity", "surfaceTemperature", "surfacePressure", "distanceToArrival", "star_age", "x", "y", "z", "bodies_on_record", "n_species", "max_value"]
CATEGORY = ["subType", "atmosphereType", "volcanismType", "star_class", "parent_star_class"]
BANDS = {"gravity": 8, "surfaceTemperature": 8, "surfacePressure": 8, "distanceToArrival": 8, "star_age": 8}


def auc(frame, name, lines):
    held = [zlib.crc32(s.encode()) % 2 for s in frame.sector]
    held = np.array(held)
    if min(frame.life.sum(), (~frame.life).sum()) < 200:
        lines.append(f"  {name:32} too few")
        return
    x = frame[NUMERIC + CATEGORY].copy()
    for c in CATEGORY:
        x[c] = x[c].astype("category").cat.codes
    scores = np.zeros(len(frame))
    for h in (0, 1):
        model = HistGradientBoostingClassifier(max_iter=200, categorical_features=[x.columns.get_loc(c) for c in CATEGORY], random_state=0)
        model.fit(x[held != h], frame.life[held != h])
        scores[held == h] = model.predict_proba(x[held == h])[:, 1]
    top = frame.life[scores >= np.quantile(scores, 0.75)].mean()
    low = frame.life[scores <= np.quantile(scores, 0.25)].mean()
    lines.append(f"  {name:32} {len(frame):>8,} bodies, {frame.life.mean():.3f} with life, AUC {roc_auc_score(frame.life, scores):.3f}, "
                 f"top quarter {top:.3f}, bottom quarter {low:.3f}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--audit", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--from-year", default="2025")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)
    d = pd.concat([pd.read_parquet(p) for p in sorted((args.audit / "candidates").glob("part_*.parquet"))], ignore_index=True)
    d = d[(d.updateTime >= args.from_year) & (d.system_signal_bodies - d.signals_updateTime.notna().astype(int) > 0)].reset_index(drop=True)
    d["life"] = d.bio.fillna(0).gt(0) | d.landmark_count.fillna(0).gt(0)
    d["n_species"] = d.species.map(lambda s: len(json.loads(s)))
    pd.set_option("display.width", 200, "display.max_rows", 300)
    lines = [f"Well-scanned candidates from {args.from_year}: {len(d):,}, with life {d.life.mean():.3f}", "", "Held-out tree models:"]
    auc(d, "All candidates", lines)
    main_species = ["Stratum Tectonicas", "Tubus Cavas", "Frutexa Flammasis", "Stratum Cucumisis", "Recepta Umbrux", "Aleoida Gravis"]
    only = d[d.species == '["Stratum Tectonicas"]']
    auc(only, "Only Stratum Tectonicas predicted", lines)
    for s in main_species:
        auc(d[d.species.str.contains(s, regex=False)], s, lines)
    t = d[d.species.str.contains("Stratum Tectonicas", regex=False)]
    other = {"Stratum Tectonicas alone": only, "Others": d[~d.species.str.contains("Stratum Tectonicas", regex=False)]}
    for name, f in other.items():
        lines += ["", f"{name}: {len(f):,} bodies, {f.life.mean():.3f} with life"]
        for c in CATEGORY:
            g = f.groupby(c, observed=True).life.agg(["size", "mean"])
            lines += [f"  by {c}:", g[g["size"] >= 100].sort_values("size", ascending=False).head(12).round(3).to_string()]
        for c, n in BANDS.items():
            b = pd.qcut(f[c], n, duplicates="drop")
            lines += [f"  by {c}:", f.groupby(b, observed=True).life.agg(["size", "mean"]).round(3).to_string()]
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
