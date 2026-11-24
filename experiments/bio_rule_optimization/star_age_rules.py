"""Test every species for a star-age rule of its own.

Each predicted species is split four ways: all its bodies, by body type, by atmosphere, and by both.
In each group a rule is fitted on half the sectors, with its own limits: minimum age, maximum age, or
both (an age band), each allowed to lose at most 1% of the right predictions. It is scored on the other
half. Age is the main star's: stars in a system almost always share one. Only bodies where the truth is
known are used. Writes all_groups.tsv with every group tested and report.txt with the rules that hold up.
"""

import argparse
import glob
import json
import sys
import zlib
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

sys.path.insert(0, str(Path(__file__).parent))

import species_colour_trees as trees

MIN_RIGHT = 10  # in each half
MIN_WRONG = 20
LOSS = 0.01
CLEAN_MAX_LOSS = 0.02  # on held-out sectors
CLEAN_MIN_SHARE = 0.20
CLEAN_MIN_REMOVED = 50


def fit_rule(age, y):
    """(low, high, wrong removed) with the most wrong removed, losing at most LOSS of the right."""
    right, wrong = np.sort(age[y == 1]), age[y == 0]
    n = len(right)
    best = (-np.inf, np.inf, 0)
    for lo_share, hi_share in ((LOSS, 0.0), (0.0, LOSS), (LOSS / 2, LOSS / 2)):
        lo = right[int(lo_share * n)] if lo_share else -np.inf
        hi = right[n - 1 - int(hi_share * n)] if hi_share else np.inf
        removed = int(((wrong < lo) | (wrong > hi)).sum())
        if removed > best[2]:
            best = (float(lo), float(hi), removed)
    return best


def test_group(a):
    """Row of results for one group; a holds (y, age, half)."""
    y, age, half = a[:, 0], a[:, 1], a[:, 2]
    ok = ~np.isnan(age)
    y, age, half = y[ok], age[ok], half[ok]
    row = {"right": int(y.sum()), "wrong": int((1 - y).sum())}
    if min(y[half == 0].sum(), y[half == 1].sum()) < MIN_RIGHT or min((1 - y[half == 0]).sum(), (1 - y[half == 1]).sum()) < MIN_WRONG:
        return {**row, "status": "too few bodies"}
    auc = roc_auc_score(y, age)
    lo, hi, _ = fit_rule(age[half == 0], y[half == 0])
    drop = ((age < lo) | (age > hi)) & (half == 1)
    te_right, te_wrong = int((y[half == 1] == 1).sum()), int((y[half == 1] == 0).sum())
    lost, removed = int((y[drop] == 1).sum()), int((y[drop] == 0).sum())
    clean = lost <= CLEAN_MAX_LOSS * te_right and removed >= CLEAN_MIN_SHARE * te_wrong and removed >= CLEAN_MIN_REMOVED
    return {**row, "status": "rule" if clean else "no rule", "auc": round(max(auc, 1 - auc), 3),
            "min_age": None if lo == -np.inf else round(lo), "max_age": None if hi == np.inf else round(hi),
            "test_right": te_right, "test_right_lost": lost, "test_wrong": te_wrong, "test_wrong_removed": removed}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)

    lm = pd.read_parquet(args.root / "landmarks/landmarks.parquet", columns=["body_id64", "genus", "species"])
    found_genera, found_species = defaultdict(set), defaultdict(set)
    for b, g, s in zip(lm.body_id64, lm.genus, lm.species):
        found_genera[b].add(g)
        found_species[b].add(s)
    genus_of = dict(zip(lm.species, lm.genus))
    walk = pd.read_parquet(args.root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "api_genera"])
    api = dict(zip(walk.body_id64, walk.api_genera))
    del walk, lm

    rows = defaultdict(list)
    cols = ["body_id64", "sector", "subType", "atmosphere", "db_genera", "orbit_predictions", "main_age"]
    for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet"))):
        d = pd.read_parquet(p, columns=cols)
        for t in d[d.orbit_predictions.notna()].itertuples(index=False):
            b = t.body_id64
            a = json.loads(api[b]) if api.get(b) else []
            listed = set(a or ([trees.CODE_GENUS.get(c, c) for c in json.loads(t.db_genera)] if isinstance(t.db_genera, str) and t.db_genera else []))
            half = zlib.crc32(t.sector.encode()) % 2
            for s in {k.split("|")[0] for k in json.loads(t.orbit_predictions)}:
                g = genus_of.get(s) or s.split(" ")[0]
                if s in found_species[b]:
                    y = 1
                elif g in found_genera[b] or (listed and g not in listed):
                    y = 0
                else:
                    continue
                rows[s, t.subType, t.atmosphere].append((y, t.main_age, half))

    species = defaultdict(list)
    for key, v in rows.items():
        species[key[0]].append((key[1], key[2], np.array(v, dtype=float)))
    del rows
    out = []
    for s, parts in sorted(species.items()):
        groups = {("all", "all"): [p[2] for p in parts]}
        for body, atm, a in parts:
            groups.setdefault((body, "all"), []).append(a)
            groups.setdefault(("all", atm), []).append(a)
            groups[body, atm] = [a]
        for (body, atm), arrays in groups.items():
            out.append({"species": s, "body": body, "atmosphere": atm, **test_group(np.concatenate(arrays))})
    df = pd.DataFrame(out)
    df.to_csv(args.output / "all_groups.tsv", sep="\t", index=False)

    rules = df[df.status == "rule"].sort_values("test_wrong_removed", ascending=False)
    pd.set_option("display.width", 200, "display.max_rows", 500, "display.max_colwidth", 26)
    lines = [f"{df.species.nunique()} species, {len(df):,} groups tested: " + ", ".join(f"{k} {v:,}" for k, v in df.status.value_counts().items()),
             f"Species with a rule in some group: {rules.species.nunique()}", "",
             f"Rules: at most {CLEAN_MAX_LOSS:.0%} of right predictions lost and at least {CLEAN_MIN_SHARE:.0%} of wrong removed on held-out sectors", "",
             rules.drop(columns="status").to_string(index=False)]
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
