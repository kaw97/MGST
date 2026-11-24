"""Test whether each species' star-age requirement differs between classes of the dominant star.

Age is the dominant star's: the star giving the body the most orbit-averaged light (orbit_age, from
build_bio_analysis_db.py), not the main star's. Predictions are the final engine's, with every veto and
temperature and gravity limit except the two star-age rules, so those rules are tested afresh.

- Age rules (a minimum, maximum or band, from star_age_rules.fit_rule) are fitted on half the sectors and
  scored on the other half, in every group: species alone, by body type and atmosphere, by dominant star class,
  and by body type, atmosphere and class together.
- Per-class rules are compared with one rule for the whole species, or the whole body type and atmosphere pair,
  in both directions on held-out sectors. A split by class is useful only if its rules remove clearly more wrong
  predictions and what they remove beyond the pooled rule is almost all wrong.

Writes known.parquet (one row per prediction of known truth, for star_age_by_class_figures.py), groups.tsv,
splits.tsv, ranges.tsv (right bodies' 1st-99th percentile age per class) and report.txt. Needs scikit-learn, so it
runs in the mgst environment; the figures run in mgst-plot.
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

sys.path.insert(0, str(Path(__file__).parent))

import species_reference as ref
import srvsurv_engine as engine
from star_age_rules import fit_rule, test_group

AGE_RULES = {"Fonticulua Campestris", "Osseus Discus"}
MIN_PART_RIGHT = 10
# a split by class must clear the same bar as in temp_gravity_by_group.py
GAIN = 1.25
MIN_EXTRA = 50
EXTRA_PURITY = 0.90
MAX_LOSS = 0.02
MIN_RANGE_RIGHT = 30


def load(root, limit_parts=0):
    lm = pd.read_parquet(root / "landmarks/landmarks.parquet", columns=["body_id64", "genus", "species"])
    found_genera, found_species = defaultdict(set), defaultdict(set)
    for b, g, s in zip(lm.body_id64, lm.genus, lm.species):
        found_genera[b].add(g)
        found_species[b].add(s)
    genus_of = dict(zip(lm.species, lm.genus))
    walk = pd.read_parquet(root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "api_genera"])
    api = dict(zip(walk.body_id64, walk.api_genera))
    del walk, lm
    chain = pd.read_parquet(ref.CHAIN_DISTANCES, columns=["body_id64", "chain_ls", "chain_complete"]).drop_duplicates("body_id64")
    chain = chain[chain.chain_complete].set_index("body_id64").chain_ls
    cols = ["body_id64", "sector", "subType", "atmosphere", "gravity", "surfaceTemperature", "distanceToArrival", "orbit_surfaceTemperature",
            "region", "db_genera", "orbit_predictions", "main_age", "orbit_age", "orbit_star_class", "main_class"]
    vetoes = {s: f for s, f in ref.VETOES.items() if s not in AGE_RULES}
    out = defaultdict(list)
    for p in sorted(glob.glob(str(root / "bodies/part_*.parquet")))[: limit_parts or None]:
        d = pd.read_parquet(p, columns=cols)
        d = d[d.orbit_predictions.notna()].reset_index(drop=True)
        d["chain_or_arrival"] = d.body_id64.map(chain).fillna(d.distanceToArrival)
        preds = [{k.split("|")[0] for k in json.loads(x)} for x in d.orbit_predictions]
        names = set().union(*preds) if preds else set()
        dropped = {s: (f(d).fillna(False).to_numpy() if s in vetoes else np.zeros(len(d), bool)) for s, f in
                   ((s, vetoes.get(s)) for s in names)}
        for s in names:
            dropped[s] = dropped[s] | ref.temp_gravity_limited(d, s)
        for i, t in enumerate(d.itertuples(index=False)):
            b = t.body_id64
            a = json.loads(api[b]) if api.get(b) else []
            listed = set(a or ([ref.CODE_GENUS.get(c, c) for c in json.loads(t.db_genera)] if isinstance(t.db_genera, str) and t.db_genera else []))
            for s in preds[i]:
                if dropped[s][i]:
                    continue
                words = s.split(" ")
                if words[0] == "Tussock" and words[1] in engine.TUSSOCK_MISSING_REGIONS and t.region == 9:
                    continue
                g = genus_of.get(s) or ref.full_name_genus(s) or words[0]
                y = 1 if s in found_species[b] else (0 if g in found_genera[b] or (listed and g not in listed) else -1)
                if y < 0:
                    continue
                for k, v in (("species", s), ("y", y), ("age", t.orbit_age), ("main_age", t.main_age), ("star", t.orbit_star_class),
                             ("main_class", t.main_class), ("half", zlib.crc32(t.sector.encode()) % 2),
                             ("body_atmosphere", f"{t.subType} / {t.atmosphere}")):
                    out[k].append(v)
    df = pd.DataFrame(out)
    df["star"] = df.star.fillna("none").astype(str)
    return df[df.age.notna()].reset_index(drop=True)


def drops(rule, age):
    lo, hi, _ = rule
    return (age < lo) | (age > hi)


def held_out(k):
    """Held-out (right lost, wrong removed) for one pooled age rule and for a rule per dominant star class."""
    pooled, split = np.zeros(2, int), np.zeros(2, int)
    age, y, half, cls = k.age.to_numpy(float), k.y.to_numpy(), k.half.to_numpy(), k.star.to_numpy()
    for h in (0, 1):
        tr, te = half == h, half != h
        base = fit_rule(age[tr], y[tr])
        d_pool = drops(base, age[te])
        d_split = np.zeros(te.sum(), bool)
        for c in np.unique(cls):
            ptr, pte = tr & (cls == c), cls[te] == c
            rule = fit_rule(age[ptr], y[ptr]) if y[ptr].sum() >= MIN_PART_RIGHT and (1 - y[ptr]).sum() >= 1 else base
            d_split[pte] = drops(rule, age[te][pte])
        yt = y[te]
        pooled += [int((yt[d_pool] == 1).sum()), int((yt[d_pool] == 0).sum())]
        split += [int((yt[d_split] == 1).sum()), int((yt[d_split] == 0).sum())]
    return pooled, split


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--limit-parts", type=int, default=0)
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)
    df = load(args.root, args.limit_parts)
    df.to_parquet(args.output / "known.parquet")
    lines = [f"{len(df):,} known predictions with a dominant star age"]
    differ = df.age.to_numpy(float) != df.main_age.to_numpy(float)
    lines.append(f"Dominant star age differs from the main star's on {differ.sum():,} ({differ.mean():.2%}); "
                 f"dominant class differs from the main star's on {(df.star != df.main_class.fillna('none')).mean():.2%}")

    groups, splits, ranges = [], [], []
    for s, sp in df.groupby("species"):
        scopes = [("all", sp)] + list(sp.groupby("body_atmosphere"))
        for scope, part in scopes:
            arr = lambda f: np.column_stack([f.y.to_numpy(float), f.age.to_numpy(float), f.half.to_numpy(float)])
            groups.append({"species": s, "scope": scope, "star": "all", **test_group(arr(part))})
            for c, cp in part.groupby("star"):
                groups.append({"species": s, "scope": scope, "star": c, **test_group(arr(cp))})
                r = cp.age[cp.y == 1]
                if len(r) >= MIN_RANGE_RIGHT:
                    lo, hi = np.quantile(r, [0.01, 0.99])
                    w = cp.age[cp.y == 0]
                    ranges.append({"species": s, "scope": scope, "star": c, "right": len(r), "wrong": len(w),
                                   "age_low": round(lo), "age_high": round(hi),
                                   "wrong_below": int((w < lo).sum()), "wrong_above": int((w > hi).sum())})
            if part.y.sum() >= 2 * MIN_PART_RIGHT and (1 - part.y).sum() >= 20 and part.star.nunique() > 1:
                pooled, per_class = held_out(part)
                extra_lost, extra_removed = per_class[0] - pooled[0], per_class[1] - pooled[1]
                useful = (per_class[1] >= GAIN * pooled[1] and extra_removed >= MIN_EXTRA and per_class[0] <= MAX_LOSS * part.y.sum()
                          and extra_removed >= EXTRA_PURITY * (extra_removed + max(extra_lost, 0)))
                splits.append({"species": s, "scope": scope, "classes": part.star.nunique(), "right": int(part.y.sum()),
                               "wrong": int((1 - part.y).sum()), "pooled_right_lost": int(pooled[0]), "pooled_wrong_removed": int(pooled[1]),
                               "class_right_lost": int(per_class[0]), "class_wrong_removed": int(per_class[1]),
                               "extra_right_lost": int(extra_lost), "extra_wrong_removed": int(extra_removed), "useful": useful})
    groups, splits, ranges = pd.DataFrame(groups), pd.DataFrame(splits), pd.DataFrame(ranges)
    groups.to_csv(args.output / "groups.tsv", sep="\t", index=False)
    splits.to_csv(args.output / "splits.tsv", sep="\t", index=False)
    ranges.to_csv(args.output / "ranges.tsv", sep="\t", index=False)

    # the two applied age rules, measured with each age
    applied = {"Fonticulua Campestris": ("Rocky Ice world / Argon", "above", 10_000), "Osseus Discus": ("High metal content world / Water", "below", 8_500)}
    lines += ["", "Applied age rules, removed with the main star's age and with the dominant star's (right lost, wrong removed):"]
    for s, (scope, side, limit) in applied.items():
        part = df[(df.species == s) & (df.body_atmosphere == scope)]
        for col in ("main_age", "age"):
            v = part[col].to_numpy(float)
            d = v > limit if side == "above" else v < limit
            lines.append(f"  {s}, {scope}, {side} {limit:,} My, {col}: {int((part.y[d] == 1).sum()):,} right, {int((part.y[d] == 0).sum()):,} wrong")

    pd.set_option("display.width", 260, "display.max_rows", 3000, "display.max_colwidth", 44)
    rules = groups[groups.status == "rule"].sort_values("test_wrong_removed", ascending=False)
    lines += ["", f"{groups.species.nunique()} species, {len(groups):,} groups tested: " + ", ".join(f"{k} {v:,}" for k, v in groups.status.value_counts().items()),
              "Rules that hold on held-out sectors (star 'all' means every class together):",
              rules.drop(columns="status").to_string(index=False) if len(rules) else "none", "",
              f"Per-class rules against one pooled rule: {len(splits):,} tested, {int(splits.useful.sum()) if len(splits) else 0} useful"]
    useful = splits[splits.useful].sort_values("extra_wrong_removed", ascending=False) if len(splits) else splits
    lines += [useful.to_string(index=False) if len(useful) else "none", ""]
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines[:12]))


if __name__ == "__main__":
    main()
