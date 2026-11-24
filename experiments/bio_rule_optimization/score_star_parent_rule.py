"""Score the parent hierarchy star from star_parent_rule.py against SrvSurvey's star and the orbit-averaged star.

All predictions are before vetoes and limits, so only the star differs. Reports species and colour precision and
recall on complete bodies, head to head on every confirmed record split by situation, class and light share, and
writes every record the hierarchy gets wrong and a light model gets right to exceptions.tsv.
"""

import argparse
import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import binomtest

MODELS = {"SrvSurvey": "srv_predictions", "Orbit": "orbit_predictions",
          "Hierarchy, orbit brightness": "parent_orbit_predictions", "Hierarchy, SrvSurvey brightness": "parent_srv_predictions"}
COLS = ["body_id64", "system_id64", "name", "n_stars", "has_prediction", "srv_star_id", "orbit_star_id", "srv_star_class",
        "orbit_star_class", "orbit_share", "srv_predictions", "orbit_predictions"]


def parse(s):
    return set(json.loads(s)) if s else set()


def duel(rec, a, b):
    """Records where only a or only b is right, with counts by system and a sign test on systems."""
    only_a, only_b = rec[a] & ~rec[b], rec[b] & ~rec[a]
    per = rec[only_a | only_b].assign(d=lambda x: x[a].astype(int) - x[b].astype(int)).groupby("system_id64").d.sum()
    sa, sb = int((per > 0).sum()), int((per < 0).sum())
    p = binomtest(sa, sa + sb, 0.5).pvalue if sa + sb else float("nan")
    return int(only_a.sum()), int(only_b.sum()), sa, sb, p


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--parents", type=Path, required=True, help="Output of star_parent_rule.py")
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)

    lm = pd.read_parquet(args.root / "landmarks/landmarks.parquet", columns=["body_id64", "species", "colour"])
    lm = lm[lm.colour.notna()]
    lm["key"] = lm.species + "|" + lm.colour
    ids = set(lm.body_id64.unique().tolist())
    parts = [pd.read_parquet(p) for p in sorted(glob.glob(str(args.parents / "part_*.parquet")))]
    parent = pd.concat(parts, ignore_index=True).drop_duplicates("body_id64")
    del parts
    frames = []
    for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet"))):
        d = pd.read_parquet(p, columns=COLS)
        frames.append(d[d.has_prediction & d.body_id64.isin(ids)])
    df = pd.concat(frames, ignore_index=True).merge(parent, on="body_id64", how="left")
    del frames, parent
    for name in ("orbit", "srv"):  # no parent star on record: nothing to walk, so the orbit star stands
        col = f"parent_{name}_predictions"
        df[col] = df[col].where(df[col].notna(), df.orbit_predictions)
        df[f"parent_{name}_id"] = df[f"parent_{name}_id"].where(df[f"parent_{name}_id"].notna(), df.orbit_star_id)
        df[f"parent_{name}_class"] = df[f"parent_{name}_class"].where(df[f"parent_{name}_class"].notna(), df.orbit_star_class)
    df["kind"] = df.kind.fillna("single star")
    df = df.set_index("body_id64", drop=False)
    print(f"{len(df):,} bodies with a confirmed colour", flush=True)

    lines = ["# Parent hierarchy star against the two light models", "", f"{len(df):,} bodies with a confirmed colour", "",
             "How each body's hierarchy star is found:"]
    lines += [f"  {k:14} {v:>9,}" for k, v in df.kind.value_counts().items()]

    walk = pd.read_parquet(args.root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "complete"])
    complete = df.index.intersection(walk.body_id64[walk.complete])
    truth = lm[lm.body_id64.isin(complete)].groupby("body_id64").key.agg(set)
    lines += ["", f"Complete bodies ({len(truth):,}), before vetoes and limits:"]
    sub = df.loc[truth.index]
    for label, col in MODELS.items():
        ts = fp_s = fn_s = tc = fp_c = fn_c = 0
        for t, s in zip(truth, sub[col]):
            pred = parse(s)
            a, b = {x.split("|")[0] for x in t}, {x.split("|")[0] for x in pred}
            ts += len(a & b); fp_s += len(b - a); fn_s += len(a - b)
            tc += len(t & pred); fp_c += len(pred - t); fn_c += len(t - pred)
        lines.append(f"  {label:32} species P {ts / (ts + fp_s):.4%} R {ts / (ts + fn_s):.4%} FP {fp_s:,} FN {fn_s:,}   "
                     f"colour P {tc / (tc + fp_c):.4%} R {tc / (tc + fn_c):.4%} FP {fp_c:,} FN {fn_c:,}")

    mask = (df.srv_predictions != df.orbit_predictions) | (df.parent_orbit_predictions != df.orbit_predictions) | (df.parent_srv_predictions != df.orbit_predictions)
    rec = lm[lm.body_id64.isin(df.index[mask])].copy()
    b = df.loc[rec.body_id64]
    for short, col in (("srv", "srv_predictions"), ("orbit", "orbit_predictions"), ("po", "parent_orbit_predictions"), ("ps", "parent_srv_predictions")):
        rec[short] = [k in parse(s) for k, s in zip(rec.key, b[col])]
    for c in ("system_id64", "name", "kind", "n_candidates", "orbit_share", "parent_share", "srv_star_id", "orbit_star_id", "parent_orbit_id", "parent_srv_id",
              "srv_star_class", "orbit_star_class", "parent_orbit_class", "parent_srv_class"):
        rec[c] = b[c].to_numpy()
    lines += ["", f"Head to head: {len(rec):,} confirmed records on {rec.body_id64.nunique():,} bodies where any two stars' predictions differ"]

    def row(label, sub, a="po", b="orbit"):
        x, y, sx, sy, p = duel(sub, a, b)
        share = x / max(x + y, 1)
        lines.append(f"  {label:50} {a} only {x:>7,}  {b} only {y:>7,}  {a} wins {share:6.1%}  systems {sx:>6,} vs {sy:>6,}  p={p:.2g}")

    lines.append("Overall:")
    row("hierarchy (orbit brightness) vs orbit", rec, "po", "orbit")
    row("hierarchy (orbit brightness) vs SrvSurvey", rec, "po", "srv")
    row("hierarchy (SrvSurvey brightness) vs orbit", rec, "ps", "orbit")
    row("hierarchy (orbit vs SrvSurvey brightness)", rec, "po", "ps")
    row("orbit vs SrvSurvey", rec, "orbit", "srv")

    star = rec[rec.kind == "star"]
    same_o, same_s = star.parent_orbit_id == star.orbit_star_id, star.parent_orbit_id == star.srv_star_id
    lines.append("One star above the body (the hierarchy needs no brightness):")
    row("only SrvSurvey picks that star; vs orbit", star[~same_o & same_s], "po", "orbit")
    row("only orbit picks that star; vs SrvSurvey", star[same_o & ~same_s], "po", "srv")
    row("neither light model picks it; vs orbit", star[~same_o & ~same_s], "po", "orbit")
    row("neither light model picks it; vs SrvSurvey", star[~same_o & ~same_s], "po", "srv")

    bary = rec[rec.kind == "barycentre"]
    lines.append("Barycentre above the body:")
    row("all; hierarchy vs orbit", bary, "po", "orbit")
    row("all; hierarchy vs SrvSurvey", bary, "po", "srv")
    row("all; orbit brightness vs SrvSurvey brightness", bary, "po", "ps")
    for n in (2, 3):
        row(f"{n} candidates; orbit vs SrvSurvey brightness", bary[bary.n_candidates == n], "po", "ps")
    row("4+ candidates; orbit vs SrvSurvey brightness", bary[bary.n_candidates >= 4], "po", "ps")
    row("pick outside the barycentre; hierarchy vs orbit", bary[bary.parent_orbit_id != bary.orbit_star_id], "po", "orbit")
    lines.append("  by the winning star's share among the barycentre's stars, orbit vs SrvSurvey brightness:")
    for lo, hi in ((0, .55), (.55, .6), (.6, .7), (.7, .8), (.8, .9), (.9, 1.01)):
        row(f"{lo:.2f}-{min(hi, 1):.2f}", bary[(bary.parent_share >= lo) & (bary.parent_share < hi)], "po", "ps")

    lines.append("By hierarchy star class, one star above the body, vs orbit:")
    for cls, n in star.parent_orbit_class.value_counts().items():
        s = star[star.parent_orbit_class == cls]
        if (s.po != s.orbit).sum():
            row(str(cls), s, "po", "orbit")
    lines.append("By class pair (orbit's pick -> hierarchy), one star above the body, vs orbit (most contested first):")
    contested = star[star.po != star.orbit]
    pairs = contested.orbit_star_class.astype(str) + " -> " + contested.parent_orbit_class.astype(str)
    for pair in pairs.value_counts().head(20).index:
        row(pair, contested[pairs == pair], "po", "orbit")

    exceptions = rec[~rec.po & (rec.orbit | rec.srv)]
    exceptions.drop(columns=["key"]).sort_values(["kind", "name"]).to_csv(args.output / "exceptions.tsv", sep="\t", index=False)
    lines += ["", f"Exceptions: {len(exceptions):,} records on {exceptions.body_id64.nunique():,} bodies in "
              f"{exceptions.system_id64.nunique():,} systems where the hierarchy is wrong and a light model is right, by situation:"]
    lines += [f"  {k:14} {v:>7,}" for k, v in exceptions.kind.value_counts().items()]
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
