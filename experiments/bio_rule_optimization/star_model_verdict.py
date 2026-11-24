"""SrvSurvey's brightest star against the orbit-averaged star, scored on species and colour.

Reads the analysis database from build_bio_analysis_db.py. A prediction is
"Genus Species|Colour"; SrvSurvey may list several colours for one species.

- Overall and disagreement-body precision and recall on complete bodies (every
  genus on the body has a confirmed species), so a false positive is certain.
- Head to head on every confirmed species and colour record, complete or not:
  which star's prediction names the colour actually seen, where the two differ.
  Bodies in a system share stars, so wins are also counted per system.
- The same head to head split by the orbit star's share of the flux and by
  whether the colour is set by the star.
"""

import argparse
import glob
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import binomtest

COLS = ["body_id64", "system_id64", "has_prediction", "atmosphere", "srv_star_class", "orbit_star_class",
        "srv_star_id", "orbit_star_id", "orbit_share", "srv_predictions", "orbit_predictions"]


def parse(s):
    return set(json.loads(s)) if s else set()


def score(rows):
    """Species and species+colour precision and recall over (truth, srv, orbit) rows."""
    out = {}
    for name, idx in (("srv", 1), ("orbit", 2)):
        tp_s = fp_s = fn_s = tp_c = fp_c = fn_c = 0
        for r in rows:
            truth, pred = r[0], r[idx]
            ts, ps = {x.split("|")[0] for x in truth}, {x.split("|")[0] for x in pred}
            tp_s += len(ts & ps); fp_s += len(ps - ts); fn_s += len(ts - ps)
            tp_c += len(truth & pred); fp_c += len(pred - truth); fn_c += len(truth - pred)
        out[name] = dict(sp_p=tp_s / max(tp_s + fp_s, 1), sp_r=tp_s / max(tp_s + fn_s, 1),
                         col_p=tp_c / max(tp_c + fp_c, 1), col_r=tp_c / max(tp_c + fn_c, 1),
                         tp_s=tp_s, fp_s=fp_s, fn_s=fn_s, tp_c=tp_c, fp_c=fp_c, fn_c=fn_c)
    return out


def score_lines(label, s, n):
    lines = [f"{label} ({n:,} bodies)"]
    for k in ("srv", "orbit"):
        v = s[k]
        lines.append(f"  {k:6} species P {v['sp_p']:.4%} R {v['sp_r']:.4%}   species+colour P {v['col_p']:.4%} R {v['col_r']:.4%}"
                     f"   (colour TP {v['tp_c']:,} FP {v['fp_c']:,} FN {v['fn_c']:,})")
    return lines


def duel(df):
    """Records where exactly one star names the right colour: wins by record and by system."""
    a, b = df.srv_hit & ~df.orbit_hit, df.orbit_hit & ~df.srv_hit
    n_srv, n_orbit = int(a.sum()), int(b.sum())
    per = df[a | b].assign(d=lambda x: x.orbit_hit.astype(int) - x.srv_hit.astype(int)).groupby("system_id64").d.sum()
    sys_orbit, sys_srv = int((per > 0).sum()), int((per < 0).sum())
    p = binomtest(sys_orbit, sys_orbit + sys_srv, 0.5).pvalue if sys_orbit + sys_srv else float("nan")
    return n_srv, n_orbit, sys_srv, sys_orbit, p


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    args = ap.parse_args()
    if args.output.exists():
        raise SystemExit(f"Output directory already exists: {args.output}")

    df = pd.concat([pd.read_parquet(p, columns=COLS) for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet")))],
                   ignore_index=True)
    df = df[df.has_prediction].set_index("body_id64")
    walk = pd.read_parquet(args.root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "complete"])
    lm = pd.read_parquet(args.root / "landmarks/landmarks.parquet", columns=["body_id64", "species", "colour"])
    lm = lm[lm.colour.notna() & lm.body_id64.isin(df.index)]
    lm["key"] = lm.species + "|" + lm.colour
    truth = lm.groupby("body_id64").key.agg(set)

    differ = df.srv_star_class != df.orbit_star_class
    pred_differ = df.srv_predictions != df.orbit_predictions
    lines = ["# SrvSurvey brightest star vs orbit-averaged star", "",
             f"{len(df):,} bodies with predictions. Different star: {(df.srv_star_id != df.orbit_star_id).sum():,}; "
             f"different star class: {differ.sum():,}; different predictions: {pred_differ.sum():,}.", ""]

    complete = df.index.intersection(walk[walk.complete].body_id64).intersection(truth.index)
    def rows_for(idx):
        return [(truth[i], parse(df.srv_predictions[i]), parse(df.orbit_predictions[i])) for i in idx]
    lines += score_lines("All complete bodies", score(rows_for(complete)), len(complete))
    dis = complete[pred_differ[complete].to_numpy()]
    lines += score_lines("Complete bodies where the predictions differ", score(rows_for(dis)), len(dis))

    # Head to head on every record on a body whose predictions differ
    rec = lm[lm.body_id64.isin(df.index[pred_differ])].copy()
    srv = df.srv_predictions[rec.body_id64].map(parse).to_numpy()
    orb = df.orbit_predictions[rec.body_id64].map(parse).to_numpy()
    rec["srv_hit"] = [k in p for k, p in zip(rec.key, srv)]
    rec["orbit_hit"] = [k in p for k, p in zip(rec.key, orb)]
    rec["system_id64"] = df.system_id64[rec.body_id64].to_numpy()
    rec["share"] = df.orbit_share[rec.body_id64].to_numpy()
    rec["srv_class"] = df.srv_star_class[rec.body_id64].to_numpy()
    rec["orbit_class"] = df.orbit_star_class[rec.body_id64].to_numpy()
    both, neither = int((rec.srv_hit & rec.orbit_hit).sum()), int((~rec.srv_hit & ~rec.orbit_hit).sum())
    lines += ["", f"Head to head: {len(rec):,} confirmed records on {rec.body_id64.nunique():,} bodies whose predictions differ",
              f"  both right {both:,}, neither right {neither:,}"]
    summary = {}
    def duel_line(label, sub):
        s, o, ss, so, p = duel(sub)
        share = o / max(o + s, 1)
        summary[label] = dict(srv=s, orbit=o, systems_srv=ss, systems_orbit=so, p=p)
        lines.append(f"  {label:34} SrvSurvey only {s:>7,}  orbit only {o:>7,}  orbit wins {share:6.1%}   "
                     f"systems {ss:>6,} vs {so:>6,}  p={p:.2g}")
    duel_line("all", rec)
    lines.append("  by orbit star's share of the flux:")
    for lo, hi in ((0, .5), (.5, .6), (.6, .7), (.7, .8), (.8, .9), (.9, 1.01)):
        duel_line(f"{lo:.1f}-{min(hi, 1):.1f}", rec[(rec.share >= lo) & (rec.share < hi)])
    lines.append("  by class pair (most records first):")
    pairs = (rec.srv_class.astype(str) + " -> " + rec.orbit_class.astype(str))
    for pair in pairs.value_counts().head(15).index:
        duel_line(pair, rec[pairs == pair])

    args.output.mkdir(parents=True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=1, default=float) + "\n")
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
