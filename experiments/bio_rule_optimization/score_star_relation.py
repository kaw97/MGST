"""Score rules for choosing a body's star by how each star relates to it, from star_relation.py.

All rules use the existing orbit-averaged light and only restrict which stars compete, or how much light a non-parent
needs to overrule the nearest parent. Scored head to head against the orbit model on confirmed records (predictions
before vetoes and limits). Threshold rules are fitted on one half of the sectors and scored on the other.
"""

import argparse
import glob
import json
import sys
import zlib
from pathlib import Path

import numpy as np
import pandas as pd

NEAR = ("direct", "own_barycentre")


def pick_orbit(stars):
    return max(stars, key=lambda s: s["flux"])


def restricted(allowed):
    def pick(stars):
        pool = [s for s in stars if s["relation"] in allowed] or stars
        return pick_orbit(pool)
    return pick


def overrule(multiple, relations=None):
    """Brightest near star, unless another star (of the given relations) gives it `multiple` times its light."""
    def pick(stars):
        near = [s for s in stars if s["relation"] in NEAR]
        if not near:
            return pick_orbit(stars)
        best_near = pick_orbit(near)
        others = [s for s in stars if s["relation"] not in NEAR and (relations is None or s["relation"] in relations)]
        if others:
            best = pick_orbit(others)
            if best["flux"] > multiple * best_near["flux"]:
                return best
        return best_near
    return pick


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--relations", type=Path, required=True, help="Output of star_relation.py")
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)

    rel = pd.concat([pd.read_parquet(p) for p in sorted(glob.glob(str(args.relations / "part_*.parquet")))], ignore_index=True).drop_duplicates("body_id64")
    ids = set(rel.body_id64.tolist())
    info = []
    for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet"))):
        d = pd.read_parquet(p, columns=["body_id64", "system_id64", "sector", "orbit_star_id", "srv_star_id"])
        info.append(d[d.body_id64.isin(ids)])
    info = pd.concat(info, ignore_index=True).drop_duplicates("body_id64").set_index("body_id64")
    lm = pd.read_parquet(args.root / "landmarks/landmarks.parquet", columns=["body_id64", "species", "colour"])
    lm = lm[lm.colour.notna() & lm.body_id64.isin(ids)]
    keys = (lm.species + "|" + lm.colour).groupby(lm.body_id64).agg(list).to_dict()

    bodies = []
    for b, s, p in zip(rel.body_id64, rel.stars, rel.predictions):
        if b in keys and b in info.index:
            preds = {c: set(json.loads(v)) for c, v in json.loads(p).items()}
            hits = {c: sum(k in v for k in keys[b]) for c, v in preds.items()}
            stars = [x for x in json.loads(s)]
            if stars:
                bodies.append({"id": b, "stars": stars, "hits": hits, "n": len(keys[b]),
                               "half": zlib.crc32(info.sector[b].encode()) % 2})
    lines = [f"{len(bodies):,} contested bodies with confirmed colours, {sum(x['n'] for x in bodies):,} records", ""]

    def tally(pick, subset=None):
        total = 0
        for x in subset if subset is not None else bodies:
            total += x["hits"][pick(x["stars"])["class"]]
        return total

    def versus(pick, subset=None):
        win = lose = 0
        for x in subset if subset is not None else bodies:
            a, o = x["hits"][pick(x["stars"])["class"]], x["hits"][pick_orbit(x["stars"])["class"]]
            win += max(a - o, 0)
            lose += max(o - a, 0)
        return win, lose

    # What relation is the orbit model's pick, when that isn't the near star?
    lines.append("Orbit pick against the brightest near star, by the orbit pick's relation and its light over the near star's:")
    lines.append(f"  {'relation':10}{'light ratio':>14}{'bodies':>9}{'orbit right':>13}{'near right':>12}{'orbit wins':>12}")
    bins = [(1, 1.5), (1.5, 2), (2, 3), (3, 5), (5, 10), (10, 100), (100, 1e4), (1e4, np.inf)]
    groups = {}
    for x in bodies:
        near = [s for s in x["stars"] if s["relation"] in NEAR]
        best = pick_orbit(x["stars"])
        if not near or best["relation"] in NEAR:
            continue
        ratio = best["flux"] / max(pick_orbit(near)["flux"], 1e-300)
        o, n = x["hits"][best["class"]], x["hits"][pick_orbit(near)["class"]]
        for lo, hi in bins:
            if lo <= ratio < hi:
                g = groups.setdefault((best["relation"], lo, hi), [0, 0, 0])
                g[0] += 1; g[1] += max(o - n, 0); g[2] += max(n - o, 0)
    for (r, lo, hi), (c, o, n) in sorted(groups.items()):
        lines.append(f"  {r:10}{f'{lo:g}-{hi:g}':>14}{c:>9,}{o:>13,}{n:>12,}{o / max(o + n, 1):>12.1%}")

    lines += ["", "Near star against other near stars is the orbit comparison; partner and ancestor overrule rules:"]
    rules = {"orbit model (all stars compete)": pick_orbit,
             "near star only": restricted(NEAR),
             "near and ancestors; partners ignored": restricted(("direct", "own_barycentre", "ancestor")),
             "near and partners; ancestors ignored": restricted(("direct", "own_barycentre", "partner")),
             "stars in the chain and barycentres (no outside)": restricted(("direct", "own_barycentre", "ancestor", "partner"))}
    base = tally(pick_orbit)
    for label, pick in rules.items():
        w, l = versus(pick)
        lines.append(f"  {label:50} right {tally(pick):>9,}  gains {w:>7,}  loses {l:>7,}  net {w - l:>+8,}")

    lines += ["", "Near star unless another star gives it this many times its light (all sectors):"]
    multiples = [1, 1.1, 1.2, 1.25, 1.3, 1.4, 1.5, 1.6, 1.75, 2, 2.5, 3, 4, 5, 10]
    for m in multiples:
        w, l = versus(overrule(m))
        lines.append(f"  x{m:<6g} right {tally(overrule(m)):>9,}  gains {w:>7,}  loses {l:>7,}  net {w - l:>+8,}")

    half = {h: [x for x in bodies if x["half"] == h] for h in (0, 1)}
    lines += ["", "Held out: multiple fitted on one half of the sectors, scored on the other, against the orbit model:"]
    for h in (0, 1):
        best = max(multiples, key=lambda m: tally(overrule(m), half[h]))
        w, l = versus(overrule(best), half[1 - h])
        lines.append(f"  fitted on half {h}: x{best:g}; on half {1 - h} gains {w:,}, loses {l:,}, net {w - l:+,}")
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
