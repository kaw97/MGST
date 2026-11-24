"""Compare SrvSurvey's star with the orbit-averaged star, species by species, on confirmed species and colour records.

Uses the predictions stored in the analysis database, before corrections, so only the star differs. A record is decided
when exactly one model's predictions name the confirmed colour. Writes species.tsv and star_model_species.png, a scatter plot of
the species where the two differ, with the largest net gains labelled.
"""

import argparse
import glob
import json
import sys
import textwrap
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.text import Text
import numpy as np
import pandas as pd

SRV, ORBIT, INK, MUTED, GRID, SURFACE = "#eb6834", "#2a78d6", "#0b0b0b", "#52514e", "#e6e5e0", "#fcfcfb"


def count(root):
    lm = pd.read_parquet(root / "landmarks/landmarks.parquet", columns=["body_id64", "species", "colour"])
    lm = lm[lm.colour.notna()]
    truth = defaultdict(list)
    for b, s, c in zip(lm.body_id64, lm.species, lm.colour):
        truth[b].append((s, c))
    del lm
    c = defaultdict(lambda: [0, 0, 0])  # records, SrvSurvey right, orbit right
    systems, only = defaultdict(set), defaultdict(int)
    for p in sorted(glob.glob(str(root / "bodies/part_*.parquet"))):
        d = pd.read_parquet(p, columns=["body_id64", "system_id64", "srv_predictions", "orbit_predictions"])
        d = d[d.orbit_predictions.notna() & d.body_id64.isin(truth.keys())]
        for b, sid, sp, op in zip(d.body_id64, d.system_id64, d.srv_predictions, d.orbit_predictions):
            same = sp == op
            srv = set(json.loads(sp))
            orbit = srv if same else set(json.loads(op))
            for s, col in truth[b]:
                k = f"{s}|{col}"
                r = c[s]
                r[0] += 1
                r[1] += k in srv
                r[2] += k in orbit
                if (k in srv) != (k in orbit):
                    systems[s].add(sid)
                    only[s, k in orbit] += 1
    rows = [{"species": s, "records": v[0], "srv_right": v[1], "orbit_right": v[2], "srv_only": only[s, False], "orbit_only": only[s, True],
             "systems_decided": len(systems[s])} for s, v in c.items()]
    t = pd.DataFrame(rows)
    t["srv_missed"], t["orbit_missed"] = t.records - t.srv_right, t.records - t.orbit_right
    return t


def place_labels(ax, fig, xs, ys, names, others):
    """Label each point at the nearest spot that overlaps no earlier label or point, with a line if it isn't right beside it."""
    renderer = fig.canvas.get_renderer()
    taken, points = [], [ax.transData.transform(q) for q in others]
    offsets = sorted(((dx * side, dy) for dx in (8, 25, 45, 70, 100, 135, 170) for dy in (0, 12, -12, 25, -25, 40, -40, 55, -55, 70, -70, 85, -85, 100, 115) for side in (1, -1)),
                     key=lambda o: (abs(o[0]) ** 2 + (1.5 * o[1]) ** 2, o[0] < 0))
    for x, y, name in zip(xs, ys, names):
        for dx, dy in offsets:
            text = ax.annotate(name, (x, y), xytext=(dx, dy), textcoords="offset points", ha="left" if dx > 0 else "right", va="center", fontsize=9, color=INK,
                               arrowprops=None if (abs(dx), dy) == (8, 0) else dict(arrowstyle="-", color=MUTED, lw=0.6, shrinkA=1, shrinkB=4))
            text.update_positions(renderer)
            box = Text.get_window_extent(text, renderer).expanded(1.03, 1.15)  # the words only, without the line
            inside = ax.get_window_extent(renderer)
            clear = box.x0 >= inside.x0 and box.x1 <= inside.x1 and box.y1 <= inside.y1 and box.y0 >= inside.y0
            if clear and not any(box.overlaps(o) for o in taken) and not any(box.contains(*q) for q in points):
                taken.append(box)
                break
            text.remove()
        else:
            print(f"No clear spot for {name}")


def figure(path, t, top, total):
    fig, ax = plt.subplots(figsize=(11, 9.4), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)
    ax.set_xscale("symlog", linthresh=1)
    ax.set_yscale("symlog", linthresh=1)
    limit = t[["srv_only", "orbit_only"]].max().max() * 2
    ax.set_xlim(-0.2, t.srv_only.max() * 3)
    ax.set_ylim(-0.2, limit)
    ax.plot([0, limit], [0, limit], color=MUTED, ls="--", lw=1)
    ax.scatter(t.srv_only, t.orbit_only, s=np.where(t.species.isin(top.species), 60, 28), c=np.where(t.species.isin(top.species), ORBIT, "#a9a8a3"),
               edgecolors=SURFACE, linewidths=0.8, zorder=3)
    ax.grid(color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(colors=MUTED)
    ticks = [0, 1, 10, 100, 1000]
    ax.set_xticks([v for v in ticks if v < limit], [f"{v:,}" for v in ticks if v < limit])
    ax.set_xlim(-0.2, t.srv_only.max() * 3)
    ax.set_yticks([v for v in ticks if v < limit], [f"{v:,}" for v in ticks if v < limit])
    ax.set_xlabel("Confirmed records only SrvSurvey's star gets right", color=INK)
    ax.set_ylabel("Confirmed records only the orbit-averaged star gets right", color=INK)
    caption = (f"Each point is one species, for the {len(t)} species where the two stars differ on at least one record. Above the dashed line the orbit-averaged "
               f"star is right more often. The {len(top)} species with the largest net gain are labelled in blue. Across all species the orbit-averaged star alone is right on "
               f"{total[1]:,} records and SrvSurvey's alone on {total[2]:,}. Predictions are before corrections, so only the star differs. A record is one species and colour "
               "confirmed on a body. Bodies in a system share stars, so records aren't independent. Axes are logarithmic above 1, and points at 0 lie on the axis.")
    fig.subplots_adjust(left=0.08, right=0.98, top=0.98, bottom=0.17)
    fig.canvas.draw()
    place_labels(ax, fig, top.srv_only, top.orbit_only, top.species, list(zip(t.srv_only, t.orbit_only)))
    fig.text(0.03, 0.012, textwrap.fill(caption, 125), fontsize=10, color=INK, va="bottom")
    fig.savefig(path, dpi=130)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--top", type=int, default=12)
    ap.add_argument("--counts", type=Path, help="species.tsv from an earlier run, to redraw without recounting")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)
    if args.counts:
        t = pd.read_csv(args.counts, sep="\t")
    else:
        t = count(args.root)
        t["gain"] = t.orbit_right - t.srv_right
    t = t.sort_values("gain", ascending=False)
    t.to_csv(args.output / "species.tsv", sep="\t", index=False)
    t["decided"] = t.srv_only + t.orbit_only
    total = (int(t.decided.sum()), int(t.orbit_only.sum()), int(t.srv_only.sum()))
    shown = t[t.decided > 0]
    figure(args.output / "star_model_species.png", shown, shown.head(args.top), total)
    print(f"{total[0]:,} decided, orbit {total[1]:,}, SrvSurvey {total[2]:,} -> {args.output}")


if __name__ == "__main__":
    main()
