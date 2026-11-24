"""Scatter plots and heatmaps showing what the star age, distance and star temperature rules remove.

Each rule gets a scatter plot and a heatmap of its own variable against gravity, over the bodies it applies to. The
Clypeus Speculumi figure compares distance from arrival with the distance up the parent chain, from
speculumi_distance.py, using arrival where an orbit on the chain isn't on record. Only bodies of known truth are drawn.
Writes one composite per kind of rule.
"""

import argparse
import glob
import json
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

import species_reference as ref
from genus_heatmaps import build_sheet
from species_reference import plt
from temp_gravity_rules import SHARE_CMAP

BINS = 40
SHADE = "#8a8a84"
AGE = "Age of the system's main star (My)"
ARRIVAL = "Distance from arrival star (ls)"
CHAIN = "Distance from nearest parent star or binary (ls)"
# rule: species, body type, atmosphere, column, x label, limit, side removed, x range
RULES = {
    "Campestris": ("Fonticulua Campestris", "Rocky Ice world", "Argon", "main_age", AGE, 10_000, "above", None),
    "Discus": ("Osseus Discus", "High metal content world", "Water", "main_age", AGE, 8_500, "below", None),
    "Arrival": ("Clypeus Speculumi", None, None, "distanceToArrival", ARRIVAL, 2_500, "below", (0, 6_000)),
    "Chain": ("Clypeus Speculumi", None, None, "chain_or_arrival", CHAIN, 2_500, "below", (0, 6_000)),
    "Araneamus": ("Stratum Araneamus", None, None, "orbit_surfaceTemperature", "Temperature of the dominant star (K)", 7_500, "at_or_below", (0, 30_000)),
}
SHEETS = {
    "star_age": (["Campestris", "Discus"], "Impact of the star age rules.",
                 "Each row is one rule, over the body type and atmosphere it applies to: a scatter plot of bodies of known truth, "
                 "with blue where the predicted species is right and orange where it is wrong, and a heatmap of the right share "
                 "with its curve above (cells under 5 known bodies are blank, curve bins under 20 omitted). The dashed line is the "
                 "limit and the grey area is what it removes."),
    "speculumi_distance": (["Arrival", "Chain"], "Impact of the Clypeus Speculumi distance rule, measured two ways.",
                           "Top row: the 2,500 ls limit on distance from the arrival star, the earlier rule. Bottom row: the applied rule, the same limit on the orbit "
                           "sizes added up the parent chain to the first star or binary, using arrival distance where an orbit on the way isn't on record. "
                           "Blue points are right and orange wrong; heatmaps show the right share with its curve above (cells under 5 known "
                           "bodies are blank, curve bins under 20 omitted). The dashed line is the limit and the grey area what it removes. "
                           "Axes stop at 6,000 ls; farther bodies are counted but not drawn."),
    "star_temperature": (["Araneamus"], "Impact of the star temperature rule.",
                         "A scatter plot of bodies of known truth, with blue where the predicted species is right and orange where it is "
                         "wrong, and a heatmap of the right share with its curve above (cells under 5 known bodies are blank, curve bins "
                         "under 20 omitted). The dashed line is the limit and the grey area is what it removes. Axes stop at 30,000 K; hotter stars such as "
                         "neutron stars are counted but not drawn."),
}


def removed(x, limit, side):
    return x > limit if side == "above" else x < limit if side == "below" else x <= limit


def x_range(x, limit, fixed):
    if fixed:
        return fixed
    low, high = np.quantile(x, [0.001, 0.999])
    low, high = min(low, limit), max(high, limit)
    pad = (high - low) * 0.02 or 1
    return low - pad, high + pad


def finish(ax, xlabel, limit, side, xlim, ylim):
    ax.set_xlim(xlim)
    ax.set_ylim(ylim)
    shade = (limit, xlim[1]) if side == "above" else (xlim[0], limit)
    ax.axvspan(*shade, color=SHADE, alpha=0.14, lw=0, zorder=0)
    ax.axvline(limit, color=ref.INK, ls="--", lw=1.2)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Gravity (g)")
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(colors=ref.MUTED, labelsize=9)


def scatter(path, a, xlabel, limit, side, xlim, ylim, rng):
    a = a[rng.permutation(len(a))]
    fig, ax = plt.subplots(figsize=(8, 6.2), constrained_layout=True, facecolor=ref.SURFACE)
    ax.set_facecolor(ref.SURFACE)
    ax.grid(color=ref.GRID, lw=0.6)
    ax.set_axisbelow(True)
    ax.scatter(a[:, 1], a[:, 2], s=4, c=np.where(a[:, 0] == 1, ref.PRESENT, ref.ABSENT), alpha=0.6, linewidths=0, rasterized=True)
    for colour, label in ((ref.PRESENT, "Right"), (ref.ABSENT, "Wrong")):
        ax.scatter([], [], s=40, color=colour, label=label)
    ax.legend(loc="lower right", bbox_to_anchor=(1, 1), ncols=2, frameon=False, fontsize=9, labelcolor=ref.INK, borderaxespad=0)
    finish(ax, xlabel, limit, side, xlim, ylim)
    ax.annotate(f"{limit:,} limit", (limit, 1), xycoords=("data", "axes fraction"), xytext=(4, -12), textcoords="offset points", fontsize=9, color=ref.INK)
    fig.savefig(path, dpi=110)
    plt.close(fig)


def heatmap(path, a, xlabel, limit, side, xlim, ylim):
    y, x, g = a[:, 0], a[:, 1], a[:, 2]
    xe, ge = np.linspace(*xlim, BINS + 1), np.linspace(*ylim, BINS + 1)
    fig = plt.figure(figsize=(8, 6.2), constrained_layout=True, facecolor=ref.SURFACE)
    grid = fig.add_gridspec(2, 2, width_ratios=[6, 0.16], height_ratios=[1.15, 5])
    ax = fig.add_subplot(grid[1, 0])
    top = fig.add_subplot(grid[0, 0], sharex=ax)
    r = np.histogram2d(x[y == 1], g[y == 1], bins=(xe, ge))[0]
    w = np.histogram2d(x[y == 0], g[y == 0], bins=(xe, ge))[0]
    ax.set_facecolor("#f7f7f4")
    mesh = ax.pcolormesh(xe, ge, np.ma.masked_where(r + w < 5, r / np.maximum(r + w, 1)).T, cmap=SHARE_CMAP, vmin=0, vmax=1, rasterized=True)
    fig.colorbar(mesh, cax=fig.add_subplot(grid[1, 1]), label="Share of known bodies that are right")
    i = np.digitize(x, xe) - 1
    inside = (i >= 0) & (i < BINS)
    n = np.bincount(i[inside], minlength=BINS)
    right = np.bincount(i[inside], weights=y[inside], minlength=BINS)
    share = np.full(BINS, np.nan)
    share[n >= 20] = right[n >= 20] / n[n >= 20]
    top.plot((xe[:-1] + xe[1:]) / 2, share, color=ref.INK, lw=1.6)
    top.set_ylim(0, 1.03)
    top.set_ylabel("Right share", color=ref.MUTED, fontsize=9)
    top.axvline(limit, color=ref.INK, ls="--", lw=1.2)
    top.tick_params(labelbottom=False, colors=ref.MUTED, labelsize=9)
    top.spines[["top", "right"]].set_visible(False)
    top.grid(color=ref.GRID, lw=0.6)
    finish(ax, xlabel, limit, side, xlim, ylim)
    fig.savefig(path, dpi=110)
    plt.close(fig)


def load(root, distances):
    """{species: frame of bodies of known truth}, with the columns the rules need."""
    lm = pd.read_parquet(root / "landmarks/landmarks.parquet", columns=["body_id64", "genus", "species"])
    found_genera, found_species = defaultdict(set), defaultdict(set)
    for b, g, s in zip(lm.body_id64, lm.genus, lm.species):
        found_genera[b].add(g)
        found_species[b].add(s)
    genus_of = dict(zip(lm.species, lm.genus))
    walk = pd.read_parquet(root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "api_genera"])
    api = dict(zip(walk.body_id64, walk.api_genera))
    del walk, lm
    wanted = {r[0] for r in RULES.values()}
    out = defaultdict(list)
    cols = ["body_id64", "subType", "atmosphere", "main_age", "gravity", "distanceToArrival", "orbit_surfaceTemperature", "db_genera", "orbit_predictions"]
    for p in sorted(glob.glob(str(root / "bodies/part_*.parquet"))):
        d = pd.read_parquet(p, columns=cols)
        d = d[d.orbit_predictions.notna()]
        for s in wanted:
            sub = d[d.orbit_predictions.str.contains(s, regex=False)]
            truth = []
            g = genus_of.get(s) or s.split(" ")[0]
            for b, db in zip(sub.body_id64, sub.db_genera):
                a = json.loads(api[b]) if api.get(b) else []
                listed = set(a or ([ref.CODE_GENUS.get(c, c) for c in json.loads(db)] if isinstance(db, str) and db else []))
                truth.append(1 if s in found_species[b] else 0 if g in found_genera[b] or (listed and g not in listed) else -1)
            out[s].append(sub.drop(columns=["db_genera", "orbit_predictions"]).assign(y=truth))
    frames = {s: (lambda f: f[f.y >= 0])(pd.concat(v, ignore_index=True)) for s, v in out.items()}
    chain = pd.read_parquet(distances / "stars.parquet", columns=["body_id64", "chain_ls", "chain_complete"]).drop_duplicates("body_id64")
    f = frames["Clypeus Speculumi"].merge(chain, on="body_id64", how="left")
    f["chain_or_arrival"] = f.chain_ls.where(f.chain_complete.fillna(False).astype(bool), f.distanceToArrival)
    frames["Clypeus Speculumi"] = f
    return frames


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--distances", type=Path, required=True, help="Output of speculumi_distance.py")
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    panels_dir = args.output / "panels"
    panels_dir.mkdir(parents=True)
    frames = load(args.root, args.distances)
    rng = np.random.default_rng(0)
    results, counts = [], {}
    for name, (s, body, atmosphere, col, xlabel, limit, side, fixed) in RULES.items():
        f = frames[s]
        if body:
            f = f[(f.subType == body) & (f.atmosphere == atmosphere)]
        f = f[f[col].notna() & f.gravity.notna()]
        a = f[["y", col, "gravity"]].to_numpy(dtype=float)
        drop = removed(a[:, 1], limit, side)
        counts[name] = {"right": int((a[:, 0] == 1).sum()), "wrong": int((a[:, 0] == 0).sum()),
                        "right_removed": int(((a[:, 0] == 1) & drop).sum()), "wrong_removed": int(((a[:, 0] == 0) & drop).sum())}
        results.append({"rule": name, "species": s, "limit": limit, **counts[name]})
        xlim, ylim = x_range(a[:, 1], limit, fixed), tuple(np.quantile(a[:, 2], [0.001, 0.999]))
        scatter(panels_dir / f"{name}_scatter.png", a, xlabel, limit, side, xlim, ylim, rng)
        heatmap(panels_dir / f"{name}_heatmap.png", a, xlabel, limit, side, xlim, ylim)
    pd.DataFrame(results).to_csv(args.output / "counts.tsv", sep="\t", index=False)
    for sheet, (rules, title, text) in SHEETS.items():
        paths, names, runs = [], [], [(True, title), (False, text)]
        for i, name in enumerate(rules):
            c, s = counts[name], RULES[name][0]
            paths += [panels_dir / f"{name}_scatter.png", panels_dir / f"{name}_heatmap.png"]
            names += [f"{s}: scatter", f"{s}: heatmap"]
            a, b = "ABCDEF"[2 * i], "ABCDEF"[2 * i + 1]
            kept = c["wrong"] - c["wrong_removed"]
            runs += [(True, f"({a}, {b})"), (False, f"{s}: {c['right']:,} right and {c['wrong']:,} wrong; the limit removes {c['wrong_removed']:,} wrong and "
                     f"{c['right_removed']:,} right, leaving {kept:,} wrong{'.' if i == len(rules) - 1 else ';'}")]
        build_sheet(paths, names, runs, 2).save(args.output / f"{sheet}.png", optimize=True)
    print(pd.DataFrame(results).to_string(index=False))


if __name__ == "__main__":
    main()
