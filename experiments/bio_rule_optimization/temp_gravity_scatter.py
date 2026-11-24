"""Scatter plot of temperature against gravity for each species, one point per body of known truth.

Uses the same predictions as temp_gravity_rules.py, before the limits are applied, for the same species (at least
MIN_RIGHT right). Writes an untitled panel per species and panels.json, for genus_heatmaps.py --kind scatter.
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))

import species_reference as ref
from species_reference import plt
from temp_gravity_rules import MIN_RIGHT, load_species

MARKER = 2.5


def axis_range(values, limits):
    """Central 99.8% of the values, extended to any limit drawn."""
    low, high = np.quantile(values, [0.001, 0.999])
    low, high = min([low, *limits]), max([high, *limits])
    pad = (high - low) * 0.02 or 1
    return low - pad, high + pad


def figure(path, name, a, rng):
    a = a[~np.isnan(a[:, 1]) & ~np.isnan(a[:, 2]) & (a[:, 0] >= 0)]
    a = a[rng.permutation(len(a))]
    y, T, g = a[:, 0], a[:, 1], a[:, 2]
    fig, ax = plt.subplots(figsize=(8, 6.2), constrained_layout=True, facecolor=ref.SURFACE)
    ax.scatter(T, g, s=MARKER, c=np.where(y == 1, ref.PRESENT, ref.ABSENT), alpha=0.6, linewidths=0, rasterized=True)
    limits = ref.LIMITS.get(name, [])
    drop = np.zeros(len(y), bool)
    t_lines, g_lines = [], []
    for body, atmosphere, t_min, t_max, g_min, g_max in limits:
        if body is not None or atmosphere is not None:
            continue
        for v, side in ((t_min, T < (t_min or 0)), (t_max, T > (t_max or 0))):
            if v is not None:
                ax.axvline(v, color=ref.INK, ls="--", lw=1.2)
                ax.annotate(f"{v:.0f} K", (v, 1), xycoords=("data", "axes fraction"), xytext=(4, -12), textcoords="offset points", fontsize=9, color=ref.INK)
                t_lines.append(v)
                drop |= side
        for v, side in ((g_min, g < (g_min or 0)), (g_max, g > (g_max or 0))):
            if v is not None:
                ax.axhline(v, color=ref.INK, ls="--", lw=1.2)
                ax.annotate(f"{v:.4f} g", (0, v), xycoords=("axes fraction", "data"), xytext=(4, 4), textcoords="offset points", fontsize=9, color=ref.INK)
                g_lines.append(v)
                drop |= side
    ax.set_xlim(axis_range(T, t_lines))
    ax.set_ylim(axis_range(g, g_lines))
    for colour, label in ((ref.PRESENT, "Right"), (ref.ABSENT, "Wrong")):
        ax.scatter([], [], s=40, color=colour, label=label)
    ax.legend(loc="lower right", bbox_to_anchor=(1, 1), ncols=2, frameon=False, fontsize=9, labelcolor=ref.INK, borderaxespad=0)
    ax.set_xlabel("Surface temperature (K)")
    ax.set_ylabel("Gravity (g)")
    ax.set_facecolor(ref.SURFACE)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(colors=ref.MUTED, labelsize=9)
    ax.grid(color=ref.GRID, lw=0.6)
    ax.set_axisbelow(True)
    fig.savefig(path, dpi=110)
    plt.close(fig)
    return {"right": int(y.sum()), "wrong": int((1 - y).sum()), "removed_right": int((y[drop] == 1).sum()),
            "removed_wrong": int((y[drop] == 0).sum()), "any_limit": bool(limits),
            "scoped_limit": any(x[0] is not None or x[1] is not None for x in limits)}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    (args.output / "panels").mkdir(parents=True)
    rng = np.random.default_rng(0)
    numbers = {}
    for s, pieces in sorted(load_species(args.root).items()):
        a = np.concatenate([p[2] for p in pieces])
        if (a[:, 0] == 1).sum() >= MIN_RIGHT:
            numbers[s] = figure(args.output / "panels" / f"{s.replace(' ', '_')}.png", s, a, rng)
    (args.output / "panels" / "panels.json").write_text(json.dumps(numbers, indent=1))
    print(f"{len(numbers)} species -> {args.output}")


if __name__ == "__main__":
    main()
