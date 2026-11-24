"""Figures for star_age_by_class.py: each species' dominant star age by dominant star class, right beside wrong.

Reads known.parquet from a star_age_by_class.py output and writes one figure per species with enough right
predictions, plus one for each applied age rule's body type and atmosphere. Runs in the mgst-plot environment.
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

import species_reference as ref
from species_reference import plt

CLASS_ORDER = ["O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "TTS", "Ae", "W", "C", "S", "MS", "D", "N", "H"]
APPLIED = {"Fonticulua Campestris": ("Rocky Ice world / Argon", 10_000), "Osseus Discus": ("High metal content world / Water", 8_500)}
MIN_CLASS = 20
MAX_POINTS = 4000


def class_key(c):
    return (CLASS_ORDER.index(c) if c in CLASS_ORDER else len(CLASS_ORDER), c)


def figure(path, name, sp, rng, limit=None):
    """Age by dominant star class, right and wrong side by side, for classes with at least MIN_CLASS known bodies."""
    counts = sp.star.value_counts()
    classes = sorted([c for c in counts.index if counts[c] >= MIN_CLASS], key=class_key)
    if not classes:
        return
    fig, ax = plt.subplots(figsize=(max(6, 1.0 * len(classes) + 2), 5.2), constrained_layout=True, facecolor=ref.SURFACE)
    for i, c in enumerate(classes):
        part = sp[sp.star == c]
        for y, offset, colour in ((1, -0.2, ref.PRESENT), (0, 0.2, ref.ABSENT)):
            v = part.age[part.y == y].to_numpy(float)
            if len(v) > MAX_POINTS:
                v = rng.choice(v, MAX_POINTS, replace=False)
            ax.scatter(i + offset + rng.uniform(-0.13, 0.13, len(v)), v, s=3, color=colour, alpha=0.35, linewidths=0, rasterized=True)
        ax.annotate(f"{int((part.y == 1).sum()):,}\n{int((part.y == 0).sum()):,}", (i, 1.0), xycoords=("data", "axes fraction"),
                    ha="center", va="bottom", fontsize=8, color=ref.MUTED)
    if limit is not None:
        ax.axhline(limit, color=ref.INK, ls="--", lw=1.2)
        ax.annotate(f"{limit:,} My", (len(classes) - 0.5, limit), xytext=(0, 3), textcoords="offset points", ha="right", fontsize=9, color=ref.INK)
    ax.set_xticks(range(len(classes)), classes)
    ax.set_xlim(-0.6, len(classes) - 0.4)
    ax.set_xlabel("Dominant star class (most orbit-averaged light)")
    ax.set_ylabel("Dominant star age (million years)")
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(colors=ref.MUTED, labelsize=9)
    ax.grid(axis="y", color=ref.GRID, lw=0.6)
    handles = [plt.Line2D([], [], ls="", marker="o", ms=6, color=ref.PRESENT, label="Right"),
               plt.Line2D([], [], ls="", marker="o", ms=6, color=ref.ABSENT, label="Wrong")]
    ax.legend(handles=handles, loc="upper left", bbox_to_anchor=(1.0, 1.0), frameon=False, fontsize=9)
    fig.suptitle(f"{name}\nCounts above each class: right, then wrong", x=0.01, ha="left", color=ref.INK, fontsize=11)
    fig.savefig(path, dpi=110)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--input", type=Path, required=True, help="star_age_by_class.py output directory")
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--min-right", type=int, default=200)
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)
    df = pd.read_parquet(args.input / "known.parquet")
    rng = np.random.default_rng(0)
    for s, sp in df.groupby("species"):
        if (sp.y == 1).sum() >= args.min_right:
            figure(args.output / f"{s.replace(' ', '_')}.png", s, sp, rng)
    for s, (scope, limit) in APPLIED.items():
        body, atmosphere = scope.split(" / ")
        figure(args.output / f"{s.replace(' ', '_')}_{body.replace(' ', '_')}_{atmosphere}.png", f"{s}, {scope}",
               df[(df.species == s) & (df.body_atmosphere == scope)], rng, limit)


if __name__ == "__main__":
    main()
