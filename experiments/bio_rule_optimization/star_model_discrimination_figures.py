"""Show what the star work decides: Clypeus Lacrimam against Speculumi, and species colours.

clypeus_distance.png plots every body confirmed to host one of the two species, arrival distance against the distance
up the parent chain (engine.chain_distance where every orbit is on record, otherwise arrival distance).

species_colours.png uses the predictions stored in the analysis database, before corrections, so only the star differs.
It takes confirmed records where both stars predict the species and have different classes, and shows how often
each names the confirmed colour, for species whose colour depends on the star. Writes clypeus.tsv and colours.tsv.
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
import numpy as np
import pandas as pd

SRV, ORBIT, INK, MUTED, GRID, SURFACE = "#eb6834", "#2a78d6", "#0b0b0b", "#52514e", "#e6e5e0", "#fcfcfb"
LACRIMAM, SPECULUMI = "#7c3aa8", "#1a9e77"
LIMIT = 2500.0
CLYPEUS = ["Clypeus Margaritus", "Clypeus Speculumi", "Clypeus Lacrimam"]


def load_clypeus(root, chains):
    lm = pd.read_parquet(root / "landmarks/landmarks.parquet", columns=["body_id64", "species"])
    lm = lm[lm.species.isin(["Clypeus Lacrimam", "Clypeus Speculumi"])].set_index("body_id64").species
    frames = []
    for p in sorted(glob.glob(str(root / "bodies/part_*.parquet"))):
        d = pd.read_parquet(p, columns=["body_id64", "distanceToArrival"])
        frames.append(d[d.body_id64.isin(lm.index)])
    d = pd.concat(frames, ignore_index=True)
    d["species"] = d.body_id64.map(lm)
    chain = pd.read_parquet(chains, columns=["body_id64", "chain_ls", "chain_complete"]).drop_duplicates("body_id64")
    chain = chain[chain.chain_complete].set_index("body_id64").chain_ls
    d["chain_ls"] = d.body_id64.map(chain).fillna(d.distanceToArrival)
    return d.rename(columns={"distanceToArrival": "arrival_ls"})


def load_colours(root):
    lm = pd.read_parquet(root / "landmarks/landmarks.parquet", columns=["body_id64", "species", "colour"])
    lm = lm[lm.colour.notna()]
    truth = defaultdict(list)
    for b, s, c in zip(lm.body_id64, lm.species, lm.colour):
        truth[b].append((s, c))
    del lm
    counts = defaultdict(lambda: [0, 0, 0])
    systems = defaultdict(set)
    for p in sorted(glob.glob(str(root / "bodies/part_*.parquet"))):
        d = pd.read_parquet(p, columns=["body_id64", "system_id64", "srv_predictions", "orbit_predictions", "srv_star_class", "orbit_star_class"])
        d = d[d.orbit_predictions.notna() & (d.srv_star_class != d.orbit_star_class) & d.body_id64.isin(truth.keys())]
        for t in d.itertuples(index=False):
            srv, orbit = set(json.loads(t.srv_predictions)), set(json.loads(t.orbit_predictions))
            srv_species, orbit_species = {k.split("|")[0] for k in srv}, {k.split("|")[0] for k in orbit}
            for s, c in truth[t.body_id64]:
                if s in srv_species and s in orbit_species:
                    r = counts[s]
                    r[0] += 1
                    r[1] += f"{s}|{c}" in srv
                    r[2] += f"{s}|{c}" in orbit
                    systems[s].add(t.system_id64)
    t = pd.DataFrame([{"species": s, "records": v[0], "srv_right": v[1], "orbit_right": v[2], "systems": len(systems[s])} for s, v in counts.items()])
    return t.sort_values("records", ascending=False)


def clypeus_counts(d):
    out = {}
    for col in ("arrival_ls", "chain_ls"):
        above = d[col] >= LIMIT
        out[col] = {"speculumi_kept": int((above & (d.species == "Clypeus Speculumi")).sum()), "lacrimam_above": int((above & (d.species == "Clypeus Lacrimam")).sum())}
    return out


def clypeus_figure(path, d):
    n = clypeus_counts(d)
    lac, spe = d[d.species == "Clypeus Lacrimam"], d[d.species == "Clypeus Speculumi"]
    fig, ax = plt.subplots(figsize=(10, 9), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)
    plot = d.sample(frac=1, random_state=0)
    floor = 3.0
    ax.scatter(plot.arrival_ls.clip(lower=floor), plot.chain_ls.clip(lower=floor), s=6, alpha=0.35, linewidths=0,
               c=np.where(plot.species == "Clypeus Lacrimam", LACRIMAM, SPECULUMI), rasterized=True)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlim(floor, 1e6)
    ax.set_ylim(floor, 1e6)
    ax.axvline(LIMIT, color=MUTED, ls="--", lw=1)
    ax.axhline(LIMIT, color=INK, ls="--", lw=1)
    ax.grid(color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(colors=MUTED)
    ax.set_xlabel("Arrival distance (ls), used by SrvSurvey", color=INK)
    ax.set_ylabel("Distance up the parent chain (ls), used now", color=INK)
    mixed = lac[(lac.arrival_ls >= LIMIT) & (lac.chain_ls < LIMIT)]
    ax.annotate(f"{len(mixed):,} Lacrimam bodies far from the\narrival star, but close to their own",
                xy=(3e4, 700), xytext=(1.2e4, 25), fontsize=10, color=INK, ha="left",
                arrowprops=dict(arrowstyle="-", color=MUTED, lw=0.7))
    ax.text(floor * 1.25, LIMIT * 1.25, "2,500 ls limit", fontsize=9, color=INK)
    ax.scatter([], [], s=40, c=LACRIMAM, label=f"Clypeus Lacrimam ({len(lac):,} bodies)")
    ax.scatter([], [], s=40, c=SPECULUMI, label=f"Clypeus Speculumi ({len(spe):,} bodies)")
    ax.legend(loc="upper left", frameon=False, fontsize=10)
    caption = (f"Each point is one body confirmed to host one of the two species. At 2,500 ls, arrival distance keeps {n['arrival_ls']['speculumi_kept']:,} Speculumi bodies "
               f"but also passes {n['arrival_ls']['lacrimam_above']:,} Lacrimam bodies. The parent chain keeps {n['chain_ls']['speculumi_kept']:,} Speculumi bodies and passes "
               f"{n['chain_ls']['lacrimam_above']:,} Lacrimam bodies. Where an orbit on the chain isn't on record, arrival distance stands in, so the point lies on the diagonal. "
               f"Arrival distances under {floor:.0f} ls are drawn at {floor:.0f} ls.")
    fig.subplots_adjust(left=0.09, right=0.97, top=0.98, bottom=0.17)
    fig.text(0.03, 0.012, textwrap.fill(caption, 118), fontsize=10, color=INK, va="bottom")
    fig.savefig(path, dpi=130)
    plt.close(fig)


def colour_figure(path, t, top, min_records):
    star = t[t.srv_right != t.orbit_right]
    shown = star[star.records >= min_records].head(top)
    extra = star[star.species.isin(CLYPEUS) & ~star.species.isin(shown.species)]
    shown = pd.concat([shown, extra]).sort_values("records")
    y = np.arange(len(shown))
    srv, orbit = 100 * shown.srv_right / shown.records, 100 * shown.orbit_right / shown.records
    fig, ax = plt.subplots(figsize=(10, 0.31 * len(shown) + 2.8), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)
    ax.hlines(y, srv, orbit, color=GRID, lw=2, zorder=1)
    ax.scatter(srv, y, s=60, color=SRV, edgecolors=SURFACE, linewidths=1, zorder=3, label="SrvSurvey's star")
    ax.scatter(orbit, y, s=60, color=ORBIT, edgecolors=SURFACE, linewidths=1, zorder=3, label="Orbit-averaged star")
    ax.set_yticks(y, shown.species, color=INK, fontsize=10)
    for label, name in zip(ax.get_yticklabels(), shown.species):
        label.set_fontweight("bold" if name in CLYPEUS else "normal")
    ax.set_xlim(-2, 102)
    ax.set_ylim(-0.7, len(shown) - 0.3)
    for i, n in enumerate(shown.records):
        ax.text(104, i, f"{n:,}", va="center", ha="left", fontsize=9, color=MUTED)
    ax.text(104, len(shown) - 0.3, "Records", va="bottom", ha="left", fontsize=9, color=MUTED)
    ax.grid(axis="x", color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.tick_params(axis="x", colors=MUTED)
    ax.set_xlabel("Confirmed colour named by the star's predictions (%)", color=INK)
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=2, frameon=False, fontsize=10)
    caption = (f"Bodies where the two stars have different classes and both predict the species. Across all {len(t)} species that is {t.records.sum():,} records: "
               f"SrvSurvey's star names the confirmed colour on {t.srv_right.sum():,} and the orbit-averaged star on {t.orbit_right.sum():,}. Shown: the {top} species with the most records "
               f"among those whose colour depends on the star, plus the Clypeus species (bold). Both stars can be right when a species has several colours predicted. "
               f"These records are under 0.3% of all confirmed records with a colour, since the two stars usually agree.")
    fig.subplots_adjust(left=0.22, right=0.93, top=1 - 0.55 / fig.get_figheight(), bottom=1.75 / fig.get_figheight())
    fig.text(0.03, 0.012, textwrap.fill(caption, 118), fontsize=10, color=INK, va="bottom")
    fig.savefig(path, dpi=130)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--chains", type=Path, default=Path("output/speculumi_distance_v2_20261101/stars.parquet"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--top", type=int, default=24)
    ap.add_argument("--min-records", type=int, default=20)
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)
    d = load_clypeus(args.root, args.chains)
    d.to_csv(args.output / "clypeus.tsv", sep="\t", index=False)
    clypeus_figure(args.output / "clypeus_distance.png", d)
    t = load_colours(args.root)
    t.to_csv(args.output / "colours.tsv", sep="\t", index=False)
    colour_figure(args.output / "species_colours.png", t, args.top, args.min_records)
    print(clypeus_counts(d), f"{t.records.sum():,} colour records, SrvSurvey {t.srv_right.sum():,}, orbit {t.orbit_right.sum():,}, {len(t)} species")
    print(t[t.species.str.startswith("Clypeus")].to_string(index=False))
    print((t.orbit_right > t.srv_right).sum(), (t.orbit_right < t.srv_right).sum(), (t.orbit_right == t.srv_right).sum())


if __name__ == "__main__":
    main()
