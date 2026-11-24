"""Test whether a species' temperature and gravity range shifts with body type, atmosphere, atmosphere mix, pressure or volcanism.

Uses the same predictions and truth as temp_gravity_rules.py, before its limits are applied (or after, with
--after-limits, to find what they still miss). A parent group is
split by one property, and separate limits fitted in each part are compared with one set of limits for the whole
parent. Both are fitted on one half of the sectors and scored on the other, in both directions. A split counts as
useful only if, on held-out sectors, its limits remove clearly more wrong predictions than the pooled ones and what
they remove beyond them is still almost all wrong.

Parents and the splits tried in each:
- all bodies of the species: body type, atmosphere, body type and atmosphere together
- each body type and atmosphere pair: atmosphere mix (main gas alone, or which second gas), share of the second
  gas, pressure tertile, volcanism

Writes ranges.tsv (right bodies' 1st-99th percentile temperature and gravity in every part), splits.tsv (held-out
results for every split), report.txt, and a scatter per species in figures/ coloured by body type and atmosphere.
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
from species_reference import plt
from temp_gravity_rules import band

GASES = ["Ammonia", "Argon", "Carbon_dioxide", "Helium", "Hydrogen", "Iron", "Methane", "Neon", "Nitrogen", "Oxygen",
         "Silicates", "Sulphur_dioxide", "Water"]
MIN_PART_RIGHT = 10  # per training half, to fit a part's own limits; smaller parts use the pooled limits
MIN_RANGE_RIGHT = 30
GAIN = 1.25
MIN_EXTRA = 50
EXTRA_PURITY = 0.90
MAX_LOSS = 0.02
# light-mode categorical slots, in order (dataviz reference palette); the last is for everything else
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6"]
OTHER = "#b5b4ad"


def mix_features(d):
    """Second gas name ('pure' if none) and its share, from the atm_ percentages."""
    cols = [f"atm_{g}" for g in GASES if f"atm_{g}" in d.columns]
    comp = d[cols].fillna(0).to_numpy()
    order = np.argsort(-comp, axis=1)
    second = comp[np.arange(len(d)), order[:, 1]]
    names = np.array([c[4:] for c in cols])[order[:, 1]]
    mix = np.where(second > 0, names, "pure")
    share = np.select([second <= 0, second < 5, second < 20], ["pure", "under 5%", "5-20%"], "20% or more")
    none = comp.sum(axis=1) == 0
    return np.where(none, "unknown", mix), np.where(none, "unknown", share)


def load(root, limit_parts=0, after_limits=False):
    """One row per (body, predicted species) of known or unknown truth, with the properties used to split.

    With after_limits, predictions the current temperature and gravity limits drop are left out.
    """
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
    cols = ["body_id64", "sector", "subType", "main_age", "gravity", "surfaceTemperature", "surfacePressure", "atmosphere",
            "volcanism", "distanceToArrival", "orbit_surfaceTemperature", "region", "db_genera", "orbit_predictions",
            *[f"atm_{g}" for g in GASES]]
    out = defaultdict(list)
    for p in sorted(glob.glob(str(root / "bodies/part_*.parquet")))[: limit_parts or None]:
        d = pd.read_parquet(p, columns=[c for c in cols if c != "atm_Iron"] + ["atm_Iron"])
        d = d[d.orbit_predictions.notna()].reset_index(drop=True)
        d["chain_or_arrival"] = d.body_id64.map(chain).fillna(d.distanceToArrival)
        mix, share = mix_features(d)
        vetoed = {s: set(np.flatnonzero(f(d).fillna(False).to_numpy()).tolist()) for s, f in ref.VETOES.items()}
        if after_limits:
            for s in ref.LIMITS:
                vetoed.setdefault(s, set()).update(np.flatnonzero(ref.temp_gravity_limited(d, s)).tolist())
        for i, t in enumerate(d.itertuples(index=False)):
            b = t.body_id64
            a = json.loads(api[b]) if api.get(b) else []
            listed = set(a or ([ref.CODE_GENUS.get(c, c) for c in json.loads(t.db_genera)] if isinstance(t.db_genera, str) and t.db_genera else []))
            volc = "none" if t.volcanism in (None, "No volcanism") else t.volcanism
            for s in {k.split("|")[0] for k in json.loads(t.orbit_predictions)}:
                if s in vetoed and i in vetoed[s]:
                    continue
                words = s.split(" ")
                if words[0] == "Tussock" and words[1] in engine.TUSSOCK_MISSING_REGIONS and t.region == 9:
                    continue
                g = genus_of.get(s) or ref.full_name_genus(s) or words[0]
                y = 1 if s in found_species[b] else (0 if g in found_genera[b] or (listed and g not in listed) else -1)
                for k, v in (("species", s), ("y", y), ("T", t.surfaceTemperature), ("g", t.gravity), ("P", t.surfacePressure),
                             ("half", zlib.crc32(t.sector.encode()) % 2), ("body", t.subType), ("atmosphere", t.atmosphere),
                             ("mix", mix[i]), ("mix_share", share[i]), ("volcanism", volc)):
                    out[k].append(v)
    df = pd.DataFrame(out)
    df["body_atmosphere"] = df.body + " / " + df.atmosphere.fillna("none")
    return df[df["T"].notna() & df["g"].notna()].reset_index(drop=True)


def fit(T, g, y):
    return band(T, y), band(g, y)


def dropped(rule, T, g):
    (tl, th, _), (gl, gh, _) = rule
    return (T < tl) | (T > th) | (g < gl) | (g > gh)


def held_out(k, labels):
    """Held-out (right lost, wrong removed) for pooled limits and for limits fitted per part, both halves added."""
    pooled, split = np.zeros(2, int), np.zeros(2, int)
    T, g, y, half = k["T"].to_numpy(), k["g"].to_numpy(), k["y"].to_numpy(), k["half"].to_numpy()
    for h in (0, 1):
        tr, te = half == h, half != h
        base = fit(T[tr], g[tr], y[tr])
        d_pool = dropped(base, T[te], g[te])
        d_split = np.zeros(te.sum(), bool)
        lab_te = labels[te]
        for lab in np.unique(labels):
            ptr, pte = tr & (labels == lab), lab_te == lab
            if y[ptr].sum() >= MIN_PART_RIGHT and (1 - y[ptr]).sum() >= 1:
                rule = fit(T[ptr], g[ptr], y[ptr])
            else:
                rule = base
            d_split[pte] = dropped(rule, T[te][pte], g[te][pte])
        yt = y[te]
        pooled += [int((yt[d_pool] == 1).sum()), int((yt[d_pool] == 0).sum())]
        split += [int((yt[d_split] == 1).sum()), int((yt[d_split] == 0).sum())]
    return pooled, split


def ranges(k, split, parent):
    """Right bodies' temperature and gravity spread in each part, against the parent's."""
    rows = []
    r = k[k.y == 1]
    if len(r) < MIN_RANGE_RIGHT:
        return rows
    pt, pg = np.quantile(r["T"], [0.01, 0.99]), np.quantile(r["g"], [0.01, 0.99])
    for lab, part in r.groupby(split):
        if len(part) < MIN_RANGE_RIGHT:
            continue
        t, g = np.quantile(part["T"], [0.01, 0.99]), np.quantile(part["g"], [0.01, 0.99])
        rows.append({"parent": parent, "split": split, "part": lab, "right": len(part),
                     "T_low": round(t[0], 1), "T_high": round(t[1], 1), "g_low": round(g[0], 4), "g_high": round(g[1], 4),
                     "parent_T_low": round(pt[0], 1), "parent_T_high": round(pt[1], 1),
                     "parent_g_low": round(pg[0], 4), "parent_g_high": round(pg[1], 4),
                     "T_span_share": round((t[1] - t[0]) / max(pt[1] - pt[0], 1e-9), 3),
                     "g_span_share": round((g[1] - g[0]) / max(pg[1] - pg[0], 1e-9), 3)})
    return rows


def evaluate(df):
    split_rows, range_rows = [], []
    for s, sp in df.groupby("species"):
        parents = [("all", sp, ["body", "atmosphere", "body_atmosphere"])]
        parents += [(ba, part, ["mix", "mix_share", "pressure", "volcanism"]) for ba, part in sp.groupby("body_atmosphere")]
        for parent, p, splits in parents:
            k = p[p.y >= 0].copy()
            if k.y.sum() < 2 * MIN_PART_RIGHT or (1 - k.y).sum() < 20:
                continue
            k["pressure"] = pd.qcut(k["P"].rank(method="first"), 3, labels=["low", "middle", "high"]).astype(str) if k["P"].notna().all() else "unknown"
            for split in splits:
                labels = np.array([str(v) for v in k[split].fillna("none")])
                if len(np.unique(labels)) < 2:
                    continue
                range_rows += [{"species": s, **r} for r in ranges(k, split, parent)]
                pooled, part = held_out(k, labels)
                right, wrong = int(k.y.sum()), int((1 - k.y).sum())
                extra_lost, extra_removed = part[0] - pooled[0], part[1] - pooled[1]
                useful = (part[1] >= GAIN * pooled[1] and extra_removed >= MIN_EXTRA and part[0] <= MAX_LOSS * right
                          and extra_removed >= EXTRA_PURITY * (extra_removed + max(extra_lost, 0)))
                split_rows.append({"species": s, "parent": parent, "split": split, "parts": len(np.unique(labels)),
                                   "right": right, "wrong": wrong, "pooled_right_lost": int(pooled[0]), "pooled_wrong_removed": int(pooled[1]),
                                   "split_right_lost": int(part[0]), "split_wrong_removed": int(part[1]),
                                   "extra_right_lost": int(extra_lost), "extra_wrong_removed": int(extra_removed), "useful": useful})
    return pd.DataFrame(split_rows), pd.DataFrame(range_rows)


def figure(path, name, sp, rng):
    """Right and wrong bodies across temperature and gravity, coloured by body type and atmosphere."""
    k = sp[sp.y >= 0]
    k = k.iloc[rng.permutation(len(k))]
    top = k[k.y == 1].body_atmosphere.value_counts().index[: len(SERIES)].tolist()
    colour = {lab: SERIES[i] for i, lab in enumerate(top)}
    tl, th = np.quantile(k["T"], [0.001, 0.999])
    gl, gh = np.quantile(k["g"], [0.001, 0.999])
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.4), sharex=True, sharey=True, constrained_layout=True, facecolor=ref.SURFACE)
    for ax, y, title in ((axes[0], 1, "Right"), (axes[1], 0, "Wrong")):
        part = k[k.y == y]
        c = part.body_atmosphere.map(colour).fillna(OTHER)
        ax.scatter(part["T"], part["g"], s=3, c=c, alpha=0.6, linewidths=0, rasterized=True)
        ax.set_title(f"{title} ({len(part):,})", loc="left", color=ref.INK, fontsize=11)
        ax.set_xlim(tl - (th - tl) * 0.02, th + (th - tl) * 0.02)
        ax.set_ylim(gl - (gh - gl) * 0.02, gh + (gh - gl) * 0.02)
        ax.set_xlabel("Surface temperature (K)")
        ax.spines[["top", "right"]].set_visible(False)
        ax.tick_params(colors=ref.MUTED, labelsize=9)
        ax.grid(color=ref.GRID, lw=0.6)
    axes[0].set_ylabel("Gravity (g)")
    handles = [plt.Line2D([], [], ls="", marker="o", ms=6, color=colour[lab], label=lab) for lab in top]
    handles.append(plt.Line2D([], [], ls="", marker="o", ms=6, color=OTHER, label="Other"))
    fig.legend(handles=handles, loc="outside right upper", frameon=False, fontsize=9, title="Body type / atmosphere")
    fig.suptitle(name, x=0.01, ha="left", color=ref.INK, fontsize=12)
    fig.savefig(path, dpi=110)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--limit-parts", type=int, default=0)
    ap.add_argument("--min-figure-right", type=int, default=200)
    ap.add_argument("--after-limits", action="store_true", help="Leave out what the current temperature and gravity limits drop")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    (args.output / "figures").mkdir(parents=True)

    df = load(args.root, args.limit_parts, args.after_limits)
    print(f"{len(df):,} predictions, {int((df.y >= 0).sum()):,} of known truth", flush=True)
    splits, rngs = evaluate(df)
    splits.to_csv(args.output / "splits.tsv", sep="\t", index=False)
    rngs.to_csv(args.output / "ranges.tsv", sep="\t", index=False)
    rng = np.random.default_rng(0)
    for s, sp in df.groupby("species"):
        if (sp.y == 1).sum() >= args.min_figure_right:
            figure(args.output / "figures" / f"{s.replace(' ', '_')}.png", s, sp, rng)

    pd.set_option("display.width", 260, "display.max_rows", 3000, "display.max_colwidth", 50)
    lines = [f"{splits.species.nunique()} species, {len(splits):,} splits tested, {int(splits.useful.sum())} useful", "",
             f"Useful: held out, split limits remove at least {GAIN}x the pooled limits' wrong and {MIN_EXTRA} more, lose at most "
             f"{MAX_LOSS:.0%} of right, and what they remove beyond the pooled limits is at least {EXTRA_PURITY:.0%} wrong.", ""]
    lines += ["Splits by property (all, useful, extra wrong removed by useful ones):"]
    for split, part in splits.groupby("split"):
        u = part[part.useful]
        lines.append(f"  {split:16} {len(part):>5} {len(u):>4} {int(u.extra_wrong_removed.sum()):>8,}")
    useful = splits[splits.useful].sort_values("extra_wrong_removed", ascending=False)
    lines += ["", "Useful splits:", useful.to_string(index=False) if len(useful) else "none", ""]
    narrow = rngs[(rngs[["T_span_share", "g_span_share"]].min(axis=1) <= 0.5)].sort_values("right", ascending=False)
    lines += ["Parts whose right bodies span half the parent's range or less, on either axis (largest first, top 80):",
              narrow.head(80).to_string(index=False), ""]
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines[:12]))


if __name__ == "__main__":
    main()
