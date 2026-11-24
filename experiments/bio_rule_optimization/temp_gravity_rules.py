"""Test every species for an interaction between surface temperature and gravity.

Each predicted species is split into all its bodies, by body type, by atmosphere, and by both. Three rule shapes
are fitted in each group. What a rule removes must be almost all wrong (MIN_PURITY), so a limit stops at a sharp edge
instead of spending an allowance of right predictions, and it may lose at most 1% of the right ones:
- box: separate limits on temperature and gravity, which needs no interaction
- line: limits on a slanted combination of the two
- pocket: cells of a temperature and gravity grid where wrong predictions pile up
Each is fitted on half the sectors and scored on the other half, in both directions. A rule must hold up in both,
and it shows an interaction only if a line or pocket removes clearly more than the box. Predictions use the orbit
star with the vetoes, but before TEMP_GRAVITY_LIMITS, so it reproduces the fit behind those limits. Bodies of unknown truth are never counted as wrong; they only show how many
predictions a rule would remove. Writes all_groups.tsv, report.txt and a heatmap per species, with a title in figures/
and without one in panels/, plus panels/panels.json with the numbers for captions.
"""

import argparse
import glob
import json
import sys
import zlib
from array import array
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

import species_reference as ref
import srvsurv_engine as engine
from matplotlib.colors import LinearSegmentedColormap
from species_reference import plt

MIN_RIGHT = 10  # in each half
MIN_WRONG = 20
LOSS = 0.01
MIN_PURITY = 0.95  # wrong share of what a rule removes when fitting
CLEAN_MIN_PURITY = 0.90  # on held-out sectors
CLEAN_MAX_LOSS = 0.02
CLEAN_MIN_SHARE = 0.05
CLEAN_MIN_REMOVED = 50
INTERACTION_GAIN = 1.25  # a line or pocket must remove this many times what the box removes
GRID = 16
ANGLES = [a for a in range(15, 180, 15) if a != 90]
SHARE_CMAP = LinearSegmentedColormap.from_list("share", [ref.ABSENT, "#f0efea", ref.PRESENT])


def tail(u, y, budget):
    """Index count of the longest low tail of sorted u that is pure enough and within the allowance; removed wrong."""
    right = np.cumsum(y)
    wrong = np.cumsum(1 - y)
    ok = (right <= budget) & (wrong >= MIN_PURITY * (right + wrong))
    # a tail ends where the next value differs, so a limit can separate it
    ok[:-1] &= u[:-1] < u[1:]
    ok[-1] = False
    if not ok.any():
        return 0, 0
    i = int(np.argmax(wrong * ok))
    return i + 1, int(wrong[i])


def band(u, y):
    """Limits on u, keeping everything between; each side cuts only where the removed part is almost all wrong."""
    order = np.argsort(u)
    us, ys = u[order], y[order]
    budget = LOSS * ys.sum() / 2
    n_lo, r_lo = tail(us, ys, budget)
    n_hi, r_hi = tail(-us[::-1], ys[::-1], budget)
    lo = float((us[n_lo - 1] + us[n_lo]) / 2) if n_lo else -np.inf
    hi = float((us[-n_hi] + us[-n_hi - 1]) / 2) if n_hi else np.inf
    return lo, hi, r_lo + r_hi


def fit_box(T, g, y):
    """Separate limits on temperature and gravity."""
    return band(T, y), band(g, y)


def box_drop(rule, T, g):
    (tl, th, _), (gl, gh, _) = rule
    return (T < tl) | (T > th) | (g < gl) | (g > gh)


def fit_line(T, g, y):
    """Band on the slanted direction that removes the most wrong: a*T + b*g within lo..hi."""
    ts, gs = max(np.std(T), 1e-6), max(np.std(g), 1e-6)
    best = (0.0, 0.0, -np.inf, np.inf, 0)
    for angle in ANGLES:
        a, b = np.cos(np.radians(angle)) / ts, np.sin(np.radians(angle)) / gs
        lo, hi, removed = band(a * T + b * g, y)
        if removed > best[4]:
            best = (a, b, lo, hi, removed)
    return best


def line_drop(rule, T, g):
    u = rule[0] * T + rule[1] * g
    return (u < rule[2]) | (u > rule[3])


def fit_pocket(T, g, y):
    """Grid edges and the cells to drop, most lopsided first, within the allowance on right predictions."""
    te = np.unique(np.quantile(T, np.linspace(0, 1, GRID + 1)))
    ge = np.unique(np.quantile(g, np.linspace(0, 1, GRID + 1)))
    ti, gi = np.clip(np.searchsorted(te, T, side="right") - 1, 0, len(te) - 2), np.clip(np.searchsorted(ge, g, side="right") - 1, 0, len(ge) - 2)
    key = ti * GRID * 2 + gi
    right, wrong = defaultdict(int), defaultdict(int)
    for k, v in zip(key.tolist(), y.tolist()):
        (right if v == 1 else wrong)[k] += 1
    budget = int(LOSS * int(y.sum()))
    cells, used = set(), 0
    for k in sorted(wrong, key=lambda c: -wrong[c] / (right[c] + 1)):
        if wrong[k] >= 5 and wrong[k] >= MIN_PURITY * (wrong[k] + right[k]) and used + right[k] <= budget:
            cells.add(k)
            used += right[k]
    return te, ge, cells


def pocket_drop(rule, T, g):
    te, ge, cells = rule
    ti = np.searchsorted(te, T, side="right") - 1
    gi = np.searchsorted(ge, g, side="right") - 1
    inside = (ti >= 0) & (ti < len(te) - 1) & (gi >= 0) & (gi < len(ge) - 1)
    key = ti * GRID * 2 + gi
    return inside & np.isin(key, list(cells) or [-1])


SHAPES = {"box": (fit_box, box_drop), "line": (fit_line, line_drop), "pocket": (fit_pocket, pocket_drop)}


def box_text(rule):
    (tl, th, _), (gl, gh, _) = rule
    parts = [f"T >= {tl:.0f}" if tl > -np.inf else "", f"T <= {th:.0f}" if th < np.inf else "",
             f"g >= {gl:.4f}" if gl > -np.inf else "", f"g <= {gh:.4f}" if gh < np.inf else ""]
    return ", ".join(p for p in parts if p)


def line_text(rule):
    a, b, lo, hi, removed = rule
    if not removed:
        return ""
    # written as T + k*g within limits
    k = b / a
    lo, hi = sorted((lo / a, hi / a))
    parts = [f">= {lo:.1f}" if lo > -np.inf else "", f"<= {hi:.1f}" if hi < np.inf else ""]
    return f"T {k:+.0f}*g " + " and ".join(p for p in parts if p)


def pocket_text(rule):
    te, ge, cells = rule
    out = []
    for k in sorted(cells):
        ti, gi = k // (GRID * 2), k % (GRID * 2)
        if ti < len(te) - 1 and gi < len(ge) - 1:
            out.append(f"T {te[ti]:.0f}-{te[ti + 1]:.0f}, g {ge[gi]:.4f}-{ge[gi + 1]:.4f}")
    return "; ".join(out)


TEXT = {"box": box_text, "line": line_text, "pocket": pocket_text}


def test_group(a):
    """Row of results for one group; a holds (y, T, g, half), with y -1 for unknown truth."""
    a = a[~np.isnan(a[:, 1]) & ~np.isnan(a[:, 2])]
    unknown = a[a[:, 0] == -1]
    k = a[a[:, 0] != -1]
    y, T, g, half = k[:, 0], k[:, 1], k[:, 2], k[:, 3]
    row = {"right": int(y.sum()), "wrong": int((1 - y).sum()), "unknown": len(unknown)}
    for h in (0, 1):
        if y[half == h].sum() < MIN_RIGHT or (1 - y[half == h]).sum() < MIN_WRONG:
            return {**row, "status": "too few bodies"}
    row["corr_right"] = round(float(np.corrcoef(T[y == 1], g[y == 1])[0, 1]), 3) if np.std(T[y == 1]) and np.std(g[y == 1]) else None
    row["corr_wrong"] = round(float(np.corrcoef(T[y == 0], g[y == 0])[0, 1]), 3) if np.std(T[y == 0]) and np.std(g[y == 0]) else None
    clean = {}
    for shape, (fit, drop) in SHAPES.items():
        lost = removed = 0
        ok = True
        for h in (0, 1):
            tr, te = half == h, half != h
            d = drop(fit(T[tr], g[tr], y[tr]), T[te], g[te])
            n_lost, n_removed = int((y[te][d] == 1).sum()), int((y[te][d] == 0).sum())
            lost, removed = lost + n_lost, removed + n_removed
            ok &= (n_lost <= CLEAN_MAX_LOSS * y[te].sum() and n_removed >= CLEAN_MIN_SHARE * (1 - y[te]).sum()
                   and n_removed >= CLEAN_MIN_REMOVED / 2 and n_removed >= CLEAN_MIN_PURITY * (n_removed + n_lost))
        clean[shape] = ok
        row.update({f"{shape}_right_lost": lost, f"{shape}_wrong_removed": removed, f"{shape}_clean": ok})
        rule = fit(T, g, y)
        row[f"{shape}_rule"] = TEXT[shape](rule)
        row[f"{shape}_unknown_removed"] = int(drop(rule, unknown[:, 1], unknown[:, 2]).sum()) if len(unknown) else 0
    best_box = row["box_wrong_removed"]
    row["interaction"] = any(clean[s] and row[f"{s}_wrong_removed"] >= INTERACTION_GAIN * best_box and row[f"{s}_wrong_removed"] >= best_box + CLEAN_MIN_REMOVED
                             for s in ("line", "pocket"))
    row["status"] = "interaction" if row["interaction"] else ("box rule" if clean["box"] else "no rule")
    return row


def share_curve(values, y, edges, min_bodies=20):
    """Bin centres and right share per bin, blank where a bin has too few known bodies."""
    i = np.clip(np.digitize(values, edges) - 1, 0, len(edges) - 2)
    n = np.bincount(i, minlength=len(edges) - 1)
    right = np.bincount(i, weights=y, minlength=len(edges) - 1)
    share = np.full(len(n), np.nan)
    share[n >= min_bodies] = right[n >= min_bodies] / n[n >= min_bodies]
    return (edges[:-1] + edges[1:]) / 2, share


def figure(path, panel_path, name, a):
    """Heatmap of the right share across temperature and gravity, with the share along each axis and the limits drawn on.

    Saves it with a title at path and without one at panel_path, and returns the numbers in the title.
    """
    a = a[~np.isnan(a[:, 1]) & ~np.isnan(a[:, 2]) & (a[:, 0] >= 0)]
    y, T, g = a[:, 0], a[:, 1], a[:, 2]
    te = np.linspace(*np.quantile(T, [0.001, 0.999]), 41)
    ge = np.linspace(*np.quantile(g, [0.001, 0.999]), 41)
    fig = plt.figure(figsize=(8, 6.2), constrained_layout=True, facecolor=ref.SURFACE)
    grid = fig.add_gridspec(2, 3, width_ratios=[5, 1.15, 0.14], height_ratios=[1.15, 5])
    main_ax = fig.add_subplot(grid[1, 0])
    top = fig.add_subplot(grid[0, 0], sharex=main_ax)
    side = fig.add_subplot(grid[1, 1], sharey=main_ax)
    r, _, _ = np.histogram2d(T[y == 1], g[y == 1], bins=(te, ge))
    w, _, _ = np.histogram2d(T[y == 0], g[y == 0], bins=(te, ge))
    share = np.ma.masked_where(r + w < 5, r / np.maximum(r + w, 1))
    main_ax.set_facecolor("#f7f7f4")
    mesh = main_ax.pcolormesh(te, ge, share.T, cmap=SHARE_CMAP, vmin=0, vmax=1, rasterized=True)
    fig.colorbar(mesh, cax=fig.add_subplot(grid[1, 2]), label="Share of known bodies that are right")
    x, c = share_curve(T, y, te)
    top.plot(x, c, color=ref.INK, lw=1.6)
    top.set_ylim(0, 1.03)
    top.set_ylabel("Right share", color=ref.MUTED, fontsize=9)
    x, c = share_curve(g, y, ge)
    side.plot(c, x, color=ref.INK, lw=1.6)
    side.set_xlim(0, 1.03)
    side.set_xlabel("Right share", color=ref.MUTED, fontsize=9)
    limits = ref.LIMITS.get(name, [])
    drop = np.zeros(len(y), bool)
    for body, atmosphere, t_min, t_max, g_min, g_max in limits:
        if body is not None or atmosphere is not None:
            continue
        for v in (t_min, t_max):
            if v is not None:
                main_ax.axvline(v, color=ref.INK, ls="--", lw=1.2)
                top.axvline(v, color=ref.INK, ls="--", lw=1.2)
                top.annotate(f"{v:.0f} K", (v, 1.0), xytext=(4, -10), textcoords="offset points", fontsize=9, color=ref.INK)
                drop |= T < v if v == t_min else T > v
        for v in (g_min, g_max):
            if v is not None:
                main_ax.axhline(v, color=ref.INK, ls="--", lw=1.2)
                side.axhline(v, color=ref.INK, ls="--", lw=1.2)
                side.annotate(f"{v:.4f} g", (0.02, v), xytext=(0, 3), textcoords="offset points", fontsize=9, color=ref.INK)
                drop |= g < v if v == g_min else g > v
    note = f"{int(y.sum()):,} right, {int((1 - y).sum()):,} wrong. "
    if drop.any():
        note += f"Dashed limits remove {int((y[drop] == 0).sum()):,} wrong and {int((y[drop] == 1).sum()):,} right."
    elif not limits:
        note += "No limit applied."
    if any(x[0] is not None or x[1] is not None for x in limits):
        note += "\nLimits for one body type or atmosphere aren't drawn."
    main_ax.set_xlabel("Surface temperature (K)")
    main_ax.set_ylabel("Gravity (g)")
    for ax in (main_ax, top, side):
        ax.spines[["top", "right"]].set_visible(False)
        ax.tick_params(colors=ref.MUTED, labelsize=9)
    top.tick_params(labelbottom=False)
    side.tick_params(labelleft=False)
    top.grid(color=ref.GRID, lw=0.6)
    side.grid(color=ref.GRID, lw=0.6)
    title = fig.suptitle(f"{name}\n{note}", color=ref.INK, fontsize=11, x=0.02, ha="left")
    fig.savefig(path, dpi=110)
    title.set_visible(False)
    title.set_in_layout(False)
    fig.savefig(panel_path, dpi=110)
    plt.close(fig)
    return {"right": int(y.sum()), "wrong": int((1 - y).sum()), "removed_right": int((y[drop] == 1).sum()),
            "removed_wrong": int((y[drop] == 0).sum()), "any_limit": bool(limits),
            "scoped_limit": any(x[0] is not None or x[1] is not None for x in limits)}


def load_species(root, limit_parts=0):
    """Per species, a list of (body type, atmosphere, rows of truth, temperature, gravity, half); truth -1 is unknown."""
    lm = pd.read_parquet(root / "landmarks/landmarks.parquet", columns=["body_id64", "genus", "species"])
    found_genera, found_species = defaultdict(set), defaultdict(set)
    for b, g, s in zip(lm.body_id64, lm.genus, lm.species):
        found_genera[b].add(g)
        found_species[b].add(s)
    genus_of = dict(zip(lm.species, lm.genus))
    walk = pd.read_parquet(root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "api_genera"])
    api = dict(zip(walk.body_id64, walk.api_genera))
    del walk, lm

    rows = defaultdict(lambda: array("f"))
    cols = ["body_id64", "sector", "subType", "main_age", "gravity", "surfaceTemperature", "atmosphere", "distanceToArrival",
            "orbit_surfaceTemperature", "region", "db_genera", "orbit_predictions"]
    parts = sorted(glob.glob(str(root / "bodies/part_*.parquet")))
    chain = pd.read_parquet(ref.CHAIN_DISTANCES, columns=["body_id64", "chain_ls", "chain_complete"]).drop_duplicates("body_id64")
    chain = chain[chain.chain_complete].set_index("body_id64").chain_ls
    for p in parts[: limit_parts or None]:
        d = pd.read_parquet(p, columns=cols)
        d = d[d.orbit_predictions.notna()].reset_index(drop=True)
        d["chain_or_arrival"] = d.body_id64.map(chain).fillna(d.distanceToArrival)
        vetoed = {s: set(np.flatnonzero(f(d).fillna(False).to_numpy()).tolist()) for s, f in ref.VETOES.items()}
        for i, t in enumerate(d.itertuples(index=False)):
            b = t.body_id64
            a = json.loads(api[b]) if api.get(b) else []
            listed = set(a or ([ref.CODE_GENUS.get(c, c) for c in json.loads(t.db_genera)] if isinstance(t.db_genera, str) and t.db_genera else []))
            half = zlib.crc32(t.sector.encode()) % 2
            for s in {k.split("|")[0] for k in json.loads(t.orbit_predictions)}:
                if s in vetoed and i in vetoed[s]:
                    continue
                parts_of_name = s.split(" ")
                if parts_of_name[0] == "Tussock" and parts_of_name[1] in engine.TUSSOCK_MISSING_REGIONS and t.region == 9:
                    continue
                g = genus_of.get(s) or ref.full_name_genus(s) or parts_of_name[0]
                if s in found_species[b]:
                    y = 1
                elif g in found_genera[b] or (listed and g not in listed):
                    y = 0
                else:
                    y = -1
                rows[s, t.subType, t.atmosphere].extend((y, t.surfaceTemperature, t.gravity, half))
    species = defaultdict(list)
    for key in list(rows):
        species[key[0]].append((key[1], key[2], np.frombuffer(rows.pop(key), dtype=np.float32).reshape(-1, 4).astype(float)))
    return species


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--limit-parts", type=int, default=0)
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    (args.output / "figures").mkdir(parents=True)
    (args.output / "panels").mkdir()

    species = load_species(args.root, args.limit_parts)

    out, panels = [], {}
    for s, pieces in sorted(species.items()):
        groups = {("all", "all"): [p[2] for p in pieces]}
        for body, atm, a in pieces:
            groups.setdefault((body, "all"), []).append(a)
            groups.setdefault(("all", atm), []).append(a)
            groups[body, atm] = [a]
        for (body, atm), arrays in groups.items():
            out.append({"species": s, "body": body, "atmosphere": atm, **test_group(np.concatenate(arrays))})
        everything = np.concatenate(groups["all", "all"])
        if (everything[:, 0] == 1).sum() >= MIN_RIGHT:
            stem = s.replace(" ", "_")
            panels[s] = figure(args.output / "figures" / f"{stem}.png", args.output / "panels" / f"{stem}.png", s, everything)
    (args.output / "panels" / "panels.json").write_text(json.dumps(panels, indent=1))
    df = pd.DataFrame(out)
    df.to_csv(args.output / "all_groups.tsv", sep="\t", index=False)

    pd.set_option("display.width", 260, "display.max_rows", 2000, "display.max_colwidth", 60)
    show = ["species", "body", "atmosphere", "right", "wrong", "unknown", "corr_right", "corr_wrong"]
    def table(frame, shape):
        return frame.sort_values(f"{shape}_wrong_removed", ascending=False)[show + [f"{shape}_right_lost", f"{shape}_wrong_removed", f"{shape}_unknown_removed", f"{shape}_rule"]].to_string(index=False)
    inter = df[df.status == "interaction"]
    lines = [f"{df.species.nunique()} species, {len(df):,} groups tested: " + ", ".join(f"{k} {v:,}" for k, v in df.status.value_counts().items()),
             f"Species with an interaction in some group: {inter.species.nunique()}; with a box rule: {df[df.status == 'box rule'].species.nunique()}", "",
             f"Rules must hold in both directions on held-out sectors: at least {CLEAN_MIN_PURITY:.0%} of what is removed is wrong, at most {CLEAN_MAX_LOSS:.0%} of right lost, at least {CLEAN_MIN_SHARE:.0%} of wrong removed.",
             "Removal counts add both held-out halves; the rule text is refitted on all known bodies.", ""]
    for shape in ("line", "pocket"):
        sel = inter[inter[f"{shape}_clean"].fillna(False).astype(bool)]
        lines += [f"Interaction, {shape}:", table(sel, shape) if len(sel) else "none", ""]
    sel = df[df.status == "box rule"]
    lines += ["Separate limits are enough (box):", table(sel, "box") if len(sel) else "none", ""]
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines[:5]))


if __name__ == "__main__":
    main()
