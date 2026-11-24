"""Per-species reference: prediction criteria tables and distribution figures.

Reads the analysis database, the engine's corrected criteria and per_species.tsv from
score_rule_corrections.py. Writes one PNG per species and SPECIES_REFERENCE.md. Distributions
are P(value | species confirmed) and P(value | predicted by the corrected rules, certainly absent).
"""

import argparse
import glob
import json
import sys
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:  # the mgst environment has no matplotlib; the rule definitions here still work there
    plt = None
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

import srvsurv_engine as engine

CODE_GENUS = {**engine.CODEX_GENUS, "$Codex_Ent_Brancae_Name;": "Brain Tree", "$Codex_Ent_Cone_Name;": "Bark Mounds",
              "$Codex_Ent_Ground_Struct_Ice_Name;": "Crystalline Shards", "$Codex_Ent_Sphere_Name;": "Anemone",
              "$Codex_Ent_Tube_Name;": "Tubers", "$Codex_Ent_Vents_Name;": "Amphora"}
PRESENT, ABSENT, INK, MUTED, GRID, SURFACE = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#e6e5e0", "#fcfcfb"
MAX_ATMOSPHERES = 8
MIN_ABSENT = 30
# Veto tests on database columns, the same limits as engine.SPECIES_VETOES
VETOES = {
    "Clypeus Speculumi": lambda d: d.chain_or_arrival < 2500, "Electricae Radialem": lambda d: d.atmosphere == "Methane",
    "Fonticulua Campestris": lambda d: (d.subType == "Rocky Ice world") & (d.atmosphere == "Argon") & (d.main_age > 10_000),
    "Osseus Discus": lambda d: (d.subType == "High metal content world") & (d.atmosphere == "Water") & (d.main_age < 8_500),
    "Fonticulua Lapida": lambda d: d.gravity > 0.2753, "Frutexa Flabellum": lambda d: d.atmosphere != "Ammonia",
    "Frutexa Metallicum": lambda d: d.gravity > 0.2753, "Fumerola Nitris": lambda d: d.gravity > 0.2743,
    "Stratum Araneamus": lambda d: d.orbit_surfaceTemperature <= 7500, "Stratum Laminamus": lambda d: d.surfaceTemperature <= 165,
    "Tubus Cavas": lambda d: d.gravity > 0.1515, "Tubus Compagibus": lambda d: d.gravity > 0.1515,
    "Tubus Rosarium": lambda d: d.gravity > 0.1515, "Tubus Sororibus": lambda d: d.gravity > 0.1515,
}
VETO_TEXT = {
    "Clypeus Speculumi": "under 2,500 ls from the first star or binary up the parent chain (orbit sizes added up; arrival distance if an orbit on the way isn't on record)", "Electricae Radialem": "Methane atmosphere",
    "Fonticulua Campestris": "on a Rocky Ice body with an Argon atmosphere and a main star older than 10,000 My",
    "Osseus Discus": "on a High metal content body with a Water atmosphere and a main star younger than 8,500 My",
    "Fonticulua Lapida": "gravity > 0.2753 g", "Frutexa Flabellum": "atmosphere other than Ammonia",
    "Frutexa Metallicum": "gravity > 0.2753 g", "Fumerola Nitris": "gravity > 0.2743 g",
    "Stratum Araneamus": "chosen star at or below 7,500 K", "Stratum Laminamus": "surface temperature at or below 165 K",
    "Tubus Cavas": "gravity > 0.1515 g", "Tubus Compagibus": "gravity > 0.1515 g",
    "Tubus Rosarium": "gravity > 0.1515 g", "Tubus Sororibus": "gravity > 0.1515 g",
}
CHAIN_DISTANCES = Path(__file__).parents[2] / "output/speculumi_distance_v2_20261101/stars.parquet"  # from speculumi_distance.py
LIMITS = defaultdict(list)
for _limit in engine.TEMP_GRAVITY_LIMITS:
    LIMITS[f"{_limit[0]} {_limit[1]}"].append(_limit[2:])


def temp_gravity_limited(d, name):
    """True where a temperature and gravity limit drops the species."""
    out = np.zeros(len(d), bool)
    for body, atmosphere, t_min, t_max, g_min, g_max in LIMITS.get(name, []):
        scope = np.ones(len(d), bool)
        if body is not None:
            scope &= (d.subType == body).to_numpy()
        if atmosphere is not None:
            scope &= (d.atmosphere == atmosphere).to_numpy()
        T, g = d.surfaceTemperature.to_numpy(), d.gravity.to_numpy()
        hit = np.zeros(len(d), bool)
        for v, side, x in ((t_min, -1, T), (t_max, 1, T), (g_min, -1, g), (g_max, 1, g)):
            if v is not None:
                hit |= (x < v) if side < 0 else (x > v)
        out |= scope & hit
    return out


def chain_or_arrival(ids, arrival):
    """engine.chain_distance where every orbit is on record, otherwise arrival distance, for predicted Clypeus Speculumi bodies."""
    chain = pd.read_parquet(CHAIN_DISTANCES, columns=["body_id64", "chain_ls", "chain_complete"]).drop_duplicates("body_id64")
    chain = chain[chain.chain_complete].set_index("body_id64").chain_ls
    return ids.map(chain).fillna(arrival)


def limit_mask(d, limit):
    """Rows outside one (body, atmosphere, min K, max K, min g, max g) limit of engine.TEMP_GRAVITY_LIMITS."""
    body, atmosphere, t_min, t_max, g_min, g_max = limit
    t_min, t_max = -np.inf if t_min is None else t_min, np.inf if t_max is None else t_max
    g_min, g_max = -np.inf if g_min is None else g_min, np.inf if g_max is None else g_max
    scope = np.ones(len(d), bool)
    if body is not None:
        scope &= (d.subType == body).to_numpy()
    if atmosphere is not None:
        scope &= (d.atmosphere == atmosphere).to_numpy()
    return scope & ((d.surfaceTemperature < t_min) | (d.surfaceTemperature > t_max) | (d.gravity < g_min) | (d.gravity > g_max)).to_numpy()


def limit_text(limit):
    body, atmosphere, t_min, t_max, g_min, g_max = limit
    parts = [f"below {t_min:g} K" if t_min is not None else "", f"above {t_max:g} K" if t_max is not None else "",
             f"below {g_min:g} g" if g_min is not None else "", f"above {g_max:g} g" if g_max is not None else ""]
    scope = " with ".join(x for x in (body, atmosphere and f"{atmosphere} atmosphere") if x)
    return (f"on {scope}: " if scope else "") + " or ".join(p for p in parts if p)


# Species whose names already hold their genus; the criteria file them under other genus labels
FULL_NAME_GENERA = {"Anemone": "Anemone", "Tubers": "Tubers", "Brain Tree": "Brain Tree", "Bark Mounds": "Bark Mounds",
                    "Crystalline Shards": "Crystalline Shards", "Amphora Plant": "Amphora"}
COLUMNS = ["Body", "Atmosphere", "Composition", "Temp (K)", "Gravity (g)", "Pressure (atm)", "Volcanism", "Regions", "Other"]


def fmt_range(c, scale=1.0):
    lo = "" if c["min"] is None else f"{c['min'] / scale:.4g}"
    hi = "" if c["max"] is None else f"{c['max'] / scale:.4g}"
    return f"{lo} – {hi}" if lo and hi else f"≥ {lo}" if lo else f"≤ {hi}"


def clause_text(c):
    if c["op"] == "range":
        return fmt_range(c)
    if c["op"] == "comp":
        return " or ".join(f"{k} ≥ {v:g}%" for k, v in c["compositions"].items())
    word = {"not": "not ", "all": "all of "}.get(c["op"], "")
    return word + ", ".join(v.strip() for v in c["values"])


def add_clause(row, c, corrected_regions):
    p = c["prop"]
    if p == "gravity":
        row["Gravity (g)"] = fmt_range(c, engine.G_TO_MS2 / 10.0)
    elif p == "pressure":
        row["Pressure (atm)"] = fmt_range(c, engine.ATM_TO_PA / 100_000.0)
    elif p == "temp":
        row["Temp (K)"] = fmt_range(c)
    elif p == "body":
        row["Body"] = clause_text(c)
    elif p in ("atmosType", "atmosphere"):
        row["Atmosphere"] = clause_text(c)
    elif p == "atmosComp":
        row["Composition"] = clause_text(c)
    elif p == "volcanism":
        row["Volcanism"] = clause_text(c)
    elif p == "regions":
        row["Regions"] = ", ".join(map(str, sorted(c["region_ids"])))
    else:
        row["Other"] = (row.get("Other", "") + f" {p}: {clause_text(c)};").strip()


def full_name_genus(species):
    return next((g for suffix, g in FULL_NAME_GENERA.items() if species.endswith(suffix)), None)


def species_rules():
    """{species: (ruleset rows, {colour: clause text})} from the corrected criteria."""
    rules = defaultdict(lambda: ([], {}))

    def walk(node, genus, species, inherited, common):
        common = node["commonChildren"] if node["commonChildren"] is not None else common
        genus, species, variant = node["genus"] or genus, node["species"] or species, node["variant"]
        name = species if species and full_name_genus(species) else f"{genus} {species}"
        if variant:
            if not inherited:  # nothing above the colour: its clauses are the species' presence rules
                row = {}
                for c in node["query"]:
                    add_clause(row, c, None)
                rules[name][0].append(row)
                rules[name][1][variant] = ""
                return
            text = "; ".join(f"{c['prop']}: {clause_text(c)}" for c in node["query"])
            old = rules[name][1].get(variant)
            rules[name][1][variant] = f"{old} / {text}" if old and text != old else text
            return
        clauses = inherited + node["query"]
        kids = (common if node["useCommonChildren"] else node["children"]) or []
        if species and clauses and (not kids or all(k["variant"] for k in kids)):
            row = {}
            for c in clauses:
                add_clause(row, c, None)
            rules[name][0].append(row)
        for k in kids:
            walk(k, genus, species, clauses, common)

    for root in engine.corrected_criteria():
        walk(root, None, None, [], None)
    return rules


def style(ax, title):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=10, color=INK)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(MUTED)
    ax.tick_params(colors=MUTED, labelsize=8, length=0)


def numeric_panel(ax, title, present, absent, unit):
    style(ax, title)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    both = np.concatenate([present, absent]) if len(absent) >= MIN_ABSENT else present
    lo, hi = np.percentile(both, [0.2, 99.8])
    if hi <= lo:
        lo, hi = lo - 0.5 * max(abs(lo), 1e-3), hi + 0.5 * max(abs(hi), 1e-3)
    bins = np.linspace(lo, hi, 36)
    ax.hist(np.clip(present, lo, hi), bins=bins, weights=np.full(len(present), 1 / len(present)),
            color=PRESENT, histtype="stepfilled", alpha=0.35, edgecolor=PRESENT, linewidth=2)
    if len(absent) >= MIN_ABSENT:
        ax.hist(np.clip(absent, lo, hi), bins=bins, weights=np.full(len(absent), 1 / len(absent)),
                color=ABSENT, histtype="step", linewidth=2)
    ax.set_xlabel(unit, fontsize=8, color=MUTED)
    ax.set_ylabel("share of bodies", fontsize=8, color=MUTED)


def atmosphere_panel(ax, present, absent):
    style(ax, "Atmosphere")
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    p = present.value_counts(normalize=True)
    a = absent.value_counts(normalize=True) if len(absent) >= MIN_ABSENT else pd.Series(dtype=float)
    names = list((p.add(a, fill_value=0)).sort_values(ascending=False).index)
    shown = names[:MAX_ATMOSPHERES]
    if len(names) > MAX_ATMOSPHERES:
        rest = names[MAX_ATMOSPHERES:]
        p = pd.concat([p.reindex(shown), pd.Series({"Other": p.reindex(rest).sum()})])
        a = pd.concat([a.reindex(shown), pd.Series({"Other": a.reindex(rest).sum()})])
        shown = shown + ["Other"]
    y = np.arange(len(shown))[::-1]
    h = 0.36 if len(a) else 0.6
    ax.barh(y + (h / 2 + 0.02 if len(a) else 0), p.reindex(shown).fillna(0), height=h, color=PRESENT)
    if len(a):
        ax.barh(y - h / 2 - 0.02, a.reindex(shown).fillna(0), height=h, color=ABSENT)
    ax.set_yticks(y, shown, fontsize=8, color=INK)
    ax.set_xlim(0, 1)
    ax.set_xlabel("share of bodies", fontsize=8, color=MUTED)


def figure(path, name, pres, abse):
    fig, axes = plt.subplots(1, 3, figsize=(11, 2.9), gridspec_kw={"width_ratios": [1, 1, 0.9]}, facecolor=SURFACE)
    numeric_panel(axes[0], "Surface temperature", pres.surfaceTemperature.dropna().to_numpy(), abse.surfaceTemperature.dropna().to_numpy(), "K")
    numeric_panel(axes[1], "Surface gravity", pres.gravity.dropna().to_numpy(), abse.gravity.dropna().to_numpy(), "g")
    atmosphere_panel(axes[2], pres.atmosphere.dropna(), abse.atmosphere.dropna())
    handles = [plt.Rectangle((0, 0), 1, 1, color=PRESENT)]
    labels = [f"Confirmed present ({len(pres):,})"]
    if len(abse) >= MIN_ABSENT:
        handles.append(plt.Rectangle((0, 0), 1, 1, color=ABSENT))
        labels.append(f"Predicted, certainly absent ({len(abse):,})")
    fig.suptitle(name, x=0.01, ha="left", fontsize=12, fontweight="bold", color=INK)
    fig.legend(handles, labels, loc="upper right", ncol=2, frameon=False, fontsize=9, labelcolor=INK)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(path, dpi=100, facecolor=SURFACE)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--per-species", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--heatmaps", type=Path, help="Folder of temperature and gravity heatmaps to link, from temp_gravity_rules.py")
    ap.add_argument("--only", help="Comma-separated species, for a trial run")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    (args.output / "species_figures").mkdir(parents=True)

    cols = ["body_id64", "subType", "main_age", "gravity", "surfaceTemperature", "atmosphere", "distanceToArrival", "orbit_surfaceTemperature",
            "region", "db_genera", "orbit_predictions"]
    df = pd.concat([pd.read_parquet(p, columns=cols) for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet")))],
                   ignore_index=True)
    df = df[df.orbit_predictions.notna()]
    df = df.assign(chain_or_arrival=chain_or_arrival(df.body_id64, df.distanceToArrival)).set_index("body_id64")
    lm = pd.read_parquet(args.root / "landmarks/landmarks.parquet", columns=["body_id64", "genus", "species", "colour"])
    lm = lm[lm.body_id64.isin(df.index)]
    walk = pd.read_parquet(args.root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "api_genera"])
    api = dict(zip(walk.body_id64, walk.api_genera))
    found_genera, present = defaultdict(set), defaultdict(set)
    for b, g, s in zip(lm.body_id64, lm.genus, lm.species):
        found_genera[b].add(g)
        present[s].add(b)
    predicted = defaultdict(list)
    for b, p in zip(df.index, df.orbit_predictions):
        for s in {k.split("|")[0] for k in json.loads(p)}:
            predicted[s].append(b)
    lists = {}
    for b, db in zip(df.index, df.db_genera):
        a = json.loads(api[b]) if api.get(b) else []
        lists[b] = set(a or ([CODE_GENUS.get(c, c) for c in json.loads(db)] if isinstance(db, str) and db else []))
    genus_of = dict(zip(lm.species, lm.genus))
    plain = df.drop(columns=["db_genera", "orbit_predictions"])
    del df, api, walk

    rules = species_rules()
    scores = pd.read_csv(args.per_species, sep="\t").set_index("species")
    colours = lm[lm.colour.notna()].groupby("species").colour.value_counts()
    genus_name = lambda n: genus_of.get(n) or full_name_genus(n) or n.split(" ")[0]
    names = sorted(set(rules) | set(present), key=lambda n: (genus_name(n), n))
    if args.only:
        names = [n for n in names if n in args.only.split(",")]
    lines = ["# Species prediction reference", "",
             "One entry per species in the corrected engine (see [PREDICTION_ALGORITHM.md](PREDICTION_ALGORITHM.md)). "
             "Each ruleset row is one way to qualify; every filled column in the row must hold, and an empty cell means no limit. "
             "Gravity is in Spansh g and pressure in atm, converted from the journal units in SrvSurvey's files. "
             "Regions are numbers from SrvSurvey's region list, with corrections included.", "",
             "The figures show P(value | group), so each group's bars add up to 1: how bodies in that group are spread, "
             "not the chance of finding the species at a value. The blue group is every body with the species confirmed. "
             "The orange group is bodies the corrected rules predict that certainly lack it, shown only with at least "
             f"{MIN_ABSENT} bodies. Values past the plotted range are piled into the end bins. "
             "Both groups come from bodies Spansh has scanned, which may not represent the whole galaxy.", ""]
    genus = None
    for name in names:
        g = genus_name(name)
        if g != genus:
            genus = g
            lines += [f"## {genus}", ""]
        pres = plain.reindex(sorted(present.get(name, ()))).dropna(how="all")
        lines += [f"### {name}", ""]
        short = name.split(" ", 1)[1] if " " in name else name
        if name in VETO_TEXT:
            lines += [f"**Correction:** dropped when {VETO_TEXT[name]}.", ""]
        if name in LIMITS:
            lines += ["**Temperature and gravity limits:** dropped " + "; ".join(limit_text(x) for x in LIMITS[name]) + ".", ""]
        if genus == "Tussock" and short in engine.TUSSOCK_MISSING_REGIONS:
            lines += ["**Correction:** regions 8, 16, 17 and 35 added, region 9 removed.", ""]
        if name in scores.index:
            r = scores.loc[name]
            prec = r.tp / (r.tp + r.fp) if r.tp + r.fp else float("nan")
            rec = r.tp / (r.tp + r.fn) if r.tp + r.fn else float("nan")
            lines += [f"On complete bodies with the final rules: precision {prec:.1%}, recall {rec:.1%} "
                      f"({int(r.tp):,} right, {int(r.fp):,} wrong, {int(r.fn):,} missed).", ""]
        rows, colour_rules = rules.get(name, ([], {}))
        if rows:
            used = [c for c in COLUMNS if any(c in x for x in rows)]
            lines += ["| " + " | ".join(used) + " |", "|" + "---|" * len(used)]
            lines += ["| " + " | ".join(x.get(c, "").replace("|", "/") for c in used) + " |" for x in rows] + [""]
        else:
            lines += ["SrvSurvey has no criteria for this species, so the engine never predicts it.", ""]
        if rows and not colour_rules:
            lines += [("The colour is part of this name, so each colour is listed as its own entry. " if " " in name and name.split(" ")[0] not in ("Bark", "Crystalline", "Amphora")
                       else "The codex has one form of this life form. ")
                      + "Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.", ""]
        if colour_rules:
            counts = colours.get(name) if name in colours.index.get_level_values(0) else None
            lines += ["| Colour | Decided by | Confirmed records |", "|---|---|---|"]
            for v, text in sorted(colour_rules.items()):
                n = int(counts.get(v, 0)) if counts is not None else 0
                lines.append(f"| {v} | {text.replace('|', '/') or 'always'} | {n:,} |")
            lines.append("")
        if len(pres):
            keep = np.array(sorted(predicted.get(name, ())), dtype=np.int64)
            d = plain.loc[keep] if len(keep) else plain.iloc[:0]
            if name in VETOES and len(d):
                d = d[~VETOES[name](d).fillna(False).to_numpy()]
            for limit in LIMITS.get(name, ()):
                d = d[~limit_mask(d, limit)]
            if genus == "Tussock" and short in engine.TUSSOCK_MISSING_REGIONS:
                d = d[d.region != 9]
            gname = genus_of[name]
            known = np.fromiter((b not in present[name] and (gname in found_genera[b] or (bool(lists[b]) and gname not in lists[b]))
                                 for b in d.index), bool, len(d))
            file = f"{name.replace(' ', '_')}.png"
            figure(args.output / "species_figures" / file, name, pres, d[known])
            lines += [f"![Temperature, gravity and atmosphere distributions for {name}](species_figures/{file})", ""]
            if args.heatmaps and (args.heatmaps / file).exists():
                lines += [f"![Right share across temperature and gravity for {name}]({args.heatmaps.name}/{file})", ""]
        else:
            lines += ["No confirmed records on database bodies, so no figure.", ""]
    (args.output / "SPECIES_REFERENCE.md").write_text("\n".join(lines) + "\n")
    print(f"{len(names)} species -> {args.output}")


if __name__ == "__main__":
    main()
