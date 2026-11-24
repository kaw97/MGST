"""Decision trees for species and colour, scored against SrvSurvey's rules.

Reads the analysis database from build_bio_analysis_db.py. Three levels of tree, using
properties known before landing and no position (x, y, z, region, nebula distance):

- genus present, on every body with a genus list (a missing genus is a certain absence)
- species given genus, on bodies where the genus was found and its species confirmed
- colour given species, on bodies with one confirmed colour

All scores are out of fold, with folds grouped by sector. Methods are compared on complete
bodies (every genus on the body has a confirmed species), so a false positive is certain.
All bodies here have at least one biological signal: nothing says whether a body has life at all.

Rule proposals, per species, for the review list:
- veto: tree leaves on bodies SrvSurvey predicts the species for, where it is almost never there
- widen: tree leaves on bodies SrvSurvey does not predict it for, where it usually is there
- colour: species whose colour a tree gets right more often than SrvSurvey does
Each is checked on held-out sectors and on bodies far from Sol after fitting on near ones.
"""

import argparse
import glob
import json
import multiprocessing as mp
import os
import zlib
from pathlib import Path

os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score
from sklearn.tree import DecisionTreeClassifier

CODE_GENUS = {
    "$Codex_Ent_Aleoids_Genus_Name;": "Aleoida", "$Codex_Ent_Bacterial_Genus_Name;": "Bacterium",
    "$Codex_Ent_Brancae_Name;": "Brain Tree", "$Codex_Ent_Cactoid_Genus_Name;": "Cactoida",
    "$Codex_Ent_Clypeus_Genus_Name;": "Clypeus", "$Codex_Ent_Conchas_Genus_Name;": "Concha",
    "$Codex_Ent_Cone_Name;": "Bark Mounds", "$Codex_Ent_Electricae_Genus_Name;": "Electricae",
    "$Codex_Ent_Fonticulus_Genus_Name;": "Fonticulua", "$Codex_Ent_Fumerolas_Genus_Name;": "Fumerola",
    "$Codex_Ent_Fungoids_Genus_Name;": "Fungoida", "$Codex_Ent_Ground_Struct_Ice_Name;": "Crystalline Shards",
    "$Codex_Ent_Osseus_Genus_Name;": "Osseus", "$Codex_Ent_Recepta_Genus_Name;": "Recepta",
    "$Codex_Ent_Shrubs_Genus_Name;": "Frutexa", "$Codex_Ent_Sphere_Name;": "Anemone",
    "$Codex_Ent_Stratum_Genus_Name;": "Stratum", "$Codex_Ent_Tube_Name;": "Tubers",
    "$Codex_Ent_Tubus_Genus_Name;": "Tubus", "$Codex_Ent_Tussocks_Genus_Name;": "Tussock",
    "$Codex_Ent_Vents_Name;": "Amphora",
}
NON_FEATURES = {
    "body_id64", "system_id64", "name", "sector", "body_type", "landable", "bio_signals", "geo_signals", "db_genera",
    "has_prediction", "x", "y", "z", "nebula_distance", "region", "srv_predictions", "orbit_predictions",
    "srv_star_id", "orbit_star_id", "orbit_share", "n_star_candidates", "srv_star_class", "orbit_star_class",
}
ONE_HOT = ["atmosphere", "volcanism", "subType", "terraformingState", "star_class", "star_luminosity_class", "main_class",
           "main_luminosity_class"]
FOLDS = 5
FAR_LY = 10_000
VETO_RIGHT = 0.02    # a veto leaf may hold at most this share of right predictions
WIDEN_RATE = 0.5     # a widen leaf must have the species on at least this share of bodies
MIN_LEAF_RULE = 100
MAX_TRAIN = 1_000_000  # tree fits use at most this many bodies, to bound each worker's memory
D = {}               # shared arrays, set in the parent before forking


def load(root, star, limit_parts):
    parts = sorted(glob.glob(str(root / "bodies/part_*.parquet")))
    if limit_parts:
        parts = parts[:limit_parts]
    df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    df = df[df.has_prediction].reset_index(drop=True)
    other = "srv" if star == "orbit" else "orbit"
    df = df.drop(columns=[c for c in df.columns if c.startswith(f"{other}_") and c not in
                          (f"{other}_predictions", f"{other}_star_id", f"{other}_star_class")])
    df = df.rename(columns={c: "star_" + c[len(star) + 1:] for c in df.columns if c.startswith(f"{star}_")
                            and c not in (f"{star}_predictions", f"{star}_star_id", f"{star}_star_class")})
    df["star_class"] = df[f"{star}_star_class"]
    return df


def feature_matrix(df):
    num = [c for c in df.columns if c not in NON_FEATURES and c not in ONE_HOT
           and not c.startswith(("srv_", "orbit_")) and pd.api.types.is_numeric_dtype(df[c])]
    num += [c for c in ("is_moon", "tidally_locked") if c not in num]
    X, names = [], []
    for c in num:
        X.append(pd.to_numeric(df[c], errors="coerce").astype("float32").fillna(-1).to_numpy())
        names.append(c)
    for c in ONE_HOT:
        s = df[c].astype(str)
        for v, n in s.value_counts().items():
            if n >= 200:
                X.append((s == v).to_numpy(dtype="float32"))
                names.append(f"{c}={v}")
    return np.stack(X, axis=1), names


def tree_rules(model, names, nodes=None):
    """{leaf node: readable conditions}."""
    t = model.tree_
    out = {}
    def walk(node, conds):
        if t.children_left[node] == -1:
            out[node] = " and ".join(conds) or "all"
            return
        f, thr = names[t.feature[node]], t.threshold[node]
        if "=" in f:
            walk(t.children_left[node], conds + [f.replace("=", " != ")])
            walk(t.children_right[node], conds + [f.replace("=", " = ")])
        else:
            walk(t.children_left[node], conds + [f"{f} <= {thr:.4g}"])
            walk(t.children_right[node], conds + [f"{f} > {thr:.4g}"])
    walk(0, [])
    return out


def cap(mask, seed=0):
    """Random subset of a boolean mask, at most MAX_TRAIN rows."""
    idx = np.flatnonzero(mask)
    if len(idx) > MAX_TRAIN:
        idx = np.sort(np.random.default_rng(seed).choice(idx, MAX_TRAIN, replace=False))
    out = np.zeros(len(mask), bool)
    out[idx] = True
    return out


# Each task returns plain arrays so it can run in a forked worker
def task_genus(args):
    g, fold, with_signals = args
    gi = D["genera"].index(g)
    train = cap(D["has_list"] & (D["fold"] != fold))
    test = np.flatnonzero(D["has_list"] & (D["fold"] == fold))
    y = D["genus_present"][:, gi]
    if not y[train].any() or y[train].all():
        return ("genus", g, fold, with_signals, test, np.full(len(test), float(y[train].mean() if train.any() else 0), "float32"))
    def matrix(rows):
        return np.column_stack([D["X"][rows], D["signals"][rows]]) if with_signals else D["X"][rows]
    m = DecisionTreeClassifier(max_depth=8, min_samples_leaf=200, random_state=0).fit(matrix(train), y[train])
    return ("genus", g, fold, with_signals, test, m.predict_proba(matrix(test))[:, 1].astype("float32"))


def task_species(args):
    g, fold = args
    idx = D["genus_species"][g]
    cols = D["species_idx"][g]
    y = D["species_label"][g]
    has = y >= 0
    train = cap(has & (D["fold"] != fold))
    test = np.flatnonzero(D["complete"] & (D["fold"] == fold))
    p = np.zeros((len(test), len(cols)), "float32")
    if train.any() and len(test):
        m = DecisionTreeClassifier(max_depth=10, min_samples_leaf=50, random_state=0).fit(D["X"][train], y[train])
        p[:, m.classes_.astype(int)] = m.predict_proba(D["X"][test])
    return ("species", g, fold, test, p)


def task_colour(args):
    si, fold = args
    y = D["colour"][:, si]
    train = cap((y >= 0) & (D["fold"] != fold))
    records = np.flatnonzero((y >= 0) & (D["fold"] == fold))
    complete = np.flatnonzero(D["complete"] & (D["fold"] == fold))
    if not train.any():
        return ("colour", si, fold, records, np.full(len(records), -1, "int16"), complete, np.full(len(complete), -1, "int16"))
    m = DecisionTreeClassifier(max_depth=8, min_samples_leaf=20, random_state=0).fit(D["X"][train], y[train])
    def predict(rows):
        return m.predict(D["X"][rows]).astype("int16") if len(rows) else np.zeros(0, "int16")
    return ("colour", si, fold, records, predict(records), complete, predict(complete))


def fit_rule_tree(train, y, depth):
    n = int(train.sum())
    return DecisionTreeClassifier(max_depth=depth, min_samples_leaf=max(MIN_LEAF_RULE, n // 200), random_state=0).fit(D["X"][train], y[train])


def veto_leaves(model, train, y):
    leaf = model.apply(D["X"][train])
    out = []
    for node in np.unique(leaf):
        a = leaf == node
        if y[train][a].mean() <= VETO_RIGHT:
            out.append(int(node))
    return out


def task_veto(args):
    """Veto trees on bodies SrvSurvey predicts the species for and whose truth is known."""
    si, fold = args
    pop = D["pred"][:, si] & D["known"][:, si]
    y = D["present"][:, si]
    if fold == "far":
        train, test = pop & ~D["far"], pop & D["far"]
    elif fold == "full":
        train, test = pop, np.zeros(len(pop), bool)
    else:
        train, test = pop & (D["fold"] != fold), pop & (D["fold"] == fold)
    train = cap(train)
    if train.sum() < 2 * MIN_LEAF_RULE or y[train].all() or not y[train].any():
        return ("veto", si, fold, np.zeros(0, int), 0, 0, [])
    m = fit_rule_tree(train, y, 4)
    nodes = veto_leaves(m, train, y)
    rules = []
    flagged = np.zeros(0, int)
    wrong = right = 0
    if nodes:
        idx = np.flatnonzero(test)
        if len(idx):
            hit = np.isin(m.apply(D["X"][idx]), nodes)
            wrong, right = int((~y[idx][hit]).sum()), int(y[idx][hit].sum())
        if fold in range(FOLDS):
            c = np.flatnonzero(D["complete"] & (D["fold"] == fold) & D["pred"][:, si])
            flagged = c[np.isin(m.apply(D["X"][c]), nodes)] if len(c) else flagged
        if fold == "full":
            names = tree_rules(m, D["names"])
            leaf = m.apply(D["X"][train])
            rules = [dict(rule=names[n], bodies=int((leaf == n).sum()), present=int(y[train][leaf == n].sum())) for n in nodes]
    return ("veto", si, fold, flagged, wrong, right, rules)


def task_widen(args):
    """Widening trees on bodies with the genus found and species known where SrvSurvey says no."""
    si, fold = args
    gi = D["species_genus"][si]
    pop = ~D["pred"][:, si] & D["genus_resolved"][:, gi]
    y = D["present"][:, si]
    if fold == "far":
        train, test = pop & ~D["far"], pop & D["far"]
    elif fold == "full":
        train, test = pop, np.zeros(len(pop), bool)
    else:
        train, test = pop & (D["fold"] != fold), pop & (D["fold"] == fold)
    train = cap(train)
    if train.sum() < 2 * MIN_LEAF_RULE or y[train].sum() < MIN_LEAF_RULE:
        return ("widen", si, fold, 0, 0, [])
    m = fit_rule_tree(train, y, 4)
    leaf = m.apply(D["X"][train])
    nodes = [int(n) for n in np.unique(leaf) if y[train][leaf == n].mean() >= WIDEN_RATE]
    right = wrong = 0
    rules = []
    if nodes:
        idx = np.flatnonzero(test)
        if len(idx):
            hit = np.isin(m.apply(D["X"][idx]), nodes)
            right, wrong = int(y[idx][hit].sum()), int((~y[idx][hit]).sum())
        if fold == "full":
            names = tree_rules(m, D["names"])
            rules = [dict(rule=names[n], bodies=int((leaf == n).sum()), present=int(y[train][leaf == n].sum())) for n in nodes]
    return ("widen", si, fold, right, wrong, rules)


def task_colour_rule(si):
    """Shallow colour tree on all records, for readable splits."""
    y = D["colour"][:, si]
    train = y >= 0
    if train.sum() < 200 or len(np.unique(y[train])) < 2:
        return ("colour_rule", si, "")
    m = DecisionTreeClassifier(max_depth=2, min_samples_leaf=20, random_state=0).fit(D["X"][train], y[train])
    return ("colour_rule", si, "; ".join(sorted({D["names"][f] for f in m.tree_.feature if f >= 0})))


def run(pool, fn, tasks, label):
    out = []
    for i, r in enumerate(pool.imap_unordered(fn, tasks, chunksize=1)):
        out.append(r)
        if (i + 1) % max(len(tasks) // 5, 1) == 0:
            print(f"  {label}: {i + 1}/{len(tasks)}", flush=True)
    return out


def set_score(truth, pred):
    tp = fp = fn = 0
    for t, p in zip(truth, pred):
        tp += len(t & p); fp += len(p - t); fn += len(t - p)
    return dict(p=tp / max(tp + fp, 1), r=tp / max(tp + fn, 1), tp=tp, fp=fp, fn=fn)


def score_row(label, truth_c, pred_c):
    truth_s = [{x.split("|")[0] for x in t} for t in truth_c]
    pred_s = [{x.split("|")[0] for x in p} for p in pred_c]
    s, c = set_score(truth_s, pred_s), set_score(truth_c, pred_c)
    return f"  {label:42} {s['p']:7.2%} {s['r']:7.2%}  {c['p']:7.2%} {c['r']:7.2%}  {s['fp']:>9,} {s['fn']:>7,}"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--star", choices=["srv", "orbit"], required=True, help="Star used for tree features and the baseline")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--limit-parts", type=int, default=0)
    args = ap.parse_args()
    if args.output.exists():
        raise SystemExit(f"Output directory already exists: {args.output}")

    df = load(args.root, args.star, args.limit_parts)
    n = len(df)
    print(f"{n:,} bodies", flush=True)
    lm = pd.read_parquet(args.root / "landmarks/landmarks.parquet", columns=["body_id64", "genus", "species", "colour"])
    walk = pd.read_parquet(args.root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "api_genera"])
    pos = pd.Series(np.arange(n), index=df.body_id64)
    lm = lm[lm.body_id64.isin(pos.index)].copy()
    lm["row"] = pos[lm.body_id64].to_numpy()

    # Species, genera and colour targets, from what the rules can predict
    base_col, other = f"{args.star}_predictions", "orbit_predictions" if args.star == "srv" else "srv_predictions"
    cache = {}   # identical prediction lists share one set
    def parse(s):
        return cache.setdefault(s, set(json.loads(s)))
    base_preds = [parse(s) for s in df[base_col]]
    other_preds = [parse(s) for s in df[other]]
    keys = sorted(set().union(*base_preds, *other_preds))
    species = sorted({k.split("|")[0] for k in keys})
    si = {s: i for i, s in enumerate(species)}
    lm_genus = lm.drop_duplicates("species").set_index("species").genus.to_dict()
    genus_of = {s: lm_genus.get(s, s.split(" ")[0]) for s in species}
    genera = sorted(set(genus_of.values()))
    gi = {g: i for i, g in enumerate(genera)}

    # Genus lists: Spansh's when it has one, otherwise the database's
    api = dict(zip(walk.body_id64, walk.api_genera))
    lists = []
    for bid, db in zip(df.body_id64, df.db_genera):
        a = api.get(bid)
        got = json.loads(a) if a else []
        if not got and db:
            got = [CODE_GENUS.get(c, c) for c in json.loads(db)]
        lists.append(set(got))
    has_list = np.array([bool(x) for x in lists])
    found = [set() for _ in range(n)]
    for r, g in zip(lm.row, lm.genus):
        found[r].add(g)
    complete = np.array([bool(l) and l <= f for l, f in zip(lists, found)])

    genus_present = np.zeros((n, len(genera)), bool)
    genus_resolved = np.zeros((n, len(genera)), bool)   # genus found with its species confirmed
    for r, l in enumerate(lists):
        for g in l:
            if g in gi:
                genus_present[r, gi[g]] = True
    present = np.zeros((n, len(species)), bool)
    colour = np.full((n, len(species)), -1, "int16")
    colours = {s: sorted({k.split("|")[1] for k in keys if k.startswith(s + "|")}) for s in species}
    truth = [set() for _ in range(n)]
    records = lm[lm.species.isin(si)]
    for r, s, g, c in zip(records.row, records.species, records.genus, records.colour):
        present[r, si[s]] = True
        genus_resolved[r, gi[g]] = True
        if c is not None:
            truth[r].add(f"{s}|{c}")
            if c in colours[s]:
                colour[r, si[s]] = colours[s].index(c)
    species_genus = np.array([gi[genus_of[s]] for s in species])
    # A species' truth is known if its genus is absent from a list, or the genus' species is confirmed
    known = genus_resolved[:, species_genus] | (has_list[:, None] & ~genus_present[:, species_genus])
    pred = np.zeros((n, len(species)), bool)
    for r, p in enumerate(base_preds):
        for k in p:
            pred[r, si[k.split("|")[0]]] = True

    X, names = feature_matrix(df)
    fold = np.array([zlib.crc32(s.encode()) % FOLDS for s in df.sector])
    far = np.sqrt(df.x.to_numpy() ** 2 + df.y.to_numpy() ** 2 + df.z.to_numpy() ** 2) > FAR_LY
    print(f"{X.shape[1]} features, {has_list.sum():,} with a genus list, {complete.sum():,} complete, "
          f"{far.sum():,} over {FAR_LY:,} ly, fold sizes {np.bincount(fold).tolist()}", flush=True)

    species_label, genus_species, species_idx = {}, {}, {}
    for g in genera:
        cols = [si[s] for s in species if genus_of[s] == g]
        label = np.full(n, -1, "int16")
        for j, c in enumerate(cols):
            label[present[:, c]] = j
        species_label[g], species_idx[g], genus_species[g] = label, cols, [species[c] for c in cols]
    D.update(X=X, signals=df.bio_signals.fillna(-1).to_numpy("float32"), names=names, fold=fold, far=far,
             has_list=has_list, complete=complete, genus_present=genus_present, genus_resolved=genus_resolved,
             genera=genera, present=present, known=known, pred=pred, colour=colour, species_genus=species_genus,
             species_label=species_label, species_idx=species_idx, genus_species=genus_species)

    del df, lists, found
    ctx = mp.get_context("fork")
    with ctx.Pool(args.workers, maxtasksperchild=20) as pool:
        r_genus = run(pool, task_genus, [(g, f, w) for g in genera for f in range(FOLDS) for w in (False, True)], "genus")
        r_species = run(pool, task_species, [(g, f) for g in genera for f in range(FOLDS)], "species")
        r_colour = run(pool, task_colour, [(s, f) for s in range(len(species)) for f in range(FOLDS)], "colour")
        rule_folds = list(range(FOLDS)) + ["far", "full"]
        r_veto = run(pool, task_veto, [(s, f) for s in range(len(species)) for f in rule_folds], "veto")
        r_widen = run(pool, task_widen, [(s, f) for s in range(len(species)) for f in rule_folds], "widen")
        r_rule = run(pool, task_colour_rule, list(range(len(species))), "colour rules")

    # Assemble out-of-fold predictions
    pg = {w: np.full((n, len(genera)), np.nan, "float32") for w in (False, True)}
    for _, g, f, w, test, p in r_genus:
        pg[w][test, gi[g]] = p
    comp_idx = np.flatnonzero(complete)
    comp_row = {r: i for i, r in enumerate(comp_idx)}
    ps = np.zeros((len(comp_idx), len(species)), "float32")
    for _, g, f, test, p in r_species:
        rows = np.array([comp_row[r] for r in test], int)
        for j, c in enumerate(species_idx[g]):
            ps[rows, c] = p[:, j]
    colour_oof = np.full((n, len(species)), -1, "int16")
    colour_comp = np.full((len(comp_idx), len(species)), -1, "int16")
    for _, s, f, rec, p, comp, q in r_colour:
        colour_oof[rec, s] = p
        colour_comp[[comp_row[r] for r in comp], s] = q
    vetoed = np.zeros((n, len(species)), bool)
    veto_cv = {s: [0, 0] for s in range(len(species))}
    veto_far, veto_rules = {}, {}
    for _, s, f, flagged, wrong, right, rules in r_veto:
        if f in range(FOLDS):
            vetoed[flagged, s] = True
            veto_cv[s][0] += wrong; veto_cv[s][1] += right
        elif f == "far":
            veto_far[s] = (wrong, right)
        else:
            veto_rules[s] = rules
    widen_cv = {s: [0, 0] for s in range(len(species))}
    widen_far, widen_rules = {}, {}
    for _, s, f, right, wrong, rules in r_widen:
        if f in range(FOLDS):
            widen_cv[s][0] += right; widen_cv[s][1] += wrong
        elif f == "far":
            widen_far[s] = (right, wrong)
        else:
            widen_rules[s] = rules
    colour_splits = {s: t for _, s, t in r_rule}

    # Method table on complete bodies
    truth_c = [truth[r] for r in comp_idx]
    base_c = [base_preds[r] for r in comp_idx]
    other_c = [other_preds[r] for r in comp_idx]
    label_base = "SrvSurvey rules, " + ("orbit star" if args.star == "orbit" else "SrvSurvey star")
    label_other = "SrvSurvey rules, " + ("SrvSurvey star" if args.star == "orbit" else "orbit star")
    veto_c = [{k for k in p if not vetoed[r, si[k.split("|")[0]]]} for p, r in zip(base_c, comp_idx)]

    p_all = pg[False][comp_idx][:, species_genus] * ps
    p_sig = pg[True][comp_idx][:, species_genus] * ps
    pres_c = present[comp_idx]
    base_recall = (pres_c & pred[comp_idx]).sum() / max(pres_c.sum(), 1)
    def threshold_for(p, recall):
        vals = np.sort(p[pres_c])
        return float(vals[int((1 - recall) * len(vals))]) if len(vals) else 0.5
    def tree_preds(p, t):
        out = []
        for i, row in enumerate(p):
            s = set()
            for c in np.flatnonzero(row >= t):
                v = colour_comp[i, c]
                if v >= 0 and colours[species[c]]:
                    s.add(f"{species[c]}|{colours[species[c]][v]}")
            out.append(s)
        return out

    lines = ["# Species and colour decision trees vs SrvSurvey", "",
             f"Star for features and baseline: {args.star}. {n:,} bodies with predictions, {has_list.sum():,} with a genus list, "
             f"{complete.sum():,} complete.", "All scores out of fold, grouped by sector. Every body here has at least one biological signal.",
             "", f"## Methods on complete bodies ({len(comp_idx):,})",
             f"  {'method':42} {'sp P':>7} {'sp R':>7}  {'col P':>7} {'col R':>7}  {'sp FP':>9} {'sp FN':>7}"]
    lines.append(score_row(label_base, truth_c, base_c))
    lines.append(score_row(label_other, truth_c, other_c))
    lines.append(score_row(label_base.replace("rules,", "+ tree vetoes,"), truth_c, veto_c))
    t = threshold_for(p_all, base_recall)
    lines.append(score_row(f"trees alone, same recall (p>={t:.3f})", truth_c, tree_preds(p_all, t)))
    lines.append(score_row("trees alone, p>=0.5", truth_c, tree_preds(p_all, 0.5)))
    t2 = threshold_for(p_sig, base_recall)
    lines.append(score_row(f"trees + FSS signal count, same recall (p>={t2:.3f})", truth_c, tree_preds(p_sig, t2)))
    lines.append(score_row("trees + FSS signal count, p>=0.5", truth_c, tree_preds(p_sig, 0.5)))

    # Genus level over all bodies with a list
    lines += ["", "## Genus presence on all bodies with a list (lower selection bias than complete bodies)",
              f"  {'genus':12} {'bodies':>9} {'rate':>6} | {'SrvSurvey P':>11} {'R':>7} | {'tree AUC':>8} {'P at same R':>11} | {'+signals AUC':>12} {'P at same R':>11}"]
    genus_rows = []
    pred_g = np.zeros((n, len(genera)), bool)
    for c, g in enumerate(species_genus):
        pred_g[:, g] |= pred[:, c]
    for g in genera:
        j = gi[g]
        y, sp = genus_present[has_list, j], pred_g[has_list, j]
        if not y.any() or y.all():
            continue
        prec, rec = y[sp].mean() if sp.any() else 0.0, sp[y].mean()
        def at_recall(p):
            vals = np.sort(p[y])
            th = vals[int((1 - rec) * (len(vals) - 1))]
            return y[p >= th].mean()
        p0, p1 = pg[False][has_list, j], pg[True][has_list, j]
        lines.append(f"  {g:12} {has_list.sum():>9,} {y.mean():6.1%} | {prec:11.2%} {rec:7.2%} | {roc_auc_score(y, p0):8.3f} {at_recall(p0):11.2%} | "
                     f"{roc_auc_score(y, p1):12.3f} {at_recall(p1):11.2%}")
        genus_rows.append(dict(genus=g, rate=y.mean(), srv_precision=prec, srv_recall=rec, tree_auc=roc_auc_score(y, p0),
                               tree_precision_same_recall=at_recall(p0), signals_auc=roc_auc_score(y, p1),
                               signals_precision_same_recall=at_recall(p1)))

    # Per species
    rows = []
    for c, s in enumerate(species):
        y, sp, pr = pres_c[:, c], pred[comp_idx, c], p_all[:, c]
        tp, fp, fn = int((y & sp).sum()), int((~y & sp).sum()), int((y & ~sp).sum())
        rec = tp / max(tp + fn, 1)
        tree_p = np.nan
        if y.any() and rec > 0:
            vals = np.sort(pr[y])
            th = vals[int((1 - rec) * (len(vals) - 1))]
            tree_p = y[pr >= th].mean()
        col = colour[:, c] >= 0
        p_rows = np.flatnonzero(col)
        listed = single = pick = 0
        for r in p_rows:
            k = f"{s}|{colours[s][colour[r, c]]}"
            cand = [x for x in base_preds[r] if x.startswith(s + "|")]
            listed += k in cand
            single += cand == [k]
            pick += (k in cand) / max(len(cand), 1)
        n_col = len(p_rows)
        rows.append(dict(
            species=s, genus=genus_of[s], complete_tp=tp, complete_fp=fp, complete_fn=fn,
            srv_precision=tp / max(tp + fp, 1), srv_recall=rec, tree_precision_same_recall=tree_p,
            n_colours=len(colours[s]), colour_records=n_col, colour_srv_listed=listed / max(n_col, 1),
            colour_srv_single=single / max(n_col, 1), colour_srv_pick=pick / max(n_col, 1),
            colour_tree=(colour_oof[p_rows, c] == colour[p_rows, c]).mean() if n_col else np.nan,
            colour_tree_features=colour_splits.get(c, ""),
            veto_wrong=veto_cv[c][0], veto_right=veto_cv[c][1],
            veto_far_wrong=veto_far.get(c, (0, 0))[0], veto_far_right=veto_far.get(c, (0, 0))[1],
            widen_right=widen_cv[c][0], widen_wrong=widen_cv[c][1],
            widen_far_right=widen_far.get(c, (0, 0))[0], widen_far_wrong=widen_far.get(c, (0, 0))[1],
            veto_rules=json.dumps(veto_rules.get(c, [])), widen_rules=json.dumps(widen_rules.get(c, [])),
        ))
    table = pd.DataFrame(rows)
    table["veto_score"] = table.veto_wrong - 20 * table.veto_right
    table["widen_score"] = table.widen_right - table.widen_wrong
    table["colour_gain"] = table.colour_tree - table.colour_srv_pick   # pick = guessing one colour from SrvSurvey's list

    def rule_text(js):
        return " || ".join(f"{x['rule']} [{x['present']}/{x['bodies']}]" for x in json.loads(js))[:900]

    lines += ["", "## Easy wins: vetoes (held-out wrong predictions removed; right ones lost; then far-from-Sol test)"]
    for _, r in table[(table.veto_wrong > 0) & (table.veto_rules != "[]")].sort_values("veto_score", ascending=False).head(25).iterrows():
        lines.append(f"  {r.species:26} wrong {r.veto_wrong:>8,} right {r.veto_right:>6,} | far wrong {r.veto_far_wrong:>7,} right {r.veto_far_right:>5,} | "
                     f"species precision {r.srv_precision:.1%}\n      {rule_text(r.veto_rules)}")
    lines += ["", "## Easy wins: widening (held-out bodies SrvSurvey misses; bodies wrongly added; then far-from-Sol test)"]
    for _, r in table[(table.widen_right > 0) & (table.widen_rules != "[]")].sort_values("widen_score", ascending=False).head(25).iterrows():
        lines.append(f"  {r.species:26} found {r.widen_right:>7,} wrong {r.widen_wrong:>7,} | far found {r.widen_far_right:>6,} wrong {r.widen_far_wrong:>6,} | "
                     f"species recall {r.srv_recall:.1%}\n      {rule_text(r.widen_rules)}")
    lines += ["", "## Easy wins: colour (tree vs guessing one colour from SrvSurvey's list; 'listed' = right colour is on the list)"]
    for _, r in table[(table.colour_records >= 300) & (table.n_colours > 1)].sort_values("colour_gain", ascending=False).head(25).iterrows():
        lines.append(f"  {r.species:26} records {r.colour_records:>7,} SrvSurvey pick {r.colour_srv_pick:6.1%} listed {r.colour_srv_listed:6.1%} tree {r.colour_tree:6.1%}  "
                     f"[{r.colour_tree_features}]")
    lines += ["", "## Worst SrvSurvey species precision against tree at the same recall (complete bodies)"]
    t3 = table[table.complete_tp >= 100].assign(gap=lambda x: x.tree_precision_same_recall - x.srv_precision)
    for _, r in t3.sort_values("gap", ascending=False).head(25).iterrows():
        lines.append(f"  {r.species:26} SrvSurvey P {r.srv_precision:6.1%} R {r.srv_recall:6.1%}  tree P {r.tree_precision_same_recall:6.1%}  (TP {r.complete_tp:,}, FP {r.complete_fp:,}, FN {r.complete_fn:,})")

    # Colour coverage of every target
    counts = records[records.colour.notna()].groupby(["species", "colour"]).size()
    cov = []
    for s in species:
        c = si[s]
        for j, v in enumerate(colours[s]):
            rec = colour[:, c] == j
            k = f"{s}|{v}"
            cov.append(dict(species=s, colour=v, records=int(counts.get((s, v), 0)),
                            srv_recall=np.mean([k in base_preds[r] for r in np.flatnonzero(rec)]) if rec.any() else np.nan,
                            tree_recall=(colour_oof[rec, c] == j).mean() if rec.any() else np.nan))
    cov = pd.DataFrame(cov)
    bands = pd.cut(cov.records, [-1, 0, 29, 99, 999, 10**9], labels=["0", "1-29", "30-99", "100-999", "1000+"])
    lines += ["", "## Colour coverage of SrvSurvey's targets"] + [f"  {k:8} {v}" for k, v in bands.value_counts().sort_index().items()]

    args.output.mkdir(parents=True)
    table.to_csv(args.output / "per_species.tsv", sep="\t", index=False)
    pd.DataFrame(genus_rows).to_csv(args.output / "per_genus.tsv", sep="\t", index=False)
    cov.to_csv(args.output / "colour_coverage.tsv", sep="\t", index=False)
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
