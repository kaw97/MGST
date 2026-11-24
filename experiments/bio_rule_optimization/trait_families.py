"""Which families of traits decide species presence, and which decide colour.

Reads the analysis database. Each family is scored alone, in trees with folds grouped by sector:
- presence: every body where the species' presence is known (AUC)
- beyond the rules: the same, only on bodies SrvSurvey's rules already predict it for (AUC)
- colour: bodies with one confirmed colour (accuracy; guess = commonest colour)
Stars are the orbit-averaged star. Every body has at least one biological signal.
"""

import argparse
import gc
import glob
import json
import multiprocessing as mp
import os
import sys
import zlib
from collections import defaultdict
from pathlib import Path

os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score
from sklearn.tree import DecisionTreeClassifier

sys.path.insert(0, str(Path(__file__).parent))

import species_colour_trees as trees

CRITERIA = "/mnt/z/SrvSurvey/SrvSurvey/bio-criteria"
FOLDS = 3
MAX_ROWS = 200_000
MIN_RECORDS = 100
BODY = ["gravity", "surfaceTemperature", "surfacePressure", "radius", "earthMasses"]
ORBIT = ["distanceToArrival", "semiMajorAxis", "orbitalEccentricity", "orbitalInclination", "orbitalPeriod",
         "rotationalPeriod", "axialTilt", "is_moon", "tidally_locked"]
STAR = ["orbit_surfaceTemperature", "orbit_solarRadius", "orbit_solarMasses", "orbit_age", "orbit_absoluteMagnitude",
        "orbit_luminosity", "orbit_share"]
SYSTEM = ["main_surfaceTemperature", "main_solarRadius", "main_solarMasses", "main_age", "main_absoluteMagnitude",
          "main_luminosity", "n_stars"]
POSITION = ["x", "y", "z", "nebula_distance"]
ONE_HOT = {"body": ["atmosphere", "volcanism", "subType", "terraformingState"], "star": ["orbit_star_class", "orbit_luminosity_class"],
           "system": ["main_class", "main_luminosity_class"], "position": ["region"]}
D = {}


def criteria_traits():
    """{species: (traits above the colour level, traits on the colour level)} in SrvSurvey's files."""
    out = defaultdict(lambda: (set(), set()))

    def walk(n, genus, species, path, common):
        common = n.get("commonChildren", common)
        genus, species, variant = n.get("genus") or genus, n.get("species") or species, n.get("variant")
        props = {q.split("[")[0].strip(" &!") for q in n.get("query") or [] if not q.strip().startswith("#")}
        if species:
            out[f"{genus} {species}"][1 if variant else 0].update(props if variant else props | path)
        for c in (common if n.get("useCommonChildren") else n.get("children")) or []:
            walk(c, genus, species, path if variant else path | props, common)

    for f in sorted(glob.glob(f"{CRITERIA}/*.json")):
        walk(json.load(open(f, encoding="utf-8-sig")), None, None, set(), None)
    return out


def matrix(df):
    groups = {"body": BODY + [c for c in df if c.startswith(("atm_", "solid_"))], "orbit": ORBIT, "star": STAR, "system": SYSTEM,
              "materials": [c for c in df if c.startswith("mat_")], "position": POSITION}
    X, fam = [], []
    for family, cols in groups.items():
        for c in cols:
            X.append(pd.to_numeric(df[c], errors="coerce").astype("float32").fillna(-1).to_numpy())
            fam.append(family)
        for c in ONE_HOT.get(family, []):
            s = df[c].astype(str)
            for v, n in s.value_counts().items():
                if n >= 200:
                    X.append((s == v).to_numpy(dtype="float32"))
                    fam.append(family)
    return np.stack(X, axis=1), np.array(fam)


def task(args):
    species, kind, family = args
    rows, y = D["tasks"][(species, kind)]
    cols = np.flatnonzero(D["fam"] == family) if family != "all" else np.arange(D["X"].shape[1])
    X = D["X"][rows][:, cols]
    folds = D["fold"][rows]
    scores = np.full(len(rows), np.nan, "float32")
    for f in range(FOLDS):
        tr, te = folds != f, folds == f
        if not te.any() or len(set(y[tr])) < 2:
            continue
        if kind == "colour":
            m = DecisionTreeClassifier(max_depth=10, min_samples_leaf=5, random_state=0).fit(X[tr], y[tr])
            scores[te] = m.predict(X[te]) == y[te]
        else:
            m = DecisionTreeClassifier(max_depth=8, min_samples_leaf=20, random_state=0).fit(X[tr], y[tr])
            scores[te] = m.predict_proba(X[te])[:, 1]
    ok = ~np.isnan(scores)
    if not ok.any():
        return species, kind, family, None
    if kind == "colour":
        return species, kind, family, float(scores[ok].mean())
    return species, kind, family, float(roc_auc_score(y[ok], scores[ok])) if len(set(y[ok])) == 2 else None


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--workers", type=int, default=10)
    args = ap.parse_args()
    if args.output.exists():
        raise SystemExit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)

    df = pd.concat([pd.read_parquet(p) for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet")))], ignore_index=True)
    df = df[df.has_prediction & df.orbit_predictions.notna()].reset_index(drop=True)
    for c in ("is_moon", "tidally_locked"):
        df[c] = df[c].astype("float32")
    D["X"], D["fam"] = matrix(df)
    D["fold"] = np.array([zlib.crc32(s.encode()) % FOLDS for s in df.sector], "int8")
    row = dict(zip(df.body_id64, range(len(df))))

    lm = pd.read_parquet(args.root / "landmarks/landmarks.parquet", columns=["body_id64", "genus", "species", "colour"])
    lm = lm[lm.body_id64.isin(row)]
    walk = pd.read_parquet(args.root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "api_genera"])
    api = dict(zip(walk.body_id64, walk.api_genera))
    lists = []
    for b, db in zip(df.body_id64, df.db_genera):
        a = json.loads(api[b]) if api.get(b) else []
        lists.append(set(a or ([trees.CODE_GENUS.get(c, c) for c in json.loads(db)] if db else [])))
    found_genera = [set() for _ in row]
    has = defaultdict(set)
    colours = defaultdict(dict)
    genus_of = {}
    for b, g, s, c in zip(lm.body_id64, lm.genus, lm.species, lm.colour):
        r = row[b]
        found_genera[r].add(g)
        has[s].add(r)
        genus_of[s] = g
        if c is not None:
            colours[s][r] = c
    preds = defaultdict(set)
    listed = defaultdict(int)
    for r, p in enumerate(df.orbit_predictions):
        by = defaultdict(int)
        for k in json.loads(p):
            s = k.split("|")[0]
            preds[s].add(r)
            by[s] += 1
        for s, n in by.items():
            if n > 1 and r in colours.get(s, {}):
                listed[s] += 1
    has_list = [bool(x) for x in lists]
    rng = np.random.default_rng(0)
    D["tasks"] = {}
    summary = {}
    for s, g in genus_of.items():
        if len(has[s]) < MIN_RECORDS:
            continue
        # present, or certainly absent (genus not on a list, or genus confirmed as another species only)
        known = np.fromiter((r in has[s] or g in found_genera[r] or (has_list[r] and g not in lists[r]) for r in range(len(row))), bool, len(row))
        y = np.zeros(len(row), bool)
        y[list(has[s])] = True
        pred = np.zeros(len(row), bool)
        pred[list(preds[s])] = True
        for kind, mask in (("presence", known), ("beyond", known & pred)):
            idx = np.flatnonzero(mask)
            if y[idx].sum() < MIN_RECORDS or (~y[idx]).sum() < MIN_RECORDS:
                continue
            pos, neg = idx[y[idx]], idx[~y[idx]]
            pos = rng.choice(pos, min(len(pos), MAX_ROWS // 2), replace=False)
            neg = rng.choice(neg, min(len(neg), MAX_ROWS // 2), replace=False)
            take = np.sort(np.concatenate([pos, neg]))
            D["tasks"][(s, kind)] = (take, y[take])
        rec = np.array(sorted(colours.get(s, {})))
        col = np.array([colours[s][r] for r in rec]) if len(rec) else np.array([])
        if len(rec) >= MIN_RECORDS and len(set(col)) > 1:
            if len(rec) > MAX_ROWS:
                keep = np.sort(rng.choice(len(rec), MAX_ROWS, replace=False))
                rec, col = rec[keep], col[keep]
            D["tasks"][(s, "colour")] = (rec, col)
            summary[s] = dict(records=len(colours[s]), n_colours=len(set(col)), guess=float(pd.Series(col).value_counts(normalize=True).iloc[0]),
                              two_listed=listed[s] / len(colours[s]))
    families = ["body", "orbit", "star", "system", "materials", "position", "all"]
    jobs = [(s, k, f) for s, k in D["tasks"] for f in families]
    # Workers only need D: free the parent's Python objects so forking does not multiply them
    del df, lists, found_genera, has, preds, colours, row, api, walk, lm
    gc.collect()
    gc.freeze()
    print(f"{len(jobs):,} fits-sets", flush=True)
    res = defaultdict(dict)
    progress = open(args.output / "progress.jsonl", "w")
    with mp.get_context("fork").Pool(args.workers) as pool:
        for i, (s, k, f, v) in enumerate(pool.imap_unordered(task, jobs, chunksize=4)):
            res[(s, k)][f] = v
            progress.write(json.dumps([s, k, f, v]) + "\n")
            progress.flush()
            if i % 500 == 0:
                print(i, flush=True)
    traits = criteria_traits()
    out = []
    for (s, k), v in res.items():
        name = s
        t = traits.get(name, (set(), set()))
        out.append(dict(species=s, kind=k, srv_presence=",".join(sorted(t[0])), srv_colour=",".join(sorted(t[1])), **summary.get(s, {}) if k == "colour" else {}, **v))
    table = pd.DataFrame(out).sort_values(["kind", "species"])
    table.to_csv(args.output / "trait_families.tsv", sep="\t", index=False)
    print(f"-> {args.output / 'trait_families.tsv'}")


if __name__ == "__main__":
    main()
