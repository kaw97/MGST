"""Explore one species ruleset for properties that separate confirmed from wrong predictions.

Works on an extract from extract_body_features.py. For bodies the ruleset matches
(species gates applied) it reports per-property ROC AUC (confirmed vs definitely
wrong), category hit rates, best single threshold cuts and a shallow decision tree,
all fitted on training sectors and scored on held-out sectors (sweep_precision split).
Properties only known after a body is visited (signal counts) are diagnostic only.
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.metrics import roc_auc_score
from sklearn.tree import DecisionTreeClassifier, export_text

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mgst.bio.region import find_region
from mgst.bio.species_rules import HIGH_VALUE_SPECIES, score_body
from attribute_differences import _parse_lists, outcome
from sweep_precision import is_test

DIAGNOSTIC = {"sig_$SAA_SignalType_Biological;", "sig_$SAA_SignalType_Geological;", "sig_$SAA_SignalType_Human;",
              "sig_$SAA_SignalType_Other;", "sig_$PlanetaryMiningLocation_Name;", "landmark_count"}
NOT_FEATURES = {"system_id64", "id64", "bodyId", "landmark_species", "landmark_genuses", "signals_genuses",
                "sector", "subType", "atmosphereType", "outcome", "test", "label"}
CATEGORICAL = ["volcanismType", "terraformingState", "own_star_class", "main_star_class", "own_star_luminosity",
               "ring_0_type", "is_moon", "orbits_barycentre", "own_star_is_main", "region", "rotationalPeriodTidallyLocked"]


def load(path, species, ruleset_index):
    single = [{**species, "rulesets": [species["rulesets"][ruleset_index]]}]
    rows = []
    for r in pq.read_table(path).to_pylist():
        r["star_class"] = r.get("main_star_class")
        r["parent_star_class"] = r.get("own_star_class")
        r["star_age"] = r.get("main_star_age")
        if not score_body(r, single):
            continue
        _parse_lists(r)
        oc = outcome(r, species)
        r["outcome"] = "TP" if oc == "TP" else "unverified" if oc == "unverified" else "wrong"
        r["confirmed_species"] = sorted(r.pop("_lm_species"))
        r.pop("_genera")
        r["test"] = is_test(r["sector"])
        x, y, z = r.get("system_x"), r.get("system_y"), r.get("system_z")
        reg = find_region(x, y, z) if x is not None else None
        r["region"] = reg[1] if reg else None
        r["dist_sol"] = (x * x + y * y + z * z) ** 0.5 if x is not None else None
        if r.get("radius") and r.get("earthMasses"):
            r["density_proxy"] = r["earthMasses"] / (r["radius"] / 6371.0) ** 3
        v = r.get("volcanismType")
        r["volcanismType"] = v if v and v != "No volcanism" else "None"
        rows.append(r)
    return pd.DataFrame(rows)


def hit(tp, wrong):
    return tp / (tp + wrong) if tp + wrong else float("nan")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="output/body_features/thin_water_hmc_rocky.parquet")
    parser.add_argument("--species", required=True)
    parser.add_argument("--ruleset", type=int, required=True)
    parser.add_argument("--depth", type=int, default=3)
    parser.add_argument("--min-leaf", type=int, default=40)
    args = parser.parse_args()

    species = next(s for s in HIGH_VALUE_SPECIES if s["name"] == args.species)
    df = load(args.input, species, args.ruleset)
    known = df[df.outcome != "unverified"].copy()
    known["label"] = (known.outcome == "TP").astype(int)
    c = Counter(df.outcome)
    print(f"{args.species} rs{args.ruleset}: {json.dumps(species['rulesets'][args.ruleset])}")
    print(f"Matched {len(df):,} landmark bodies: {c['TP']:,} confirmed, {c['wrong']:,} wrong, "
          f"{c['unverified']:,} unverified; hit rate {100 * hit(c['TP'], c['wrong']):.1f}%")

    print("\nSpecies confirmed on the wrong bodies (top 12):")
    wrong_species = Counter(s for lst in known[known.label == 0].confirmed_species for s in lst)
    tp_species = Counter(s for lst in known[known.label == 1].confirmed_species for s in lst)
    n_w, n_t = (known.label == 0).sum(), (known.label == 1).sum()
    for s, n in wrong_species.most_common(12):
        print(f"  {s:<28} on {100 * n / n_w:5.1f}% of wrong bodies, {100 * tp_species[s] / n_t:5.1f}% of confirmed")

    numeric = [col for col in known.columns if col not in NOT_FEATURES and col not in CATEGORICAL
               and col != "confirmed_species" and pd.api.types.is_numeric_dtype(known[col])
               and not pd.api.types.is_bool_dtype(known[col])
               and known[col].notna().sum() >= 50 and known[col].nunique() > 1]
    print(f"\nProperty separation (ROC AUC, confirmed vs wrong; 0.5 = no signal). * = known only after a visit")
    aucs = []
    for col in numeric:
        sub = known[known[col].notna()]
        if sub.label.nunique() < 2:
            continue
        auc = roc_auc_score(sub.label, sub[col])
        aucs.append((abs(auc - 0.5), col, auc, sub[sub.label == 1][col].median(), sub[sub.label == 0][col].median(),
                     known[col].isna().mean()))
    for _, col, auc, med_tp, med_w, miss in sorted(aucs, reverse=True)[:18]:
        flag = "*" if col in DIAGNOSTIC else " "
        print(f"  {flag}{col:<34} AUC {auc:.3f}  median confirmed {med_tp:.5g}  wrong {med_w:.5g}  missing {100 * miss:.0f}%")

    print("\nCategories (rows with >= 30 bodies):")
    for col in CATEGORICAL:
        if col not in known.columns:
            continue
        g = known.groupby(known[col].astype(str)).label.agg(["sum", "count"])
        g = g[g["count"] >= 30]
        if len(g) < 2:
            continue
        parts = [f"{k}: {int(v['sum'])}/{int(v['count'])} ({100 * v['sum'] / v['count']:.0f}%)" for k, v in g.iterrows()]
        print(f"  {col}: " + "; ".join(parts))

    train, test = known[~known.test], known[known.test]
    base_test = hit(test.label.sum(), (test.label == 0).sum())
    print(f"\nBest single threshold per property (fitted on training sectors; held-out base hit rate {100 * base_test:.1f}%)")
    cuts = []
    for col in [c for c in numeric if c not in DIAGNOSTIC]:
        vals = train[col].dropna()
        if len(vals) < 50:
            continue
        for q in np.linspace(0.02, 0.98, 49):
            t = vals.quantile(q)
            for side in ("min", "max"):
                keep_tr = train[col].isna() | ((train[col] >= t) if side == "min" else (train[col] <= t))
                kept, removed = train[keep_tr], train[~keep_tr]
                if removed.label.sum() > 0.10 * train.label.sum() or len(removed) < 30:
                    continue
                gain = (removed.label == 0).sum() - 3 * removed.label.sum()
                cuts.append((gain, col, side, t))
    seen, shown = set(), 0
    for gain, col, side, t in sorted(cuts, reverse=True):
        if col in seen:
            continue
        seen.add(col)
        keep = test[col].isna() | ((test[col] >= t) if side == "min" else (test[col] <= t))
        r = test[~keep]
        k = test[keep]
        print(f"  {col} {'>=' if side == 'min' else '<='} {t:.5g}: held-out removes {(r.label == 0).sum()} wrong, "
              f"{r.label.sum()} confirmed; kept hit rate {100 * hit(k.label.sum(), (k.label == 0).sum()):.1f}%")
        shown += 1
        if shown >= 10:
            break

    feats = [c for c in numeric if c not in DIAGNOSTIC]
    X = pd.get_dummies(known[feats + [c for c in CATEGORICAL if c in known.columns]].astype(
        {c: str for c in CATEGORICAL if c in known.columns}), dtype=float)
    Xtr, Xte = X[~known.test.values], X[known.test.values]
    tree = DecisionTreeClassifier(max_depth=args.depth, min_samples_leaf=args.min_leaf, random_state=0)
    tree.fit(Xtr, train.label)
    print(f"\nDecision tree (depth {args.depth}, trained on training sectors):")
    print(export_text(tree, feature_names=list(X.columns), show_weights=True, decimals=4))
    leaves_tr, leaves_te = tree.apply(Xtr), tree.apply(Xte)
    print("Leaves on held-out sectors:")
    for leaf in sorted(set(leaves_tr)):
        tr = train.label.values[leaves_tr == leaf]
        te = test.label.values[leaves_te == leaf]
        print(f"  leaf {leaf}: train {tr.sum()} confirmed / {(tr == 0).sum()} wrong ({100 * hit(tr.sum(), (tr == 0).sum()):.0f}%)"
              f"   held-out {te.sum()} / {(te == 0).sum()} ({100 * hit(te.sum(), (te == 0).sum()):.0f}%)")
    if len(set(test.label)) == 2:
        print(f"Tree held-out ROC AUC: {roc_auc_score(test.label, tree.predict_proba(Xte)[:, 1]):.3f}")


if __name__ == "__main__":
    main()
