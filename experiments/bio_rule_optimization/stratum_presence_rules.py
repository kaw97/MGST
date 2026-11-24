"""Rules for whether Stratum is really present on Stratum Tectonicas candidates.

Reads features.parquet from build_signal_features.py and keeps bodies with an
FSS genus list (complete whenever present: its length always equals the signal
count), so Stratum presence or absence is known exactly.

For each atmosphere, scored with 5-fold cross-validation grouped by sector:
- gravity + temperature tree (depth 3)
- readable tree on all pre-visit properties (depth 4)
- gradient boosting on all pre-visit properties (ceiling for readable rules)
- the same two again with the FSS bio signal count, which is known before landing

Then, per atmosphere, a readable tree is fitted on 80% of sectors and each leaf
is listed with its Stratum rate on those sectors and on the held-out 20%, so the
rates can be trusted. Permutation importance of the boosted model shows which
other properties matter.
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import GroupKFold, GroupShuffleSplit
from sklearn.tree import DecisionTreeClassifier

STRATUM = "$Codex_Ent_Stratum_Genus_Name;"
ATMOSPHERES = ["SulphurDioxide", "CarbonDioxide", "Ammonia", "Water", "Oxygen", "CarbonDioxideRich"]
ONE_HOT = ["volcanism", "terraformingState", "star_class", "main_class", "star_luminosity_class"]
CATEGORICAL = ONE_HOT + ["region"]
EXCLUDE = {
    "body_id64", "system_id64", "name", "sector", "bio_signals", "geo_signals", "genera_known",
    "landmark_species", "predicted", "subType", "atmosphere", "main_luminosity_class",
}
MIN_LEAF = 500


def numeric_columns(df):
    return [c for c in df.columns if c not in EXCLUDE and c not in CATEGORICAL]


def tree_matrix(df, num_cols, levels):
    """Numeric columns (missing -> -1) plus one-hot columns for common category levels."""
    X = df[num_cols].apply(pd.to_numeric, errors="coerce").astype(float).fillna(-1.0)
    for c in ONE_HOT:
        for v in levels[c]:
            X[f"{c}={v}"] = (df[c].astype(str) == v).astype(float)
    return X


def boost_matrix(df, num_cols):
    X = df[num_cols].apply(pd.to_numeric, errors="coerce").astype(float)
    for c in CATEGORICAL:
        X[c] = df[c].astype(str).astype("category")
    return X


def cv_auc(make, X, y, groups):
    p = np.zeros(len(y))
    for tr, te in GroupKFold(5).split(X, y, groups):
        m = make().fit(X.iloc[tr], y[tr])
        p[te] = m.predict_proba(X.iloc[te])[:, 1]
    return roc_auc_score(y, p)


def tree(depth, n):
    return lambda: DecisionTreeClassifier(max_depth=depth, min_samples_leaf=max(MIN_LEAF, n // 200), random_state=0)


def boost():
    return HistGradientBoostingClassifier(max_iter=300, learning_rate=0.08, categorical_features="from_dtype",
                                          random_state=0)


def leaves(model, X_tr, y_tr, X_te, y_te):
    """(path, n_train, rate_train, n_test, rate_test) for each leaf, lowest Stratum rate first."""
    t = model.tree_
    names = X_tr.columns
    paths = {}

    def walk(node, conds):
        if t.children_left[node] == -1:
            paths[node] = conds
            return
        f, thr = names[t.feature[node]], t.threshold[node]
        if "=" in f:  # one-hot column
            walk(t.children_left[node], conds + [f"{f.replace('=', ' != ')}"])
            walk(t.children_right[node], conds + [f"{f.replace('=', ' = ')}"])
        else:
            walk(t.children_left[node], conds + [f"{f} <= {thr:.4g}"])
            walk(t.children_right[node], conds + [f"{f} > {thr:.4g}"])

    walk(0, [])
    ltr, lte = model.apply(X_tr), model.apply(X_te)
    rows = []
    for node, conds in paths.items():
        a, b = ltr == node, lte == node
        rows.append((" and ".join(conds) or "all", int(a.sum()), y_tr[a].mean() if a.any() else np.nan,
                     int(b.sum()), y_te[b].mean() if b.any() else np.nan))
    return sorted(rows, key=lambda r: r[2])


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--features", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    args = ap.parse_args()
    if args.output.exists():
        raise SystemExit(f"Output directory already exists: {args.output}")

    df = pd.read_parquet(args.features)
    lines = [f"# Stratum presence on Stratum Tectonicas candidates", ""]
    known = df.genera_known.notna()
    lines.append("Genus-list coverage (bodies with a list are the ones scored; check they look like the rest):")
    for atm in ATMOSPHERES:
        a = df.atmosphere == atm
        if a.any():
            lines.append(f"  {atm:18} {a.sum():>9,} bodies, {known[a].mean():5.1%} with a genus list; mean signals "
                         f"with list {df.bio_signals[a & known].mean():.2f}, without {df.bio_signals[a & ~known].mean():.2f}")
    df = df[known].reset_index(drop=True)
    y_all = df.genera_known.map(lambda s: STRATUM in json.loads(s)).to_numpy()
    lines.append(f"\n{len(df):,} bodies with a genus list, Stratum present on {y_all.mean():.1%}\n")

    num_cols = numeric_columns(df)
    levels = {c: [v for v, n in df[c].astype(str).value_counts().items() if n >= 2000] for c in ONE_HOT}
    rules = {}

    for atm in ATMOSPHERES:
        sub = df[df.atmosphere == atm].reset_index(drop=True)
        if len(sub) < 1000:
            continue
        y = y_all[(df.atmosphere == atm).to_numpy()]
        groups = sub.sector.to_numpy()
        lines.append(f"## {atm}: {len(sub):,} bodies with a genus list, Stratum present on {y.mean():.1%}")
        if y.min() == y.max():
            lines.append("   Stratum is always present or always absent; no rule needed.\n")
            continue

        Xt = tree_matrix(sub, num_cols, levels)
        Xb = boost_matrix(sub, num_cols)
        Xt_sig = Xt.assign(bio_signals=sub.bio_signals.astype(float))
        Xb_sig = Xb.assign(bio_signals=sub.bio_signals.astype(float))
        n = len(sub)
        lines.append("   Cross-validated AUC (0.5 = no better than chance):")
        for label, make, X in [
            ("gravity + temperature tree (depth 3)", tree(3, n), Xt[["gravity", "surfaceTemperature"]]),
            ("readable tree, all pre-visit (depth 4)", tree(4, n), Xt),
            ("boosted model, all pre-visit", boost, Xb),
            ("readable tree + FSS signal count", tree(4, n), Xt_sig),
            ("boosted model + FSS signal count", boost, Xb_sig),
        ]:
            lines.append(f"     {label:42} {cv_auc(make, X, y, groups):.3f}")

        tr, te = next(GroupShuffleSplit(1, test_size=0.2, random_state=0).split(Xt, y, groups))
        rules[atm] = {}
        for label, X in [("before the FSS scan", Xt), ("after the FSS scan (signal count known)", Xt_sig)]:
            m = tree(4, len(tr))().fit(X.iloc[tr], y[tr])
            lines.append(f"\n   Readable rules, {label}. Stratum rate on 80% of sectors (fit) and held-out 20% (test):")
            lines.append(f"     {'fit n':>8} {'fit':>6} {'test n':>8} {'test':>6}  conditions")
            rows = leaves(m, X.iloc[tr], y[tr], X.iloc[te], y[te])
            for path, ntr, rtr, nte, rte in rows:
                lines.append(f"     {ntr:>8,} {rtr:6.1%} {nte:>8,} {rte:6.1%}  {path}")
            rules[atm][label] = [dict(conditions=p, n_fit=a, rate_fit=b, n_test=c, rate_test=d) for p, a, b, c, d in rows]

        model = boost().fit(Xb.iloc[tr], y[tr])
        sample = np.random.default_rng(0).choice(te, min(len(te), 30_000), replace=False)
        pi = permutation_importance(model, Xb.iloc[sample], y[sample], scoring="roc_auc", n_repeats=3,
                                    random_state=0, n_jobs=1)
        imp = pd.Series(pi.importances_mean, index=Xb.columns).sort_values(ascending=False)
        lines.append("\n   Boosted model, permutation importance (drop in held-out AUC):")
        for c, v in imp.head(12).items():
            lines.append(f"     {c:34} {v:.4f}")
        lines.append("")
        print("\n".join(lines[-60:]), flush=True)

    args.output.mkdir(parents=True)
    (args.output / "rules.json").write_text(json.dumps(rules, indent=2, default=float) + "\n")
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print(f"-> {args.output / 'report.txt'}")


if __name__ == "__main__":
    main()
