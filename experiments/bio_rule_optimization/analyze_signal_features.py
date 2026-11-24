"""What predicts the bio signal count on Stratum Tectonicas candidates?

Reads features.parquet from build_signal_features.py and reports:
1. Each property on its own: AUC for separating 1-signal from multi-signal
   bodies, Spearman correlation with the count, and per-category rates.
2. All properties together: gradient-boosted models scored with sector-grouped
   cross-validation (neighbouring systems never split across train and test),
   against baselines, with permutation importance on held-out folds.

The geological signal count is reported separately because it is only known
after the FSS scan, at the same time as the bio count.
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_absolute_error, roc_auc_score
from sklearn.model_selection import GroupKFold

CATEGORICAL = [
    "subType", "atmosphere", "volcanism", "terraformingState", "star_class", "main_class",
    "star_luminosity_class", "main_luminosity_class", "region",
]
EXCLUDE = {
    "body_id64", "system_id64", "name", "sector", "bio_signals", "geo_signals", "genera_known",
    "landmark_species", "predicted",
}


def prepare(df: pd.DataFrame, with_geo: bool):
    cols = [c for c in df.columns if c not in EXCLUDE]
    if with_geo:
        cols.append("geo_signals")
    X = df[cols].copy()
    for c in X.columns:
        if c in CATEGORICAL:
            X[c] = X[c].astype("category")
        elif X[c].dtype == bool or X[c].dtype == object:
            X[c] = pd.to_numeric(X[c].map({True: 1, False: 0, None: np.nan}), errors="coerce")
    return X


def univariate(df: pd.DataFrame, one: np.ndarray, lines: list):
    lines.append("## Single properties: numeric (|AUC - 0.5| sorted; AUC > 0.5 means higher values -> more signals)")
    rows = []
    for c in df.columns:
        if c in EXCLUDE or c in CATEGORICAL:
            continue
        v = pd.to_numeric(df[c].map(lambda x: float(x) if isinstance(x, (bool, np.bool_)) else x), errors="coerce")
        mask = v.notna().to_numpy()
        if mask.sum() < 1000 or v[mask].nunique() < 2:
            continue
        auc = roc_auc_score(~one[mask], v[mask])
        rho = spearmanr(v[mask], df["bio_signals"][mask]).statistic
        rows.append((c, auc, rho, int(mask.sum())))
    rows.sort(key=lambda r: -abs(r[1] - 0.5))
    lines.append(f"{'property':34} {'AUC':>6} {'Spearman':>9} {'n':>9}")
    for c, auc, rho, n in rows[:30]:
        lines.append(f"{c:34} {auc:6.3f} {rho:9.3f} {n:9,}")

    lines.append("\n## Single properties: categories (levels with >= 1,000 bodies)")
    for c in CATEGORICAL + ["n_predicted_genera", "n_star_candidates", "is_moon", "predicts_bacterium"]:
        if c not in df:
            continue
        g = df.groupby(df[c].astype(str))["bio_signals"].agg(
            bodies="size", mean_signals="mean", one_signal=lambda s: (s == 1).mean())
        g = g[g.bodies >= 1000].sort_values("one_signal")
        if len(g) < 2:
            continue
        lines.append(f"\n{c}:")
        for level, r in g.iterrows():
            lines.append(f"  {level:30} {int(r.bodies):>9,}  mean {r.mean_signals:5.2f}  1-signal {r.one_signal:6.1%}")


def cross_validate(df: pd.DataFrame, X: pd.DataFrame, one: np.ndarray, label: str, lines: list, importance: bool):
    groups = df["sector"].to_numpy()
    y = df["bio_signals"].to_numpy()
    p_one, p_count = np.zeros(len(df)), np.zeros(len(df))
    imps = []
    for fold, (tr, te) in enumerate(GroupKFold(n_splits=5).split(X, y, groups)):
        clf = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.08, categorical_features="from_dtype",
                                             random_state=fold)
        clf.fit(X.iloc[tr], ~one[tr])
        p_one[te] = clf.predict_proba(X.iloc[te])[:, 1]
        reg = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.08, categorical_features="from_dtype",
                                            random_state=fold)
        reg.fit(X.iloc[tr], y[tr])
        p_count[te] = reg.predict(X.iloc[te])
        if importance and fold < 2:
            sub = np.random.default_rng(fold).choice(te, min(len(te), 40_000), replace=False)
            pi = permutation_importance(clf, X.iloc[sub], ~one[sub], scoring="roc_auc", n_repeats=3,
                                        random_state=fold, n_jobs=1)
            imps.append(pd.Series(pi.importances_mean, index=X.columns))
    auc = roc_auc_score(~one, p_one)
    mae = mean_absolute_error(y, p_count)
    rho = spearmanr(p_count, y).statistic
    lines.append(f"{label:44} AUC(1 vs more) {auc:.3f}   count MAE {mae:.3f}   Spearman {rho:.3f}")
    if imps:
        imp = pd.concat(imps, axis=1).mean(axis=1).sort_values(ascending=False)
        lines.append(f"\n  Permutation importance (drop in held-out AUC), {label}:")
        for c, v in imp.head(20).items():
            lines.append(f"    {c:34} {v:.4f}")
        lines.append("")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--features", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True, help="Report file to write (must not exist)")
    args = ap.parse_args()
    if args.report.exists():
        raise SystemExit(f"Report already exists: {args.report}")

    df = pd.read_parquet(args.features)
    one = (df["bio_signals"] == 1).to_numpy()
    lines = [
        f"# Bio signal count on Stratum Tectonicas candidates ({len(df):,} bodies, {df.sector.nunique():,} sectors)",
        "",
        "Signal count distribution: " + ", ".join(
            f"{k}: {v:,} ({v / len(df):.1%})" for k, v in df.bio_signals.value_counts().sort_index().items()),
        "",
    ]
    univariate(df, one, lines)

    lines.append("\n## All properties together (5-fold cross-validation grouped by sector)")
    y = df["bio_signals"].to_numpy()
    lines.append(f"{'baseline: constant':44} AUC(1 vs more) 0.500   count MAE {mean_absolute_error(y, np.full(len(y), np.median(y))):.3f}")
    cross_validate(df, df[["n_predicted_genera"]].astype(float), one,
                   "baseline: SrvSurvey predicted genus count", lines, False)
    X = prepare(df, with_geo=False)
    cross_validate(df, X, one, "all pre-visit properties", lines, True)
    cross_validate(df, prepare(df, with_geo=True), one, "all properties + geological signals", lines, False)

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
