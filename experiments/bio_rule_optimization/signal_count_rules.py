"""Per-atmosphere gravity/temperature rules for the bio signal count on Stratum Tectonicas candidates.

Reads features.parquet from build_signal_features.py. For each atmosphere, fits
two rectangle rules on gravity and surface temperature:

- one-signal:   gravity >= G and temperature >= T  -> expect exactly 1 signal
- multi-signal: gravity <= G and temperature <= T  -> expect 2+ signals

Each rule is the rectangle that flags the most bodies while keeping the share
of correct calls at or above the target precision (with at least MIN_FLAGGED
bodies). Rules are scored with 5-fold cross-validation grouped by sector
(thresholds fitted on four folds, scored on the fifth), then refitted on all
bodies.

Then, using the FSS genus lists (complete whenever present: their length always
equals the signal count), it reports what each rule means for Stratum
Tectonicas: how often Stratum is really there on flagged bodies, and how many
wrong and right Stratum predictions dropping it there would remove.
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import GroupKFold

STRATUM = "$Codex_Ent_Stratum_Genus_Name;"
G_GRID = np.round(np.arange(0.0, 0.65, 0.01), 2)
T_GRID = np.arange(150, 705, 5)
MIN_FLAGGED = 300


def _counts(g, t, hit):
    """Histogram of bodies and hits over the threshold grid cells."""
    gi = np.clip(np.searchsorted(G_GRID, g, side="right") - 1, 0, len(G_GRID) - 1)
    ti = np.clip(np.searchsorted(T_GRID, t, side="right") - 1, 0, len(T_GRID) - 1)
    n = np.zeros((len(G_GRID), len(T_GRID)))
    h = np.zeros_like(n)
    np.add.at(n, (gi, ti), 1)
    np.add.at(h, (gi, ti), hit)
    return n, h


def fit(g, t, hit, kind, target):
    """Best (G, T) for the rule kind; None if no rectangle reaches the target."""
    n, h = _counts(g, t, hit)
    if kind == "one":   # g >= G_GRID[i] and t >= T_GRID[j]
        N = n[::-1, ::-1].cumsum(0).cumsum(1)[::-1, ::-1]
        H = h[::-1, ::-1].cumsum(0).cumsum(1)[::-1, ::-1]
    else:               # g < G_GRID[i+1] and t < T_GRID[j+1]
        N, H = n.cumsum(0).cumsum(1), h.cumsum(0).cumsum(1)
    with np.errstate(invalid="ignore", divide="ignore"):
        ok = (N >= MIN_FLAGGED) & (H / N >= target)
    if not ok.any():
        return None
    i, j = np.unravel_index(np.where(ok, N, -1).argmax(), N.shape)
    if kind == "one":
        return float(G_GRID[i]), float(T_GRID[j])
    return float(G_GRID[min(i + 1, len(G_GRID) - 1)]), float(T_GRID[min(j + 1, len(T_GRID) - 1)])


def apply(rule, kind, g, t):
    if rule is None:
        return np.zeros(len(g), bool)
    G, T = rule
    return (g >= G) & (t >= T) if kind == "one" else (g < G) & (t < T)


def describe(rule, kind):
    if rule is None:
        return "no rule reaches the target"
    G, T = rule
    if kind == "one":
        return f"gravity >= {G:.2f} g and temperature >= {T:.0f} K"
    return f"gravity < {G:.2f} g and temperature < {T:.0f} K"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--features", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--targets", default="0.80,0.90,0.95", help="Precision targets for the one-signal rule")
    ap.add_argument("--multi-target", type=float, default=0.90, help="Precision target for the multi-signal rule")
    args = ap.parse_args()
    if args.output.exists():
        raise SystemExit(f"Output directory already exists: {args.output}")

    df = pd.read_parquet(args.features, columns=[
        "sector", "atmosphere", "gravity", "surfaceTemperature", "bio_signals", "genera_known"])
    df = df.dropna(subset=["gravity", "surfaceTemperature", "atmosphere"]).reset_index(drop=True)
    g = df.gravity.to_numpy()
    t = df.surfaceTemperature.to_numpy()
    one = (df.bio_signals == 1).to_numpy()
    known = df.genera_known.notna().to_numpy()
    stratum = df.genera_known.map(lambda s: STRATUM in json.loads(s) if s else False).to_numpy()
    groups = df.sector.to_numpy()
    targets = [float(x) for x in args.targets.split(",")]

    lines = [
        f"# Per-atmosphere signal-count rules on Stratum Tectonicas candidates ({len(df):,} bodies)",
        "",
        "Precision = share of flagged bodies the rule gets right. Coverage = share of the atmosphere's",
        "bodies of that kind (1-signal, or 2+ signals) that the rule flags. 'CV' = 5-fold cross-validation",
        "grouped by sector; 'fit' = rule refitted on all bodies (the rule to use).",
        "Stratum columns use bodies with a genus list (complete for every body that has one).",
        "",
    ]
    rules = {}
    specs = [("one", tg) for tg in targets] + [("multi", args.multi_target)]
    for atm in df.atmosphere.value_counts().index:
        m = (df.atmosphere == atm).to_numpy()
        idx = np.flatnonzero(m)
        n_atm, one_rate = m.sum(), one[m].mean()
        lines.append(f"## {atm}: {n_atm:,} bodies, {one_rate:.1%} with one signal, "
                     f"gravity {g[m].min():.2f}-{g[m].max():.2f} g, temperature {t[m].min():.0f}-{t[m].max():.0f} K")
        has_s = known & m
        lines.append(f"   Stratum present on {stratum[has_s].mean():.1%} of the {has_s.sum():,} bodies with a genus list")
        rules[atm] = {}
        for kind, tg in specs:
            hit = one if kind == "one" else ~one
            # cross-validated score
            flag_cv = np.zeros(len(df), bool)
            if len(np.unique(groups[idx])) >= 5:
                for tr, te in GroupKFold(5).split(idx, groups=groups[idx]):
                    tr, te = idx[tr], idx[te]
                    rule = fit(g[tr], t[tr], hit[tr], kind, tg)
                    flag_cv[te] = apply(rule, kind, g[te], t[te])
            rule = fit(g[m], t[m], hit[m], kind, tg)
            flag = apply(rule, kind, g, t) & m
            name = "one-signal" if kind == "one" else "multi-signal"
            lines.append(f"\n   {name} rule, target {tg:.0%}: {describe(rule, kind)}")
            for label, f in (("CV ", flag_cv & m), ("fit", flag)):
                nf = f.sum()
                if not nf:
                    lines.append(f"     {label}: flags nothing")
                    continue
                prec = hit[f].mean()
                cov = (hit & f).sum() / max((hit & m).sum(), 1)
                lines.append(f"     {label}: flags {nf:>9,} ({nf / n_atm:5.1%} of bodies)  precision {prec:6.1%}  coverage {cov:6.1%}")
            fk = flag & known
            if rule is not None and fk.any():
                s_here = stratum[fk].mean()
                s_one = stratum[fk & one].mean() if (fk & one).any() else float("nan")
                lines.append(f"     Stratum present on {s_here:.1%} of flagged bodies with a genus list "
                             f"({(fk & one).sum():,} one-signal: {s_one:.1%} Stratum)")
                if kind == "one":
                    lines.append(f"     dropping Stratum here would remove {(fk & ~stratum).sum():,} wrong and "
                                 f"{(fk & stratum).sum():,} right Stratum predictions "
                                 f"(of {(m & known & ~stratum).sum():,} wrong, {(m & known & stratum).sum():,} right in total)")
            rules[atm][f"{name}@{tg:.2f}"] = None if rule is None else {
                "gravity": rule[0], "temperature": rule[1],
                "test": "gravity >= G and temperature >= T" if kind == "one" else "gravity < G and temperature < T",
                "precision": float(hit[flag].mean()) if flag.any() else None,
                "flagged": int(flag.sum()),
            }
        lines.append("")

    args.output.mkdir(parents=True)
    (args.output / "rules.json").write_text(json.dumps(rules, indent=2) + "\n")
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
