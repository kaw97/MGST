"""Sweep main-star age as a discriminator for every species ruleset.

Runs on the landmark-body extract (needs the star_age column). For each ruleset,
evaluated on its own with species gates, reports the ROC AUC of star age
(confirmed vs definitely wrong), hit rates by age band, and the best minimum and
maximum age cuts chosen on training sectors and scored on held-out sectors.
"""

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from sklearn.metrics import roc_auc_score

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mgst.bio.species_rules import HIGH_VALUE_SPECIES, score_body
from attribute_differences import _parse_lists, outcome
from sweep_precision import is_test

BANDS = [0, 1000, 2000, 4000, 6000, 8000, 9000, 10000, 11000, 12000, 13000, 1e9]
GRID = range(250, 14001, 250)
MAX_TP_LOSS = 0.05


def label(rs):
    bodies = "/".join(b.replace(" body", "").replace("High metal content", "HMC").replace(" world", "")
                      for b in rs.get("body_type", ["any"]))
    return f"{'+'.join(rs['atmosphere'])} on {bodies}"


def best_cut(train, test, side):
    """Best cut on training data: most wrong removed minus 3x confirmed removed, TP loss <= 5%."""
    tp_total = sum(1 for a, o in train if o == "TP")
    best = None
    for t in GRID:
        drop = (lambda a: a is not None and a < t) if side == "min" else (lambda a: a is not None and a > t)
        c = Counter(o for a, o in train if drop(a))
        if c["TP"] > MAX_TP_LOSS * tp_total or c["wrong"] < 10:
            continue
        score = c["wrong"] - 3 * c["TP"]
        if best is None or score > best[0]:
            best = (score, t)
    if best is None or best[0] <= 0:
        return None
    t = best[1]
    drop = (lambda a: a is not None and a < t) if side == "min" else (lambda a: a is not None and a > t)
    return t, Counter(o for a, o in train if drop(a)), Counter(o for a, o in test if drop(a))


def main():
    out = Path("output/species_rules_eval")
    out.mkdir(parents=True, exist_ok=True)
    rulesets = [(sp, i, [{**sp, "rulesets": [rs]}]) for sp in HIGH_VALUE_SPECIES for i, rs in enumerate(sp["rulesets"])]
    data = defaultdict(list)  # (species, i) -> [(age, outcome, test)]
    table = pq.read_table("output/landmark_bodies/landmark_bodies.parquet")
    print(f"Sweeping star age over {len(rulesets)} rulesets on {len(table):,} landmark bodies", flush=True)
    for batch in table.to_batches(max_chunksize=50_000):
        for row in batch.to_pylist():
            if row.get("parent_star_class") in ("barycentre", "unknown"):
                row["parent_star_class"] = None
            parsed = False
            truth = {}
            for sp, i, single in rulesets:
                if not score_body(row, single):
                    continue
                if not parsed:
                    _parse_lists(row)
                    parsed = True
                if sp["name"] not in truth:
                    oc = outcome(row, sp)
                    truth[sp["name"]] = "TP" if oc == "TP" else "unverified" if oc == "unverified" else "wrong"
                data[(sp["name"], i)].append((row.get("star_age"), truth[sp["name"]], is_test(row["sector"])))

    lines, report = [], []
    for sp, i, _ in rulesets:
        recs = data[(sp["name"], i)]
        c = Counter(o for _, o, _ in recs)
        known = [(a, o) for a, o, _ in recs if o != "unverified" and a is not None]
        entry = {"species": sp["name"], "ruleset": i, "label": label(sp["rulesets"][i]),
                 "TP": c["TP"], "wrong": c["wrong"], "unverified": c["unverified"],
                 "age_missing": sum(1 for a, _, _ in recs if a is None)}
        auc = None
        if len({o for _, o in known}) == 2:
            auc = roc_auc_score([o == "TP" for _, o in known], [a for a, _ in known])
        entry["auc"] = auc
        hit = c["TP"] / (c["TP"] + c["wrong"]) if c["TP"] + c["wrong"] else None
        lines.append(f"\n{sp['name']} rs{i} {entry['label']}: confirmed {c['TP']:,}, wrong {c['wrong']:,}, "
                     f"unverified {c['unverified']:,}, hit {100 * hit if hit is not None else float('nan'):.1f}%, "
                     f"age AUC {auc if auc is not None else float('nan'):.3f}")
        bands = []
        for lo, hi in zip(BANDS, BANDS[1:]):
            b = Counter(o for a, o, _ in recs if a is not None and lo <= a < hi)
            if sum(b.values()) == 0:
                continue
            h = b["TP"] / (b["TP"] + b["wrong"]) if b["TP"] + b["wrong"] else None
            bands.append({"from": lo, "to": hi, **{k: b[k] for k in ("TP", "wrong", "unverified")}, "hit": h})
        entry["bands"] = bands
        if c["wrong"] >= 25 and auc is not None:
            lines.append("    " + "  ".join(
                f"{b['from'] / 1000:g}-{'' if b['to'] > 1e8 else f'{b['to'] / 1000:g}'}k: {b['TP']}/{b['wrong']}"
                for b in bands) + "   (confirmed/wrong by age in billions of years)")
            train = [(a, o) for a, o, t in recs if not t and o != "unverified"]
            test = [(a, o) for a, o, t in recs if t and o != "unverified"]
            entry["cuts"] = {}
            for side in ("min", "max"):
                res = best_cut(train, test, side)
                if res is None:
                    continue
                t, tr, te = res
                full = Counter(o for a, o, _ in recs if a is not None and (a < t if side == "min" else a > t))
                entry["cuts"][side] = {"threshold": t, "TP_lost": full["TP"], "wrong_removed": full["wrong"],
                                       "unverified_removed": full["unverified"],
                                       "held_out_TP_lost": te["TP"], "held_out_wrong_removed": te["wrong"]}
                lines.append(f"    best {'minimum' if side == 'min' else 'maximum'} age {t:,}: removes {full['wrong']:,} wrong, "
                             f"{full['TP']:,} confirmed, {full['unverified']:,} unverified "
                             f"(held-out: {te['wrong']} wrong, {te['TP']} confirmed)")
        report.append(entry)
    (out / "star_age_sweep.json").write_text(json.dumps(report, indent=2))
    (out / "star_age_sweep.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
