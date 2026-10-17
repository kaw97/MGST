"""Per-species and per-ruleset confusion statistics for the species rules on landmark bodies.

Species level over all landmark bodies:
  TP  confirmed and predicted          FN  confirmed, not predicted
  FP  definitely absent, predicted     TN  definitely absent, not predicted
  UP  unverified, predicted (excluded from FP and the rates)
  FNR = FN / (TP + FN), FDR = FP / (TP + FP), FPR = FP / (FP + TN)
"Definitely absent" follows attribute_differences.outcome().
Ruleset level, for bodies the ruleset itself matches (species gates applied):
  TP, FP, UP; FDR = FP / (TP + FP); FPR = FP / species definite negatives.
Ruleset scope = bodies with the ruleset's atmosphere and body type; the ruleset
false negative rate is the share of confirmed bodies in scope that the species
misses. Confirmed bodies outside every ruleset's scope are reported separately.
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mgst.bio.species_rules import HIGH_VALUE_SPECIES, matches_atmosphere, matches_body_type, score_body
from attribute_differences import _parse_lists, outcome

CONFIGS = {"defaults": HIGH_VALUE_SPECIES}
DETAILED = tuple(CONFIGS)


def in_scope(row, rs):
    if not matches_atmosphere(row.get("atmosphereType"), rs["atmosphere"]):
        return False
    return rs.get("body_type") is None or matches_body_type(row.get("subType"), rs["body_type"])


def pct(a, b):
    return round(100 * a / b, 3) if b else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="output/landmark_bodies/landmark_bodies.parquet")
    parser.add_argument("--output", default="output/species_rules_eval/ruleset_breakdown.json")
    args = parser.parse_args()

    species_by_cfg = CONFIGS
    singles = {
        cfg: {sp["name"]: [[{**sp, "rulesets": [rs]}] for rs in sp["rulesets"]] for sp in species_by_cfg[cfg]}
        for cfg in DETAILED
    }
    sp_counts = {cfg: defaultdict(Counter) for cfg in CONFIGS}
    rs_counts = {cfg: defaultdict(Counter) for cfg in DETAILED}  # (species, i) -> counts
    out_of_scope = {cfg: Counter() for cfg in DETAILED}

    table = pq.read_table(args.input)
    print(f"Evaluating {len(CONFIGS)} configurations on {len(table):,} landmark bodies", flush=True)
    for batch in table.to_batches(max_chunksize=50_000):
        for row in batch.to_pylist():
            if row.get("parent_star_class") in ("barycentre", "unknown"):
                row["parent_star_class"] = None
            _parse_lists(row)
            truth = {sp["name"]: outcome(row, sp) for sp in HIGH_VALUE_SPECIES}
            for cfg, species in species_by_cfg.items():
                predicted = {n for n, _ in score_body(row, species)}
                for name, oc in truth.items():
                    hit = name in predicted
                    c = sp_counts[cfg][name]
                    if oc == "TP":
                        c["TP" if hit else "FN"] += 1
                    elif oc == "unverified":
                        c["UP"] += hit
                    else:
                        c["FP" if hit else "TN"] += 1
                if cfg not in DETAILED:
                    continue
                for sp in species:
                    name, oc = sp["name"], truth[sp["name"]]
                    hit = name in predicted
                    scoped = False
                    for i, rs in enumerate(sp["rulesets"]):
                        rc = rs_counts[cfg][(name, i)]
                        if oc == "TP" and in_scope(row, rs):
                            scoped = True
                            rc["scope_confirmed"] += 1
                            rc["scope_missed"] += not hit
                        if hit and score_body(row, singles[cfg][name][i]):
                            rc["TP" if oc == "TP" else "UP" if oc == "unverified" else "FP"] += 1
                    if oc == "TP" and not scoped:
                        out_of_scope[cfg][name] += 1

    report = {}
    for cfg in CONFIGS:
        total = Counter()
        species_out = {}
        for sp in species_by_cfg[cfg]:
            c = sp_counts[cfg][sp["name"]]
            total.update(c)
            entry = {k: c[k] for k in ("TP", "FN", "FP", "TN", "UP")}
            entry.update(FNR=pct(c["FN"], c["TP"] + c["FN"]), FDR=pct(c["FP"], c["TP"] + c["FP"]),
                         FPR=pct(c["FP"], c["FP"] + c["TN"]))
            if cfg in DETAILED:
                negatives = c["FP"] + c["TN"]
                entry["missed_outside_all_rulesets"] = out_of_scope[cfg][sp["name"]]
                entry["rulesets"] = []
                for i, rs in enumerate(sp["rulesets"]):
                    rc = rs_counts[cfg][(sp["name"], i)]
                    entry["rulesets"].append({
                        "index": i, "definition": rs,
                        "TP": rc["TP"], "FP": rc["FP"], "UP": rc["UP"],
                        "FDR": pct(rc["FP"], rc["TP"] + rc["FP"]), "FPR": pct(rc["FP"], negatives),
                        "scope_confirmed": rc["scope_confirmed"], "scope_missed": rc["scope_missed"],
                        "scope_FNR": pct(rc["scope_missed"], rc["scope_confirmed"]),
                    })
            species_out[sp["name"]] = entry
        report[cfg] = {
            "total": {**{k: total[k] for k in ("TP", "FN", "FP", "TN", "UP")},
                      "FNR": pct(total["FN"], total["TP"] + total["FN"]),
                      "FDR": pct(total["FP"], total["TP"] + total["FP"]),
                      "FPR": pct(total["FP"], total["FP"] + total["TN"])},
            "species": species_out,
        }
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(report, indent=2))
    for cfg, r in report.items():
        t = r["total"]
        print(f"{cfg:<44} TP={t['TP']:>7} FN={t['FN']:>5} FP={t['FP']:>6} TN={t['TN']:>8} UP={t['UP']:>6} "
              f"FNR={t['FNR']:.2f}% FDR={t['FDR']:.2f}% FPR={t['FPR']:.3f}%")
    print(f"Saved {args.output}")


if __name__ == "__main__":
    main()
