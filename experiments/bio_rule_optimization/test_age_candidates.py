"""Test main-star age refinements found by sweep_star_age.py against landmark ground truth.

Variants are copies of HIGH_VALUE_SPECIES; the default rules are not modified.
Reports species-level TP/FN/FP/TN/UP with FNR/FDR/FPR on all landmark bodies and
on held-out sectors.
"""

import copy
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mgst.bio.species_rules import HIGH_VALUE_SPECIES, score_body
from attribute_differences import _parse_lists, outcome
from sweep_precision import is_test
from test_water_hmc_candidates import _sp, cactoida_max_age, osseus_hmc_min_age, rates


def osseus_ammonia_rocky_min_age(age):
    def apply(species):
        rs = _sp(species, "Osseus Discus")["rulesets"][2]
        assert rs["atmosphere"] == ["Ammonia"] and rs["body_type"] == ["Rocky body"]
        rs["constraints"] = [{"field": "star_age", "min": float(age)}]
    return apply


def speculumi_water_skip_age_band(lo, hi):
    """Split the Water/no-volcanism ruleset so it skips star ages in [lo, hi)."""
    def apply(species):
        sp = _sp(species, "Clypeus Speculumi")
        rs = sp["rulesets"][1]
        assert rs["atmosphere"] == ["Water"] and rs.get("volcanism") == "None"
        base = rs.get("constraints", [])
        young = {**rs, "constraints": base + [{"field": "star_age", "max": float(lo)}]}
        old = {**rs, "constraints": base + [{"field": "star_age", "min": float(hi)}]}
        sp["rulesets"] = [sp["rulesets"][0], young, old] + sp["rulesets"][2:]
    return apply


def build(*transforms):
    species = copy.deepcopy(HIGH_VALUE_SPECIES)
    for t in transforms:
        t(species)
    return species


CLEAN = osseus_hmc_min_age(9000)
CONFIGS = {
    "current rules": build(),
    "A  Osseus Discus Water/HMC: age >= 9,000": build(CLEAN),
    "B  Cactoida Vermis Water/HMC: age <= 9,500": build(cactoida_max_age(hmc_age=9500)),
    "C  Osseus Discus Ammonia/Rocky: age >= 4,000": build(osseus_ammonia_rocky_min_age(4000)),
    "D  Clypeus Speculumi Water: skip ages 1,000-2,000": build(speculumi_water_skip_age_band(1000, 2000)),
    "A+B": build(CLEAN, cactoida_max_age(hmc_age=9500)),
    "A+B+C+D": build(CLEAN, cactoida_max_age(hmc_age=9500), osseus_ammonia_rocky_min_age(4000),
                     speculumi_water_skip_age_band(1000, 2000)),
}
FOCUS = ["Osseus Discus", "Cactoida Vermis", "Clypeus Speculumi"]


def main():
    counts = {cfg: {"all": defaultdict(Counter), "test": defaultdict(Counter)} for cfg in CONFIGS}
    table = pq.read_table("output/landmark_bodies/landmark_bodies.parquet")
    print(f"Testing {len(CONFIGS)} configurations on {len(table):,} landmark bodies", flush=True)
    for batch in table.to_batches(max_chunksize=50_000):
        for row in batch.to_pylist():
            if row.get("parent_star_class") in ("barycentre", "unknown"):
                row["parent_star_class"] = None
            _parse_lists(row)
            truth = {sp["name"]: outcome(row, sp) for sp in HIGH_VALUE_SPECIES}
            test = is_test(row["sector"])
            for cfg, species in CONFIGS.items():
                predicted = {n for n, _ in score_body(row, species)}
                for split in ("all", "test") if test else ("all",):
                    c = counts[cfg][split]
                    for name, oc in truth.items():
                        hit = name in predicted
                        if oc == "TP":
                            c[name]["TP" if hit else "FN"] += 1
                        elif oc == "unverified":
                            c[name]["UP"] += hit
                        else:
                            c[name]["FP" if hit else "TN"] += 1

    report = {}
    base = counts["current rules"]["all"]
    print(f"\n{'configuration':<50}{'species':<20}{'TP':>7}{'FN':>6}{'FP':>6}{'TN':>9}{'UP':>7}{'FNR':>8}{'FDR':>8}"
          f"{'FPR':>8} | held-out FNR / FDR / FPR")
    for cfg in CONFIGS:
        report[cfg] = {}
        tot = {s: Counter() for s in ("all", "test")}
        for split in tot:
            for c in counts[cfg][split].values():
                tot[split].update(c)
        rows = [(n, counts[cfg]["all"][n], counts[cfg]["test"][n]) for n in FOCUS
                if cfg == "current rules" or counts[cfg]["all"][n] != base[n]]
        rows.append(("ALL 18 SPECIES", tot["all"], tot["test"]))
        for name, c, t in rows:
            fnr, fdr, fpr = rates(c)
            tfnr, tfdr, tfpr = rates(t)
            print(f"{cfg:<50}{name:<20}{c['TP']:>7}{c['FN']:>6}{c['FP']:>6}{c['TN']:>9}{c['UP']:>7}{fnr:>7.2f}%"
                  f"{fdr:>7.2f}%{fpr:>7.3f}% | {tfnr:.2f}% / {tfdr:.2f}% / {tfpr:.3f}%")
            report[cfg][name] = {"all": dict(c), "test": dict(t)}
        print()
    Path("output/species_rules_eval/age_candidates.json").write_text(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
