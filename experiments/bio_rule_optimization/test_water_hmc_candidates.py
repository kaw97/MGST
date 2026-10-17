"""Test candidate refinements for the Water-on-HMC rulesets against landmark ground truth.

Variants are built from copies of HIGH_VALUE_SPECIES; the default rules are not
modified. Reports species-level TP/FN/FP/TN/UP with FNR/FDR/FPR on all landmark
bodies and on held-out sectors, plus per-ruleset counts for changed species.
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

HMC = "High metal content body"


def _sp(species, name):
    return next(s for s in species if s["name"] == name)


def osseus_hmc_min_age(age):
    def apply(species):
        rs = _sp(species, "Osseus Discus")["rulesets"][1]
        assert rs["body_type"] == [HMC] and rs["atmosphere"] == ["Water"]
        rs["constraints"] = [{"field": "star_age", "min": float(age)}]
    return apply


def drop_tectonicas_water(species):
    sp = _sp(species, "Stratum Tectonicas")
    sp["rulesets"] = [rs for rs in sp["rulesets"] if rs["atmosphere"] != ["Water"]]


def cactoida_max_age(hmc_age=None, rocky_age=None):
    def apply(species):
        sp = _sp(species, "Cactoida Vermis")
        new = []
        for rs in sp["rulesets"]:
            if rs["atmosphere"] != ["Water"]:
                new.append(rs)
                continue
            for body, age in ((HMC, hmc_age), ("Rocky body", rocky_age)):
                part = {**rs, "body_type": [body]}
                if age is not None:
                    part["constraints"] = [{"field": "star_age", "max": float(age)}]
                new.append(part)
        sp["rulesets"] = new
    return apply


def build(*transforms):
    species = copy.deepcopy(HIGH_VALUE_SPECIES)
    for t in transforms:
        t(species)
    return species


CONFIGS = {
    "current rules": build(),
    **{f"Osseus HMC Water: star age >= {a:,}": build(osseus_hmc_min_age(a)) for a in (8000, 8500, 9000, 9500, 10000)},
    "Stratum Tectonicas: drop Water on HMC": build(drop_tectonicas_water),
    "Cactoida Water: HMC star age <= 9,500": build(cactoida_max_age(hmc_age=9500)),
    "Cactoida Water: HMC <= 10,000": build(cactoida_max_age(hmc_age=10000)),
    "Cactoida Water: HMC <= 9,500, Rocky <= 8,500": build(cactoida_max_age(hmc_age=9500, rocky_age=8500)),
}
FOCUS = ["Osseus Discus", "Stratum Tectonicas", "Cactoida Vermis"]


def rates(c):
    tp, fn, fp, tn = c["TP"], c["FN"], c["FP"], c["TN"]
    return (100 * fn / (tp + fn) if tp + fn else 0, 100 * fp / (tp + fp) if tp + fp else 0,
            100 * fp / (fp + tn) if fp + tn else 0)


def main():
    counts = {cfg: {"all": defaultdict(Counter), "test": defaultdict(Counter)} for cfg in CONFIGS}
    rs_counts = {cfg: defaultdict(Counter) for cfg in CONFIGS}
    singles = {cfg: {name: [[{**_sp(sp, name), "rulesets": [rs]}] for rs in _sp(sp, name)["rulesets"]]
                     for name in FOCUS} for cfg, sp in CONFIGS.items()}
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
                for name in FOCUS:
                    if name not in predicted:
                        continue
                    oc = truth[name]
                    for i, single in enumerate(singles[cfg][name]):
                        if score_body(row, single):
                            rs_counts[cfg][(name, i)]["TP" if oc == "TP" else "UP" if oc == "unverified" else "FP"] += 1

    report = {}
    base = counts["current rules"]
    print(f"\n{'configuration':<46}{'species':<20}{'TP':>7}{'FN':>6}{'FP':>6}{'UP':>7}{'FNR':>8}{'FDR':>8}{'FPR':>8}"
          f" | held-out FNR / FDR")
    for cfg in CONFIGS:
        report[cfg] = {}
        tot = {s: Counter() for s in ("all", "test")}
        for split in tot:
            for c in counts[cfg][split].values():
                tot[split].update(c)
        rows = [(name, counts[cfg]["all"][name], counts[cfg]["test"][name]) for name in FOCUS
                if cfg == "current rules" or counts[cfg]["all"][name] != base["all"][name]]
        rows.append(("ALL 18 SPECIES", tot["all"], tot["test"]))
        for name, c, t in rows:
            fnr, fdr, fpr = rates(c)
            tfnr, tfdr, _ = rates(t)
            print(f"{cfg:<46}{name:<20}{c['TP']:>7}{c['FN']:>6}{c['FP']:>6}{c['UP']:>7}{fnr:>7.2f}%{fdr:>7.2f}%"
                  f"{fpr:>7.3f}% | {tfnr:.2f}% / {tfdr:.2f}%")
            report[cfg][name] = {"all": dict(c), "test": dict(t)}
        print()
    print("Per-ruleset counts for focus species (TP / FP / UP):")
    for cfg in CONFIGS:
        print(f"  {cfg}")
        for name in FOCUS:
            rs = CONFIGS[cfg] and _sp(CONFIGS[cfg], name)["rulesets"]
            parts = []
            for i, r in enumerate(rs):
                k = rs_counts[cfg][(name, i)]
                label = f"{'+'.join(r['atmosphere'])}/{'+'.join(b.split()[0] for b in r.get('body_type', ['any']))}"
                if r.get("constraints"):
                    label += " " + ",".join(f"{c['field']}{'>=' + format(c['min'], 'g') if 'min' in c else '<=' + format(c['max'], 'g')}"
                                            for c in r["constraints"])
                parts.append(f"rs{i} {label}: {k['TP']}/{k['FP']}/{k['UP']}")
            print(f"    {name}: " + "; ".join(parts))
    Path("output/species_rules_eval").mkdir(parents=True, exist_ok=True)
    Path("output/species_rules_eval/water_hmc_candidates.json").write_text(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
