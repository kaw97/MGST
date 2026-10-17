"""Sweep every ruleset for cuts that remove false positives at little true-positive cost.

Runs on the landmark-body extract with the current species rules.
For each species ruleset (evaluated on its own, with species-level gates) it tests:
  - threshold cuts: raise a minimum / lower a maximum on numeric body properties,
    at thresholds that sacrifice a small share of the ruleset's confirmed bodies
  - category exclusions: drop one value of a categorical property

Thresholds are chosen on a training split (two thirds of sectors) and checked on
the held-out third; candidates must hold up on both. Outcomes follow
attribute_differences.outcome(): confirmed, definitely wrong, or unverified.
"""

import argparse
import json
import sys
import zlib
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mgst.bio.region import find_region
from mgst.bio.species_rules import HIGH_VALUE_SPECIES, score_body
from attribute_differences import _parse_lists, outcome

NUMERIC = [
    "gravity", "surfaceTemperature", "surfacePressure", "distanceToArrival", "dist_sol",
    "atm_Carbon_dioxide", "atm_Sulphur_dioxide", "atm_Water", "atm_Ammonia", "atm_Methane",
    "atm_Argon", "atm_Oxygen", "atm_Nitrogen", "atm_Neon",
]
CATEGORICAL = ["subType", "atmosphereType", "volcanismType", "star_class", "parent_star_class", "region"]
TP_SACRIFICE = [0.0025, 0.005, 0.01, 0.02, 0.05]
MIN_FP_REMOVED = 25


def is_test(sector):
    return zlib.crc32(sector.encode()) % 3 == 0


def tally(records, keep):
    c = Counter()
    for r in records:
        if not keep(r):
            c[r["oc"]] += 1
    return c


def hit(tp, fp):
    return tp / (tp + fp) if tp + fp else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="output/landmark_bodies/landmark_bodies.parquet")
    parser.add_argument("--output", default="output/precision_sweep/")
    args = parser.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    species = HIGH_VALUE_SPECIES
    rulesets = [(sp, i, [{**sp, "rulesets": [rs]}]) for sp in species for i, rs in enumerate(sp["rulesets"])]
    records = defaultdict(list)  # (species, ruleset index) -> body records
    species_totals = defaultdict(Counter)
    region_cache = {}

    table = pq.read_table(args.input)
    print(f"Sweeping {len(rulesets)} rulesets on {len(table):,} landmark bodies", flush=True)
    for batch in table.to_batches(max_chunksize=50_000):
        for row in batch.to_pylist():
            if row.get("parent_star_class") in ("barycentre", "unknown"):
                row["parent_star_class"] = None
            _parse_lists(row)
            truth = {}
            for sp, i, single in rulesets:
                if not score_body(row, single):
                    continue
                name = sp["name"]
                if name not in truth:
                    truth[name] = outcome(row, sp)
                    species_totals[name][truth[name]] += 1
                if "region" not in row:
                    sid = row["system_id64"]
                    if sid not in region_cache:
                        x, y, z = row.get("system_x"), row.get("system_y"), row.get("system_z")
                        reg = find_region(x, y, z) if x is not None else None
                        region_cache[sid] = reg[1] if reg else None
                    row["region"] = region_cache[sid]
                    x, y, z = row.get("system_x"), row.get("system_y"), row.get("system_z")
                    row["dist_sol"] = (x * x + y * y + z * z) ** 0.5 if x is not None else None
                    v = row.get("volcanismType")
                    row["volcanismType"] = v if v and v != "No volcanism" else "None"
                oc = truth[name]
                records[(name, i)].append({
                    "oc": "FP" if oc in ("wrong_species", "absent") else oc,
                    "test": is_test(row["sector"]),
                    **{f: row.get(f) for f in NUMERIC + CATEGORICAL},
                })

    # Species-level confusion counts on landmark bodies (TP/FN need all confirmed bodies)
    confirmed = Counter()
    for batch in table.select(["landmark_species"]).to_batches():
        for lm in batch.column(0).to_pylist():
            for s in json.loads(lm or "[]"):
                confirmed[s["species"]] += 1

    report = {"rulesets": []}
    lines = []
    for sp, i, _ in rulesets:
        name = sp["name"]
        recs = records[(name, i)]
        base = Counter(r["oc"] for r in recs)
        tp, fp = base["TP"], base["FP"]
        st = species_totals[name]
        entry = {
            "species": name, "ruleset": i, "definition": sp["rulesets"][i],
            "TP": tp, "FP": fp, "UP": base["unverified"], "hit": hit(tp, fp),
            "species_TP": st["TP"], "species_FP": st["wrong_species"] + st["absent"],
            "species_confirmed": confirmed[name], "threshold_cuts": [], "category_cuts": [],
        }
        lines.append(f"\n{name} rs{i}: TP={tp} FP={fp} UP={base['unverified']} "
                     f"hit={100 * (hit(tp, fp) or 0):.1f}%  {json.dumps(sp['rulesets'][i])}")
        if fp < MIN_FP_REMOVED:
            report["rulesets"].append(entry)
            continue
        train = [r for r in recs if not r["test"]]
        test = [r for r in recs if r["test"]]
        base_hit = hit(tp, fp)

        for feat in NUMERIC:
            tp_vals = sorted(r[feat] for r in train if r["oc"] == "TP" and r[feat] is not None)
            if len(tp_vals) < 20:
                continue
            for side in ("min", "max"):
                seen = set()
                for q in TP_SACRIFICE:
                    k = int(q * len(tp_vals))
                    t = tp_vals[k] if side == "min" else tp_vals[-1 - k]
                    if t in seen:
                        continue
                    seen.add(t)
                    keep = (lambda r, t=t: r[feat] is None or r[feat] >= t) if side == "min" else \
                           (lambda r, t=t: r[feat] is None or r[feat] <= t)
                    full, tr, te = tally(recs, keep), tally(train, keep), tally(test, keep)
                    if full["FP"] < MIN_FP_REMOVED:
                        continue
                    removed_hit = hit(full["TP"], full["FP"])
                    tr_hit, te_hit = hit(tr["TP"], tr["FP"]), hit(te["TP"], te["FP"])
                    if None in (tr_hit, te_hit) or max(tr_hit, te_hit) >= base_hit / 2:
                        continue
                    entry["threshold_cuts"].append({
                        "feature": feat, "side": side, "threshold": t, "tp_sacrifice": q,
                        "TP_lost": full["TP"], "FP_removed": full["FP"], "UP_removed": full["unverified"],
                        "removed_hit_train": tr_hit, "removed_hit_test": te_hit,
                    })

        for feat in CATEGORICAL:
            for val in {r[feat] for r in recs}:
                keep = lambda r, val=val: r[feat] != val
                full, tr, te = tally(recs, keep), tally(train, keep), tally(test, keep)
                if full["FP"] < MIN_FP_REMOVED:
                    continue
                tr_hit, te_hit = hit(tr["TP"], tr["FP"]), hit(te["TP"], te["FP"])
                if None in (tr_hit, te_hit) or max(tr_hit, te_hit) >= base_hit / 2:
                    continue
                entry["category_cuts"].append({
                    "feature": feat, "exclude": val,
                    "TP_lost": full["TP"], "FP_removed": full["FP"], "UP_removed": full["unverified"],
                    "removed_hit_train": tr_hit, "removed_hit_test": te_hit,
                })

        for kind in ("threshold_cuts", "category_cuts"):
            entry[kind].sort(key=lambda c: (-(c["FP_removed"] - 3 * c["TP_lost"])))
            for c in entry[kind][:8]:
                what = (f"{c['feature']} {'>=' if c['side'] == 'min' else '<='} {c['threshold']:.6g} "
                        f"(gives up {100 * c['tp_sacrifice']:.2g}% of TP)") if kind == "threshold_cuts" \
                    else f"exclude {c['feature']} = {c['exclude']}"
                lines.append(f"    {kind[:9]:<9} -{c['FP_removed']:>6} FP  -{c['TP_lost']:>5} TP  "
                             f"-{c['UP_removed']:>5} UP  removed hit train/test "
                             f"{100 * c['removed_hit_train']:.1f}%/{100 * c['removed_hit_test']:.1f}%  {what}")
        report["rulesets"].append(entry)

    (out / "sweep.json").write_text(json.dumps(report, indent=2, default=str))
    (out / "sweep_report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
