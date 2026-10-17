"""Test system-level context as a discriminator for one species ruleset.

For landmark bodies the ruleset matches, reads every body in their systems and derives
system and position features (body counts by kind, distance rank, moons, siblings,
parent planet, the mass code in procedural system names). Reports ROC AUC per feature
(confirmed vs definitely wrong) and a shallow decision tree scored on held-out sectors.
"""

import argparse
import multiprocessing
import re
import sys
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq
from sklearn.metrics import roc_auc_score
from sklearn.tree import DecisionTreeClassifier, export_text

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mgst.bio.species_rules import HIGH_VALUE_SPECIES, score_body
from mgst.data.parquet_reader import ParquetSectorReader
from attribute_differences import _parse_lists, outcome
from compare_engines import DATABASE
from sweep_precision import is_test

MASS_CODE = re.compile(r" [A-Z]{2}-[A-Z] ([a-h])\d")
CONTEXT_COLUMNS = ["system_id64", "bodyId", "name", "type", "subType", "atmosphereType", "distanceToArrival",
                   "parent_0_type", "parent_0_id", "earthMasses", "isLandable", "ring_0_type", "belt_0_type"]


def system_features(sector, targets):
    """targets: {system_id64: [body name, ...]} -> {body name: features}."""
    reader = ParquetSectorReader(DATABASE / sector)
    sids = list(targets)
    bodies = reader.read_bodies(columns=CONTEXT_COLUMNS, filters=pc.field("system_id64").isin(sids)).to_pylist()
    names = {s["id64"]: s["name"] for s in reader.read_systems(
        columns=["id64", "name"], filters=pc.field("id64").isin(sids)).to_pylist()}
    by_system = defaultdict(list)
    for b in bodies:
        by_system[b["system_id64"]].append(b)
    out = {}
    for sid, wanted in targets.items():
        sys_bodies = by_system.get(sid, [])
        planets = [b for b in sys_bodies if b["type"] == "Planet"]
        stars = [b for b in sys_bodies if b["type"] == "Star"]
        thin = [b for b in planets if (b.get("atmosphereType") or "").startswith("Thin")]
        m = MASS_CODE.search(names.get(sid) or "")
        ranked = sorted(planets, key=lambda b: b.get("distanceToArrival") or 0)
        by_id = {b["bodyId"]: b for b in sys_bodies}
        system = {
            "n_stars": len(stars), "n_planets": len(planets),
            "n_landable": sum(1 for b in planets if b.get("isLandable")),
            "n_hmc": sum(1 for b in planets if b["subType"] == "High metal content world"),
            "n_thin_atm": len(thin),
            "n_thin_water": sum(1 for b in thin if b["atmosphereType"] == "Thin Water"),
            "n_gas_giants": sum(1 for b in planets if "giant" in (b["subType"] or "").lower()),
            "n_ringed": sum(1 for b in sys_bodies if b.get("ring_0_type")),
            "n_belts": sum(1 for b in stars if b.get("belt_0_type")),
            "mass_code": m.group(1) if m else "named",
        }
        for b in sys_bodies:
            if b["name"] not in wanted:
                continue
            parent = by_id.get(b.get("parent_0_id")) if b.get("parent_0_type") == "Planet" else None
            out[b["name"]] = {
                **system,
                "body_id": b["bodyId"],
                "distance_rank": next((i for i, x in enumerate(ranked) if x is b), None),
                "relative_rank": next((i for i, x in enumerate(ranked) if x is b), 0) / max(len(ranked), 1),
                "n_moons": sum(1 for x in planets if x.get("parent_0_type") == "Planet" and x.get("parent_0_id") == b["bodyId"]),
                "n_siblings": sum(1 for x in sys_bodies if x is not b and x.get("parent_0_type") == b.get("parent_0_type")
                                  and x.get("parent_0_id") == b.get("parent_0_id")),
                "is_moon": parent is not None,
                "parent_planet_type": parent["subType"] if parent else "none",
                "parent_planet_mass": parent.get("earthMasses") if parent else None,
            }
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--species", default="Stratum Tectonicas")
    parser.add_argument("--ruleset", type=int, default=3)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    species = next(s for s in HIGH_VALUE_SPECIES if s["name"] == args.species)
    single = [{**species, "rulesets": [species["rulesets"][args.ruleset]]}]
    rows = []
    for r in pq.read_table("output/landmark_bodies/landmark_bodies.parquet").to_pylist():
        if r.get("parent_star_class") in ("barycentre", "unknown"):
            r["parent_star_class"] = None
        if not score_body(r, single):
            continue
        _parse_lists(r)
        oc = outcome(r, species)
        rows.append({"name": r["name"], "system_id64": r["system_id64"], "sector": r["sector"],
                     "outcome": "TP" if oc == "TP" else "unverified" if oc == "unverified" else "wrong",
                     "test": is_test(r["sector"])})
    print(f"{args.species} rs{args.ruleset}: {len(rows):,} matched landmark bodies "
          f"{dict(Counter(r['outcome'] for r in rows))}", flush=True)

    targets = defaultdict(lambda: defaultdict(list))
    for r in rows:
        targets[r["sector"]][r["system_id64"]].append(r["name"])
    feats = {}
    ctx = multiprocessing.get_context("fork")
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=ctx) as ex:
        futures = [ex.submit(system_features, sector, dict(t)) for sector, t in targets.items()]
        for f in as_completed(futures):
            feats.update(f.result())
    df = pd.DataFrame([{**r, **feats.get(r["name"], {})} for r in rows])
    print(f"Context found for {df.n_planets.notna().sum():,} of {len(df):,} bodies")

    known = df[df.outcome != "unverified"].copy()
    known["label"] = (known.outcome == "TP").astype(int)
    numeric = [c for c in known.columns if c not in ("name", "system_id64", "sector", "outcome", "test", "label")
               and pd.api.types.is_numeric_dtype(known[c]) and not pd.api.types.is_bool_dtype(known[c])]
    print("\nFeature separation (ROC AUC, confirmed vs wrong):")
    res = []
    for c in numeric:
        sub = known[known[c].notna()]
        if sub[c].nunique() > 1:
            auc = roc_auc_score(sub.label, sub[c])
            res.append((abs(auc - 0.5), c, auc, sub[sub.label == 1][c].median(), sub[sub.label == 0][c].median()))
    for _, c, auc, mt, mw in sorted(res, reverse=True):
        print(f"  {c:<20} AUC {auc:.3f}   median confirmed {mt:g}   wrong {mw:g}")
    print("\nCategories:")
    for c in ("mass_code", "is_moon", "parent_planet_type"):
        g = known.groupby(known[c].astype(str)).label.agg(["sum", "count"])
        g = g[g["count"] >= 30]
        print(f"  {c}: " + "; ".join(f"{k}: {int(v['sum'])}/{int(v['count'])} ({100 * v['sum'] / v['count']:.0f}%)"
                                     for k, v in g.iterrows()))
    X = pd.get_dummies(known[numeric + ["mass_code", "is_moon", "parent_planet_type"]].astype(
        {"mass_code": str, "is_moon": str, "parent_planet_type": str}), dtype=float)
    tr, te = ~known.test.values, known.test.values
    tree = DecisionTreeClassifier(max_depth=3, min_samples_leaf=40, random_state=0).fit(X[tr], known.label[tr])
    print("\nDecision tree (training sectors):")
    print(export_text(tree, feature_names=list(X.columns), show_weights=True, decimals=3))
    print(f"Tree held-out ROC AUC: {roc_auc_score(known.label[te], tree.predict_proba(X[te])[:, 1]):.3f}")


if __name__ == "__main__":
    main()
