"""Check that rulesets translated from SrvSurvey match the original branches body for body.

PAIRS lists (species, index in HIGH_VALUE_SPECIES rulesets, SrvSurvey branch index).
"""

import sys
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mgst.bio.species_rules import BODY_COLUMNS, HIGH_VALUE_SPECIES, RELEVANT_BODY_TYPES, score_body
from compare_engines import ATM_COLUMNS, DATABASE
from srvsurv_parser import evaluate_branch, get_all_branches

PAIRS = [
    ("Stratum Tectonicas", 4, 2),
    ("Stratum Tectonicas", 5, 3),
    ("Osseus Discus", 2, 0),
    ("Osseus Discus", 3, 1),
    ("Osseus Discus", 4, 2),
    ("Osseus Discus", 5, 3),
    ("Recepta Deltahedronix", 0, 5),
    ("Recepta Deltahedronix", 1, 6),
    ("Recepta Deltahedronix", 2, 7),
    ("Recepta Deltahedronix", 3, 8),
]
EXTRA_SECTORS = ["Eol_Prou", "Graea_Hypue", "Praea_Euq"]


def body_tables():
    yield "landmark bodies", pq.read_table("output/landmark_bodies/landmark_bodies.parquet")
    cols = list(dict.fromkeys(BODY_COLUMNS + ATM_COLUMNS))
    for name in EXTRA_SECTORS:
        path = DATABASE / name / "bodies.parquet"
        available = set(pq.read_schema(path).names)
        yield f"{name} thin-atmosphere bodies", pq.read_table(
            path,
            columns=[c for c in cols if c in available],
            filters=pc.field("subType").isin(RELEVANT_BODY_TYPES)
            & pc.starts_with(pc.field("atmosphereType"), "Thin"),
        )


def main():
    species = {sp["name"]: sp for sp in HIGH_VALUE_SPECIES}
    branches = get_all_branches()
    # One ruleset per pseudo-species, with no species-level gates
    single = {
        (name, rs_i, b_i): [{"name": "x", "min_value": 0, "rulesets": [species[name]["rulesets"][rs_i]]}]
        for name, rs_i, b_i in PAIRS
    }
    totals = {k: [0, 0, 0] for k in single}  # both, ruleset only, branch only
    for label, table in body_tables():
        print(f"Checking {len(table):,} {label}", flush=True)
        for batch in table.to_batches(max_chunksize=50_000):
            for row in batch.to_pylist():
                for key, sp_list in single.items():
                    mine = bool(score_body(row, sp_list))
                    theirs = evaluate_branch(row, branches[key[0]][key[2]], None)
                    if mine and theirs:
                        totals[key][0] += 1
                    elif mine:
                        totals[key][1] += 1
                    elif theirs:
                        totals[key][2] += 1

    ok = True
    print(f"\n{'Species':<24}{'ruleset':>8}{'branch':>7}{'both match':>11}{'MGST only':>10}{'SrvSurvey only':>15}")
    for (name, rs_i, b_i), (both, m_only, s_only) in totals.items():
        print(f"{name:<24}{rs_i:>8}{'b%d' % b_i:>7}{both:>11,}{m_only:>10}{s_only:>15}")
        ok &= m_only == 0 and s_only == 0
    print("\nAll translations identical" if ok else "\nMISMATCHES FOUND")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
