"""Analyze MGST vs SrvSurvey comparison results and propose rule merges.

Reads comparison_summary.json and disagreement sample files from compare_engines.py,
produces a structured rule diff, impact scores, and specific improvement proposals.
"""

import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mgst.bio.species_rules import HIGH_VALUE_SPECIES
from srvsurv_parser import (
    BODY_MAP,
    GAS_COLUMN_MAP,
    _load_criteria,
    get_all_branches,
)


def load_comparison(output_dir: str) -> Tuple[dict, Dict[str, Dict[str, list]]]:
    """Load comparison summary and disagreement samples."""
    d = Path(output_dir)
    with open(d / "comparison_summary.json") as f:
        summary = json.load(f)

    samples: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for fn in d.glob("samples_*.json"):
        parts = fn.stem.split("_", 1)
        if len(parts) < 2:
            continue
        rest = parts[1]
        # samples_Species_Name_CATEGORY.json
        for cat in [
            "BOTH_TP", "BOTH_FP", "MGST_ONLY_TP", "SRV_ONLY_TP",
            "MGST_ONLY_FP", "SRV_ONLY_FP", "BOTH_FN",
        ]:
            if rest.endswith(f"_{cat}"):
                sp_name = rest[: -len(f"_{cat}")].replace("_", " ")
                with open(fn) as f2:
                    samples[sp_name][cat] = json.load(f2)
                break

    return summary, dict(samples)


def describe_clauses(clauses: list) -> dict:
    """Summarize a branch's clauses into a readable dict."""
    desc: Dict[str, Any] = {}
    for c in clauses:
        t = c["type"]
        if t == "range":
            prop = c["prop"]
            lo, hi = c.get("min"), c.get("max")
            desc[prop] = {"min": lo, "max": hi}
        elif t == "is":
            desc[c["prop"]] = c["values"]
        elif t == "not":
            desc[f"!{c['prop']}"] = c["values"]
        elif t == "atmosComp":
            conds = []
            for cond in c["conditions"]:
                conds.append(f"{cond['column']} >= {cond['min']}")
            desc["atmosComp"] = " OR ".join(conds)
        elif t == "regions":
            prefix = "!" if c["negated"] else ""
            desc[f"{prefix}regions"] = sorted(c["ids"])
    return desc


def extract_mgst_rulesets(species_name: str) -> List[dict]:
    """Get MGST ruleset details for a species."""
    for sp in HIGH_VALUE_SPECIES:
        if sp["name"] == species_name:
            return sp.get("rulesets", [])
    return []


def extract_srv_branches(species_name: str) -> List[dict]:
    """Get SrvSurvey branch summaries for a species."""
    _load_criteria()
    all_branches = get_all_branches()
    branches = all_branches.get(species_name, [])
    return [describe_clauses(b) for b in branches]


def analyze_samples(bodies: list, field: str, threshold: float, op: str = ">=") -> dict:
    """Count how many sample bodies pass/fail a threshold on a field."""
    above = 0
    below = 0
    missing = 0
    for body in bodies:
        val = body.get(field)
        if val is None:
            missing += 1
        elif op == ">=" and val >= threshold:
            above += 1
        elif op == "<=" and val <= threshold:
            above += 1
        else:
            below += 1
    return {"pass": above, "fail": below, "missing": missing, "total": len(bodies)}


def diff_species(species_name: str, summary: dict, samples: dict) -> dict:
    """Produce a full diff analysis for one species."""
    mgst_rs = extract_mgst_rulesets(species_name)
    srv_branches = extract_srv_branches(species_name)
    comp = summary.get(species_name, {})

    sp_samples = samples.get(species_name, {})

    return {
        "name": species_name,
        "comparison": comp,
        "mgst_rulesets": mgst_rs,
        "srv_branches": srv_branches,
        "sample_counts": {
            cat: len(sp_samples.get(cat, []))
            for cat in ["MGST_ONLY_TP", "SRV_ONLY_TP", "MGST_ONLY_FP",
                        "SRV_ONLY_FP", "BOTH_FN"]
        },
    }


def print_species_diff(species_name: str, summary: dict, samples: dict):
    """Print a human-readable diff analysis for one species."""
    comp = summary.get(species_name, {})
    sp_samples = samples.get(species_name, {})

    print(f"\n{'='*80}")
    print(f"  {species_name}")
    print(f"{'='*80}")

    # Summary metrics
    print(f"\n  MGST:     precision={comp.get('mgst_precision',0):.1f}%  "
          f"recall={comp.get('mgst_recall',0):.1f}%")
    print(f"  SrvSurvey: precision={comp.get('srv_precision',0):.1f}%  "
          f"recall={comp.get('srv_recall',0):.1f}%")

    both_tp = comp.get("BOTH_TP", 0)
    m_tp = comp.get("MGST_ONLY_TP", 0)
    s_tp = comp.get("SRV_ONLY_TP", 0)
    m_fp = comp.get("MGST_ONLY_FP", 0)
    s_fp = comp.get("SRV_ONLY_FP", 0)
    b_fn = comp.get("BOTH_FN", 0)
    both_fp = comp.get("BOTH_FP", 0)

    print(f"\n  Agreement: {both_tp} TP,  {both_fp} FP,  {b_fn} FN")
    print(f"  MGST only: {m_tp} TP,  {m_fp} FP")
    print(f"  SrvSurvey only: {s_tp} TP,  {s_fp} FP")

    # MGST rulesets
    mgst_rs = extract_mgst_rulesets(species_name)
    print(f"\n  MGST rulesets ({len(mgst_rs)}):")
    for i, rs in enumerate(mgst_rs):
        atm = rs.get("atmosphere", [])
        bt = rs.get("body_type", ["any"])
        g = f"g=[{rs.get('min_gravity','')},{rs.get('max_gravity','')}]"
        t = f"t=[{rs.get('min_temperature','')},{rs.get('max_temperature','')}]"
        p_parts = []
        if "min_pressure" in rs:
            p_parts.append(f">={rs['min_pressure']}")
        if "max_pressure" in rs:
            p_parts.append(f"<={rs['max_pressure']}")
        p = f"p=[{','.join(p_parts)}]" if p_parts else ""
        v = rs.get("volcanism", "-")
        print(f"    rs{i}: {atm} {bt} {g} {t} {p} volc={v}")

    # SrvSurvey branches
    srv = extract_srv_branches(species_name)
    print(f"\n  SrvSurvey branches ({len(srv)}):")
    for i, b in enumerate(srv):
        parts = []
        for k, v in b.items():
            parts.append(f"{k}={v}")
        print(f"    b{i}: {', '.join(parts)}")

    # Analyze disagreement samples
    for cat in ["SRV_ONLY_TP", "MGST_ONLY_FP", "SRV_ONLY_FP", "BOTH_FN"]:
        bodies = sp_samples.get(cat, [])
        if not bodies:
            continue
        print(f"\n  {cat} samples ({len(bodies)}):")

        # Atmosphere breakdown
        atm_counts = defaultdict(int)
        body_counts = defaultdict(int)
        for b in bodies:
            atm = b.get("atmosphereType", "?")
            atm_counts[atm] += 1
            body_counts[b.get("subType", "?")] += 1
        for atm, n in sorted(atm_counts.items(), key=lambda x: -x[1]):
            print(f"    atmosType={atm}: {n}")
        for bt, n in sorted(body_counts.items(), key=lambda x: -x[1]):
            print(f"    bodyType={bt}: {n}")

        # Key stat distributions
        for field, label in [
            ("gravity", "gravity"),
            ("surfaceTemperature", "temp"),
            ("surfacePressure", "pressure"),
        ]:
            vals = [b[field] for b in bodies if b.get(field) is not None]
            if vals:
                vals.sort()
                p5 = vals[int(len(vals) * 0.05)] if len(vals) > 20 else vals[0]
                p50 = vals[len(vals) // 2]
                p95 = vals[int(len(vals) * 0.95)] if len(vals) > 20 else vals[-1]
                print(f"    {label}: p5={p5:.4f} p50={p50:.4f} p95={p95:.4f}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Analyze MGST vs SrvSurvey comparison")
    parser.add_argument("--input", default="output/engine_comparison_20261016/")
    parser.add_argument("--species", nargs="*", help="Specific species to analyze (default: all)")
    args = parser.parse_args()

    summary, samples = load_comparison(args.input)

    target_species = args.species
    if not target_species:
        target_species = [sp["name"] for sp in HIGH_VALUE_SPECIES]

    for sp_name in target_species:
        if sp_name in summary:
            print_species_diff(sp_name, summary, samples)

    # Print aggregate summary
    print(f"\n\n{'='*80}")
    print("  AGGREGATE SUMMARY")
    print(f"{'='*80}\n")

    print(f"{'Species':<28} {'M_Prec':>7} {'S_Prec':>7} {'M_Rec':>6} {'S_Rec':>6}  Winner")
    print("-" * 80)
    for sp_def in HIGH_VALUE_SPECIES:
        sp = sp_def["name"]
        c = summary.get(sp, {})
        mp = c.get("mgst_precision", 0)
        sp_prec = c.get("srv_precision", 0)
        mr = c.get("mgst_recall", 0)
        sr = c.get("srv_recall", 0)

        # Determine winner (better F1)
        mf1 = 2 * mp * mr / (mp + mr) if (mp + mr) > 0 else 0
        sf1 = 2 * sp_prec * sr / (sp_prec + sr) if (sp_prec + sr) > 0 else 0
        winner = "MGST" if mf1 > sf1 else "SRV" if sf1 > mf1 else "TIE"

        print(f"{sp:<28} {mp:>6.1f}% {sp_prec:>6.1f}% {mr:>5.1f}% {sr:>5.1f}%  {winner}")


if __name__ == "__main__":
    main()
