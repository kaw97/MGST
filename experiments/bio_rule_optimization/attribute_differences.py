"""Attribute MGST vs SrvSurvey disagreements to specific rules and rule components.

Runs on the landmark-body extract (extract_landmark_bodies.py). For each target
species and each landmark body where exactly one engine predicts the species:
  - which rule fired (SrvSurvey branch or MGST ruleset)
  - why the other engine did not fire: the failing conditions of its closest
    rule for the same atmosphere, or "no rule for this atmosphere"
  - the outcome: confirmed (TP), definitely wrong, or unverified

Definitely wrong means another species of the same genus is confirmed on the
body, or the body's records are complete and exclude the species (genus list
known and genus absent, or confirmed species count equals the bio signal count).
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from statistics import quantiles

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mgst.bio.region import find_region
from mgst.bio.species_rules import (
    HIGH_VALUE_SPECIES,
    _REGION_GROUP_SETS,
    _check_range,
    matches_atmosphere,
    matches_atmosphere_component,
    matches_body_type,
    matches_volcanism,
    score_body,
)
from srvsurv_parser import _eval_clause, get_all_branches, score_body_srvsurv

DEFAULT_SPECIES = [
    "Recepta Deltahedronix",
    "Clypeus Speculumi",
    "Osseus Discus",
    "Stratum Tectonicas",
    "Cactoida Vermis",
]
RANGE_FIELDS = {
    "gravity": "gravity",
    "temperature": "surfaceTemperature",
    "pressure": "surfacePressure",
    "temp": "surfaceTemperature",
}
SPECIES_BY_NAME = {sp["name"]: sp for sp in HIGH_VALUE_SPECIES}


# ---------------------------------------------------------------------------
# Ground-truth outcome
# ---------------------------------------------------------------------------


def _genus_token(species):
    sig = species["genus_signal"]
    return sig.replace("$Codex_Ent_", "").replace("_Genus_Name;", "")


def outcome(row, species):
    """Return 'TP', 'wrong_species', 'absent' or 'unverified'."""
    name = species["name"]
    genus_word = name.split()[0]
    confirmed = row["_lm_species"]
    if name in confirmed:
        return "TP"
    if any(s.split()[0] == genus_word for s in confirmed):
        return "wrong_species"

    bio_count = row.get("sig_$SAA_SignalType_Biological;")
    if bio_count is not None and len(confirmed) >= bio_count:
        return "absent"

    known = row["_genera"]
    token = _genus_token(species).lower()[:5]
    confirmed_genera = {s.split()[0].lower()[:5] for s in confirmed}
    if known and confirmed_genera <= known and token not in known:
        return "absent"
    return "unverified"


def _parse_lists(row):
    try:
        row["_lm_species"] = {lm["species"] for lm in json.loads(row["landmark_species"] or "[]")}
    except (json.JSONDecodeError, TypeError):
        row["_lm_species"] = set()
    genera = set()
    for col, strip in (("landmark_genuses", False), ("signals_genuses", True)):
        try:
            for g in json.loads(row.get(col) or "[]"):
                if strip:
                    g = g.replace("$Codex_Ent_", "").replace("_Genus_Name;", "")
                genera.add(g.lower()[:5])
        except (json.JSONDecodeError, TypeError):
            pass
    row["_genera"] = genera


# ---------------------------------------------------------------------------
# MGST failure reasons
# ---------------------------------------------------------------------------


def _range_failures(label, value, lo, hi):
    if value is None:
        return [f"{label} missing"]
    out = []
    if lo is not None and value < lo:
        out.append(f"{label} < {lo:g}")
    if hi is not None and value > hi:
        out.append(f"{label} > {hi:g}")
    return out


def mgst_gate_failures(species, row, region_id):
    out = []
    sc = row.get("star_class")
    excl = species.get("exclude_star_classes")
    if excl and sc and sc in excl:
        out.append(f"star class {sc} excluded")
    own = row.get("parent_star_class")
    own_excl = species.get("exclude_own_star_classes")
    if own_excl and own and own in own_excl:
        out.append(f"own star class {own} excluded")
    group = species.get("region")
    if group and (region_id is None or region_id not in _REGION_GROUP_SETS.get(group, set())):
        out.append(f"outside region {group}")
    min_d = species.get("min_distance_from_sol")
    if min_d is not None:
        x, y, z = row.get("system_x"), row.get("system_y"), row.get("system_z")
        if x is None or (x * x + y * y + z * z) < min_d * min_d:
            out.append(f"within {min_d:g} ly of Sol")
    return out


def mgst_ruleset_failures(rs, row):
    out = []
    sc = row.get("star_class")
    if rs.get("exclude_star_classes") and sc and sc in rs["exclude_star_classes"]:
        out.append(f"star class {sc} excluded (ruleset)")
    if rs.get("body_type") is not None and not matches_body_type(row.get("subType"), rs["body_type"]):
        out.append("body type not in ruleset")
    if not matches_volcanism(row.get("volcanismType"), rs.get("volcanism")):
        out.append(f"volcanism must be {rs.get('volcanism')}")
    out += _range_failures("gravity", row.get("gravity"), rs.get("min_gravity"), rs.get("max_gravity"))
    out += _range_failures("temperature", row.get("surfaceTemperature"), rs.get("min_temperature"), rs.get("max_temperature"))
    if "min_pressure" in rs or "max_pressure" in rs:
        out += _range_failures("pressure", row.get("surfacePressure"), rs.get("min_pressure"), rs.get("max_pressure"))
    comp = rs.get("atmosphere_component")
    if comp and not matches_atmosphere_component(row, comp):
        out.append("atm component " + ", ".join(f"{k} >= {v:g}" for k, v in comp.items()))
    comp_any = rs.get("atmosphere_component_any")
    if comp_any and not any(matches_atmosphere_component(row, {k: v}) for k, v in comp_any.items()):
        out.append("atm component any of " + ", ".join(f"{k} >= {v:g}" for k, v in comp_any.items()))
    for c in rs.get("constraints") or []:
        val = row.get(c["field"])
        if val is None:
            if c.get("required"):
                out.append(f"constraint {c['field']} missing")
            continue
        if ("min" in c and val < c["min"]) or ("max" in c and val > c["max"]):
            out.append(f"constraint {c['field']} {c}")
    return out


def mgst_ruleset_label(i, rs):
    bodies = "/".join(b.replace(" body", "").replace("High metal content", "HMC").replace(" world", "")
                      for b in rs.get("body_type", ["any body"]))
    return f"rs{i} {'+'.join(rs['atmosphere'])} on {bodies}"


def mgst_why_not(species, row, region_id):
    gate = mgst_gate_failures(species, row, region_id)
    candidates = [(i, rs) for i, rs in enumerate(species["rulesets"])
                  if matches_atmosphere(row.get("atmosphereType"), rs["atmosphere"])]
    if not candidates:
        return gate + [f"no MGST ruleset for {row.get('atmosphereType')}"], None
    best = min(candidates, key=lambda c: len(mgst_ruleset_failures(c[1], row)))
    return gate + mgst_ruleset_failures(best[1], row), mgst_ruleset_label(*best)


def mgst_predicts(species, row, region_id):
    if mgst_gate_failures(species, row, region_id):
        return False
    return any(
        matches_atmosphere(row.get("atmosphereType"), rs["atmosphere"])
        and not mgst_ruleset_failures(rs, row)
        for rs in species["rulesets"]
    )


def mgst_matching_ruleset(species, row):
    for i, rs in enumerate(species["rulesets"]):
        if (matches_atmosphere(row.get("atmosphereType"), rs["atmosphere"])
                and not mgst_ruleset_failures(rs, row)):
            return mgst_ruleset_label(i, rs)
    return "?"


# ---------------------------------------------------------------------------
# SrvSurvey failure reasons
# ---------------------------------------------------------------------------


def clause_label(c):
    t = c["type"]
    if t == "range":
        lo = "" if c["min"] is None else f"{c['min']:g}"
        hi = "" if c["max"] is None else f"{c['max']:g}"
        return f"{c['prop']} [{lo}~{hi}]"
    if t == "is":
        vals = c["values"]
        if c["prop"] == "volcanism" and len(vals) > 3:
            return f"volcanism in list of {len(vals)} ({', '.join(vals[:2])}, ...)"
        return f"{c['prop']} in {vals}"
    if t == "not":
        return f"{c['prop']} not {c['values']}"
    if t == "atmosComp":
        return "atmosComp " + " | ".join(f"{d['column'].replace('atm_', '')}>={d['min']:g}" for d in c["conditions"])
    if t == "regions":
        return ("not " if c["negated"] else "") + "in SrvSurvey regions"
    return t


def branch_label(i, clauses):
    atm = body = "?"
    for c in clauses:
        if c["type"] == "is" and c["prop"] == "atmosType":
            atm = "+".join(c["values"])
        if c["type"] == "is" and c["prop"] == "body":
            body = "+".join(c["values"])
    return f"b{i} {atm} on {body}"


def srv_failures(clauses, row, region_id):
    return [clause_label(c) for c in clauses if not _eval_clause(row, c, region_id)]


def _is_atmos_clause(c):
    return c["type"] == "is" and c["prop"] == "atmosType"


def srv_why_not(branches, row, region_id):
    candidates = [(i, b) for i, b in enumerate(branches)
                  if all(_eval_clause(row, c, region_id) for c in b if _is_atmos_clause(c))]
    if not candidates:
        return [f"no SrvSurvey branch for {row.get('atmosphereType')}"], None
    best = min(candidates, key=lambda c: len(srv_failures(c[1], row, region_id)))
    return srv_failures(best[1], row, region_id), branch_label(*best)


def srv_matching_branch(branches, row, region_id):
    for i, b in enumerate(branches):
        if not srv_failures(b, row, region_id):
            return branch_label(i, b)
    return "?"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _distance(row):
    x, y, z = row.get("system_x"), row.get("system_y"), row.get("system_z")
    return None if x is None else (x * x + y * y + z * z) ** 0.5


def _value_for(reason, row):
    key = reason.split()[0]
    if key == "within":
        return _distance(row)
    col = RANGE_FIELDS.get(key)
    return row.get(col) if col else None


def _qs(vals):
    if len(vals) < 4:
        return "n<4" if not vals else " ".join(f"{v:.4g}" for v in sorted(vals))
    q = quantiles(vals, n=10)
    return f"p10={q[0]:.4g} p50={q[4]:.4g} p90={q[8]:.4g} (n={len(vals)})"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="output/landmark_bodies/landmark_bodies.parquet")
    parser.add_argument("--species", nargs="*", default=DEFAULT_SPECIES)
    parser.add_argument("--output", default="output/landmark_bodies/attribution.json")
    args = parser.parse_args()

    all_branches = get_all_branches()
    targets = [SPECIES_BY_NAME[n] for n in args.species]

    # group key -> outcome counter / values for range reasons
    gains = {sp["name"]: defaultdict(Counter) for sp in targets}
    losses = {sp["name"]: defaultdict(Counter) for sp in targets}
    gain_vals = {sp["name"]: defaultdict(lambda: defaultdict(list)) for sp in targets}
    loss_vals = {sp["name"]: defaultdict(lambda: defaultdict(list)) for sp in targets}
    baseline = {sp["name"]: Counter() for sp in targets}
    region_cache = {}

    # Star-exclusion variants: current (main star), none, applied to the body's own star
    star_species = [sp for sp in targets if sp.get("exclude_star_classes")]
    no_excl = {sp["name"]: {k: v for k, v in sp.items() if k != "exclude_star_classes"} for sp in star_species}
    star_variants = {sp["name"]: defaultdict(Counter) for sp in star_species}
    star_unblocked = {sp["name"]: defaultdict(Counter) for sp in star_species}
    dist_bins = {sp["name"]: defaultdict(Counter) for sp in targets}

    table = pq.read_table(args.input)
    print(f"Loaded {len(table)} landmark bodies", flush=True)

    for batch in table.to_batches(max_chunksize=50_000):
        for row in batch.to_pylist():
            _parse_lists(row)
            sid = row["system_id64"]
            if sid not in region_cache:
                x, y, z = row.get("system_x"), row.get("system_y"), row.get("system_z")
                r = find_region(x, y, z) if x is not None else None
                region_cache[sid] = r[0] if r else None
            region_id = region_cache[sid]

            mgst = {n for n, _ in score_body(row)}
            srv = {n for n, _ in score_body_srvsurv(row)}

            for sp in star_species:
                name = sp["name"]
                cur = name in mgst
                none = mgst_predicts(no_excl[name], row, region_id)
                if not cur and not none:
                    continue
                parent = mgst_predicts(sp, {**row, "star_class": row.get("parent_star_class")}, region_id)
                oc = outcome(row, sp)
                for label, hit in (("main star (current)", cur), ("no exclusion", none),
                                   ("body's own star", parent)):
                    if hit:
                        star_variants[name][label][oc] += 1
                if none and not cur:
                    star_unblocked[name][f"main {row.get('star_class')}, own star {row.get('parent_star_class')}"][oc] += 1

            for sp in targets:
                name = sp["name"]
                m, s = name in mgst, name in srv
                if not m and not s:
                    continue
                oc = outcome(row, sp)
                if m:
                    baseline[name][oc] += 1
                if m == s:
                    continue
                branches = all_branches[name]
                if s:
                    reasons, near = mgst_why_not(sp, row, region_id)
                    key = (srv_matching_branch(branches, row, region_id), near, tuple(reasons))
                    gains[name][key][oc] += 1
                    if any(r.startswith("within") for r in reasons):
                        d = _distance(row)
                        if d is not None:
                            dist_bins[name][f"{int(d // 500) * 500}-{int(d // 500) * 500 + 500} ly"][oc] += 1
                    if len(reasons) == 1:
                        v = _value_for(reasons[0], row)
                        if v is not None:
                            gain_vals[name][key][oc].append(v)
                else:
                    reasons, near = srv_why_not(branches, row, region_id)
                    if any(r.startswith("body") for r in reasons):
                        reasons = reasons + [f"(body is {row.get('subType')})"]
                    key = (mgst_matching_ruleset(sp, row), near, tuple(reasons))
                    losses[name][key][oc] += 1
                    if len(reasons) == 1:
                        v = _value_for(reasons[0], row)
                        if v is not None:
                            loss_vals[name][key][oc].append(v)

    report = {}
    for sp in targets:
        name = sp["name"]
        b = baseline[name]
        print(f"\n{'=' * 100}\n{name}\n{'=' * 100}")
        print(f"MGST baseline on landmark bodies: TP={b['TP']} wrong_species={b['wrong_species']} "
              f"absent={b['absent']} unverified={b['unverified']}")
        report[name] = {"baseline": dict(b), "gains": [], "losses": []}

        for title, groups, vals, side in (
            ("Bodies only SrvSurvey predicts (what adopting its rule would add)", gains[name], gain_vals[name], "gains"),
            ("Bodies only MGST predicts (what adopting SrvSurvey's rule would remove)", losses[name], loss_vals[name], "losses"),
        ):
            print(f"\n  {title}")
            print(f"  {'TP':>6} {'wrongSp':>7} {'absent':>6} {'unver':>6}  fired / nearest-other-rule / blocking conditions")
            for key, c in sorted(groups.items(), key=lambda kv: -sum(kv[1].values())):
                fired, near, reasons = key
                print(f"  {c['TP']:>6} {c['wrong_species']:>7} {c['absent']:>6} {c['unverified']:>6}  "
                      f"{fired}  |  {near or '-'}  |  {'; '.join(reasons)}")
                v = vals.get(key)
                if v:
                    print(f"  {'':>30}value TP: {_qs(v.get('TP', []))}   "
                          f"wrong: {_qs(v.get('wrong_species', []) + v.get('absent', []))}")
                report[name][side].append({
                    "fired": fired, "nearest": near, "reasons": list(reasons), "counts": dict(c),
                })

        def _row(label, c):
            definite = c["wrong_species"] + c["absent"]
            hit = c["TP"] / (c["TP"] + definite) * 100 if c["TP"] + definite else 0
            print(f"  {c['TP']:>6} {c['wrong_species']:>7} {c['absent']:>6} {c['unverified']:>6}  "
                  f"{hit:5.1f}%  {label}")

        if name in star_variants:
            print(f"\n  Star-class exclusion {sp['exclude_star_classes']} variants (all MGST predictions)")
            print(f"  {'TP':>6} {'wrongSp':>7} {'absent':>6} {'unver':>6}  {'hit%':>6}")
            for label, c in star_variants[name].items():
                _row(label, c)
            print("  Bodies unblocked by dropping the exclusion, by main star / own star class:")
            for label, c in sorted(star_unblocked[name].items(), key=lambda kv: -sum(kv[1].values()))[:15]:
                _row(label, c)
            report[name]["star_variants"] = {k: dict(v) for k, v in star_variants[name].items()}
            report[name]["star_unblocked"] = {k: dict(v) for k, v in star_unblocked[name].items()}

        if dist_bins[name]:
            print("\n  Bodies blocked only by the distance-from-Sol limit, by distance:")
            for label, c in sorted(dist_bins[name].items(), key=lambda kv: int(kv[0].split("-")[0])):
                _row(label, c)
            report[name]["distance_bins"] = {k: dict(v) for k, v in dist_bins[name].items()}

    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nSaved {args.output}")


if __name__ == "__main__":
    main()
