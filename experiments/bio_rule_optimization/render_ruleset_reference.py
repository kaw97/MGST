"""Render the species ruleset reference (Markdown) from the live HIGH_VALUE_SPECIES definitions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from mgst.bio.species_rules import HIGH_VALUE_SPECIES

BODY = {"High metal content body": "HMC", "Rocky body": "Rocky", "Rocky ice body": "Rocky Ice",
        "Rocky Ice world": "Rocky Ice", "Icy body": "Icy"}


def rng(lo, hi, unit=""):
    if lo is None and hi is None:
        return "—"
    if lo is None:
        return f"≤ {hi:g}{unit}"
    if hi is None:
        return f"≥ {lo:g}{unit}"
    return f"{lo:g} – {hi:g}{unit}"


def volc(v):
    if v is None:
        return "—"
    if v == "None":
        return "none"
    return ", ".join("none" if x == "None" else x for x in v)


def extra(rs):
    parts = []
    for col, v in (rs.get("atmosphere_component") or {}).items():
        parts.append(f"{col.replace('atm_', '').replace('_', ' ')} ≥ {v:g}%")
    anyof = rs.get("atmosphere_component_any")
    if anyof:
        parts.append("any of: " + ", ".join(f"{c.replace('atm_', '').replace('_', ' ')} ≥ {v:g}%" for c, v in anyof.items()))
    for c in rs.get("constraints") or []:
        bound = f"≥ {c['min']:g}" if "min" in c else f"≤ {c['max']:g}"
        parts.append(f"{c['field']} {bound}" + (" (required)" if c.get("required") else ""))
    return "; ".join(parts) or "—"


def main():
    out = []
    for sp in HIGH_VALUE_SPECIES:
        rsets = sp["rulesets"]
        out.append(f"### {sp['name']} ({sp['value'] / 1e6:.1f}M cr) — {len(rsets)} ruleset{'s' if len(rsets) != 1 else ''}\n")
        notes = []
        if sp.get("exclude_star_classes"):
            notes.append(f"Main-star exclusions: **{', '.join(sp['exclude_star_classes'])}**")
        if sp.get("exclude_own_star_classes"):
            notes.append(f"Own-star exclusions (star the body orbits): **{', '.join(sp['exclude_own_star_classes'])}**")
        if sp.get("region"):
            notes.append(f"Region: **{sp['region']}**")
        if sp.get("min_distance_from_sol"):
            notes.append(f"Distance from Sol ≥ {sp['min_distance_from_sol']:g} ly")
        if notes:
            out.append("; ".join(notes) + "\n")
        header = "| Field | " + " | ".join(f"rs{i} ({'+'.join(rs['atmosphere'])})" for i, rs in enumerate(rsets)) + " |"
        out += [header, "|---|" + "---|" * len(rsets)]
        rows = [
            ("Body type", lambda rs: ", ".join(BODY.get(b, b) for b in rs.get("body_type", [])) or "any"),
            ("Volcanism", lambda rs: volc(rs.get("volcanism"))),
            ("Gravity (g)", lambda rs: rng(rs.get("min_gravity"), rs.get("max_gravity"))),
            ("Temperature (K)", lambda rs: rng(rs.get("min_temperature"), rs.get("max_temperature"))),
            ("Pressure (atm)", lambda rs: rng(rs.get("min_pressure"), rs.get("max_pressure"))),
            ("Other", extra),
        ]
        for name, fn in rows:
            out.append(f"| {name} | " + " | ".join(fn(rs) for rs in rsets) + " |")
        out.append("")
    print("\n".join(out))


if __name__ == "__main__":
    main()
