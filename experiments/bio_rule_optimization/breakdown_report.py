"""Render ruleset_breakdown.json as Markdown tables comparing two configurations."""

import json
import sys
from pathlib import Path

SHORT_BODY = {"High metal content body": "HMC", "Rocky body": "Rocky", "Rocky ice body": "Rocky Ice",
              "Rocky Ice world": "Rocky Ice", "Icy body": "Icy"}


def label(rs):
    bodies = "/".join(dict.fromkeys(SHORT_BODY.get(b, b) for b in rs.get("body_type", ["any body"])))
    v = rs.get("volcanism")
    volc = "" if v is None else " no volc." if v == "None" else " volc." if isinstance(v, list) and "None" not in v else " volc. or none"
    return f"{'+'.join(rs['atmosphere'])} on {bodies}{volc}"


def f(x, d=2):
    return "–" if x is None else f"{x:.{d}f}%"


def main(path="output/species_rules_eval/ruleset_breakdown.json", a="defaults", b=None):
    """Render configuration a, or compare a against b when b is given."""
    r = json.loads(Path(path).read_text())
    b = b or a
    out = [f"| Configuration | Correct | Missed | Wrong | Correctly rejected | Unverified | FNR | FDR | FPR |",
           "|---|---|---|---|---|---|---|---|---|"]
    for cfg, c in r.items():
        t = c["total"]
        out.append(f"| {cfg} | {t['TP']:,} | {t['FN']:,} | {t['FP']:,} | {t['TN']:,} | {t['UP']:,} | "
                   f"{f(t['FNR'])} | {f(t['FDR'])} | {f(t['FPR'], 3)} |")
    for name, sb in r[b]["species"].items():
        sa = r[a]["species"][name]
        changed = a != b and (
            [x["definition"] for x in sa["rulesets"]] != [x["definition"] for x in sb["rulesets"]]
            or (sa["TP"], sa["FP"], sa["UP"]) != (sb["TP"], sb["FP"], sb["UP"])
        )
        out.append(f"\n**{name}**{'' if changed or a == b else ' (unchanged)'}\n")
        out.append("| Set | Correct | Missed | Wrong | Unverified | FNR | FDR | FPR | Missed outside all rulesets |")
        out.append("|---|---|---|---|---|---|---|---|---|")
        for cfg, s in ((a, sa), (b, sb)) if changed else ((a if a == b else f"{a} = {b}", sb),):
            out.append(f"| {cfg} | {s['TP']:,} | {s['FN']:,} | {s['FP']:,} | {s['UP']:,} | {f(s['FNR'])} | "
                       f"{f(s['FDR'])} | {f(s['FPR'], 3)} | {s['missed_outside_all_rulesets']:,} |")
        out.append("\n| Set | Ruleset | Correct | Wrong | Unverified | FDR | FPR | Scope FNR (missed / confirmed in scope) |")
        out.append("|---|---|---|---|---|---|---|---|")
        for cfg, s in ((a, sa), (b, sb)) if changed else ((a if a == b else "both", sb),):
            for x in s["rulesets"]:
                out.append(f"| {cfg} | rs{x['index']} {label(x['definition'])} | {x['TP']:,} | {x['FP']:,} | {x['UP']:,} | "
                           f"{f(x['FDR'], 1)} | {f(x['FPR'], 3)} | {f(x['scope_FNR'])} ({x['scope_missed']:,} / {x['scope_confirmed']:,}) |")
    text = "\n".join(out) + "\n"
    Path(path).with_suffix(".md").write_text(text)
    print(text)


if __name__ == "__main__":
    main(*sys.argv[1:])
