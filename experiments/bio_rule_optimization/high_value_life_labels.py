"""Compare definitions of "this candidate body has no life" on the output of high_value_life_audit.py.

A biological signal is never recorded as zero, so a blank means either no life or no adequate scan. A usable definition
should give a similar share of bodies with life whichever year the data comes from. Reports that share under each
definition, by year of last update, distance from Sol and species, and writes report.txt.
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ODYSSEY = "2021-05-19"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--audit", type=Path, required=True, help="Output of high_value_life_audit.py")
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)
    cols = ["system_id64", "species", "x", "y", "z", "bio", "signals_updateTime", "updateTime", "landmark_count", "body_count",
            "bodies_on_record", "system_bio_bodies", "system_signal_bodies"]
    d = pd.concat([pd.read_parquet(p, columns=cols) for p in sorted((args.audit / "candidates").glob("part_*.parquet"))], ignore_index=True)
    d["life"] = d.bio.fillna(0).gt(0) | d.landmark_count.fillna(0).gt(0)
    d["year"] = d.updateTime.str[:4]
    d["other_bio"] = d.system_bio_bodies - d.bio.fillna(0).gt(0).astype(int)
    d["other_signal"] = d.system_signal_bodies - d.signals_updateTime.notna().astype(int)
    d["post"] = d.updateTime >= ODYSSEY
    d["full"] = d.body_count.notna() & (d.bodies_on_record >= d.body_count)
    d["sol"] = np.sqrt(d.x ** 2 + d.y ** 2 + d.z ** 2)
    defs = {
        "Everything": pd.Series(True, index=d.index),
        "Updated after Odyssey": d.post,
        "Updated after Odyssey, system fully on record": d.post & d.full,
        "Updated after Odyssey, another body has signals": d.post & (d.other_signal > 0),
        "Updated after Odyssey, another body has life": d.post & (d.other_bio > 0),
        "Own signal record (life or other signals only)": d.signals_updateTime.notna(),
        "Updated 2025 or later": d.updateTime >= "2025",
        "Updated 2025 or later, another body has signals": (d.updateTime >= "2025") & (d.other_signal > 0),
    }
    lines = [f"{len(d):,} candidate bodies in {d.system_id64.nunique():,} systems; recorded signal of zero: {(d.bio == 0).sum():,}",
             f"With life: {d.life.sum():,}; landmark but no signal count: {(d.landmark_count.fillna(0).gt(0) & d.bio.isna()).sum():,}; "
             f"signal record without life: {(d.signals_updateTime.notna() & ~d.life).sum():,}", ""]
    rows, by_year = [], {}
    for name, m in defs.items():
        s = d[m]
        rows.append({"definition": name, "bodies": len(s), "with_life": int(s.life.sum()), "share": round(s.life.mean(), 3),
                     "systems_with_life_share": round(s.groupby("system_id64").life.max().mean(), 3)})
        by_year[name] = s[s.year >= "2021"].groupby("year").life.mean().round(3)
    pd.set_option("display.width", 220, "display.max_columns", 40, "display.max_rows", 200, "display.max_colwidth", 60)
    lines += [pd.DataFrame(rows).to_string(index=False), "", "Share with life by year of last update:", pd.DataFrame(by_year).T.to_string(), ""]
    p = d[d.post].copy()
    p["sol_band"] = pd.cut(p.sol, [0, 500, 2000, 5000, 10000, 20000, 30000, 100000])
    t = p.groupby("sol_band", observed=True).agg(bodies=("life", "size"), share=("life", "mean"), recent=("year", lambda y: (y >= "2025").mean())).round(3)
    lines += ["Updated after Odyssey, by distance from Sol:", t.to_string(), ""]
    r = d[(d.updateTime >= "2025")].copy()
    r["sol_band"] = pd.cut(r.sol, [0, 500, 2000, 5000, 10000, 20000, 30000, 100000])
    lines += ["Updated 2025 or later, by distance from Sol:", r.groupby("sol_band", observed=True).agg(bodies=("life", "size"), share=("life", "mean")).round(3).to_string(), ""]
    # Do bodies in one system share a fate, given the same scan era?
    sib = r[r.bodies_on_record > 0]
    sib = sib.assign(n=sib.groupby("system_id64").life.transform("size"), s=sib.groupby("system_id64").life.transform("sum"))
    sib = sib[sib.n >= 2]
    other = (sib.s - sib.life.astype(int)) / (sib.n - 1)
    lines += ["Updated 2025 or later, candidate bodies with another candidate in the system:",
              f"  share with life when no other has life {sib[other == 0].life.mean():.3f} ({(other == 0).sum():,}); when all others do {sib[other == 1].life.mean():.3f} ({(other == 1).sum():,})", ""]
    rows = []
    for s in sorted({x for v in d.species.unique() for x in json.loads(v)}):
        m = d.species.str.contains(s, regex=False)
        rows.append({"species": s, "bodies": int(m.sum()), "post_odyssey": round(d[m & d.post].life.mean(), 3),
                     "from_2025": round(d[m & (d.updateTime >= "2025")].life.mean(), 3), "n_2025": int((m & (d.updateTime >= "2025")).sum()),
                     "from_2025_other_signal": round(d[m & (d.updateTime >= "2025") & (d.other_signal > 0)].life.mean(), 3)})
    lines += ["By predicted species:", pd.DataFrame(rows).sort_values("bodies", ascending=False).to_string(index=False)]
    (args.output / "report.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
