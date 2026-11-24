"""Compare SrvSurvey's brightest-star choice with the orbit-averaged model for named bodies.

For each body: ranks every star in its system under both models, shows the
species/colour predictions each star choice gives (via srvsurv_engine), and,
when a raw Spansh body record is supplied, the confirmed species and colours.

    python -I compare_star_models.py --sector Blau_Eur --body "Blau Eur NI-T d3-40 B 6" \
        [--api-dir DIR_WITH_<id64>.json]
"""

import argparse
import json
import sys
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

import srvsurv_engine as engine
from star_brightness import rank_stars


def load_system(database: Path, sector: str, body_name: str):
    bodies_path = database / sector / "bodies.parquet"
    hit = pq.read_table(bodies_path, columns=["system_id64"], filters=pc.field("name") == body_name)
    if hit.num_rows == 0:
        sys.exit(f"Body not found in {sector}: {body_name}")
    sid = hit.column("system_id64")[0].as_py()
    bodies = pq.read_table(bodies_path, filters=pc.field("system_id64") == sid).to_pylist()
    s = pq.read_table(database / sector / "systems.parquet", filters=pc.field("id64") == sid).to_pylist()[0]
    return bodies, (s["x"], s["y"], s["z"])


def confirmed_from_api(api_dir: Path, id64: int):
    path = api_dir / f"{id64}.json"
    if not path.exists():
        return None
    rec = json.loads(path.read_text())
    rec = rec.get("record", rec)
    return sorted({(lm["subtype"], lm.get("variant")) for lm in rec.get("landmarks") or []})


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--database", type=Path, default=Path("/data/Databases_v2/galaxy_sectors_parquet"))
    ap.add_argument("--sector", required=True)
    ap.add_argument("--body", required=True)
    ap.add_argument("--api-dir", type=Path, help="Directory of raw Spansh body records named <id64>.json")
    ap.add_argument("--samples", type=int, default=200_000)
    args = ap.parse_args()

    bodies, coords = load_system(args.database, args.sector, args.body)
    system = engine.SystemContext(bodies)
    body = next(b for b in bodies if b["name"] == args.body)
    ranked = rank_stars(system, body, args.samples)

    srv_total = sum(r["srvsurvey_score"] or 0 for r in ranked)
    flux_total = sum(r["flux"] for r in ranked)
    srv_pick = system.brightest_star(body)
    print(f"\n=== {args.body}")
    print(f"{'star':34} {'class':6} {'L (sun)':>10} {'SrvSurvey share':>16} {'avg d':>9} {'flux share':>11}")
    for r in ranked:
        s = r["star"]
        cls = engine.flatten_star_type(engine.journal_star_type(s.get("subType")))
        srv = f"{r['srvsurvey_score'] / srv_total:16.2%}" if r["srvsurvey_score"] is not None and srv_total else f"{'not candidate':>16}"
        print(f"{s['name']:34} {cls:6} {r['luminosity']:10.3g} {srv} {r['avg_distance']:9.3g} {r['flux'] / flux_total:11.2%}")
    print(f"SrvSurvey picks: {srv_pick['name'] if srv_pick else None}   orbit-averaged picks: {ranked[0]['star']['name']}")

    props = engine.body_props(body, system, coords, "srvsurvey")
    if props is None:
        print("SrvSurvey makes no prediction (no parent star)")
        return
    known = engine.known_genera_from_codex(body.get("signals_genuses"))
    choices = {"SrvSurvey": props["Star"][0],
               "orbit-averaged": engine.flatten_star_type(engine.journal_star_type(ranked[0]["star"].get("subType")))}
    for label, cls in choices.items():
        preds = engine.predict_props({**props, "Star": [cls]}, known, corrections=False)
        print(f"{label:15} star {cls:5}: " + ", ".join(f"{g} {s} - {v}" for g, s, v in sorted(preds)))

    if args.api_dir:
        conf = confirmed_from_api(args.api_dir, int(body["id64"]))
        if conf is not None:
            print(f"{'confirmed':15}           : " + (", ".join(f"{s} - {v}" for s, v in conf) or "(no landmarks)"))


if __name__ == "__main__":
    main()
