"""Is the star choice behind Stratum Tectonicas over-prediction?

On complete landmark bodies (every genus in the genus list has a confirmed
species), predicts species as SrvSurvey would before the genera are known
(no genus filter), once with SrvSurvey's brightest star and once with the
orbit-averaged star. Records, for every body where either choice predicts
Stratum Tectonicas, whether it is really there, the body's signal count, and
whether the two star choices differ.
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

import srvsurv_engine as engine
from check_srvsurv_engine import BODY_COLUMNS
from sector_pool import run_sectors
from star_brightness import OrbitModel

TARGET = ("Stratum", "Tectonicas")
SAMPLES = 5_000


def check_sector(sector_dir: str):
    sector = Path(sector_dir)
    bodies_path = sector / "bodies.parquet"
    names = pq.read_schema(bodies_path).names
    if "landmark_species" not in names:
        return []
    lm = pq.read_table(bodies_path, columns=["system_id64"], filters=pc.field("landmark_species").is_valid())
    sids = set(lm.column("system_id64").to_pylist())
    if not sids:
        return []
    cols = [c for c in names if c in BODY_COLUMNS or c.startswith(("atm_", "mat_", "parent_"))]
    cols += [c for c in ("orbitalEccentricity", "orbitalInclination", "ascendingNode", "argOfPeriapsis", "orbitalPeriod") if c in names]
    bodies = pq.read_table(bodies_path, columns=cols, filters=pc.field("system_id64").isin(list(sids))).to_pylist()
    coords = {
        s["id64"]: (s["x"], s["y"], s["z"])
        for s in pq.read_table(sector / "systems.parquet", columns=["id64", "x", "y", "z"],
                               filters=pc.field("id64").isin(list(sids))).to_pylist()
    }
    by_system = defaultdict(list)
    for b in bodies:
        by_system[b["system_id64"]].append(b)

    rows = []
    for sid, sys_bodies in by_system.items():
        if sid not in coords:
            continue
        system = engine.SystemContext(sys_bodies)
        model = None
        for b in sys_bodies:
            if not b.get("landmark_species") or b.get("type") != "Planet":
                continue
            confirmed = {lm["species"] for lm in json.loads(b["landmark_species"])}
            genera = engine.known_genera_from_codex(b.get("signals_genuses")) or set()
            if not genera or not genera <= {s.split(" ")[0] for s in confirmed}:
                continue  # not complete
            props = engine.body_props(b, system, coords[sid], "srvsurvey")
            if props is None:
                continue
            srv_star = props["Star"][0]
            srv_preds = engine.predict_props(props, corrections=False)

            candidates = system.parent_stars(b)
            typed_stars = [s for s in system.stars if s.get("subType")]
            classes = {engine.flatten_star_type(engine.journal_star_type(s["subType"])) for s in typed_stars}
            if not any(
                (g, s) == TARGET
                for cls in classes
                for g, s, _ in engine.predict_props({**props, "Star": [cls]}, corrections=False)
            ):
                continue  # no star choice predicts it
            orbit_star = srv_star
            if len(typed_stars) > 1:
                model = model or OrbitModel(system, SAMPLES)
                best = max(typed_stars, key=lambda s: model.flux(b, s))
                orbit_star = engine.flatten_star_type(engine.journal_star_type(best["subType"]))
            orbit_preds = srv_preds if orbit_star == srv_star else engine.predict_props({**props, "Star": [orbit_star]}, corrections=False)

            srv_hit = any((g, s) == TARGET for g, s, _ in srv_preds)
            orbit_hit = any((g, s) == TARGET for g, s, _ in orbit_preds)
            if not (srv_hit or orbit_hit):
                continue
            rows.append({
                "name": b["name"],
                "signals": len(genera),
                "present": "Stratum Tectonicas" in confirmed,
                "confirmed": sorted(confirmed),
                "subType": b.get("subType"),
                "atmosphere": props["AtmosphereType"],
                "n_stars": len(system.stars),
                "n_candidates": len(candidates),
                "srv_star": srv_star,
                "orbit_star": orbit_star,
                "srv_predicts": srv_hit,
                "orbit_predicts": orbit_hit,
            })
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--database", default="/data/Databases_v2/galaxy_sectors_parquet")
    ap.add_argument("--output", required=True, help="New output directory (must not exist)")
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    out = Path(args.output)
    if out.exists():
        sys.exit(f"Output directory already exists: {out}")
    out.mkdir(parents=True)

    sectors = sorted(str(d) for d in Path(args.database).iterdir() if (d / "bodies.parquet").exists())
    n, keys = 0, None
    with open(out / "bodies.tsv", "w") as f:
        for rows in run_sectors(check_sector, sectors, args.workers):
            for r in rows:
                if keys is None:
                    keys = list(r.keys())
                    f.write("\t".join(keys) + "\n")
                f.write("\t".join(json.dumps(v) if isinstance(v, list) else str(v) for v in r.values()) + "\n")
                n += 1
            f.flush()
    print(f"{n} complete bodies where either star choice predicts Stratum Tectonicas -> {out / 'bodies.tsv'}")

if __name__ == "__main__":
    main()
