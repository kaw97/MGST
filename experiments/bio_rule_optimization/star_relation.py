"""For bodies where the star choice changes a prediction, record how each star in the system relates to the body.

Relations, walking up the body's parents and skipping planets:
- direct: the first star above the body
- own_barycentre: under a barycentre reached before any star
- ancestor: a star higher up the chain, which the body's star orbits
- partner: under a barycentre reached after a star, so a companion of a star above the body
- outside: none of these
Writes each star's relation, class and orbit-averaged light, and the predictions for each class, so rules choosing
among them can be scored offline with score_star_relation.py.
"""

import argparse
import glob
import json
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

import srvsurv_engine as engine
from build_bio_analysis_db import ORBIT_COLUMNS, pred_json, star_class
from build_signal_features import COLUMNS
from sector_pool import run_sectors
from star_brightness import OrbitModel

SAMPLES = 5_000
WANTED = None  # sorted id64 array, set in the parent before forking


def relations(system, body):
    """{star bodyId: relation} for stars with a known type."""
    found, seen_star = {}, False
    for ptype, pid in system._chain[int(body["bodyId"])]:
        if ptype == "Star":
            if pid in system.bodies and system.bodies[pid].get("subType"):
                found.setdefault(pid, "ancestor" if seen_star else "direct")
            seen_star = True
        elif ptype == "Null":
            for s in system.stars:
                if s.get("subType") and system._has_parent(s, pid):
                    found.setdefault(int(s["bodyId"]), "partner" if seen_star else "own_barycentre")
    for s in system.stars:
        if s.get("subType"):
            found.setdefault(int(s["bodyId"]), "outside")
    return found


def process_sector(sector_dir):
    sector = Path(sector_dir)
    path = sector / "bodies.parquet"
    names = pq.read_schema(path).names
    ids_table = pq.read_table(path, columns=["id64", "system_id64"])
    ids = ids_table.column("id64").to_numpy(zero_copy_only=False)
    keep = np.isin(ids, WANTED)
    if not keep.any():
        return []
    wanted = set(ids[keep].tolist())
    sids = list(set(ids_table.column("system_id64").to_numpy(zero_copy_only=False)[keep].tolist()))
    del ids_table
    cols = [c for c in names if c in COLUMNS or c in ORBIT_COLUMNS or c.startswith(("atm_", "mat_", "solid_", "parent_"))]
    bodies = pq.read_table(path, columns=cols, filters=pc.field("system_id64").isin(sids)).to_pylist()
    coords = {s["id64"]: (s["x"], s["y"], s["z"])
              for s in pq.read_table(sector / "systems.parquet", columns=["id64", "x", "y", "z"],
                                     filters=pc.field("id64").isin(sids)).to_pylist()}
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
            if b["id64"] not in wanted or b.get("type") != "Planet":
                continue
            props = engine.body_props(b, system, coords[sid], "srvsurvey")
            if props is None:
                continue
            model = model or OrbitModel(system, SAMPLES)
            stars, predictions = [], {}
            for star_id, relation in relations(system, b).items():
                star = system.bodies[star_id]
                cls = star_class(star)
                stars.append({"id": star_id, "class": cls, "relation": relation, "flux": model.flux(b, star)})
                if cls not in predictions:
                    predictions[cls] = pred_json(engine.predict_props({**props, "Star": [cls]}, corrections=False))
            rows.append({"body_id64": b["id64"], "stars": json.dumps(stars), "predictions": json.dumps(predictions)})
    return rows


def main():
    global WANTED
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--database", default="/data/Databases_v2/galaxy_sectors_parquet")
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--parents", type=Path, required=True, help="Output of star_parent_rule.py")
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit-sectors", type=int, default=0)
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)

    frames = [pd.read_parquet(p, columns=["body_id64", "parent_orbit_predictions", "parent_srv_predictions"])
              for p in sorted(glob.glob(str(args.parents / "part_*.parquet")))]
    parent = pd.concat(frames, ignore_index=True).drop_duplicates("body_id64")
    del frames
    wanted, sectors = [], set()
    for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet"))):
        d = pd.read_parquet(p, columns=["body_id64", "sector", "srv_predictions", "orbit_predictions"])
        d = d[d.orbit_predictions.notna()].merge(parent, on="body_id64", how="inner")
        hit = d[(d.srv_predictions != d.orbit_predictions)
                | (d.parent_orbit_predictions.notna() & (d.parent_orbit_predictions != d.orbit_predictions))
                | (d.parent_srv_predictions.notna() & (d.parent_srv_predictions != d.orbit_predictions))]
        wanted.append(hit.body_id64.to_numpy())
        sectors.update(hit.sector.unique().tolist())
    WANTED = np.unique(np.concatenate(wanted))
    del parent, wanted
    print(f"{len(WANTED):,} bodies in {len(sectors):,} sectors", flush=True)

    dirs = sorted(str(Path(args.database) / s) for s in sectors)
    if args.limit_sectors:
        dirs = dirs[: args.limit_sectors]
    buffer, part, n = [], 0, 0
    for rows in run_sectors(process_sector, dirs, args.workers):
        buffer.extend(rows)
        if len(buffer) >= 20_000:
            pd.DataFrame(buffer).to_parquet(args.output / f"part_{part:05d}.parquet")
            n += len(buffer)
            buffer, part = [], part + 1
    if buffer:
        pd.DataFrame(buffer).to_parquet(args.output / f"part_{part:05d}.parquet")
        n += len(buffer)
    print(f"{n:,} bodies -> {args.output}", flush=True)


if __name__ == "__main__":
    main()
