"""Pick each body's star from its parent hierarchy, for testing against the two light models.

Walks up the body's parents, skipping planets. If the first thing found is a star, that star is the pick, however
bright any other star is. If it is a barycentre, the stars under that barycentre are compared by brightness, under
the orbit-averaged model (parent_orbit) and under SrvSurvey's formula (parent_srv). Light models are not changed.

Covers bodies with a landmark in systems of two or more stars, plus every body predicted Clypeus Speculumi, for which
it also records the distance to the star. Writes part_*.parquet under --output for score_star_parent_rule.py.
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
LS_PER_AU = 499.005
WANTED = None  # sorted id64 array, set in the parent before forking
DISTANCE = None  # sorted id64 array of Clypeus Speculumi bodies


def hierarchy_candidates(system, body):
    """(kind, stars): the first star above the body, or the stars under the first barycentre above it."""
    for ptype, pid in system._chain[int(body["bodyId"])]:
        if ptype == "Star":
            star = system.bodies.get(pid)
            return ("star", [star]) if star is not None and star.get("subType") else ("missing", [])
        if ptype == "Null":
            stars = [s for s in system.stars if s.get("subType") and system._has_parent(s, pid)]
            if stars:  # a barycentre of two planets has none, so keep walking
                return ("barycentre", stars)
    return ("missing", [])


def sma_distance(system, body, star):
    """Orbit sizes added up the chain to the star, in AU; None if the star isn't above the body."""
    total = engine._num(body.get("semiMajorAxis"))
    for parent in system.parent_bodies(body):
        if parent["bodyId"] == star["bodyId"]:
            return total
        total += engine._num(parent.get("semiMajorAxis"))
    return None


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
            kind, candidates = hierarchy_candidates(system, b)
            row = {"body_id64": b["id64"], "kind": kind, "n_candidates": len(candidates)}
            picks = {}
            if len(candidates) == 1:
                picks = {"orbit": candidates[0], "srv": candidates[0]}
            elif candidates:
                model = model or OrbitModel(system, SAMPLES)
                flux = [model.flux(b, s) for s in candidates]
                total = sum(flux)
                picks["orbit"] = candidates[int(np.argmax(flux))]
                row["parent_share"] = max(flux) / total if total > 0 else None
                bright = [system.relative_brightness(b, s) for s in candidates]
                picks["srv"] = candidates[int(np.argmax(bright))]
            for name, star in picks.items():
                cls = star_class(star)
                row[f"parent_{name}_id"] = star.get("bodyId")
                row[f"parent_{name}_class"] = cls
                row[f"parent_{name}_predictions"] = pred_json(engine.predict_props({**props, "Star": [cls]}, corrections=False))
            if picks and np.isin(b["id64"], DISTANCE):
                star = picks["orbit"]
                model = model or OrbitModel(system, SAMPLES)
                row["parent_mean_ls"] = LS_PER_AU / np.sqrt(model.mean_inverse_square(b, star))
                sma = sma_distance(system, b, star)
                row["parent_sma_ls"] = None if sma is None else sma * LS_PER_AU
            rows.append(row)
    return rows


def main():
    global WANTED, DISTANCE
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--database", default="/data/Databases_v2/galaxy_sectors_parquet")
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit-sectors", type=int, default=0)
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)

    landmark = pd.read_parquet(args.root / "landmarks/landmarks.parquet", columns=["body_id64"]).body_id64.unique()
    multi, speculumi = [], []
    for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet"))):
        d = pd.read_parquet(p, columns=["body_id64", "n_stars", "orbit_predictions", "srv_predictions"])
        d = d[d.orbit_predictions.notna()]
        multi.append(d.body_id64[d.n_stars > 1].to_numpy())
        hit = d.orbit_predictions.str.contains("Clypeus Speculumi") | d.srv_predictions.str.contains("Clypeus Speculumi")
        speculumi.append(d.body_id64[hit].to_numpy())
    DISTANCE = np.sort(np.concatenate(speculumi))
    WANTED = np.union1d(np.intersect1d(np.concatenate(multi), landmark), DISTANCE)
    print(f"{len(WANTED):,} bodies, {len(DISTANCE):,} predicted Clypeus Speculumi", flush=True)
    del landmark, multi, speculumi

    sectors = sorted(str(d) for d in Path(args.database).iterdir() if (d / "bodies.parquet").exists())
    if args.limit_sectors:
        sectors = sectors[: args.limit_sectors]
    buffer, part, n = [], 0, 0
    for rows in run_sectors(process_sector, sectors, args.workers):
        buffer.extend(rows)
        if len(buffer) >= 50_000:
            pd.DataFrame(buffer).to_parquet(args.output / f"part_{part:05d}.parquet")
            n += len(buffer)
            buffer, part = [], part + 1
    if buffer:
        pd.DataFrame(buffer).to_parquet(args.output / f"part_{part:05d}.parquet")
        n += len(buffer)
    print(f"{n:,} bodies -> {args.output}", flush=True)


if __name__ == "__main__":
    main()
