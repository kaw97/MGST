"""Distance from every body predicted Clypeus Speculumi to each star in its system, with each star's light and relation.

Distance is the orbit-averaged one behind the light model: 1 / sqrt(mean(1 / d^2)), in light-seconds, so it is the
distance at which a star would deliver its orbit-averaged light. Also adds up orbit sizes from the body to the first
star or star-bearing barycentre above it (chain_ls), with chain_complete false if any orbit on the way is not on record.
Writes stars.parquet.
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
from build_signal_features import COLUMNS
from build_bio_analysis_db import ORBIT_COLUMNS
from sector_pool import run_sectors
from star_brightness import OrbitModel
from star_relation import relations

SAMPLES = 5_000
LS_PER_AU = engine.LS_PER_AU
WANTED = None


def process_sector(sector_dir):
    path = Path(sector_dir) / "bodies.parquet"
    names = pq.read_schema(path).names
    table = pq.read_table(path, columns=["id64", "system_id64"])
    ids = table.column("id64").to_numpy(zero_copy_only=False)
    keep = np.isin(ids, WANTED)
    if not keep.any():
        return []
    wanted = set(ids[keep].tolist())
    sids = list(set(table.column("system_id64").to_numpy(zero_copy_only=False)[keep].tolist()))
    cols = [c for c in names if c in COLUMNS or c in ORBIT_COLUMNS or c.startswith("parent_")]
    by_system = defaultdict(list)
    for b in pq.read_table(path, columns=cols, filters=pc.field("system_id64").isin(sids)).to_pylist():
        by_system[b["system_id64"]].append(b)
    rows = []
    for sys_bodies in by_system.values():
        system = engine.SystemContext(sys_bodies)
        model = None
        for b in sys_bodies:
            if b["id64"] not in wanted:
                continue
            model = model or OrbitModel(system, SAMPLES)
            chain_ls, chain_complete = engine.chain_distance(system, b)
            for star_id, relation in relations(system, b).items():
                star = system.bodies[star_id]
                rows.append({"body_id64": b["id64"], "star_id": star_id, "star_type": star.get("subType"), "relation": relation,
                             "chain_ls": chain_ls, "chain_complete": chain_complete,
                             "flux": model.flux(b, star), "mean_ls": LS_PER_AU / np.sqrt(model.mean_inverse_square(b, star))})
    return rows


def main():
    global WANTED
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--database", default="/data/Databases_v2/galaxy_sectors_parquet")
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)
    wanted, sectors = [], set()
    for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet"))):
        d = pd.read_parquet(p, columns=["body_id64", "sector", "orbit_predictions"])
        d = d[d.orbit_predictions.notna() & d.orbit_predictions.str.contains("Clypeus Speculumi")]
        wanted.append(d.body_id64.to_numpy())
        sectors.update(d.sector.unique().tolist())
    WANTED = np.unique(np.concatenate(wanted))
    print(f"{len(WANTED):,} bodies in {len(sectors):,} sectors", flush=True)
    rows = []
    for r in run_sectors(process_sector, sorted(str(Path(args.database) / s) for s in sectors), args.workers):
        rows.extend(r)
    pd.DataFrame(rows).to_parquet(args.output / "stars.parquet")
    print(f"{len(rows):,} rows -> {args.output}", flush=True)


if __name__ == "__main__":
    main()
