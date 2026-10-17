"""Extract every landmark-bearing body (the ground-truth set) into one Parquet file.

Each row carries the body fields both prediction engines need, plus the
system's star class letter and galactic coordinates, so rule attribution
can be run repeatedly without rescanning the galaxy.
"""

import argparse
import multiprocessing
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mgst.bio.species_rules import BODY_COLUMNS
from mgst.data.parquet_reader import ParquetSectorReader
from compare_engines import ATM_COLUMNS, DATABASE

LANDMARK_COLUMNS = ["landmark_species", "landmark_genuses", "signals_genuses"]
PARENT_COLUMNS = [f"parent_{i}_{k}" for i in range(7) for k in ("type", "id")]


def parent_star_class(row, star_by_body):
    """Class letter of the first star up the parent chain, 'barycentre' if none."""
    sid = row["system_id64"]
    for i in range(7):
        ptype = row.get(f"parent_{i}_type")
        if ptype is None:
            break
        if ptype == "Star":
            return star_by_body.get((sid, row.get(f"parent_{i}_id")), "unknown")
    return "barycentre"


def extract_sector(sector_dir):
    reader = ParquetSectorReader(Path(sector_dir))
    if not reader.has_bodies:
        return None
    cols = list(dict.fromkeys(BODY_COLUMNS + ATM_COLUMNS + LANDMARK_COLUMNS + PARENT_COLUMNS))
    bodies = reader.read_bodies(
        columns=cols, filters=pc.field("landmark_species").is_valid()
    )
    if len(bodies) == 0:
        return None
    sids = pc.unique(bodies.column("system_id64"))

    star_classes = {}
    star_by_body = {}
    stars = reader.read_bodies(
        columns=["system_id64", "bodyId", "spectralClass", "mainStar"],
        filters=(pc.field("type") == "Star") & pc.field("system_id64").isin(sids),
    )
    for s in stars.to_pylist():
        sc = s.get("spectralClass")
        if not sc:
            continue
        star_by_body[(s["system_id64"], s.get("bodyId"))] = sc[0]
        if s.get("mainStar") or s["system_id64"] not in star_classes:
            star_classes[s["system_id64"]] = sc[0]

    coords = {}
    systems = reader.read_systems(
        columns=["id64", "x", "y", "z"], filters=pc.field("id64").isin(sids)
    )
    for s in systems.to_pylist():
        coords[s["id64"]] = (s.get("x"), s.get("y"), s.get("z"))

    present_parent_cols = [c for c in PARENT_COLUMNS if c in bodies.column_names]
    parent_rows = bodies.select(["system_id64"] + present_parent_cols).to_pylist()
    bodies = bodies.append_column(
        "parent_star_class",
        pa.array([parent_star_class(r, star_by_body) for r in parent_rows], pa.string()),
    )
    bodies = bodies.drop_columns(present_parent_cols)

    sid_list = bodies.column("system_id64").to_pylist()
    xyz = [coords.get(sid, (None, None, None)) for sid in sid_list]
    bodies = bodies.append_column("star_class", pa.array([star_classes.get(s) for s in sid_list], pa.string()))
    for i, axis in enumerate(("system_x", "system_y", "system_z")):
        bodies = bodies.append_column(axis, pa.array([c[i] for c in xyz], pa.float64()))
    bodies = bodies.append_column("sector", pa.array([Path(sector_dir).name] * len(bodies), pa.string()))
    return bodies


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default=str(DATABASE))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="output/landmark_bodies/landmark_bodies.parquet")
    args = parser.parse_args()

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    sectors = sorted(d for d in Path(args.database).iterdir() if (d / "bodies.parquet").exists())
    print(f"Extracting landmark bodies from {len(sectors)} sectors...", flush=True)

    tables = []
    ctx = multiprocessing.get_context("fork")
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=ctx) as ex:
        futures = {ex.submit(extract_sector, str(s)): s.name for s in sectors}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                t = fut.result()
                if t is not None:
                    tables.append(t)
            except Exception as e:
                print(f"Error in {futures[fut]}: {e}", flush=True)
            if i % 1000 == 0:
                print(f"  {i}/{len(sectors)} sectors", flush=True)

    # Atm columns may be missing in some sectors; unify schemas before concatenating
    table = pa.concat_tables(tables, promote_options="default")
    pq.write_table(table, out)
    print(f"Wrote {len(table)} landmark bodies to {out}", flush=True)


if __name__ == "__main__":
    main()
