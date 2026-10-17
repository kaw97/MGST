"""Extract landmark bodies of chosen types and atmospheres with every usable body and star property.

Adds the properties of the star each body orbits (and of the main star) so rule
refinements can test them. Output: one Parquet file for exploration scripts.
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

from mgst.bio.species_rules import PARENT_COLUMNS
from mgst.data.parquet_reader import ParquetSectorReader
from compare_engines import DATABASE

SKIP = {"name", "timestamps", "body_stations_json", "signals_updateTime", "updateTime", "type",
        "mainStar", "spectralClass", "luminosity", "absoluteMagnitude", "solarMasses", "solarRadius", "age"}
STAR_PROPS = ["spectralClass", "luminosity", "absoluteMagnitude", "solarMasses", "solarRadius",
              "surfaceTemperature", "age", "subType"]


def body_columns(path):
    return [c for c in pq.read_schema(path).names
            if c not in SKIP and not c.startswith(("belt_", "ring_1", "ring_2", "ring_3"))
            and not (c.startswith("ring_0") and c not in ("ring_0_type", "ring_0_mass"))]


def extract_sector(sector_dir, atmosphere, body_types):
    reader = ParquetSectorReader(Path(sector_dir))
    if not reader.has_bodies:
        return None
    cols = body_columns(reader._bodies_path)
    bodies = reader.read_bodies(
        columns=cols,
        filters=pc.field("landmark_species").is_valid()
        & pc.field("subType").isin(body_types)
        & pc.field("atmosphereType").isin(atmosphere),
    )
    if len(bodies) == 0:
        return None
    sids = pc.unique(bodies.column("system_id64"))
    stars = reader.read_bodies(
        columns=["system_id64", "bodyId", "mainStar"] + STAR_PROPS,
        filters=(pc.field("type") == "Star") & pc.field("system_id64").isin(sids),
    ).to_pylist()
    by_body = {(s["system_id64"], s["bodyId"]): s for s in stars}
    main = {}
    for s in stars:
        if s.get("mainStar") or s["system_id64"] not in main:
            main[s["system_id64"]] = s
    systems = {s["id64"]: s for s in reader.read_systems(
        columns=["id64", "x", "y", "z"], filters=pc.field("id64").isin(sids)).to_pylist()}

    rows = bodies.to_pylist()
    for r in rows:
        sid = r["system_id64"]
        parent = None
        for i in range(7):
            ptype = r.get(f"parent_{i}_type")
            if ptype is None:
                break
            if ptype == "Star":
                parent = by_body.get((sid, r.get(f"parent_{i}_id")))
                break
        r["is_moon"] = r.get("parent_0_type") == "Planet"
        r["orbits_barycentre"] = r.get("parent_0_type") == "Null"
        for prefix, star in (("own_star_", parent), ("main_star_", main.get(sid))):
            for k in STAR_PROPS:
                r[prefix + k] = star.get(k) if star else None
            sc = (star or {}).get("spectralClass")
            r[prefix + "class"] = sc[0] if sc else None
        r["own_star_is_main"] = bool(parent and parent.get("mainStar"))
        sysrow = systems.get(sid, {})
        r["system_x"], r["system_y"], r["system_z"] = sysrow.get("x"), sysrow.get("y"), sysrow.get("z")
        r["sector"] = Path(sector_dir).name
        for c in PARENT_COLUMNS:
            r.pop(c, None)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default=str(DATABASE))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--atmosphere", default="Thin Water")
    parser.add_argument("--body-types", default="High metal content world,Rocky body")
    parser.add_argument("--output", default="output/body_features/thin_water_hmc_rocky.parquet")
    args = parser.parse_args()
    atmosphere = [a.strip() for a in args.atmosphere.split(",")]
    body_types = [b.strip() for b in args.body_types.split(",")]

    sectors = sorted(d for d in Path(args.database).iterdir() if (d / "bodies.parquet").exists())
    print(f"Extracting {args.atmosphere} landmark bodies ({args.body_types}) from {len(sectors)} sectors", flush=True)
    rows = []
    ctx = multiprocessing.get_context("fork")
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=ctx) as ex:
        futures = {ex.submit(extract_sector, str(s), atmosphere, body_types): s.name for s in sectors}
        for fut in as_completed(futures):
            try:
                rows.extend(fut.result() or [])
            except Exception as e:
                print(f"Error in {futures[fut]}: {e}", flush=True)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), out)
    print(f"Wrote {len(rows):,} bodies to {out}", flush=True)


if __name__ == "__main__":
    main()
