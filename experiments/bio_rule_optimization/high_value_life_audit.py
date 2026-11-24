"""One row per body the high-value search would flag, with the evidence on whether it has life.

Applies the same rules as scripts/find_high_value_systems.py (species_rules.score_body) without a date filter. For
each candidate records the body's biological and geological signals, landmarks, update times, and how well its system
has been scanned: bodies on record against the system's bodyCount, and whether any body in it has signals on record.
Writes candidates/part_*.parquet, which high_value_life_labels.py reads.
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from mgst.bio.species_rules import BODY_COLUMNS, RELEVANT_BODY_TYPES, parent_chain_ls, parent_star_class, score_body, star_barycentres
from sector_pool import run_sectors

BIO, GEO = "sig_$SAA_SignalType_Biological;", "sig_$SAA_SignalType_Geological;"
EXTRA = ["type", "bodyId", "id64", "name", "spectralClass", "mainStar", "age", "solarRadius", "solarMasses", "absoluteMagnitude",
         "signals_updateTime", "landmark_count", "landmark_species", GEO]


def process_sector(sector_dir):
    sector = Path(sector_dir)
    path = sector / "bodies.parquet"
    available = pq.read_schema(path).names
    cols = [c for c in dict.fromkeys(BODY_COLUMNS + EXTRA + ["system_id64"]) if c in available]
    rows = pq.read_table(path, columns=cols).to_pylist()
    system_cols = [c for c in ("id64", "x", "y", "z", "date", "bodyCount") if c in pq.read_schema(sector / "systems.parquet").names]
    systems = {s["id64"]: s for s in pq.read_table(sector / "systems.parquet", columns=system_cols).to_pylist()}
    by_system = defaultdict(list)
    for r in rows:
        by_system[r["system_id64"]].append(r)
    out = []
    for sid, bodies in by_system.items():
        s = systems.get(sid)
        if s is None:
            continue
        stars = [b for b in bodies if b.get("type") == "Star"]
        star_by_body = {(sid, b.get("bodyId")): b["spectralClass"][0] for b in stars if b.get("spectralClass")}
        main = next((b for b in stars if b.get("mainStar") and b.get("spectralClass")), None) or next((b for b in stars if b.get("spectralClass")), None)
        orbits = {(sid, b.get("bodyId")): b.get("semiMajorAxis") for b in bodies if b.get("semiMajorAxis")}
        centres = star_barycentres(stars)
        system_facts = None
        for b in bodies:
            if b.get("subType") not in RELEVANT_BODY_TYPES or not (b.get("atmosphereType") or "").startswith("Thin"):
                continue
            row = dict(b, star_class=main["spectralClass"][0] if main else None, parent_star_class=parent_star_class(b, star_by_body),
                       parent_chain_ls=parent_chain_ls(b, orbits, centres), system_x=s["x"], system_y=s["y"], system_z=s["z"])
            if main:
                row.update(star_temperature=main.get("surfaceTemperature"), star_abs_magnitude=main.get("absoluteMagnitude"),
                           star_solar_masses=main.get("solarMasses"), star_solar_radius=main.get("solarRadius"), star_age=main.get("age"))
            hits = score_body(row)
            if not hits:
                continue
            if system_facts is None:
                system_facts = {
                    "system_date": s.get("date"), "body_count": s.get("bodyCount"), "bodies_on_record": len(bodies),
                    "system_bio_bodies": sum(bool(x.get(BIO)) for x in bodies), "system_geo_bodies": sum(bool(x.get(GEO)) for x in bodies),
                    "system_signal_bodies": sum(x.get("signals_updateTime") is not None for x in bodies),
                    "system_landable": sum(bool(x.get("isLandable")) for x in bodies),
                    "system_latest_update": max((x["updateTime"] for x in bodies if x.get("updateTime")), default=None),
                }
            out.append({
                "body_id64": b.get("id64"), "system_id64": sid, "sector": sector.name, "name": b.get("name"),
                "species": json.dumps(sorted(n for n, _ in hits)), "max_value": max(v for _, v in hits),
                "subType": b.get("subType"), "atmosphereType": b.get("atmosphereType"), "volcanismType": b.get("volcanismType"),
                "gravity": b.get("gravity"), "surfaceTemperature": b.get("surfaceTemperature"), "surfacePressure": b.get("surfacePressure"),
                "isLandable": b.get("isLandable"), "distanceToArrival": b.get("distanceToArrival"), "star_class": row["star_class"],
                "parent_star_class": row["parent_star_class"], "star_age": row.get("star_age"), "x": s["x"], "y": s["y"], "z": s["z"],
                "bio": b.get(BIO), "geo": b.get(GEO), "genuses": b.get("signals_genuses"), "signals_updateTime": b.get("signals_updateTime"),
                "updateTime": b.get("updateTime"), "landmark_count": b.get("landmark_count"), "landmark_species": b.get("landmark_species"),
                **system_facts,
            })
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--database", type=Path, default=Path("/data/Databases_v2/galaxy_sectors_parquet"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--every", type=int, default=1, help="Use every Nth sector, for a trial")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)
    sectors = sorted(str(d) for d in args.database.iterdir() if (d / "bodies.parquet").exists())[:: args.every]
    (args.output / "candidates").mkdir()
    rows, n, part = [], 0, 0
    for r in run_sectors(process_sector, sectors, args.workers):
        rows.extend(r)
        if len(rows) >= 200_000:
            pd.DataFrame(rows).to_parquet(args.output / "candidates" / f"part_{part:05d}.parquet")
            n, part, rows = n + len(rows), part + 1, []
    if rows:
        pd.DataFrame(rows).to_parquet(args.output / "candidates" / f"part_{part:05d}.parquet")
        n += len(rows)
    print(f"{n:,} candidate bodies from {len(sectors):,} sectors -> {args.output}", flush=True)


if __name__ == "__main__":
    main()
