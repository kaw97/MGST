"""Feature table for predicting the bio signal count on Stratum Tectonicas candidates.

Selects every landable body where SrvSurvey (before genera are known) predicts
Stratum Tectonicas and the FSS bio signal count is recorded. For each body it
stores the body's own properties, its dominant star (SrvSurvey's brightest
parent star), the system's main star, location, and what SrvSurvey predicts.
The landmark download is not used, so the selection does not depend on which
species were confirmed.

The geological signal count is kept, but it is only known at the same time as
the bio count (after the FSS scan).
"""

import argparse
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

import srvsurv_engine as engine
from sector_pool import run_sectors

BIO = "sig_$SAA_SignalType_Biological;"
GEO = "sig_$SAA_SignalType_Geological;"
TARGET = ("Stratum", "Tectonicas")

BODY_NUMERIC = [
    "gravity", "surfaceTemperature", "surfacePressure", "radius", "earthMasses", "distanceToArrival",
    "semiMajorAxis", "orbitalEccentricity", "orbitalInclination", "orbitalPeriod", "rotationalPeriod",
    "axialTilt",
]
STAR_NUMERIC = ["surfaceTemperature", "solarRadius", "solarMasses", "age", "absoluteMagnitude"]
COLUMNS = set(BODY_NUMERIC + STAR_NUMERIC) | {
    "system_id64", "id64", "bodyId", "name", "type", "subType", "isLandable", "mainStar", "luminosity",
    "atmosphereType", "volcanismType", "terraformingState", "rotationalPeriodTidallyLocked",
    "signals_genuses", "landmark_species", BIO, GEO,
}


def _num(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return float(v)


def _star_features(prefix, star):
    out = {f"{prefix}_class": engine.flatten_star_type(engine.journal_star_type(star.get("subType"))) if star else None}
    for k in STAR_NUMERIC:
        out[f"{prefix}_{k}"] = _num(star.get(k)) if star else None
    if star:
        r, t = _num(star.get("solarRadius")), _num(star.get("surfaceTemperature"))
        out[f"{prefix}_luminosity"] = r * r * (t / 5772.0) ** 4 if r and t else None
        out[f"{prefix}_luminosity_class"] = star.get("luminosity")
    return out


def build_sector(sector_dir: str):
    sector = Path(sector_dir)
    bodies_path = sector / "bodies.parquet"
    names = pq.read_schema(bodies_path).names
    if BIO not in names:
        return []
    hit = pq.read_table(bodies_path, columns=["system_id64"], filters=pc.field(BIO) >= 1)
    sids = set(hit.column("system_id64").to_pylist())
    if not sids:
        return []
    cols = [c for c in names if c in COLUMNS or c.startswith(("atm_", "mat_", "solid_", "parent_"))]
    bodies = pq.read_table(bodies_path, columns=cols, filters=pc.field("system_id64").isin(list(sids))).to_pylist()
    systems = {
        s["id64"]: s
        for s in pq.read_table(sector / "systems.parquet", columns=["id64", "x", "y", "z", "date"],
                               filters=pc.field("id64").isin(list(sids))).to_pylist()
    }
    by_system = defaultdict(list)
    for b in bodies:
        by_system[b["system_id64"]].append(b)

    rows = []
    for sid, sys_bodies in by_system.items():
        s = systems.get(sid)
        if s is None:
            continue
        coords = (s["x"], s["y"], s["z"])
        system = engine.SystemContext(sys_bodies)
        main = system.main_star()
        for b in sys_bodies:
            bio = _num(b.get(BIO))
            if not bio or b.get("type") != "Planet" or not b.get("isLandable"):
                continue
            props = engine.body_props(b, system, coords, "srvsurvey")
            if props is None:
                continue
            preds = engine.predict_props(props, corrections=False)
            if not any((g, sp) == TARGET for g, sp, _ in preds):
                continue
            star = system.brightest_star(b)
            candidates = system.parent_stars(b)
            scores = [system.relative_brightness(b, c) for c in candidates]
            total = sum(x for x in scores if x > 0 and math.isfinite(x))
            top = system.relative_brightness(b, star)

            row = {
                "body_id64": b["id64"], "system_id64": sid, "name": b["name"], "sector": sector.name,
                "bio_signals": int(bio), "geo_signals": int(_num(b.get(GEO)) or 0),
                "genera_known": b.get("signals_genuses"),
                "landmark_species": b.get("landmark_species"),
                "subType": b.get("subType"), "atmosphere": props["AtmosphereType"],
                "volcanism": b.get("volcanismType"), "terraformingState": b.get("terraformingState"),
                "tidally_locked": b.get("rotationalPeriodTidallyLocked"),
                "is_moon": b.get("parent_0_type") == "Planet",
                "region": int(props["Region"]) if props["Region"] else None,
                "nebula_distance": props["Nebulae"],
                "x": s["x"], "y": s["y"], "z": s["z"],
                "n_stars": len(system.stars), "n_star_candidates": len(candidates),
                "star_share": top / total if total and math.isfinite(top) else None,
                "n_predicted_genera": len({g for g, _, _ in preds}),
                "n_predicted_species": len({(g, sp) for g, sp, _ in preds}),
                "predicts_bacterium": any(g == "Bacterium" for g, _, _ in preds),
                "predicted": json.dumps(sorted({f"{g} {sp}" for g, sp, _ in preds})),
            }
            for k in BODY_NUMERIC:
                row[k] = _num(b.get(k))
            for k, v in b.items():
                if k.startswith(("atm_", "mat_", "solid_")):
                    row[k] = _num(v)
            row.update(_star_features("star", star))
            row.update(_star_features("main", main))
            rows.append(row)
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--database", default="/data/Databases_v2/galaxy_sectors_parquet")
    ap.add_argument("--output", required=True, help="New output directory (must not exist)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit-sectors", type=int, default=0)
    ap.add_argument("--sector-stride", type=int, default=1, help="Use every Nth sector (spatial sample)")
    args = ap.parse_args()

    out = Path(args.output)
    if out.exists():
        sys.exit(f"Output directory already exists: {out}")
    (out / "parts").mkdir(parents=True)

    sectors = sorted(str(d) for d in Path(args.database).iterdir() if (d / "bodies.parquet").exists())
    sectors = sectors[:: args.sector_stride]
    if args.limit_sectors:
        sectors = sectors[: args.limit_sectors]

    buffer, part, n = [], 0, 0
    for rows in run_sectors(build_sector, sectors, args.workers):
        buffer.extend(rows)
        if len(buffer) >= 50_000:
            pd.DataFrame(buffer).to_parquet(out / "parts" / f"part_{part:04d}.parquet")
            n += len(buffer)
            buffer, part = [], part + 1
    if buffer:
        pd.DataFrame(buffer).to_parquet(out / "parts" / f"part_{part:04d}.parquet")
        n += len(buffer)

    frames = [pd.read_parquet(p) for p in sorted((out / "parts").glob("*.parquet"))]
    pd.concat(frames, ignore_index=True).to_parquet(out / "features.parquet")
    print(f"{n} bodies -> {out / 'features.parquet'}")


if __name__ == "__main__":
    main()
