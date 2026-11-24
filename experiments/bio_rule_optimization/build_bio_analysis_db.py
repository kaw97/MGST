"""Build the exobiology analysis database from the raw colour walk and the galaxy Parquet.

Writes under --root (next to raw/):
- landmarks/landmarks.parquet: one row per body and species, with colour
- walk_bodies/walk_bodies.parquet: one row per walked body, with Spansh's genus list and signal count
- bodies/part_*.parquet: one row per database body that was walked or has a genus list, with
  features and predicted species and colours under SrvSurvey's star and the orbit-averaged star
- manifest.json

The galaxy database and raw/ are only read. Predictions ignore the genus list, so
any genus filter can be applied afterwards.
"""

import argparse
import glob
import gzip
import json
import math
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

import srvsurv_engine as engine
from build_signal_features import BIO, BODY_NUMERIC, COLUMNS, GEO, _num, _star_features
from sector_pool import run_sectors
from star_brightness import OrbitModel

SAMPLES = 5_000
ORBIT_COLUMNS = {"ascendingNode", "argOfPeriapsis"}
# Spansh genus-list names that differ from landmark genus names
API_GENUS = {
    "Bacterial": "Bacterium", "Tussocks": "Tussock", "Fungoids": "Fungoida", "Shrubs": "Frutexa",
    "Fonticulus": "Fonticulua", "Conchas": "Concha", "Cactoids": "Cactoida", "Aleoids": "Aleoida",
    "Fumerolas": "Fumerola", "Brain Trees": "Brain Tree", "Anemones": "Anemone",
}
WALK_IDS = None  # sorted id64 array, set in the parent before forking


def extract_walk(raw_dir: Path, root: Path):
    """Flatten raw pages into landmark and body tables, dropping repeats."""
    seen, landmarks, bodies = set(), [], []
    for path in sorted(raw_dir.glob("part_*.jsonl.gz")):
        with gzip.open(path, "rt") as f:
            for line in f:
                for b in json.loads(line)["results"]:
                    if b["id64"] in seen:
                        continue
                    seen.add(b["id64"])
                    per_species = defaultdict(list)
                    for lm in b.get("landmarks", []):
                        if lm.get("value", 0) > 0:
                            per_species[(lm["type"], lm["subtype"], lm["value"])].append(lm)
                    for (genus, species, value), items in per_species.items():
                        colours = sorted({x["variant"] for x in items if x.get("variant")})
                        landmarks.append({
                            "body_id64": b["id64"], "genus": genus, "species": species, "value": value,
                            "colour": colours[0] if len(colours) == 1 else None,
                            "all_colours": json.dumps(colours), "n_landmarks": len(items),
                        })
                    genera = sorted({API_GENUS.get(g["name"], g["name"]) for g in b.get("genuses", [])})
                    signals = next((s.get("count") for s in b.get("signals", []) if s.get("name") == "Biological"), None)
                    found = {k[0] for k in per_species}
                    bodies.append({
                        "body_id64": b["id64"], "system_id64": b.get("system_id64"), "name": b.get("name"),
                        "distance": b.get("distance"), "api_genera": json.dumps(genera), "api_signals": signals,
                        "n_species": len(per_species),
                        "complete": bool(genera) and set(genera) <= found,
                    })
    (root / "landmarks").mkdir()
    (root / "walk_bodies").mkdir()
    pd.DataFrame(landmarks).to_parquet(root / "landmarks" / "landmarks.parquet")
    walk = pd.DataFrame(bodies)
    walk.to_parquet(root / "walk_bodies" / "walk_bodies.parquet")
    return walk


def star_class(star):
    return engine.flatten_star_type(engine.journal_star_type(star["subType"]))


def pred_json(preds):
    return json.dumps(sorted(f"{g} {s}|{v}" for g, s, v in preds))


def build_sector(sector_dir: str):
    sector = Path(sector_dir)
    path = sector / "bodies.parquet"
    names = pq.read_schema(path).names
    id_table = pq.read_table(path, columns=[c for c in ("id64", "system_id64", "signals_genuses") if c in names])
    ids = id_table.column("id64").to_numpy(zero_copy_only=False)
    keep = np.isin(ids, WALK_IDS, assume_unique=False)
    if "signals_genuses" in names:
        keep |= id_table.column("signals_genuses").is_valid().to_numpy(zero_copy_only=False)
    if not keep.any():
        return []
    wanted = set(ids[keep].tolist())
    sids = list(set(id_table.column("system_id64").to_numpy(zero_copy_only=False)[keep].tolist()))
    del id_table
    cols = [c for c in names if c in COLUMNS or c in ORBIT_COLUMNS or c.startswith(("atm_", "mat_", "solid_", "parent_"))]
    bodies = pq.read_table(path, columns=cols, filters=pc.field("system_id64").isin(sids)).to_pylist()
    systems = {
        s["id64"]: s
        for s in pq.read_table(sector / "systems.parquet", columns=["id64", "x", "y", "z"],
                               filters=pc.field("id64").isin(sids)).to_pylist()
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
        model = None
        for b in sys_bodies:
            if b["id64"] not in wanted:
                continue
            row = {
                "body_id64": b["id64"], "system_id64": sid, "name": b["name"], "sector": sector.name,
                "body_type": b.get("type"), "landable": b.get("isLandable"),
                "bio_signals": _num(b.get(BIO)), "geo_signals": _num(b.get(GEO)),
                "db_genera": b.get("signals_genuses"), "subType": b.get("subType"),
                "volcanism": b.get("volcanismType"), "terraformingState": b.get("terraformingState"),
                "tidally_locked": b.get("rotationalPeriodTidallyLocked"),
                "is_moon": b.get("parent_0_type") == "Planet",
                "x": s["x"], "y": s["y"], "z": s["z"], "n_stars": len(system.stars),
            }
            for k in BODY_NUMERIC:
                row[k] = _num(b.get(k))
            for k, v in b.items():
                if k.startswith(("atm_", "mat_", "solid_")):
                    row[k] = _num(v)
            props = engine.body_props(b, system, coords, "srvsurvey") if b.get("type") == "Planet" else None
            row["has_prediction"] = props is not None
            if props is not None:
                srv = system.brightest_star(b)
                row.update({
                    "atmosphere": props["AtmosphereType"], "nebula_distance": props["Nebulae"],
                    "region": int(props["Region"]) if props["Region"] else None,
                    "n_star_candidates": len(system.parent_stars(b)),
                })
                row.update(_star_features("srv", srv))
                row.update(_star_features("main", main))
                srv_preds = engine.predict_props(props, corrections=False)
                row["srv_star_class"] = props["Star"][0]
                row["srv_predictions"] = pred_json(srv_preds)
                best, share = srv, 1.0
                stars = [x for x in system.parent_stars(b) if x.get("subType")]
                if len(stars) > 1:
                    model = model or OrbitModel(system, SAMPLES)
                    flux = [model.flux(b, x) for x in stars]
                    total = sum(flux)
                    i = int(np.argmax(flux))
                    best, share = stars[i], flux[i] / total if total > 0 else None
                row.update(_star_features("orbit", best))
                row["orbit_star_id"] = best.get("bodyId")
                row["srv_star_id"] = srv.get("bodyId") if srv else None
                row["orbit_share"] = share
                orbit_class = star_class(best) if best and best.get("subType") else props["Star"][0]
                row["orbit_star_class"] = orbit_class
                row["orbit_predictions"] = (row["srv_predictions"] if orbit_class == props["Star"][0]
                                            else pred_json(engine.predict_props({**props, "Star": [orbit_class]}, corrections=False)))
            rows.append(row)
    return rows


def main():
    global WALK_IDS
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--database", default="/data/Databases_v2/galaxy_sectors_parquet")
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--raw", type=Path, default=None, help="Raw walk directory (default: <root>/raw)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit-sectors", type=int, default=0)
    args = ap.parse_args()

    raw = args.raw or args.root / "raw"
    if (args.root / "bodies").exists():
        sys.exit(f"Already built: {args.root / 'bodies'}")
    args.root.mkdir(parents=True, exist_ok=True)
    existing = args.root / "walk_bodies" / "walk_bodies.parquet"
    walk = pd.read_parquet(existing) if existing.exists() else extract_walk(raw, args.root)
    print(f"{len(walk):,} walked bodies, {int(walk.complete.sum()):,} complete", flush=True)
    WALK_IDS = np.sort(walk.body_id64.to_numpy())
    del walk

    sectors = sorted(str(d) for d in Path(args.database).iterdir() if (d / "bodies.parquet").exists())
    if args.limit_sectors:
        sectors = sectors[: args.limit_sectors]
    (args.root / "bodies").mkdir()
    buffer, part, n = [], 0, 0
    for rows in run_sectors(build_sector, sectors, args.workers):
        buffer.extend(rows)
        if len(buffer) >= 50_000:
            pd.DataFrame(buffer).to_parquet(args.root / "bodies" / f"part_{part:05d}.parquet")
            n += len(buffer)
            buffer, part = [], part + 1
    if buffer:
        pd.DataFrame(buffer).to_parquet(args.root / "bodies" / f"part_{part:05d}.parquet")
        n += len(buffer)
    commit = subprocess.run(["git", "-C", "/mnt/z/SrvSurvey", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    (args.root / "manifest.json").write_text(json.dumps({
        "built": datetime.now(timezone.utc).isoformat(), "bodies": n, "sectors": len(sectors), "database": args.database,
        "raw": str(raw), "srvsurvey_commit": commit, "orbit_samples": SAMPLES,
        "raw_parts": len(glob.glob(str(raw / "part_*.jsonl.gz"))),
    }, indent=1))
    print(f"{n:,} bodies -> {args.root / 'bodies'}", flush=True)


if __name__ == "__main__":
    main()
