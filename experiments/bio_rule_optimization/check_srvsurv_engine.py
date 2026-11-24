"""Check srvsurv_engine species predictions against Spansh landmark bodies.

For every body with landmark data, runs the full SrvSurvey port with its system
as context and compares predicted species (any variant) to confirmed species.
A prediction counts as wrong only on a complete body (every genus in its genus
list has a confirmed species), where an absent species is definitely absent.

Writes per-species and per-body TSVs to the output directory.
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

import srvsurv_engine as engine
from sector_pool import run_sectors

# Columns the engine reads, besides the atm_*, mat_* and parent_* families
BODY_COLUMNS = {
    "system_id64", "id64", "bodyId", "name", "type", "subType", "isLandable", "mainStar",
    "solarRadius", "surfaceTemperature", "semiMajorAxis", "gravity", "surfacePressure",
    "atmosphereType", "volcanismType", "distanceToArrival", "signals_genuses", "landmark_species",
}


def check_sector(sector_dir: str):
    sector = Path(sector_dir)
    bodies_path = sector / "bodies.parquet"
    if "landmark_species" not in pq.read_schema(bodies_path).names:
        return [], Counter()
    lm = pq.read_table(bodies_path, columns=["system_id64"], filters=pc.field("landmark_species").is_valid())
    sids = set(lm.column("system_id64").to_pylist())
    if not sids:
        return [], Counter()

    names = pq.read_schema(bodies_path).names
    cols = [c for c in names if c in BODY_COLUMNS or c.startswith(("atm_", "mat_", "parent_"))]
    bodies = pq.read_table(bodies_path, columns=cols, filters=pc.field("system_id64").isin(list(sids))).to_pylist()
    systems = {
        s["id64"]: (s["x"], s["y"], s["z"])
        for s in pq.read_table(sector / "systems.parquet", columns=["id64", "x", "y", "z"],
                               filters=pc.field("id64").isin(list(sids))).to_pylist()
    }
    by_system = defaultdict(list)
    for b in bodies:
        by_system[b["system_id64"]].append(b)

    rows, stats = [], Counter()
    for sid, sys_bodies in by_system.items():
        if sid not in systems:
            stats["system missing coords"] += len([b for b in sys_bodies if b.get("landmark_species")])
            continue
        preds = engine.predict_system(sys_bodies, systems[sid], star_model="srvsurvey", corrections=False)
        for b in sys_bodies:
            if not b.get("landmark_species"):
                continue
            confirmed = {lm["species"] for lm in json.loads(b["landmark_species"])}
            genera = engine.known_genera_from_codex(b.get("signals_genuses")) or set()
            confirmed_genera = {s.split(" ")[0] for s in confirmed}
            complete = bool(genera) and genera <= confirmed_genera
            p = preds.get(int(b["id64"]))
            if p is None:
                stats["skipped (no parent star / not landable)"] += 1
                continue
            predicted = {f"{g} {s}" for g, s, _ in p["predictions"]}
            stats["bodies checked"] += 1
            stats["complete bodies"] += complete
            rows.append({
                "body_id64": b["id64"], "name": b["name"], "complete": complete,
                "star": p["star"], "primary_star": p["primary_star"],
                "confirmed": sorted(confirmed), "predicted": sorted(predicted),
            })
    return rows, stats


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--database", default="/data/Databases_v2/galaxy_sectors_parquet")
    ap.add_argument("--output", required=True, help="New output directory (must not exist)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit-sectors", type=int, default=0, help="Only check the first N sectors (testing)")
    args = ap.parse_args()

    out = Path(args.output)
    if out.exists():
        sys.exit(f"Output directory already exists: {out}")
    out.mkdir(parents=True)

    sectors = sorted(str(d) for d in Path(args.database).iterdir() if (d / "bodies.parquet").exists())
    if args.limit_sectors:
        sectors = sectors[: args.limit_sectors]

    # per species: TP = confirmed & predicted; FN = confirmed & not predicted;
    # FP = predicted, absent, and body complete (precision uses TP on complete bodies only); unknown = predicted, absent, body incomplete
    sp, stats = defaultdict(Counter), Counter()
    with open(out / "bodies.tsv", "w") as f:
        f.write("body_id64\tname\tcomplete\tstar\tprimary_star\tconfirmed\tpredicted\n")
        for rows, st in run_sectors(check_sector, sectors, args.workers):
            stats.update(st)
            for r in rows:
                conf, pred = set(r["confirmed"]), set(r["predicted"])
                for s in conf:
                    sp[s]["TP" if s in pred else "FN"] += 1
                    if r["complete"] and s in pred:
                        sp[s]["TPc"] += 1
                for s in pred - conf:
                    sp[s]["FP" if r["complete"] else "unknown"] += 1
                f.write(f"{r['body_id64']}\t{r['name']}\t{r['complete']}\t{r['star']}\t{r['primary_star']}\t"
                        f"{json.dumps(r['confirmed'])}\t{json.dumps(r['predicted'])}\n")
            f.flush()

    with open(out / "species.tsv", "w") as f:
        f.write("species\tTP\tFN\tFP\tunknown\trecall\tprecision\n")
        for s in sorted(sp):
            c = sp[s]
            rec = c["TP"] / (c["TP"] + c["FN"]) if c["TP"] + c["FN"] else float("nan")
            prec = c["TPc"] / (c["TPc"] + c["FP"]) if c["TPc"] + c["FP"] else float("nan")
            f.write(f"{s}\t{c['TP']}\t{c['FN']}\t{c['FP']}\t{c['unknown']}\t{rec:.4f}\t{prec:.4f}\n")

    tot = Counter()
    for c in sp.values():
        tot.update(c)
    lines = [f"{k}: {v}" for k, v in sorted(stats.items())]
    lines += [
        f"species seen: {len(sp)}",
        f"TP {tot['TP']}  FN {tot['FN']}  FP {tot['FP']}  unknown {tot['unknown']}",
        f"recall {tot['TP'] / (tot['TP'] + tot['FN']):.4f}  precision (complete bodies) {tot['TPc'] / (tot['TPc'] + tot['FP']):.4f}",
    ]
    (out / "summary.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
