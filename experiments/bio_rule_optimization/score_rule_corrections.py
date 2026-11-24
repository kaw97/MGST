"""Score the engine's rule corrections end to end, by re-predicting every analysis-database body.

Each body is re-predicted with and without corrections under SrvSurvey's star and under the engine's
default orbit star, keeping only the differences, which are applied to the predictions stored in the
database. Bodies where the engine's orbit star class differs from the database's are counted. Reports the method table on
complete bodies, and right/wrong/unknown counts per corrected species on all bodies. Writes lists/ with every failure of
the final engine and every improvement over SrvSurvey as is, by system and body, on all bodies where the truth is known:
- failures_missed.tsv: species confirmed on the body, not predicted
- failures_wrong_colour.tsv: species confirmed with a colour that isn't among the predicted colours
- failures_false_predictions.tsv: species predicted, certainly absent
- improvements.tsv: species_now_predicted, colour_fixed or false_prediction_removed
"""

import argparse
import gc
import glob
import json
import pickle
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow.compute as pc
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

import species_colour_trees as trees
import srvsurv_engine as engine
from build_bio_analysis_db import ORBIT_COLUMNS
from build_signal_features import COLUMNS
from sector_pool import run_sectors

DATABASE = "/data/Databases_v2/galaxy_sectors_parquet"
WANT = {}  # sector -> {body id64: (orbit star class, orbit star temperature)}, set before forking
WANT_SYSTEMS = {}  # sector -> system id64s whose names the lists need, set before forking
KNOWN_SPECIES = set()  # species the engine can predict, filled from the database's predictions
LIST_COLUMNS = ["system", "body", "species", "kind", "confirmed_colour", "predicted_colours", "srvsurvey_colours",
                "srvsurvey_right", "complete", "engine_star", "srvsurvey_star", "cause"]


def work(sector):
    path = f"{DATABASE}/{sector}/bodies.parquet"
    names = pq.read_schema(path).names
    want = WANT[sector]
    sids = list(set(pq.read_table(path, columns=["id64", "system_id64"], filters=pc.field("id64").isin(list(want)))
                    .column("system_id64").to_pylist()))
    cols = [c for c in names if c in COLUMNS or c in ORBIT_COLUMNS or c.startswith(("atm_", "mat_", "solid_", "parent_"))]
    rows = pq.read_table(path, columns=cols, filters=pc.field("system_id64").isin(sids)).to_pylist()
    systems = {s["id64"]: s for s in pq.read_table(f"{DATABASE}/{sector}/systems.parquet", columns=["id64", "x", "y", "z"],
                                                   filters=pc.field("id64").isin(sids)).to_pylist()}
    by_system = defaultdict(list)
    for r in rows:
        by_system[r["system_id64"]].append(r)
    n, mismatch, out, new_star = 0, 0, [], {}
    for sid, bodies in by_system.items():
        s = systems.get(sid)
        if s is None:
            continue
        context = engine.SystemContext(bodies)
        for b in bodies:
            if b["id64"] not in want or b.get("type") != "Planet":
                continue
            coords = (s["x"], s["y"], s["z"])
            props = engine.body_props(b, context, coords, "srvsurvey")
            if props is None:
                continue
            orbit = engine.body_props(b, context, coords)
            n += 1
            mismatch += orbit["Star"][0] != want[b["id64"]][0]
            # Stored predictions keep the star the database was built with, so orbit-star differences are taken from that star
            moved = orbit["Star"][0] != want[b["id64"]][0]
            orbit_old = engine.predict_props(orbit, corrections=False)
            stored = engine.predict_props({**orbit, "Star": [want[b["id64"]][0]]}, corrections=False) if moved else orbit_old
            diffs = []
            for new, old in ((engine.predict_props(props), engine.predict_props(props, corrections=False)),
                             (engine.predict_props(orbit), stored), (orbit_old, stored)):
                diffs.append(({f"{g} {sp}|{v}" for g, sp, v in new - old}, {f"{g} {sp}|{v}" for g, sp, v in old - new}))
            if moved:
                new_star[b["id64"]] = orbit["Star"][0]
            if any(a or r for a, r in diffs):
                out.append((b["id64"], diffs))
    return n, mismatch, out, new_star


def system_names(sector):
    t = pq.read_table(f"{DATABASE}/{sector}/systems.parquet", columns=["id64", "name"],
                      filters=pc.field("id64").isin(WANT_SYSTEMS[sector]))
    return dict(zip(t.column("id64").to_pylist(), t.column("name").to_pylist()))


def colours_by_species(keys):
    out = defaultdict(list)
    for k in sorted(keys):
        s, c = k.split("|", 1)
        out[s].append(c)
    return out


def write_lists(root, out_dir, diff, names, new_star):
    """Stream every body with known truth and write the failure and improvement lists; returns row counts."""
    lm = pd.read_parquet(root / "landmarks/landmarks.parquet", columns=["body_id64", "genus", "species", "colour"])
    found_genera, found_species, truth = defaultdict(set), defaultdict(set), defaultdict(set)
    for b, g, s, c in zip(lm.body_id64, lm.genus, lm.species, lm.colour):
        found_genera[b].add(g)
        found_species[b].add(s)
        if c is not None:
            truth[b].add((s, c))
    genus_of = dict(zip(lm.species, lm.genus))
    del lm
    walk = pd.read_parquet(root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "api_genera"])
    api = dict(zip(walk.body_id64, walk.api_genera))
    del walk
    out_dir.mkdir()
    files = {k: open(out_dir / f"{k}.tsv", "w") for k in
             ("failures_missed", "failures_wrong_colour", "failures_false_predictions", "improvements")}
    for f in files.values():
        f.write("\t".join(LIST_COLUMNS) + "\n")
    counts = defaultdict(int)
    cols = ["body_id64", "system_id64", "name", "db_genera", "srv_predictions", "orbit_predictions", "srv_star_class", "orbit_star_class"]
    for path in sorted(glob.glob(str(root / "bodies/part_*.parquet"))):
        part = pd.read_parquet(path, columns=cols)
        for t in part[part.orbit_predictions.notna()].itertuples(index=False):
            b = t.body_id64
            a = json.loads(api[b]) if api.get(b) else []
            listed = set(a or ([trees.CODE_GENUS.get(c, c) for c in json.loads(t.db_genera)] if isinstance(t.db_genera, str) and t.db_genera else []))
            found = found_species.get(b, ())
            if not found and not listed:
                continue
            srv_keys, final = set(json.loads(t.srv_predictions)), set(json.loads(t.orbit_predictions))
            rule_changed = set()
            if b in diff:
                added, removed = diff[b][1]
                final = (final - removed) | added
                rule_changed = {k.split("|")[0] for k in added | removed}
            ours, srv = colours_by_species(final), colours_by_species(srv_keys)
            complete = bool(listed) and listed <= found_genera.get(b, set())
            engine_star = new_star.get(b, t.orbit_star_class)
            star_changed = t.srv_star_class != engine_star
            confirmed = {s: c for s, c in truth.get(b, ())}

            def row(kind, s, file):
                cause = "+".join(x for x, on in (("star", star_changed), ("rule", s in rule_changed)) if on)
                conf = confirmed.get(s, "")
                srv_right = (conf in srv.get(s, ())) if conf else (s in srv) == (s in found)
                files[file].write("\t".join(map(str, (names.get(t.system_id64, ""), t.name, s, kind, conf, ",".join(ours.get(s, ())),
                                                      ",".join(srv.get(s, ())), srv_right, complete, engine_star,
                                                      t.srv_star_class, cause))) + "\n")
                counts[file, complete] += 1
                counts[kind] += 1

            def absent(s):
                g = genus_of.get(s) or s.split(" ")[0]
                return s not in found and (g in found_genera.get(b, ()) or (bool(listed) and g not in listed))

            for s in found:
                if s not in KNOWN_SPECIES:
                    continue  # airless forms: scored by score_airless_species.py
                conf = confirmed.get(s)
                if s not in ours:
                    row("missed", s, "failures_missed")
                elif conf and conf not in ours[s]:
                    row("wrong_colour", s, "failures_wrong_colour")
                if s in ours and s not in srv:
                    row("species_now_predicted", s, "improvements")
                elif conf and conf in ours.get(s, ()) and conf not in srv.get(s, ()):
                    row("colour_fixed", s, "improvements")
            for s in ours:
                if absent(s):
                    row("false_prediction", s, "failures_false_predictions")
            for s in srv:
                if s not in ours and absent(s):
                    row("false_prediction_removed", s, "improvements")
    for f in files.values():
        f.close()
    return counts


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=Path("/data/Databases_v3/bio_analysis"))
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--limit-sectors", type=int, default=0, help="Only this many sectors, to test the script")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)

    cols = ["body_id64", "system_id64", "sector", "has_prediction", "orbit_predictions", "orbit_star_class", "orbit_surfaceTemperature"]
    df = pd.concat([pd.read_parquet(p, columns=cols) for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet")))],
                   ignore_index=True)
    df = df[df.has_prediction & df.orbit_predictions.notna()]
    n_bodies = len(df)
    ids = set(df.body_id64)
    for i, (sector, g) in enumerate(df.groupby("sector")):
        if args.limit_sectors and i >= args.limit_sectors:
            break
        WANT[sector] = dict(zip(g.body_id64, zip(g.orbit_star_class, g.orbit_surfaceTemperature)))
        WANT_SYSTEMS[sector] = g.system_id64.unique().tolist()
    del df
    gc.collect()
    gc.freeze()  # keep forked workers from copying the parent's objects
    diff, predicted, mismatched, new_star = {}, 0, 0, {}
    sectors = sorted(WANT)
    # Read names now: forking from the parent after scoring, when it holds far more, ran out of memory
    names = {}
    for part in run_sectors(system_names, sectors, args.workers):
        names.update(part)
    WANT_SYSTEMS.clear()
    for n, mismatch, rows, moved in run_sectors(work, sectors, args.workers):
        predicted += n
        mismatched += mismatch
        diff.update(rows)
        new_star.update(moved)
    WANT.clear()
    gc.unfreeze()
    print(f"re-predicted {predicted:,} of {n_bodies:,} bodies, {len(diff):,} changed, "
          f"{mismatched:,} with a different orbit star class from the database", flush=True)

    lm = pd.read_parquet(args.root / "landmarks/landmarks.parquet", columns=["body_id64", "genus", "species", "colour"])
    lm = lm[lm.body_id64.isin(ids)]
    found_genera, found_species, truth = defaultdict(set), defaultdict(set), defaultdict(set)
    for b, g, s, c in zip(lm.body_id64, lm.genus, lm.species, lm.colour):
        found_genera[b].add(g)
        found_species[b].add(s)
        if c is not None:
            truth[b].add(f"{s}|{c}")
    # Genus lists and stored predictions, only for bodies that are scored or changed
    needed = set(found_genera) | set(diff)
    walk = pd.read_parquet(args.root / "walk_bodies/walk_bodies.parquet", columns=["body_id64", "api_genera"])
    api = dict(zip(walk.body_id64, walk.api_genera))
    del walk, ids
    lists, stored = {}, {"srv": {}, "orbit": {}}
    for p in sorted(glob.glob(str(args.root / "bodies/part_*.parquet"))):
        part = pd.read_parquet(p, columns=["body_id64", "db_genera", "srv_predictions", "orbit_predictions"])
        part = part[part.orbit_predictions.notna() & part.body_id64.isin(needed)]
        for b, db, sp, op in zip(part.body_id64, part.db_genera, part.srv_predictions, part.orbit_predictions):
            a = json.loads(api[b]) if api.get(b) else []
            lists[b] = set(a or ([trees.CODE_GENUS.get(c, c) for c in json.loads(db)] if db else []))
            stored["srv"][b], stored["orbit"][b] = sp, op
    complete = [b for b in lists if lists[b] and lists[b] <= found_genera[b]]

    parse = lambda s: set(json.loads(s))

    def corrected(name, i):
        def pred(b):
            p = parse(stored[name][b])
            if b in diff:
                p = (p - diff[b][i][1]) | diff[b][i][0]
            return p
        return pred

    rows_for = {"SrvSurvey star, as is": lambda b: parse(stored["srv"][b]), "orbit star, as is": corrected("orbit", 2),
                "SrvSurvey star, corrected": corrected("srv", 0), "orbit star, corrected": corrected("orbit", 1)}
    predictable = {k.split("|")[0] for b in complete for f in rows_for.values() for k in f(b)}
    lines = [f"Re-predicted {predicted:,} of {n_bodies:,} bodies; orbit star class differs from the database on {mismatched:,}",
             f"Complete bodies: {len(complete):,}", ""]
    for label, pred in rows_for.items():
        tp = fp = fn = tc = fc = nc = 0
        for b in complete:
            p, t = pred(b), truth[b]
            ps, ts = {k.split("|")[0] for k in p}, found_species[b] & predictable
            tp += len(ts & ps); fp += len(ps - ts); fn += len(ts - ps)
            tc += len(t & p); fc += len(p - t); nc += len(t - p)
        lines.append(f"{label:27} species P {tp / (tp + fp):.2%} R {tp / (tp + fn):.2%} FP {fp:,} FN {fn:,} | "
                     f"colour P {tc / (tc + fc):.2%} R {tc / (tc + nc):.2%} FP {fc:,} FN {nc:,}")

    # What changed per species, under each star, on every body where the truth is known
    # The orbit counts are against stored predictions, so a body whose star moved also counts that change
    for star_i, name in ((0, "SrvSurvey"), (1, "orbit")):
        counts = defaultdict(lambda: [0] * 6)  # added right/wrong/unknown, removed right/wrong/unknown
        for b, d in diff.items():
            added, removed = d[star_i]
            for keys, base in ((added, 0), (removed, 3)):
                for s in {k.split("|")[0] for k in keys}:
                    g = s.split(" ")[0]
                    known = g in found_genera[b] or (lists.get(b) and g not in lists[b])
                    counts[s][base + (0 if s in found_species[b] else 1 if known else 2)] += 1
        lines += ["", f"Changes by species, {name} star: added right/wrong/unknown | removed right/wrong/unknown"]
        for s, c in sorted(counts.items()):
            lines.append(f"  {s:24} {c[0]:>7,} {c[1]:>7,} {c[2]:>7,} | {c[3]:>7,} {c[4]:>7,} {c[5]:>7,}")
    final = rows_for["orbit star, corrected"]
    table = defaultdict(lambda: [0] * 5)  # present and predicted, predicted wrongly, missed, right colour, wrong colour
    for b in complete:
        p, t = final(b), truth[b]
        ps = {k.split("|")[0] for k in p}
        for s in found_species[b] | ps:
            if s in found_species[b] and s in ps:
                table[s][0] += 1
            elif s in ps:
                table[s][1] += 1
            elif s in predictable:
                table[s][2] += 1
        for k in t:
            table[k.split("|")[0]][3 if k in p else 4] += 1
    pd.DataFrame([[s, *c] for s, c in sorted(table.items())],
                 columns=["species", "tp", "fp", "fn", "colour_right", "colour_missed"]).to_csv(args.output / "per_species.tsv", sep="\t", index=False)
    KNOWN_SPECIES.update(predictable)
    del stored, lists, truth, found_genera, found_species, table, lm, api
    gc.collect()
    # Saved first, so that a failure while writing the lists keeps the scores and the changes
    (args.output / "results.txt").write_text("\n".join(lines) + "\n")
    with open(args.output / "changes.pkl", "wb") as f:
        pickle.dump({"diff": diff, "names": names, "new_star": new_star}, f, protocol=pickle.HIGHEST_PROTOCOL)
    counts = write_lists(args.root, args.output / "lists", diff, names, new_star)
    lines += ["", "Lists (all bodies with known truth; of which complete):"]
    for name in ("failures_missed", "failures_wrong_colour", "failures_false_predictions", "improvements"):
        lines.append(f"  {name:28} {counts[name, True] + counts[name, False]:>10,} ({counts[name, True]:,})")
    for kind in ("species_now_predicted", "colour_fixed", "false_prediction_removed"):
        lines.append(f"  improvement: {kind:26} {counts[kind]:>10,}")
    (args.output / "results.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
