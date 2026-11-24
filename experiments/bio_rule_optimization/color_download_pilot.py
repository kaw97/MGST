"""Pilot for the colour download: cursor reach, true population size, colour names.

For a few species, checks that a distance filter reaches past the API's 10,000
result window, counts each species' true population by splitting distance
shells until counts are exact, and samples pages to compare landmark colours
with SrvSurvey's criteria. Raw responses are kept in the output directory.
"""

import argparse
import gzip
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))

import srvsurv_engine as engine

API = "https://spansh.co.uk/api/bodies/search"
DELAY = 2.0
MAX_DISTANCE = 70_000.0
MAX_COUNT_REQUESTS = 60
SPECIES = ["Stratum Tectonicas", "Bacterium Aurasus", "Tussock Ventusa", "Frutexa Flabellum",
           "Stratum Frigus", "Bacterium Acies"]
STATE = {"requests": 0}


def search(species, raw, size=1, dmin=None, dmax=None):
    filters = {"type": {"value": ["Planet"]}, "landmark_subtype": {"value": [species]}}
    if dmin is not None or dmax is not None:
        filters["distance"] = {k: v for k, v in (("min", dmin), ("max", dmax)) if v is not None}
    payload = {"filters": filters, "sort": [{"distance": {"direction": "asc"}}], "size": size, "from": 0}
    time.sleep(DELAY)
    for attempt in range(4):
        STATE["requests"] += 1
        try:
            r = requests.post(API, json=payload, timeout=120)
            r.raise_for_status()
            data = r.json()
            raw.write(json.dumps({"request": payload, "response": data}) + "\n")
            raw.flush()
            return data
        except (requests.RequestException, ValueError) as e:
            print(f"  retry {attempt + 1} after {e}", flush=True)
            time.sleep(10 * (attempt + 1))
    raise SystemExit("API unavailable; stopping")


def population(species, raw):
    """(bodies counted exactly, shells still over the window, lower bound flag)."""
    stack, exact, saturated, used = [(0.0, MAX_DISTANCE)], 0, [], 0
    while stack:
        a, b = stack.pop()
        if used >= MAX_COUNT_REQUESTS:
            saturated.append((a, b))
            continue
        used += 1
        n = search(species, raw, dmin=a, dmax=b)["count"]
        if n >= 10_000:
            mid = (a + b) / 2
            stack += [(mid, b), (a, mid)]
        else:
            exact += n
    return exact, saturated, used


def criteria_colours():
    out = defaultdict(set)

    def walk(n, g=None, s=None, common=None):
        common = n["commonChildren"] if n["commonChildren"] is not None else common
        g, s = n["genus"] or g, n["species"] or s
        if n["variant"]:
            out[f"{g} {s}"].add(n["variant"])
        for c in (common if n["useCommonChildren"] else n["children"]) or []:
            walk(c, g, s, common)

    for c in engine.load_criteria():
        walk(c)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)

    targets = criteria_colours()
    report = {}
    seen = Counter()
    unknown, per_body_conflicts, landmark_records = Counter(), 0, 0
    bodies_seen, species_on_body = set(), 0
    with gzip.open(args.output / "raw.jsonl.gz", "wt") as raw:
        for sp in SPECIES:
            print(f"{sp}: counting", flush=True)
            exact, saturated, used = population(sp, raw)
            # Cursor test: page beyond the first 10,000 by distance, then far out
            first = search(sp, raw, size=100)
            ds = [b.get("distance") for b in first["results"]]
            pages = [first]
            far = None
            for d in (1_000.0, 5_000.0, 20_000.0):
                p = search(sp, raw, size=100, dmin=d)
                pages.append(p)
                if p["results"]:
                    far = (d, p["count"], p["results"][0].get("distance"), p["results"][-1].get("distance"))
            for p in pages:
                for b in p["results"]:
                    if b.get("id64") in bodies_seen:
                        continue
                    bodies_seen.add(b.get("id64"))
                    colours = defaultdict(set)
                    for lm in b.get("landmarks", []):
                        if lm.get("value", 0) <= 0:
                            continue
                        colours[lm["subtype"]].add(lm.get("variant"))
                    species_on_body += len(colours)
                    for s, vs in colours.items():
                        landmark_records += 1
                        per_body_conflicts += len(vs) > 1
                        for v in vs:
                            seen[(s, v)] += 1
                            if v not in targets.get(s, ()):
                                unknown[(s, v)] += 1
            report[sp] = {
                "exact_count": exact, "unresolved_shells": saturated, "count_requests": used,
                "first_page_distance": [ds[0], ds[-1]] if ds else None,
                "furthest_cursor_test": far,
                "colours_in_criteria": len(targets.get(sp, ())),
                "colours_seen": sorted(v for s, v in seen if s == sp and v),
            }
            print(json.dumps({sp: report[sp]}), flush=True)

    summary = {
        "requests": STATE["requests"], "bodies_sampled": len(bodies_seen),
        "species_per_body": species_on_body / max(len(bodies_seen), 1),
        "species_records": landmark_records, "bodies_species_with_several_colours": per_body_conflicts,
        "records_not_in_criteria": sum(unknown.values()), "records_total": sum(seen.values()),
        "not_in_criteria_top": [[*k, v] for k, v in unknown.most_common(40)],
        "species": report,
    }
    (args.output / "summary.json").write_text(json.dumps(summary, indent=1))
    print(json.dumps({k: v for k, v in summary.items() if k != "species"}, indent=1))


if __name__ == "__main__":
    main()
