"""Walk every Spansh body with a confirmed exobiology landmark, keeping colours.

Makes one distance-ordered walk over all bodies carrying any listed species and
stores each raw API page. Bodies in a system share a distance, so each page
restarts at the last distance (repeats are removed later by id64); `from` only
advances if a whole page sits at one distance. Can stop and resume at any time:
each page is its own gzip member, and state is saved after it is written.

One request at a time. The delay rises to the slow delay after an error or a
slow response, and recovers after a run of healthy requests.
"""

import argparse
import ast
import gzip
import json
import os
import sys
import time
from pathlib import Path

import requests

API = "https://spansh.co.uk/api/bodies/search"
PAGE_SIZE = 100
SLOW_RESPONSE = 5.0
RECOVER_AFTER = 100
MAX_FAILURES = 20
PART_PAGES = 1000
REPO = Path(__file__).resolve().parent.parent


def species_list():
    """Every species named by the old download script, the SrvSurvey criteria or the engine check."""
    names = set()
    tree = ast.parse((REPO / "scripts/download_spansh_landmarks.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, (ast.List, ast.Dict)):
            for c in ast.walk(node.value):
                if isinstance(c, ast.Constant) and isinstance(c.value, str) and " " in c.value:
                    names.add(c.value)
    sys.path.insert(0, str(REPO / "experiments/bio_rule_optimization"))
    import srvsurv_engine as engine

    def walk(n, g=None, s=None, common=None):
        common = n["commonChildren"] if n["commonChildren"] is not None else common
        g, s = n["genus"] or g, n["species"] or s
        if g and s:
            names.add(f"{g} {s}")
        for c in (common if n["useCommonChildren"] else n["children"]) or []:
            walk(c, g, s, common)

    for c in engine.load_criteria():
        walk(c)
    return sorted(names)


def payload(species, dmin, offset, size=PAGE_SIZE):
    filters = {"type": {"value": ["Planet"]}, "landmark_subtype": {"value": species}}
    if dmin is not None:
        filters["distance"] = {"min": dmin}
    return {"filters": filters, "sort": [{"distance": {"direction": "asc"}}], "size": size, "from": offset}


def preflight(species, delay):
    """Check that a list of species means 'any of them', and that the full list is accepted.

    Counts are limited to beyond 20,000 ly so they stay under the API's 10,000 count cap.
    """
    def count(names):
        time.sleep(delay)
        r = requests.post(API, json=payload(names, 20_000.0, 0, 1), timeout=120)
        r.raise_for_status()
        return r.json()["count"]

    a, b = "Stratum Frigus", "Tussock Ventusa"
    na, nb, both = count([a]), count([b]), count([a, b])
    print(f"preflight: {a} {na:,}  {b} {nb:,}  either {both:,}", flush=True)
    if not (max(na, nb) < both <= na + nb):
        sys.exit("A species list does not act as 'any of'; not walking.")
    time.sleep(delay)
    r = requests.post(API, json=payload(species, None, 0), timeout=120)
    r.raise_for_status()
    data = r.json()
    wanted = set(species)
    bad = [x["id64"] for x in data["results"] if not wanted & {l.get("subtype") for l in x.get("landmarks", [])}]
    print(f"preflight: {len(species)} species, count {data['count']:,}, page {len(data['results'])}, "
          f"bodies without a listed species {len(bad)}", flush=True)
    if not data["results"] or bad:
        sys.exit("Full species list not honoured; not walking.")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--output", type=Path, default=Path("/data/Databases_v3/bio_analysis/raw"))
    ap.add_argument("--delay", type=float, default=1.0)
    ap.add_argument("--slow-delay", type=float, default=2.0)
    ap.add_argument("--preflight-only", action="store_true")
    args = ap.parse_args()

    species = species_list()
    preflight(species, args.delay)
    if args.preflight_only:
        return

    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "species.json").write_text(json.dumps(species, indent=0))
    state_path = args.output / "state.json"
    state = {"dmin": None, "offset": 0, "pages": 0, "results": 0, "finished": False}
    if state_path.exists():
        state = json.loads(state_path.read_text())
        print(f"resuming: {state}", flush=True)
    if state["finished"]:
        return

    delay, healthy, failures, started = args.delay, 0, 0, time.time()
    while True:
        time.sleep(delay)
        t = time.time()
        try:
            r = requests.post(API, json=payload(species, state["dmin"], state["offset"]), timeout=180)
            r.raise_for_status()
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            failures += 1
            delay, healthy = args.slow_delay, 0
            print(f"error {failures}/{MAX_FAILURES} at {state['dmin']}: {e}", flush=True)
            if failures >= MAX_FAILURES:
                sys.exit("Too many failures in a row; stopped. Run again to resume.")
            time.sleep(min(30 * failures, 600))
            continue
        failures = 0
        healthy += 1
        if time.time() - t > SLOW_RESPONSE:
            delay, healthy = args.slow_delay, 0
        elif healthy >= RECOVER_AFTER:
            delay = args.delay

        results = data.get("results", [])
        part = args.output / f"part_{state['pages'] // PART_PAGES:05d}.jsonl.gz"
        with gzip.open(part, "at") as f:
            f.write(json.dumps({"dmin": state["dmin"], "offset": state["offset"], "results": results}) + "\n")

        state["pages"] += 1
        state["results"] += len(results)
        if len(results) < PAGE_SIZE:
            state["finished"] = True
        else:
            last = results[-1]["distance"]
            if last == state["dmin"]:
                state["offset"] += PAGE_SIZE
            else:
                state["dmin"], state["offset"] = last, 0
        tmp = state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state))
        os.replace(tmp, state_path)

        if state["pages"] % 500 == 0 or state["finished"]:
            rate = state["pages"] / (time.time() - started)
            print(f"{state['pages']:,} pages, {state['results']:,} results, distance {state['dmin']}, "
                  f"{rate * 3600:,.0f} pages/h, delay {delay}", flush=True)
        if state["finished"]:
            print("finished", flush=True)
            return


if __name__ == "__main__":
    main()
