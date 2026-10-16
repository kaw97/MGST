"""Download species-level landmark data from Spansh API.

Downloads all confirmed biological species sightings across the galaxy,
building a reference dataset for validating our species prediction rules.

Strategy: For each species name, use the `landmark_subtype` filter to get
ONLY bodies with confirmed species-level scan data. This is much faster
than filtering by genus (which returns all FSS-detected bodies).

Output: Parquet files mapping body_id64 -> confirmed species landmarks.
"""

import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

API = "https://spansh.co.uk/api/bodies/search"
DELAY = 2.0  # seconds between requests
PAGE_SIZE = 100
MAX_BODIES_PER_SPECIES = 10_000
OUTPUT_DIR = Path("Databases/spansh_landmarks")

# All species we want landmark data for.
# Includes our 18 targets plus common same-genus competitors
# (needed to understand false positives — e.g., bodies where Stratum Paleas
# appears instead of Stratum Tectonicas).
TARGET_SPECIES = [
    # Stratum genus
    "Stratum Tectonicas",
    "Stratum Paleas",
    "Stratum Cucumisis",
    "Stratum Excutitus",
    "Stratum Araneamus",
    "Stratum Frigeas",
    "Stratum Laminamus",
    # Tussock genus
    "Tussock Stigmasis",
    "Tussock Virgam",
    "Tussock Cultro",
    "Tussock Catena",
    "Tussock Pennata",
    "Tussock Pennatis",
    "Tussock Ignis",
    "Tussock Divisa",
    "Tussock Serrati",
    "Tussock Triticum",
    "Tussock Capillum",
    "Tussock Caputus",
    "Tussock Propagito",
    "Tussock Albata",
    # Fonticulua genus
    "Fonticulua Fluctus",
    "Fonticulua Segmentatus",
    "Fonticulua Campestris",
    "Fonticulua Digitos",
    "Fonticulua Lapida",
    "Fonticulua Upupam",
    # Clypeus genus
    "Clypeus Margaritus",
    "Clypeus Speculumi",
    "Clypeus Lacrimam",
    # Cactoida genus
    "Cactoida Vermis",
    "Cactoida Cortexum",
    "Cactoida Lapis",
    "Cactoida Pullulanta",
    "Cactoida Peperatis",
    # Fumerola genus
    "Fumerola Extremus",
    "Fumerola Nitris",
    "Fumerola Carbosis",
    "Fumerola Aquatis",
    # Recepta genus
    "Recepta Deltahedronix",
    "Recepta Conditivus",
    "Recepta Umbrux",
    # Osseus genus
    "Osseus Discus",
    "Osseus Pumice",
    "Osseus Spiralis",
    "Osseus Cornibus",
    "Osseus Fractus",
    "Osseus Pellebantus",
    # Aleoida genus
    "Aleoida Gravis",
    "Aleoida Arcus",
    "Aleoida Coronamus",
    "Aleoida Laminiae",
    "Aleoida Spica",
    # Tubus genus
    "Tubus Cavas",
    "Tubus Compagibus",
    "Tubus Conifer",
    "Tubus Rosarium",
    "Tubus Sororibus",
    # Frutexa (Shrubs) genus
    "Frutexa Flammasis",
    "Frutexa Flabellum",
    "Frutexa Acus",
    "Frutexa Collum",
    "Frutexa Fera",
    "Frutexa Metallicum",
    "Frutexa Sponsae",
    # Concha genus
    "Concha Biconcavis",
    "Concha Aureolas",
    "Concha Labiata",
    "Concha Renibus",
    # Bacterium (reference — most common genus)
    "Bacterium Informem",
    "Bacterium Aurasus",
    "Bacterium Vesicula",
]


def fetch_page(filters, size=PAGE_SIZE):
    payload = {
        "filters": filters,
        "sort": [{"distance": {"direction": "asc"}}],
        "size": size,
    }
    for attempt in range(3):
        try:
            resp = requests.post(API, json=payload, timeout=120)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                wait = DELAY * (attempt + 2)
                print(f"    Retry {attempt+1} after {wait}s: {e}", flush=True)
                time.sleep(wait)
            else:
                raise


def extract_body_data(body):
    """Extract relevant fields from a Spansh body record."""
    bio_landmarks = []
    seen_species = set()
    for lm in body.get("landmarks", []):
        if lm.get("value", 0) > 0:
            sp = lm["subtype"]
            if sp not in seen_species:
                seen_species.add(sp)
                bio_landmarks.append({
                    "genus": lm["type"],
                    "species": sp,
                    "value": lm["value"],
                })

    genuses = [g["name"] for g in body.get("genuses", [])]

    bio_signal_count = 0
    for sig in body.get("signals", []):
        if sig.get("name") == "Biological":
            bio_signal_count = sig.get("count", 0)

    return {
        "body_id64": body.get("id64"),
        "system_id64": body.get("system_id64"),
        "body_name": body.get("name", ""),
        "system_name": body.get("system_name", ""),
        "system_x": body.get("system_x"),
        "system_y": body.get("system_y"),
        "system_z": body.get("system_z"),
        "subtype": body.get("subtype", ""),
        "atmosphere": body.get("atmosphere", ""),
        "gravity": body.get("gravity"),
        "surface_temperature": body.get("surface_temperature"),
        "surface_pressure": body.get("surface_pressure"),
        "volcanism_type": body.get("volcanism_type", ""),
        "bio_signal_count": bio_signal_count,
        "genuses_json": json.dumps(genuses),
        "landmarks_json": json.dumps(bio_landmarks),
    }


def download_species(species_name):
    """Download bodies with a confirmed landmark, using distance-based cursor."""
    filters = {
        "type": {"value": ["Planet"]},
        "landmark_subtype": {"value": [species_name]},
    }

    bodies = []
    page = 0
    min_distance = 0.0

    while True:
        cur_filters = dict(filters)
        if min_distance > 0:
            cur_filters["distance"] = {"min": min_distance}

        try:
            data = fetch_page(cur_filters)
        except Exception as e:
            print(f"    FATAL error at page {page}: {e}", flush=True)
            break

        results = data.get("results", [])
        if not results:
            break

        for body in results:
            bodies.append(extract_body_data(body))

        last_dist = results[-1].get("distance", 0)
        min_distance = last_dist + 0.0001

        page += 1

        if page % 10 == 0:
            print(f"    Page {page}: {len(bodies)} bodies (dist={last_dist:.1f} ly)",
                  flush=True)

        if len(results) < PAGE_SIZE:
            break

        if len(bodies) >= MAX_BODIES_PER_SPECIES:
            print(f"    Capped at {MAX_BODIES_PER_SPECIES} bodies", flush=True)
            break

        time.sleep(DELAY)

    return bodies


def bodies_to_parquet(all_bodies, output_path):
    """Convert collected body data to a Parquet file."""
    if not all_bodies:
        return

    rows = {col: [] for col in [
        "body_id64", "system_id64", "body_name", "system_name",
        "system_x", "system_y", "system_z",
        "subtype", "atmosphere", "gravity", "surface_temperature",
        "surface_pressure", "volcanism_type", "bio_signal_count",
        "genuses_json", "landmarks_json",
    ]}

    for body in all_bodies:
        for col in rows:
            rows[col].append(body.get(col))

    schema = pa.schema([
        ("body_id64", pa.int64()),
        ("system_id64", pa.int64()),
        ("body_name", pa.string()),
        ("system_name", pa.string()),
        ("system_x", pa.float64()),
        ("system_y", pa.float64()),
        ("system_z", pa.float64()),
        ("subtype", pa.string()),
        ("atmosphere", pa.string()),
        ("gravity", pa.float64()),
        ("surface_temperature", pa.float64()),
        ("surface_pressure", pa.float64()),
        ("volcanism_type", pa.string()),
        ("bio_signal_count", pa.int64()),
        ("genuses_json", pa.string()),
        ("landmarks_json", pa.string()),
    ])

    table = pa.table(rows, schema=schema)
    pq.write_table(table, output_path, compression="zstd")
    print(f"  Wrote {len(table)} rows to {output_path}", flush=True)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Resume support
    status_file = OUTPUT_DIR / "download_status.json"
    if status_file.exists():
        with open(status_file) as f:
            status = json.load(f)
    else:
        status = {"completed_species": [], "species_counts": {}}

    completed = set(status["completed_species"])
    print(f"Resume: {len(completed)} species already downloaded", flush=True)

    all_species_counts = Counter(status.get("species_counts", {}))

    for species_name in TARGET_SPECIES:
        if species_name in completed:
            continue

        print(f"\n--- {species_name} ---", flush=True)
        bodies = download_species(species_name)
        count = len(bodies)
        print(f"  TOTAL: {count} bodies with {species_name} landmark", flush=True)

        if bodies:
            safe_name = species_name.replace(" ", "_")
            species_file = OUTPUT_DIR / f"species_{safe_name}.parquet"
            bodies_to_parquet(bodies, species_file)

        status["completed_species"].append(species_name)
        status["species_counts"][species_name] = count
        all_species_counts[species_name] = count
        with open(status_file, "w") as f:
            json.dump(status, f, indent=2)

    # Merge all species files into one deduplicated file
    print(f"\n{'='*70}", flush=True)
    print("Merging all species files...", flush=True)
    print(f"{'='*70}", flush=True)

    seen = {}
    for pf in sorted(OUTPUT_DIR.glob("species_*.parquet")):
        table = pq.read_table(pf)
        for row in table.to_pylist():
            bid = row["body_id64"]
            if bid is None:
                continue
            if bid in seen:
                # Merge landmarks from different species queries
                existing_lm = json.loads(seen[bid]["landmarks_json"])
                new_lm = json.loads(row["landmarks_json"])
                existing_sp = {lm["species"] for lm in existing_lm}
                for lm in new_lm:
                    if lm["species"] not in existing_sp:
                        existing_lm.append(lm)
                seen[bid]["landmarks_json"] = json.dumps(existing_lm)
                # Merge genuses
                eg = set(json.loads(seen[bid]["genuses_json"]))
                ng = set(json.loads(row["genuses_json"]))
                seen[bid]["genuses_json"] = json.dumps(sorted(eg | ng))
            else:
                seen[bid] = row

    merged = list(seen.values())
    print(f"Unique bodies: {len(merged)}", flush=True)

    final_file = OUTPUT_DIR / "all_landmarks.parquet"
    bodies_to_parquet(merged, final_file)

    # Summary
    print(f"\n{'='*70}", flush=True)
    print(f"FINAL SUMMARY", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"Total unique bodies with landmarks: {len(merged)}", flush=True)
    print(f"\nSpecies counts:", flush=True)
    for sp, count in sorted(all_species_counts.items(), key=lambda x: -x[1]):
        print(f"  {sp:<35}: {count:>7}", flush=True)


if __name__ == "__main__":
    main()
