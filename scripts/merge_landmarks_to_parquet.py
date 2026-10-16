"""Merge Spansh landmark data into the galaxy Parquet database.

Takes the downloaded landmarks Parquet (from download_spansh_landmarks.py)
and adds landmark columns to each sector's bodies.parquet file.

New columns added to bodies.parquet:
  - landmark_genuses: JSON list of genus names confirmed on this body
  - landmark_species: JSON list of {genus, species, value} dicts
  - landmark_count: number of confirmed bio species on this body

Bodies not in the landmark database get NULL values (not scanned or no bio).
"""

import argparse
import json
import multiprocessing
import os
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def load_landmark_lookup(landmarks_path):
    """Load landmarks Parquet into a dict: body_id64 -> landmark data."""
    table = pq.read_table(landmarks_path)
    lookup = {}
    for row in table.to_pylist():
        bid = row["body_id64"]
        if bid is None:
            continue
        landmarks = json.loads(row["landmarks_json"])
        genuses = json.loads(row["genuses_json"])
        lookup[bid] = {
            "genuses": genuses,
            "species": landmarks,
            "count": len(landmarks),
        }
    return lookup


def process_sector(sector_dir, landmark_lookup):
    """Add landmark columns to a sector's bodies.parquet."""
    bodies_path = Path(sector_dir) / "bodies.parquet"
    if not bodies_path.exists():
        return 0, 0

    table = pq.read_table(bodies_path)
    n_bodies = len(table)

    # Get body id64s
    if "id64" not in table.schema.names:
        return n_bodies, 0

    id64_col = table.column("id64")

    # Build new columns
    landmark_genuses = []
    landmark_species = []
    landmark_count = []
    matches = 0

    for i in range(n_bodies):
        bid = id64_col[i].as_py()
        data = landmark_lookup.get(bid) if bid is not None else None

        if data is not None:
            matches += 1
            landmark_genuses.append(json.dumps(data["genuses"]))
            landmark_species.append(json.dumps(data["species"]))
            landmark_count.append(data["count"])
        else:
            landmark_genuses.append(None)
            landmark_species.append(None)
            landmark_count.append(None)

    # Remove old landmark columns if they exist (for re-runs)
    cols_to_remove = {"landmark_genuses", "landmark_species", "landmark_count"}
    existing_names = set(table.schema.names)
    keep_indices = [
        i for i, name in enumerate(table.schema.names)
        if name not in cols_to_remove
    ]
    if cols_to_remove & existing_names:
        table = table.select(keep_indices)

    # Add new columns
    table = table.append_column(
        "landmark_genuses", pa.array(landmark_genuses, type=pa.string())
    )
    table = table.append_column(
        "landmark_species", pa.array(landmark_species, type=pa.string())
    )
    table = table.append_column(
        "landmark_count", pa.array(landmark_count, type=pa.int64())
    )

    # Write back
    pq.write_table(table, bodies_path, compression="zstd")

    return n_bodies, matches


def main():
    parser = argparse.ArgumentParser(description="Merge landmarks into Parquet DB")
    parser.add_argument(
        "--landmarks",
        default="Databases/spansh_landmarks/all_landmarks.parquet",
        help="Path to merged landmarks Parquet file",
    )
    parser.add_argument(
        "--database",
        default="/data/Databases_v2/galaxy_sectors_parquet",
        help="Path to galaxy sectors Parquet database",
    )
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel workers (1 recommended for disk I/O)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count matches without writing")
    args = parser.parse_args()

    landmarks_path = Path(args.landmarks)
    database_path = Path(args.database)

    if not landmarks_path.exists():
        print(f"Landmarks file not found: {landmarks_path}", flush=True)
        sys.exit(1)

    print(f"Loading landmarks from {landmarks_path}...", flush=True)
    lookup = load_landmark_lookup(landmarks_path)
    print(f"Loaded {len(lookup)} body landmark records", flush=True)

    # Discover sectors
    sectors = sorted([
        d for d in database_path.iterdir()
        if d.is_dir() and (d / "bodies.parquet").exists()
    ])
    print(f"Found {len(sectors)} sectors to process", flush=True)

    if args.dry_run:
        print("DRY RUN — counting matches only", flush=True)

    total_bodies = 0
    total_matches = 0

    try:
        from tqdm import tqdm
        progress = tqdm(total=len(sectors), desc="Sectors", unit="sector")
    except ImportError:
        progress = None

    for sector in sectors:
        if args.dry_run:
            # Just count matches
            bodies_path = sector / "bodies.parquet"
            table = pq.read_table(bodies_path, columns=["id64"])
            n = len(table)
            m = sum(1 for row in table.column("id64") if row.as_py() in lookup)
            total_bodies += n
            total_matches += m
        else:
            n, m = process_sector(str(sector), lookup)
            total_bodies += n
            total_matches += m

        if progress:
            progress.update(1)
            if total_matches > 0:
                progress.set_postfix(matches=total_matches)

    if progress:
        progress.close()

    print(f"\nDone! Processed {total_bodies} bodies across {len(sectors)} sectors", flush=True)
    print(f"Landmark matches: {total_matches} ({total_matches/total_bodies*100:.2f}%)", flush=True)


if __name__ == "__main__":
    main()
