#!/usr/bin/env python3
"""Build sector_index.json for a Parquet sector database.

Reads coords (x, y, z) from each sector's systems.parquet using column
pruning and computes the mean as the sector center.  The output format
matches the one consumed by SectorResolver.resolve_corridor_mode().

Usage:
    python scripts/build_parquet_sector_index.py \
        --source /data/Databases_v2/galaxy_sectors_parquet \
        --workers 8
"""

import argparse
import json
import logging
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def _process_sector(sector_dir: str) -> dict:
    """Compute center coords for a single sector directory.

    Args:
        sector_dir: Path string to sector directory.

    Returns:
        Dict with sector name and center coords, or error info.
    """
    sector_path = Path(sector_dir)
    systems_path = sector_path / "systems.parquet"
    sector_name = sector_path.name

    try:
        table = pq.read_table(systems_path, columns=["x", "y", "z"])
        n = len(table)
        if n == 0:
            return {"name": sector_name, "error": "empty"}

        x_arr = table.column("x")
        y_arr = table.column("y")
        z_arr = table.column("z")

        # Use PyArrow compute for fast mean
        import pyarrow.compute as pc
        cx = pc.mean(x_arr).as_py()
        cy = pc.mean(y_arr).as_py()
        cz = pc.mean(z_arr).as_py()

        return {
            "name": sector_name,
            "center_coords": {
                "x": round(cx, 2),
                "y": round(cy, 2),
                "z": round(cz, 2),
            },
            "system_count": n,
        }
    except Exception as e:
        return {"name": sector_name, "error": str(e)}


def build_index(source_dir: Path, workers: int = 8) -> dict:
    """Build the sector index from a Parquet database.

    Args:
        source_dir: Root directory of the Parquet database.
        workers: Number of parallel workers.

    Returns:
        Complete index dict ready for JSON serialization.
    """
    # Discover all sector directories
    sector_dirs = sorted(
        d for d in source_dir.iterdir()
        if d.is_dir() and (d / "systems.parquet").exists()
    )

    print(f"Found {len(sector_dirs)} sector directories")

    sectors = {}
    errors = 0
    start = time.time()

    mp_context = multiprocessing.get_context("fork")
    with ProcessPoolExecutor(max_workers=workers, mp_context=mp_context) as executor:
        futures = {
            executor.submit(_process_sector, str(sd)): sd
            for sd in sector_dirs
        }

        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()

            if "error" in result:
                errors += 1
                logger.warning("Sector %s: %s", result["name"], result["error"])
            else:
                sectors[result["name"]] = {
                    "center_coords": result["center_coords"],
                    "system_count": result["system_count"],
                }

            if done % 1000 == 0 or done == len(sector_dirs):
                elapsed = time.time() - start
                print(f"  Processed {done}/{len(sector_dirs)} sectors "
                      f"({elapsed:.1f}s)")

    elapsed = time.time() - start
    print(f"Computed centers for {len(sectors)} sectors in {elapsed:.1f}s "
          f"({errors} errors)")

    index = {
        "metadata": {
            "created_at": datetime.now().isoformat(),
            "source_database": str(source_dir),
            "total_sectors": len(sectors),
            "format": "parquet",
        },
        "sectors": sectors,
    }
    return index


def main():
    parser = argparse.ArgumentParser(
        description="Build sector_index.json for a Parquet sector database"
    )
    parser.add_argument(
        "--source", required=True, type=Path,
        help="Root directory of the Parquet sector database",
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Output path (default: <source>/sector_index.json)",
    )
    parser.add_argument(
        "--workers", type=int, default=8,
        help="Number of parallel workers (default: 8)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    if not args.source.is_dir():
        print(f"Error: source directory not found: {args.source}")
        return 1

    index = build_index(args.source, args.workers)

    output_path = args.output or (args.source / "sector_index.json")
    with open(output_path, "w") as f:
        json.dump(index, f, indent=2)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"Sector index written to: {output_path} ({size_mb:.1f} MB)")
    return 0


if __name__ == "__main__":
    exit(main())
