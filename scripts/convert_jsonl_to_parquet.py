#!/usr/bin/env python3
"""Convert a JSONL.gz sector database to Parquet format.

Reads an existing JSONL.gz galaxy sector database and converts it to
a Parquet-based database with normalized tables (systems, bodies, stations)
per sector directory.

Usage:
    python scripts/convert_jsonl_to_parquet.py \
        --source Databases/galaxy_sectors_compressed \
        --target Databases/galaxy_sectors_parquet \
        --workers 12
"""

import argparse
import gzip
import json
import logging
import multiprocessing
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from mgst.data.parquet_schema import (
    flatten_body,
    flatten_station,
    flatten_system,
)

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def convert_sector_file(
    source_path: Path,
    target_dir: Path,
    compression: str = "zstd",
    compression_level: int = 3,
    flush_every: int = 5000,
) -> Tuple[str, int, int, int, float]:
    """Convert a single sector JSONL.gz file to Parquet tables.

    Streams the JSONL file and flushes to Parquet in chunks to keep
    memory bounded even for very large sectors (1GB+ compressed).

    Returns:
        Tuple of (sector_name, systems_count, bodies_count, stations_count, elapsed_seconds)
    """
    sector_name = source_path.stem
    if sector_name.endswith(".jsonl"):
        sector_name = sector_name[: -len(".jsonl")]

    start = time.monotonic()

    sector_dir = target_dir / sector_name
    sector_dir.mkdir(parents=True, exist_ok=True)

    systems_path = sector_dir / "systems.parquet"
    bodies_path = sector_dir / "bodies.parquet"
    stations_path = sector_dir / "stations.parquet"

    flat_systems: List[dict] = []
    flat_bodies: List[dict] = []
    flat_stations: List[dict] = []

    total_systems = 0
    total_bodies = 0
    total_stations = 0
    part_index = 0

    def _flush(force: bool = False) -> int:
        nonlocal flat_systems, flat_bodies, flat_stations, part_index
        if not flat_systems and not force:
            return part_index

        suffix = f".part{part_index}" if part_index > 0 else ""

        if flat_systems:
            _write_table(flat_systems, Path(str(systems_path) + suffix),
                         compression, compression_level)
        if flat_bodies:
            _write_table(flat_bodies, Path(str(bodies_path) + suffix),
                         compression, compression_level)
        if flat_stations:
            _write_table(flat_stations, Path(str(stations_path) + suffix),
                         compression, compression_level)

        flat_systems = []
        flat_bodies = []
        flat_stations = []
        part_index += 1
        return part_index

    with gzip.open(source_path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                system = json.loads(line)
            except json.JSONDecodeError:
                continue

            sid = system.get("id64", 0)
            flat_systems.append(flatten_system(system))
            total_systems += 1

            for body in system.get("bodies", []):
                flat_bodies.append(flatten_body(body, sid))
                total_bodies += 1

            for station in system.get("stations", []):
                flat_stations.append(flatten_station(station, sid))
                total_stations += 1

            if len(flat_systems) >= flush_every:
                _flush()

    # Final flush
    _flush()

    if total_systems == 0:
        return sector_name, 0, 0, 0, time.monotonic() - start

    # Consolidate parts if there were multiple flushes
    if part_index > 1:
        for base_path in [systems_path, bodies_path, stations_path]:
            parts = sorted(sector_dir.glob(base_path.name + ".part*"))
            if parts:
                # Read and concatenate all parts
                tables = [pq.read_table(p) for p in [base_path] + parts
                          if p.exists()]
                if tables:
                    merged = pa.concat_tables(tables, promote_options="default")
                    pq.write_table(merged, base_path,
                                   compression=compression,
                                   compression_level=compression_level)
                    del merged, tables
                # Remove part files
                for p in parts:
                    p.unlink()

    elapsed = time.monotonic() - start
    return sector_name, total_systems, total_bodies, total_stations, elapsed


def _write_table(
    rows: List[dict],
    path: Path,
    compression: str = "zstd",
    compression_level: int = 3,
) -> None:
    """Write a list of flat dicts to a Parquet file."""
    # Build column-oriented data from union of all keys
    all_keys = {}
    for row in rows:
        for k in row:
            if k not in all_keys:
                all_keys[k] = None

    columns = {key: [row.get(key) for row in rows] for key in all_keys}
    table = pa.table(columns)
    pq.write_table(table, path, compression=compression,
                   compression_level=compression_level)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert JSONL.gz sector database to Parquet format"
    )
    parser.add_argument(
        "--source",
        type=Path,
        required=True,
        help="Source directory containing sector JSONL.gz files",
    )
    parser.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Target directory for Parquet output",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )
    parser.add_argument(
        "--compression",
        choices=["zstd", "snappy", "gzip", "none"],
        default="zstd",
        help="Parquet compression codec (default: zstd)",
    )
    parser.add_argument(
        "--compression-level",
        type=int,
        default=3,
        help="Compression level (default: 3, for zstd range 1-22)",
    )
    parser.add_argument(
        "--flush-every",
        type=int,
        default=5000,
        help="Systems per chunk before flushing to disk (default: 5000, lower = less memory)",
    )
    parser.add_argument(
        "--retry-from",
        type=Path,
        default=None,
        help="File containing sector names to retry (one per line), skips already-converted sectors",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    source_dir = args.source
    target_dir = args.target

    if not source_dir.exists():
        logger.error("Source directory does not exist: %s", source_dir)
        sys.exit(1)

    # Find sector files (optionally filtered to retry list)
    if args.retry_from:
        with open(args.retry_from) as f:
            retry_names = {line.strip() for line in f if line.strip()}
        sector_files = sorted(
            sf for sf in source_dir.glob("*.jsonl.gz")
            if sf.stem.replace(".jsonl", "") in retry_names
        )
        logger.info("Retrying %d sectors from %s", len(sector_files), args.retry_from)
    else:
        sector_files = sorted(source_dir.glob("*.jsonl.gz"))

    if not sector_files:
        logger.error("No .jsonl.gz files found in %s", source_dir)
        sys.exit(1)

    logger.info("Found %d sector files in %s", len(sector_files), source_dir)
    logger.info("Output directory: %s", target_dir)
    logger.info("Workers: %d, Compression: %s (level %d)",
                args.workers, args.compression, args.compression_level)

    target_dir.mkdir(parents=True, exist_ok=True)

    total_systems = 0
    total_bodies = 0
    total_stations = 0
    completed = 0
    errors = 0
    start_time = time.monotonic()

    compression = args.compression if args.compression != "none" else None

    # Use "fork" start method for compatibility (forkserver can fail on some platforms)
    mp_context = multiprocessing.get_context("fork")

    with ProcessPoolExecutor(max_workers=args.workers,
                             mp_context=mp_context) as executor:
        futures = {
            executor.submit(
                convert_sector_file,
                sf,
                target_dir,
                compression,
                args.compression_level,
                args.flush_every,
            ): sf
            for sf in sector_files
        }

        for future in as_completed(futures):
            source_file = futures[future]
            try:
                sector_name, sys_count, bod_count, sta_count, elapsed = future.result()
                total_systems += sys_count
                total_bodies += bod_count
                total_stations += sta_count
                completed += 1

                if completed % 100 == 0 or args.verbose:
                    pct = completed / len(sector_files) * 100
                    elapsed_total = time.monotonic() - start_time
                    rate = completed / elapsed_total if elapsed_total > 0 else 0
                    eta = (len(sector_files) - completed) / rate if rate > 0 else 0
                    logger.info(
                        "[%d/%d %.1f%%] %s: %d systems, %d bodies, %d stations "
                        "(%.1fs) | Total: %d systems | ETA: %.0fs",
                        completed,
                        len(sector_files),
                        pct,
                        sector_name,
                        sys_count,
                        bod_count,
                        sta_count,
                        elapsed,
                        total_systems,
                        eta,
                    )
            except Exception as e:
                errors += 1
                logger.error("Error converting %s: %s", source_file.name, e)

    elapsed_total = time.monotonic() - start_time
    logger.info("=" * 60)
    logger.info("Conversion complete in %.1f seconds", elapsed_total)
    logger.info("  Sectors: %d converted, %d errors", completed, errors)
    logger.info("  Systems: %d", total_systems)
    logger.info("  Bodies:  %d", total_bodies)
    logger.info("  Stations: %d", total_stations)

    # Spot-check a random sector
    sector_dirs = sorted(target_dir.iterdir())
    if sector_dirs:
        sample = sector_dirs[len(sector_dirs) // 2]
        if sample.is_dir():
            files = list(sample.iterdir())
            logger.info("  Sample sector %s: %s",
                        sample.name,
                        [f.name for f in files])
            for f in files:
                if f.suffix == ".parquet":
                    t = pq.read_table(f)
                    logger.info("    %s: %d rows, %d columns",
                                f.name, len(t), len(t.column_names))


if __name__ == "__main__":
    main()
