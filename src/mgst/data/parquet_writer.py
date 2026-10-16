"""Parquet sector writer with batched flushing and schema evolution.

Buffers flattened system/body/station rows per sector and flushes them
to Parquet files with zstd compression. Handles schema evolution
transparently — new columns discovered in later batches get null values
in earlier row groups.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from mgst.data.parquet_schema import (
    SchemaDiscoverer,
    flatten_body,
    flatten_station,
    flatten_system,
)

logger = logging.getLogger(__name__)


class ParquetSectorWriter:
    """Buffers and writes flattened rows to per-sector Parquet files.

    Usage::

        writer = ParquetSectorWriter(output_dir, flush_threshold=1000)
        for system in systems:
            writer.add_system(sector_name, system)
        writer.finalize()

    Each sector becomes a directory with systems.parquet, bodies.parquet,
    and optionally stations.parquet.
    """

    def __init__(
        self,
        output_dir: Path,
        flush_threshold: int = 1000,
        compression: str = "zstd",
        compression_level: int = 3,
        global_buffer_limit: int = 50000,
    ) -> None:
        """
        Args:
            output_dir: Root directory for Parquet output.
            flush_threshold: Number of systems per sector before flushing.
            compression: Parquet compression codec (default zstd).
            compression_level: Compression level for the codec.
            global_buffer_limit: Max total systems buffered across all sectors
                before forcing a flush of the largest sectors.
        """
        self.output_dir = Path(output_dir)
        self.flush_threshold = flush_threshold
        self.compression = compression
        self.compression_level = compression_level
        self.global_buffer_limit = global_buffer_limit

        # Per-sector buffers: sector_name -> list of flat dicts
        self._system_buffers: Dict[str, List[dict]] = {}
        self._body_buffers: Dict[str, List[dict]] = {}
        self._station_buffers: Dict[str, List[dict]] = {}

        # Per-sector system count (for flush threshold)
        self._sector_system_counts: Dict[str, int] = {}

        # Per-sector part counters (for multi-part writes before finalize)
        self._sector_parts: Dict[str, int] = {}

        # Global buffer count
        self._total_buffered_systems = 0

        # Running schema discoverer — tracks all encountered columns
        self._discoverer = SchemaDiscoverer()

        # Track which sectors have stations (avoid writing empty files)
        self._sectors_with_stations: set = set()

        # Stats
        self._total_systems_written = 0
        self._total_bodies_written = 0
        self._total_stations_written = 0

    def add_system(self, sector_name: str, system: dict) -> None:
        """Add a system to the writer's buffer for the given sector.

        The system dict is flattened and its bodies/stations are extracted.
        When the sector's buffer reaches flush_threshold, it is automatically
        flushed to disk.
        """
        # Let the schema discoverer observe this system
        self._discoverer.observe_system(system)

        sid = system.get("id64", 0)

        # Flatten and buffer
        flat_sys = flatten_system(system)
        self._system_buffers.setdefault(sector_name, []).append(flat_sys)

        for body in system.get("bodies", []):
            flat_body = flatten_body(body, sid)
            self._body_buffers.setdefault(sector_name, []).append(flat_body)

        stations = system.get("stations", [])
        if stations:
            self._sectors_with_stations.add(sector_name)
            for station in stations:
                flat_station = flatten_station(station, sid)
                self._station_buffers.setdefault(sector_name, []).append(
                    flat_station
                )

        count = self._sector_system_counts.get(sector_name, 0) + 1
        self._sector_system_counts[sector_name] = count
        self._total_buffered_systems += 1

        if count >= self.flush_threshold:
            self._flush_sector(sector_name)
        elif self._total_buffered_systems >= self.global_buffer_limit:
            self._flush_largest_sectors()

    def add_flat(
        self,
        sector_name: str,
        flat_system: dict,
        flat_bodies: list,
        flat_stations: list,
    ) -> None:
        """Add pre-flattened system data to sector buffers.

        This bypasses flatten_system/body/station and SchemaDiscoverer,
        accepting data that has already been flattened by worker processes.
        """
        self._system_buffers.setdefault(sector_name, []).append(flat_system)

        if flat_bodies:
            buf = self._body_buffers.setdefault(sector_name, [])
            buf.extend(flat_bodies)

        if flat_stations:
            self._sectors_with_stations.add(sector_name)
            buf = self._station_buffers.setdefault(sector_name, [])
            buf.extend(flat_stations)

        count = self._sector_system_counts.get(sector_name, 0) + 1
        self._sector_system_counts[sector_name] = count
        self._total_buffered_systems += 1

        if count >= self.flush_threshold:
            self._flush_sector(sector_name)
        elif self._total_buffered_systems >= self.global_buffer_limit:
            self._flush_largest_sectors()

    def _flush_largest_sectors(self) -> None:
        """Flush the largest sectors to bring total buffered systems under the limit."""
        # Sort sectors by buffered system count, flush from largest
        sectors_by_size = sorted(
            self._sector_system_counts.items(),
            key=lambda x: x[1],
            reverse=True,
        )
        for sector_name, count in sectors_by_size:
            if self._total_buffered_systems < self.global_buffer_limit // 2:
                break
            if count > 0:
                self._flush_sector(sector_name)

    def _flush_sector(self, sector_name: str) -> None:
        """Write buffered rows for a sector to Parquet files on disk."""
        systems = self._system_buffers.pop(sector_name, [])
        bodies = self._body_buffers.pop(sector_name, [])
        stations = self._station_buffers.pop(sector_name, [])
        flushed = self._sector_system_counts.get(sector_name, 0)
        self._sector_system_counts[sector_name] = 0
        self._total_buffered_systems -= flushed

        if not systems:
            return

        sector_dir = self.output_dir / sector_name
        sector_dir.mkdir(parents=True, exist_ok=True)

        part = self._sector_parts.get(sector_name, 0)
        self._sector_parts[sector_name] = part + 1

        # Write systems
        self._write_table(
            systems, sector_dir / f"systems.part{part}.parquet"
        )
        self._total_systems_written += len(systems)

        # Write bodies
        if bodies:
            self._write_table(
                bodies, sector_dir / f"bodies.part{part}.parquet"
            )
            self._total_bodies_written += len(bodies)

        # Write stations
        if stations:
            self._write_table(
                stations, sector_dir / f"stations.part{part}.parquet"
            )
            self._total_stations_written += len(stations)

    def _write_table(self, rows: List[dict], path: Path) -> None:
        """Write a list of flat dicts to a Parquet file.

        Builds column-oriented data from the union of all keys across
        all rows. Missing keys in any row become null values.
        """
        # Collect union of all keys (preserving insertion order via dict)
        all_keys: Dict[str, None] = {}
        for row in rows:
            for k in row:
                if k not in all_keys:
                    all_keys[k] = None

        # Build column arrays
        columns = {key: [row.get(key) for row in rows] for key in all_keys}

        table = pa.table(columns)
        pq.write_table(
            table,
            path,
            compression=self.compression,
            compression_level=self.compression_level,
        )

    def finalize(self) -> None:
        """Flush all remaining buffers and consolidate multi-part files.

        After calling this, each sector directory will contain at most:
        - systems.parquet
        - bodies.parquet
        - stations.parquet (only if the sector has stations)
        """
        # Flush any remaining buffers
        remaining_sectors = list(self._system_buffers.keys())
        for sector_name in remaining_sectors:
            self._flush_sector(sector_name)

        # Consolidate multi-part files into single files
        self._consolidate_all()

        logger.info(
            "Parquet write complete: %d systems, %d bodies, %d stations",
            self._total_systems_written,
            self._total_bodies_written,
            self._total_stations_written,
        )

    def _consolidate_all(self) -> None:
        """Merge multi-part Parquet files into single files per sector."""
        if not self.output_dir.exists():
            return

        for sector_dir in sorted(self.output_dir.iterdir()):
            if not sector_dir.is_dir():
                continue
            for table_name in ("systems", "bodies", "stations"):
                self._consolidate_table(sector_dir, table_name)

    def _consolidate_table(self, sector_dir: Path, table_name: str) -> None:
        """Consolidate multi-part files for one table in one sector."""
        parts = sorted(sector_dir.glob(f"{table_name}.part*.parquet"))
        if not parts:
            return

        if len(parts) == 1:
            # Just rename the single part file
            parts[0].rename(sector_dir / f"{table_name}.parquet")
            return

        # Read all parts and concatenate
        tables = []
        for part_path in parts:
            tables.append(pq.read_table(part_path))

        # Unify schemas (later parts may have new columns)
        merged = pa.concat_tables(tables, promote_options="default")

        # Write consolidated file
        pq.write_table(
            merged,
            sector_dir / f"{table_name}.parquet",
            compression=self.compression,
            compression_level=self.compression_level,
        )

        # Remove part files
        for part_path in parts:
            part_path.unlink()

    @property
    def discoverer(self) -> SchemaDiscoverer:
        """Access the schema discoverer for schema inspection."""
        return self._discoverer

    @property
    def total_systems_written(self) -> int:
        return self._total_systems_written

    @property
    def total_bodies_written(self) -> int:
        return self._total_bodies_written

    @property
    def total_stations_written(self) -> int:
        return self._total_stations_written

    @property
    def sectors_written(self) -> int:
        """Number of sectors that have had data flushed."""
        return len(self._sector_parts)
