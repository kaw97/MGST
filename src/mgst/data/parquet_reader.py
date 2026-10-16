"""Parquet sector reader with column pruning and predicate filtering.

Reads normalized Parquet sector databases (systems.parquet, bodies.parquet,
stations.parquet per sector directory) with support for:
- Column pruning: read only the columns needed by the pattern
- Row-group filtering: skip row groups whose statistics don't match predicates
- System dict reconstruction: join tables and unflatten to dicts compatible
  with JSONPatternMatcher
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pyarrow as pa
import pyarrow.parquet as pq

from mgst.data.parquet_schema import (
    unflatten_body,
    unflatten_station,
    unflatten_system,
    unflatten_system_with_bodies,
)

logger = logging.getLogger(__name__)


class ParquetSectorReader:
    """Reads and queries a single Parquet sector directory.

    A sector directory contains:
    - systems.parquet — one row per system
    - bodies.parquet — one row per body, FK system_id64
    - stations.parquet — one row per station, FK system_id64 (optional)
    """

    def __init__(self, sector_dir: Path) -> None:
        self.sector_dir = Path(sector_dir)
        self._systems_path = self.sector_dir / "systems.parquet"
        self._bodies_path = self.sector_dir / "bodies.parquet"
        self._stations_path = self.sector_dir / "stations.parquet"

    @property
    def name(self) -> str:
        return self.sector_dir.name

    @property
    def has_systems(self) -> bool:
        return self._systems_path.exists()

    @property
    def has_bodies(self) -> bool:
        return self._bodies_path.exists()

    @property
    def has_stations(self) -> bool:
        return self._stations_path.exists()

    def read_systems(
        self,
        columns: Optional[List[str]] = None,
        filters: Optional[Any] = None,
    ) -> pa.Table:
        """Read systems.parquet with optional column pruning and filtering.

        Args:
            columns: List of column names to read (None = all columns).
                     'id64' is always included for joining.
            filters: PyArrow filter expression for row-group pruning.

        Returns:
            PyArrow Table of matching systems.
        """
        if not self._systems_path.exists():
            return pa.table({})

        # Ensure id64 is always included (needed for joining)
        if columns is not None:
            available = pq.read_schema(self._systems_path).names
            columns = list(set(columns) | {"id64"})
            columns = [c for c in columns if c in available]

        return pq.read_table(
            self._systems_path,
            columns=columns,
            filters=filters,
        )

    def read_bodies(
        self,
        columns: Optional[List[str]] = None,
        filters: Optional[Any] = None,
        system_ids: Optional[Set[int]] = None,
    ) -> pa.Table:
        """Read bodies.parquet with optional column pruning and filtering.

        Args:
            columns: List of column names to read (None = all columns).
                     'system_id64' is always included for joining.
            filters: PyArrow filter expression for row-group pruning.
            system_ids: If provided, only return bodies for these system IDs.

        Returns:
            PyArrow Table of matching bodies.
        """
        if not self._bodies_path.exists():
            return pa.table({"system_id64": pa.array([], type=pa.int64())})

        # Ensure system_id64 is always included
        if columns is not None:
            available = pq.read_schema(self._bodies_path).names
            columns = list(set(columns) | {"system_id64"})
            columns = [c for c in columns if c in available]

        table = pq.read_table(
            self._bodies_path,
            columns=columns,
            filters=filters,
        )

        # Post-filter by system IDs if provided
        if system_ids is not None:
            mask = pa.compute.is_in(
                table.column("system_id64"),
                value_set=pa.array(list(system_ids), type=pa.int64()),
            )
            table = table.filter(mask)

        return table

    def read_stations(
        self,
        columns: Optional[List[str]] = None,
        filters: Optional[Any] = None,
        system_ids: Optional[Set[int]] = None,
    ) -> pa.Table:
        """Read stations.parquet with optional column pruning and filtering.

        Args:
            columns: List of column names to read (None = all columns).
            filters: PyArrow filter expression for row-group pruning.
            system_ids: If provided, only return stations for these system IDs.

        Returns:
            PyArrow Table of matching stations.
        """
        if not self._stations_path.exists():
            return pa.table({"system_id64": pa.array([], type=pa.int64())})

        if columns is not None:
            available = pq.read_schema(self._stations_path).names
            columns = list(set(columns) | {"system_id64"})
            columns = [c for c in columns if c in available]

        table = pq.read_table(
            self._stations_path,
            columns=columns,
            filters=filters,
        )

        if system_ids is not None:
            mask = pa.compute.is_in(
                table.column("system_id64"),
                value_set=pa.array(list(system_ids), type=pa.int64()),
            )
            table = table.filter(mask)

        return table

    def get_system_ids(
        self,
        filters: Optional[Any] = None,
    ) -> Set[int]:
        """Get set of system id64 values, optionally filtered."""
        table = self.read_systems(columns=["id64"], filters=filters)
        if len(table) == 0:
            return set()
        return set(table.column("id64").to_pylist())

    def get_body_system_ids(
        self,
        filters: Optional[Any] = None,
    ) -> Set[int]:
        """Get set of system_id64 values from bodies table, optionally filtered."""
        table = self.read_bodies(columns=["system_id64"], filters=filters)
        if len(table) == 0:
            return set()
        return set(table.column("system_id64").to_pylist())

    def reconstruct_system_dicts(
        self,
        system_ids: Optional[Set[int]] = None,
    ) -> List[dict]:
        """Reconstruct complete system dicts for the given system IDs.

        Joins systems + bodies + stations tables, unflattens each,
        and returns dicts compatible with JSONPatternMatcher.matches().

        Args:
            system_ids: Set of system id64 values to reconstruct.
                       None = reconstruct all systems in the sector.

        Returns:
            List of complete system dicts.
        """
        # Read all tables (full columns for reconstruction)
        sys_table = self.read_systems()
        if len(sys_table) == 0:
            return []

        if system_ids is not None:
            # Filter systems table by id64
            mask = pa.compute.is_in(
                sys_table.column("id64"),
                value_set=pa.array(list(system_ids), type=pa.int64()),
            )
            sys_table = sys_table.filter(mask)

        actual_ids = set(sys_table.column("id64").to_pylist())
        bod_table = self.read_bodies(system_ids=actual_ids)
        sta_table = self.read_stations(system_ids=actual_ids)

        sys_rows = sys_table.to_pylist()
        bod_rows = bod_table.to_pylist()
        sta_rows = sta_table.to_pylist()

        # Group bodies and stations by system_id64
        bodies_by_sys: Dict[int, List[dict]] = {}
        for row in bod_rows:
            sid = row.get("system_id64")
            bodies_by_sys.setdefault(sid, []).append(row)

        stations_by_sys: Dict[int, List[dict]] = {}
        for row in sta_rows:
            sid = row.get("system_id64")
            stations_by_sys.setdefault(sid, []).append(row)

        # Reconstruct
        results = []
        for flat_sys in sys_rows:
            sid = flat_sys.get("id64")
            flat_bodies = bodies_by_sys.get(sid, [])
            flat_stations = stations_by_sys.get(sid, [])
            system = unflatten_system_with_bodies(
                flat_sys, flat_bodies, flat_stations
            )
            results.append(system)

        return results

    def system_count(self) -> int:
        """Return the number of systems in this sector."""
        if not self._systems_path.exists():
            return 0
        metadata = pq.read_metadata(self._systems_path)
        return metadata.num_rows

    def body_count(self) -> int:
        """Return the number of bodies in this sector."""
        if not self._bodies_path.exists():
            return 0
        metadata = pq.read_metadata(self._bodies_path)
        return metadata.num_rows

    def get_system_columns(self) -> List[str]:
        """Return the list of columns in systems.parquet."""
        if not self._systems_path.exists():
            return []
        return pq.read_schema(self._systems_path).names

    def get_body_columns(self) -> List[str]:
        """Return the list of columns in bodies.parquet."""
        if not self._bodies_path.exists():
            return []
        return pq.read_schema(self._bodies_path).names


def detect_parquet_database(path: Path) -> bool:
    """Check if a path is a Parquet sector database.

    A Parquet database is a directory containing subdirectories,
    each with a systems.parquet file.
    """
    path = Path(path)
    if not path.is_dir():
        return False

    # Check a few subdirectories for systems.parquet
    for subdir in list(path.iterdir())[:5]:
        if subdir.is_dir() and (subdir / "systems.parquet").exists():
            return True

    return False


def get_sector_readers(
    database_dir: Path,
    sector_names: Optional[List[str]] = None,
) -> List[ParquetSectorReader]:
    """Get ParquetSectorReader instances for sectors in a database.

    Args:
        database_dir: Root directory of the Parquet database.
        sector_names: If provided, only return readers for these sectors.
                     Otherwise, return readers for all sectors.

    Returns:
        List of ParquetSectorReader instances.
    """
    database_dir = Path(database_dir)
    readers = []

    if sector_names:
        for name in sector_names:
            sector_dir = database_dir / name
            if sector_dir.is_dir() and (sector_dir / "systems.parquet").exists():
                readers.append(ParquetSectorReader(sector_dir))
    else:
        for subdir in sorted(database_dir.iterdir()):
            if subdir.is_dir() and (subdir / "systems.parquet").exists():
                readers.append(ParquetSectorReader(subdir))

    return readers
