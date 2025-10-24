"""
Search Mode System for MGST

This module defines the different search modes available in MGST and provides
the logic for determining which sector files to search based on the selected mode.

Search Modes:
1. galaxy - Search entire galaxy (all sectors)
2. sectors - Search specific sectors
3. corridor - Search corridor between two points
4. pattern - Pattern-based system search
"""

import re
import math
from pathlib import Path
from typing import List, Set, Tuple, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
import json
import logging

logger = logging.getLogger(__name__)


class SearchMode(Enum):
    """Enumeration of available search modes."""
    GALAXY = "galaxy"
    SECTORS = "sectors"
    CORRIDOR = "corridor"
    PATTERN = "pattern"


@dataclass
class Coordinates:
    """3D coordinates in Elite Dangerous coordinate system."""
    x: float
    y: float
    z: float

    def distance_to(self, other: 'Coordinates') -> float:
        """Calculate distance to another coordinate."""
        return math.sqrt(
            (self.x - other.x) ** 2 +
            (self.y - other.y) ** 2 +
            (self.z - other.z) ** 2
        )

    def distance_to_line(self, start: 'Coordinates', end: 'Coordinates') -> float:
        """Calculate distance from this point to a line segment.

        Uses the formula for point-to-line distance in 3D space.
        """
        # Vector from start to end
        line_vec = Coordinates(end.x - start.x, end.y - start.y, end.z - start.z)
        line_length = start.distance_to(end)

        if line_length == 0:
            return self.distance_to(start)

        # Vector from start to point
        point_vec = Coordinates(self.x - start.x, self.y - start.y, self.z - start.z)

        # Project point onto line
        dot_product = (point_vec.x * line_vec.x +
                      point_vec.y * line_vec.y +
                      point_vec.z * line_vec.z)

        projection_length = dot_product / line_length

        # Clamp to line segment
        projection_length = max(0, min(line_length, projection_length))

        # Find closest point on line
        closest = Coordinates(
            start.x + (line_vec.x / line_length) * projection_length,
            start.y + (line_vec.y / line_length) * projection_length,
            start.z + (line_vec.z / line_length) * projection_length
        )

        return self.distance_to(closest)


@dataclass
class SearchParameters:
    """Parameters for different search modes."""
    mode: SearchMode

    # Common parameters
    database_path: Path
    sector_index_path: Optional[Path] = None  # Optional path to sector index file

    # Mode-specific parameters
    sectors: Optional[List[str]] = None
    start_coords: Optional[Coordinates] = None
    end_coords: Optional[Coordinates] = None
    radius: Optional[float] = None
    pattern_file: Optional[Path] = None


class SectorResolver:
    """Resolves which sector files to search based on search parameters."""

    def __init__(self, database_path: Path):
        self.database_path = Path(database_path)
        self._sector_coords_cache = {}

        # Sector naming pattern (just sector name, no subsector code)
        self.pattern = re.compile(r'^([A-Za-z\s_0-9]+)\.jsonl(\.gz)?$')

    def get_all_sector_files(self) -> List[Path]:
        """Get list of all sector files in the database."""
        sector_files = []

        # Look for compressed sector files
        sector_files = list(self.database_path.glob("*.jsonl.gz"))
        if not sector_files:
            # Fallback to uncompressed
            sector_files = list(self.database_path.glob("*.jsonl"))

        logger.debug(f"Found {len(sector_files)} sector files")
        return sector_files

    def parse_sector_name(self, filename: str) -> Optional[str]:
        """Parse sector filename to extract sector name.

        Args:
            filename: Sector filename (e.g., "Col_285_Sector.jsonl.gz")

        Returns:
            Sector name or None if parsing fails
        """
        match = self.pattern.match(filename)
        if match:
            sector = match.group(1).replace('_', ' ')
            return sector
        return None

    def resolve_galaxy_mode(self, params: SearchParameters) -> List[Path]:
        """Resolve sector files for galaxy-wide search."""
        return self.get_all_sector_files()

    def resolve_sectors_mode(self, params: SearchParameters) -> List[Path]:
        """Resolve sector files for specific sectors search."""
        if not params.sectors:
            raise ValueError("Sectors mode requires --sectors parameter")

        target_files = []
        all_files = self.get_all_sector_files()

        for file_path in all_files:
            sector_name = self.parse_sector_name(file_path.name)
            if sector_name and sector_name in params.sectors:
                target_files.append(file_path)

        logger.info(f"Found {len(target_files)} sector files for {len(params.sectors)} sectors")
        return target_files

    def resolve_corridor_mode(self, params: SearchParameters) -> List[Path]:
        """Resolve sector files for corridor search using spatial prefiltering."""
        if not all([params.start_coords, params.end_coords, params.radius]):
            raise ValueError("Corridor mode requires --start, --end, and --radius parameters")

        # Determine sector index path (custom path or default)
        if params.sector_index_path:
            index_file = params.sector_index_path
        else:
            index_file = self.database_path / 'sector_index.json'

        if index_file.exists():
            logger.info(f"Using sector index for spatial prefiltering: {index_file}")

            with open(index_file, 'r') as f:
                index = json.load(f)

            target_files = []
            max_distance = 2.2 * params.radius

            for sector_name, sector_data in index['sectors'].items():
                coords = sector_data['center_coords']
                center = Coordinates(coords['x'], coords['y'], coords['z'])

                # Calculate distance from sector center to corridor
                distance = center.distance_to_line(params.start_coords, params.end_coords)

                if distance <= max_distance:
                    sector_file = self.database_path / sector_data['filename']
                    if sector_file.exists():
                        target_files.append(sector_file)

            logger.info(f"Found {len(target_files)} sector files near corridor "
                       f"(radius: {params.radius}, max_distance: {max_distance:.1f} LY, "
                       f"reduced search space by {100*(1-len(target_files)/len(index['sectors'])):.1f}%)")
            return target_files
        else:
            logger.warning(f"Sector index not found at {index_file}, searching all sectors")
            return self.get_all_sector_files()

    def resolve_pattern_mode(self, params: SearchParameters) -> List[Path]:
        """Resolve sector files for pattern-based search."""
        if not params.pattern_file:
            raise ValueError("Pattern mode requires --pattern-file parameter")

        # For pattern mode, we typically search all sectors unless
        # the pattern file specifies spatial constraints
        return self.get_all_sector_files()

    def resolve_search_files(self, params: SearchParameters) -> List[Path]:
        """Resolve which sector files to search based on parameters.

        Args:
            params: Search parameters including mode and mode-specific options

        Returns:
            List of sector file paths to search
        """
        resolver_map = {
            SearchMode.GALAXY: self.resolve_galaxy_mode,
            SearchMode.SECTORS: self.resolve_sectors_mode,
            SearchMode.CORRIDOR: self.resolve_corridor_mode,
            SearchMode.PATTERN: self.resolve_pattern_mode,
        }

        resolver = resolver_map.get(params.mode)
        if not resolver:
            raise ValueError(f"Unknown search mode: {params.mode}")

        files = resolver(params)

        if not files:
            logger.warning(f"No sector files found for search mode {params.mode.value}")

        return files


def parse_coordinates(coord_str: str) -> Coordinates:
    """Parse coordinate string in format 'x,y,z' to Coordinates object.

    Args:
        coord_str: Coordinate string like "100.5,-200.0,300.25"

    Returns:
        Coordinates object
    """
    try:
        parts = coord_str.split(',')
        if len(parts) != 3:
            raise ValueError(f"Coordinates must be in format 'x,y,z', got: {coord_str}")

        x, y, z = map(float, parts)
        return Coordinates(x, y, z)
    except ValueError as e:
        raise ValueError(f"Invalid coordinate format '{coord_str}': {e}")


def validate_search_parameters(params: SearchParameters) -> None:
    """Validate search parameters for the given mode.

    Args:
        params: Search parameters to validate

    Raises:
        ValueError: If parameters are invalid for the specified mode
    """
    if not params.database_path.exists():
        raise ValueError(f"Database path does not exist: {params.database_path}")

    if params.mode == SearchMode.SECTORS and not params.sectors:
        raise ValueError("Sectors mode requires --sectors parameter")

    if params.mode == SearchMode.CORRIDOR:
        if not all([params.start_coords, params.end_coords, params.radius]):
            raise ValueError("Corridor mode requires --start, --end, and --radius parameters")
        if params.radius <= 0:
            raise ValueError("Corridor radius must be positive")

    if params.mode == SearchMode.PATTERN:
        if not params.pattern_file:
            raise ValueError("Pattern mode requires --pattern-file parameter")
        if not params.pattern_file.exists():
            raise ValueError(f"Pattern file does not exist: {params.pattern_file}")