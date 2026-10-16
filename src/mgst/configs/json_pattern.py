"""JSON Pattern Matching Configuration.

This configuration allows using JSON pattern files for filtering systems.
"""

import json
import math
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

from .base import BaseConfig
from ..core.json_pattern_matcher import JSONPatternMatcher
from ..utils.math_utils import distance_to_line_segment


class JSONPatternConfig(BaseConfig):
    """Configuration that uses JSON pattern matching."""

    def __init__(self, pattern_file: Path, corridor_params: Optional[Dict[str, Any]] = None):
        """Initialize with pattern file.

        Args:
            pattern_file: Path to JSON pattern file
            corridor_params: Optional corridor filtering parameters:
                - start_coords: (x, y, z) tuple
                - end_coords: (x, y, z) tuple
                - radius: float
        """
        self.pattern_file = Path(pattern_file)
        self.corridor_params = corridor_params

        # Load pattern
        with open(self.pattern_file, 'r') as f:
            self.pattern = json.load(f)

        # Initialize matcher
        self.matcher = JSONPatternMatcher(self.pattern)

        # Get description from pattern if available
        description = self.pattern.get('description', f'JSON Pattern: {self.pattern_file.name}')

        if corridor_params:
            description += f" (Corridor: radius {corridor_params['radius']} LY)"

        super().__init__(
            name=f"json_pattern_{self.pattern_file.stem}",
            description=description
        )

    def _distance_to_line_segment(self, point: Tuple[float, float, float],
                                  start: Tuple[float, float, float],
                                  end: Tuple[float, float, float]) -> float:
        """Calculate distance from point to line segment.

        Args:
            point: (x, y, z) coordinates
            start: Start of line segment (x, y, z)
            end: End of line segment (x, y, z)

        Returns:
            Distance in light years
        """
        return distance_to_line_segment(point, start, end)

    def filter_system(self, system_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Filter systems using JSON pattern matching.

        Args:
            system_data: Complete system record

        Returns:
            System summary if it matches pattern, None otherwise
        """
        # Get coordinates
        coords = system_data.get('coords', {})
        point = (coords.get('x', 0.0), coords.get('y', 0.0), coords.get('z', 0.0))

        # Apply corridor filtering if enabled
        if self.corridor_params:
            distance = self._distance_to_line_segment(
                point,
                self.corridor_params['start_coords'],
                self.corridor_params['end_coords']
            )
            if distance > self.corridor_params['radius']:
                return None

        # Reset matcher variables for each system
        self.matcher.reset()

        # Check if system matches pattern
        if not self.matcher.matches(system_data):
            return None

        # Create summary with essential system info
        result = {
            'name': system_data.get('name', 'Unknown'),
            'id64': system_data.get('id64', 0),
        }

        # Add coordinates
        result['x'] = point[0]
        result['y'] = point[1]
        result['z'] = point[2]

        # Add corridor distance if applicable
        if self.corridor_params:
            distance = self._distance_to_line_segment(
                point,
                self.corridor_params['start_coords'],
                self.corridor_params['end_coords']
            )
            result['corridor_distance'] = round(distance, 2)

        # Count matching bodies by type
        body_types = {}
        for body in system_data.get('bodies', []):
            subtype = body.get('subType', 'Unknown')
            body_types[subtype] = body_types.get(subtype, 0) + 1

        # Add body type counts to result
        for body_type, count in body_types.items():
            result[f'{body_type}_count'] = count

        # Add total bodies count
        result['total_bodies'] = len(system_data.get('bodies', []))

        return result

    def get_output_columns(self) -> List[str]:
        """Get output column names.

        Returns:
            List of column names for output
        """
        columns = [
            'name',
            'id64',
            'x',
            'y',
            'z',
        ]

        if self.corridor_params:
            columns.append('corridor_distance')

        columns.extend([
            'total_bodies',
            'Earth-like world_count',
            'Water world_count',
            'Ammonia world_count',
            'Black Hole_count',
            'Neutron Star_count'
        ])

        return columns
