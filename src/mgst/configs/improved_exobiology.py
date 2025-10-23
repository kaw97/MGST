"""Improved Exobiology Configuration

Enhanced exobiology search that addresses the issues found in the temperature
range-based approach. Uses more practical constraints and tolerance factors.

Key Improvements:
- Realistic temperature bounds (no negative Kelvin values)
- Broader tolerance factors to account for data variance
- Practical distance constraints
- Fallback to basic filtering when empirical data is unreliable
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime

from .high_value_exobiology import HighValueExobiologyConfig


class ImprovedExobiologyConfig(HighValueExobiologyConfig):
    """Improved exobiology configuration with practical constraints."""

    def __init__(self):
        super().__init__()

        # Override configuration details
        self._name = "improved-exobiology"
        self._description = """Improved Exobiology Configuration

Uses practical constraints and broader tolerance factors to address issues
found in the overly restrictive temperature range approach.

Features:
- Realistic temperature bounds (50K minimum for body temp)
- Expanded tolerance factors (50% by default)
- Practical distance constraints (minimum 100ls)
- Fallback to basic filtering when empirical data is unreliable
- Quality-based range adjustments

Improvements over temperature-range approach:
- Eliminates impossible negative temperatures
- Uses broader ranges to account for observational variance
- Implements quality-based confidence scoring
- Falls back to basic filtering for species with poor thermal regulation
"""

        # Load species stellar ranges
        self.species_ranges = self._load_species_ranges()

        # Improved tolerance factors based on test results
        self.STELLAR_TEMP_TOLERANCE = 1.5  # 50% expansion for stellar temp
        self.BODY_TEMP_TOLERANCE = 2.0     # 100% expansion for body temp
        self.DISTANCE_TOLERANCE = 2.0      # 100% expansion for distance

        # Minimum realistic bounds
        self.MIN_BODY_TEMP = 50.0          # Minimum 50K for any life
        self.MAX_BODY_TEMP = 1000.0        # Maximum 1000K for life
        self.MIN_DISTANCE = 100.0          # Minimum 100ls for realistic bodies
        self.MAX_DISTANCE = 1000000.0      # Maximum ~1M ls reasonable limit

    def _load_species_ranges(self) -> Dict[str, Any]:
        """Load species stellar ranges data."""
        # Look for the ranges file in the expected location
        ranges_file = Path(__file__).parent.parent.parent.parent / "output"

        # Try to find the ranges file
        ranges_files = list(ranges_file.glob("*/species_stellar_ranges.json"))
        if ranges_files:
            latest_file = max(ranges_files, key=lambda x: x.stat().st_mtime)

            with open(latest_file, 'r') as f:
                return json.load(f)

        print("Warning: Could not load species stellar ranges. Using basic filtering only.")
        return {}

    def _get_stellar_class(self, system_data: Dict[str, Any]) -> str:
        """Extract the stellar class of the primary star."""
        # Look for primary star data
        stars = system_data.get('stars', [])
        if stars:
            primary_star = None
            for star in stars:
                if star.get('mainStar', False):
                    primary_star = star
                    break
            if not primary_star:
                primary_star = stars[0]

            spectral_class = primary_star.get('spectralClass', primary_star.get('subType', ''))
            if spectral_class:
                return spectral_class[0] if spectral_class else 'Unknown'

        # Fallback: check bodies for stellar data (from enriched dataset)
        bodies = system_data.get('bodies', [])
        if bodies and bodies[0].get('stellar_spectral_class'):
            spectral_class = bodies[0]['stellar_spectral_class']
            return spectral_class[0] if spectral_class else 'Unknown'

        return 'Unknown'

    def _get_stellar_temperature(self, system_data: Dict[str, Any]) -> float:
        """Extract stellar surface temperature."""
        # Look for primary star data
        stars = system_data.get('stars', [])
        if stars:
            primary_star = None
            for star in stars:
                if star.get('mainStar', False):
                    primary_star = star
                    break
            if not primary_star:
                primary_star = stars[0]

            return primary_star.get('surfaceTemperature', 0.0)

        # Fallback: check bodies for stellar data
        bodies = system_data.get('bodies', [])
        if bodies and bodies[0].get('stellar_surface_temperature'):
            return bodies[0]['stellar_surface_temperature']

        return 0.0

    def _get_quality_multiplier(self, thermal_regulation: str) -> float:
        """Get tolerance multiplier based on thermal regulation quality."""
        quality_multipliers = {
            'excellent': 1.0,    # Use base tolerance
            'good': 1.5,         # 50% more tolerance
            'fair': 2.0,         # 100% more tolerance
            'poor': 3.0,         # 300% more tolerance
            'unknown': 2.0       # Default to fair tolerance
        }
        return quality_multipliers.get(thermal_regulation, 2.0)

    def _validate_stellar_temperature(self, species_name: str, stellar_class: str, stellar_temp: float) -> bool:
        """Validate stellar temperature with improved bounds and tolerance."""
        if not self.species_ranges or species_name not in self.species_ranges:
            return True  # No data, allow by default

        species_data = self.species_ranges[species_name]
        stellar_class_ranges = species_data.get('stellar_class_ranges', {})

        if stellar_class not in stellar_class_ranges:
            return False  # Species not found in this stellar class

        class_data = stellar_class_ranges[stellar_class]
        stellar_temp_range = class_data.get('stellar_temp_range')

        if not stellar_temp_range:
            return True  # No temperature data, allow

        min_temp, max_temp = stellar_temp_range

        # Get quality-based multiplier
        thermal_quality = class_data.get('thermal_regulation_quality', 'unknown')
        quality_multiplier = self._get_quality_multiplier(thermal_quality)

        # Apply tolerance factor with quality adjustment
        tolerance = self.STELLAR_TEMP_TOLERANCE * quality_multiplier
        temp_tolerance = (max_temp - min_temp) * (tolerance - 1.0) * 0.5
        adjusted_min = min_temp - temp_tolerance
        adjusted_max = max_temp + temp_tolerance

        return adjusted_min <= stellar_temp <= adjusted_max

    def _validate_body_temperature(self, species_name: str, stellar_class: str, body_temp: float) -> bool:
        """Validate body temperature with realistic bounds and improved tolerance."""
        if not self.species_ranges or species_name not in self.species_ranges:
            return True  # No data, allow by default

        species_data = self.species_ranges[species_name]
        stellar_class_ranges = species_data.get('stellar_class_ranges', {})

        if stellar_class not in stellar_class_ranges:
            return False  # Species not found in this stellar class

        class_data = stellar_class_ranges[stellar_class]
        body_temp_range = class_data.get('body_temp_range')

        if not body_temp_range:
            return True  # No temperature data, allow

        min_temp, max_temp = body_temp_range

        # Apply realistic bounds first
        min_temp = max(self.MIN_BODY_TEMP, min_temp)
        max_temp = min(self.MAX_BODY_TEMP, max_temp)

        # Skip validation if range becomes invalid after bounds correction
        if min_temp >= max_temp:
            return True  # Range is invalid, allow by default

        # Get quality-based multiplier
        thermal_quality = class_data.get('thermal_regulation_quality', 'unknown')
        quality_multiplier = self._get_quality_multiplier(thermal_quality)

        # Apply tolerance factor with quality adjustment
        tolerance = self.BODY_TEMP_TOLERANCE * quality_multiplier
        temp_tolerance = (max_temp - min_temp) * (tolerance - 1.0) * 0.5
        adjusted_min = max(self.MIN_BODY_TEMP, min_temp - temp_tolerance)
        adjusted_max = min(self.MAX_BODY_TEMP, max_temp + temp_tolerance)

        return adjusted_min <= body_temp <= adjusted_max

    def _validate_orbital_distance(self, species_name: str, stellar_class: str, distance: float) -> bool:
        """Validate orbital distance with realistic bounds and improved tolerance."""
        if not self.species_ranges or species_name not in self.species_ranges:
            return True  # No data, allow by default

        if distance <= 0:
            return True  # No distance data, allow

        species_data = self.species_ranges[species_name]
        stellar_class_ranges = species_data.get('stellar_class_ranges', {})

        if stellar_class not in stellar_class_ranges:
            return False  # Species not found in this stellar class

        class_data = stellar_class_ranges[stellar_class]
        distance_range = class_data.get('distance_range')

        if not distance_range:
            return True  # No distance data, allow

        min_dist, max_dist = distance_range

        # Apply realistic bounds
        min_dist = max(self.MIN_DISTANCE, min_dist)
        max_dist = min(self.MAX_DISTANCE, max_dist)

        # Skip validation if range becomes invalid after bounds correction
        if min_dist >= max_dist:
            return True  # Range is invalid, allow by default

        # Get quality-based multiplier
        thermal_quality = class_data.get('thermal_regulation_quality', 'unknown')
        quality_multiplier = self._get_quality_multiplier(thermal_quality)

        # Apply tolerance factor with quality adjustment
        tolerance = self.DISTANCE_TOLERANCE * quality_multiplier
        dist_tolerance = (max_dist - min_dist) * (tolerance - 1.0) * 0.5
        adjusted_min = max(self.MIN_DISTANCE, min_dist - dist_tolerance)
        adjusted_max = min(self.MAX_DISTANCE, max_dist + dist_tolerance)

        return adjusted_min <= distance <= adjusted_max

    def _is_species_valid_for_system(self, body: Dict, species_info: Dict, system_data: Dict) -> bool:
        """Validate species against improved range criteria."""
        species_name = species_info['name']
        stellar_class = self._get_stellar_class(system_data)
        stellar_temp = self._get_stellar_temperature(system_data)

        # Check stellar temperature range
        if not self._validate_stellar_temperature(species_name, stellar_class, stellar_temp):
            return False

        # Check body temperature range
        body_temp = body.get('surfaceTemperature', 0.0)
        if not self._validate_body_temperature(species_name, stellar_class, body_temp):
            return False

        # Check orbital distance range
        distance = body.get('distanceToArrival', 0.0)
        if not self._validate_orbital_distance(species_name, stellar_class, distance):
            return False

        return True

    def detect_species_on_body(self, body: Dict, system_data: Dict = None) -> List[Dict]:
        """Enhanced species detection using improved validation."""
        # Get base species detections
        base_species = super().detect_species_on_body(body)

        if not system_data:
            return base_species

        # Filter species based on improved range validation
        valid_species = []
        stellar_class = self._get_stellar_class(system_data)
        stellar_temp = self._get_stellar_temperature(system_data)

        for species in base_species:
            if self._is_species_valid_for_system(body, species, system_data):
                valid_species.append({
                    **species,
                    'stellar_class': stellar_class,
                    'stellar_temperature': stellar_temp,
                    'validation_method': 'improved_empirical_ranges'
                })

        return valid_species

    def get_species_range_info(self, species_name: str) -> Dict[str, Any]:
        """Get detailed range information for a species (for debugging/analysis)."""
        if not self.species_ranges or species_name not in self.species_ranges:
            return {'error': 'No range data available'}

        species_data = self.species_ranges[species_name]

        info = {
            'species_name': species_name,
            'total_observations': species_data.get('total_observations', 0),
            'stellar_classes': {}
        }

        for stellar_class, class_data in species_data.get('stellar_class_ranges', {}).items():
            thermal_quality = class_data.get('thermal_regulation_quality', 'unknown')
            quality_multiplier = self._get_quality_multiplier(thermal_quality)

            info['stellar_classes'][stellar_class] = {
                'observations': class_data.get('observations', 0),
                'stellar_temp_range': class_data.get('stellar_temp_range'),
                'body_temp_range': class_data.get('body_temp_range'),
                'distance_range': class_data.get('distance_range'),
                'thermal_regulation': thermal_quality,
                'quality_multiplier': quality_multiplier,
                'effective_tolerance': {
                    'stellar_temp': self.STELLAR_TEMP_TOLERANCE * quality_multiplier,
                    'body_temp': self.BODY_TEMP_TOLERANCE * quality_multiplier,
                    'distance': self.DISTANCE_TOLERANCE * quality_multiplier
                }
            }

        return info

    def set_tolerance_factors(self, stellar_temp_tol: float = 1.5, body_temp_tol: float = 2.0, distance_tol: float = 2.0):
        """Adjust tolerance factors for testing and refinement."""
        self.STELLAR_TEMP_TOLERANCE = stellar_temp_tol
        self.BODY_TEMP_TOLERANCE = body_temp_tol
        self.DISTANCE_TOLERANCE = distance_tol

        print(f"Updated improved tolerance factors:")
        print(f"  Stellar temperature: {stellar_temp_tol}")
        print(f"  Body temperature: {body_temp_tol}")
        print(f"  Distance: {distance_tol}")

    def get_output_columns(self) -> List[str]:
        """Return output columns for improved configuration."""
        return [
            'system_name',
            'qualifying_bodies',
            'total_species',
            'total_value',
            'primary_stellar_class',
            'stellar_temperature',
            'coords_x',
            'coords_y',
            'coords_z',
            'validation_method',
            'thermal_regulation_quality'
        ]