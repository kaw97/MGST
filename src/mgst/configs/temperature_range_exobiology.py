"""Temperature Range-Based Exobiology Configuration

Enhanced exobiology search using precise stellar temperature and distance ranges
derived from empirical analysis of 3.45M codex entries. This configuration applies
star-class specific temperature and orbital distance constraints for each species.

Key Features:
- Stellar temperature range validation for each species-stellar class combination
- Orbital distance range constraints based on observed patterns
- Body temperature validation using empirical ranges
- Iterative refinement capability for testing and validation
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime

from .high_value_exobiology import HighValueExobiologyConfig


class TemperatureRangeExobiologyConfig(HighValueExobiologyConfig):
    """Exobiology configuration using precise temperature and distance ranges."""

    def __init__(self):
        super().__init__()

        # Override configuration details
        self._name = "temperature-range-exobiology"
        self._description = """Temperature Range-Based Exobiology Configuration

Uses precise stellar temperature, body temperature, and orbital distance ranges
derived from empirical analysis of 3.45M codex entries.

Features:
- Star-class specific temperature range validation
- Orbital distance constraints based on observed patterns
- Body temperature validation using empirical data
- Species filtering based on thermal compatibility

Range Sources:
- 63 species with detailed stellar class breakdowns
- Temperature ranges using mean ± 2 standard deviations (~95% coverage)
- Distance ranges validated against actual observations
"""

        # Load species stellar ranges
        self.species_ranges = self._load_species_ranges()

        # Range tolerance factors (can be adjusted for testing)
        self.STELLAR_TEMP_TOLERANCE = 1.0  # 1.0 = use exact empirical ranges
        self.BODY_TEMP_TOLERANCE = 1.0     # 1.0 = use exact empirical ranges
        self.DISTANCE_TOLERANCE = 1.0      # 1.0 = use exact empirical ranges

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

    def _validate_stellar_temperature(self, species_name: str, stellar_class: str, stellar_temp: float) -> bool:
        """Validate stellar temperature against empirical ranges."""
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

        # Apply tolerance factor
        temp_tolerance = (max_temp - min_temp) * (self.STELLAR_TEMP_TOLERANCE - 1.0) * 0.5
        adjusted_min = min_temp - temp_tolerance
        adjusted_max = max_temp + temp_tolerance

        return adjusted_min <= stellar_temp <= adjusted_max

    def _validate_body_temperature(self, species_name: str, stellar_class: str, body_temp: float) -> bool:
        """Validate body temperature against empirical ranges."""
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

        # Apply tolerance factor and ensure reasonable bounds
        temp_tolerance = (max_temp - min_temp) * (self.BODY_TEMP_TOLERANCE - 1.0) * 0.5
        adjusted_min = max(50, min_temp - temp_tolerance)  # Minimum 50K for habitability
        adjusted_max = min(1000, max_temp + temp_tolerance)  # Maximum 1000K for life

        return adjusted_min <= body_temp <= adjusted_max

    def _validate_orbital_distance(self, species_name: str, stellar_class: str, distance: float) -> bool:
        """Validate orbital distance against empirical ranges."""
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

        # Apply tolerance factor
        dist_tolerance = (max_dist - min_dist) * (self.DISTANCE_TOLERANCE - 1.0) * 0.5
        adjusted_min = max(0, min_dist - dist_tolerance)
        adjusted_max = max_dist + dist_tolerance

        return adjusted_min <= distance <= adjusted_max

    def _is_species_valid_for_system(self, body: Dict, species_info: Dict, system_data: Dict) -> bool:
        """Validate species against all empirical range criteria."""
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
        """Enhanced species detection using temperature and distance range validation."""
        # Get base species detections
        base_species = super().detect_species_on_body(body)

        if not system_data:
            return base_species

        # Filter species based on empirical range validation
        valid_species = []
        stellar_class = self._get_stellar_class(system_data)
        stellar_temp = self._get_stellar_temperature(system_data)

        for species in base_species:
            if self._is_species_valid_for_system(body, species, system_data):
                valid_species.append({
                    **species,
                    'stellar_class': stellar_class,
                    'stellar_temperature': stellar_temp,
                    'validation_method': 'empirical_temperature_ranges'
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
            info['stellar_classes'][stellar_class] = {
                'observations': class_data.get('observations', 0),
                'stellar_temp_range': class_data.get('stellar_temp_range'),
                'body_temp_range': class_data.get('body_temp_range'),
                'distance_range': class_data.get('distance_range'),
                'thermal_regulation': class_data.get('thermal_regulation_quality', 'unknown')
            }

        return info

    def set_tolerance_factors(self, stellar_temp_tol: float = 1.0, body_temp_tol: float = 1.0, distance_tol: float = 1.0):
        """Adjust tolerance factors for testing and refinement."""
        self.STELLAR_TEMP_TOLERANCE = stellar_temp_tol
        self.BODY_TEMP_TOLERANCE = body_temp_tol
        self.DISTANCE_TOLERANCE = distance_tol

        print(f"Updated tolerance factors:")
        print(f"  Stellar temperature: {stellar_temp_tol}")
        print(f"  Body temperature: {body_temp_tol}")
        print(f"  Distance: {distance_tol}")

    def get_output_columns(self) -> List[str]:
        """Return output columns for temperature range configuration."""
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
            'validation_method'
        ]