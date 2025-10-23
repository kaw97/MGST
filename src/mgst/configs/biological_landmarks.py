"""Biological Landmarks Configuration

This configuration identifies systems containing biological signals with focus on:
1. Single-star systems (to reduce gravitational complexity variables)
2. Systems with confirmed biological scan data
3. Both confirmed and potential biological signals for analysis

The goal is to build a dataset for improving biological species detection rules,
particularly for differentiating Stratum Tectonicas from bacteria species.
"""

from typing import Dict, Any, List, Optional
from .base import BaseConfig


class BiologicalLandmarksConfig(BaseConfig):
    """Configuration for finding biological landmarks and signals"""

    def __init__(self):
        super().__init__(
            name="biological-landmarks",
            description=(
                "Biological Landmarks Configuration\n\n"
                "Identifies systems containing biological signals with focus on single-star systems.\n"
                "Collects data for improving biological species detection rules.\n\n"
                "Target Systems:\n"
                "- Single-star systems only (mainStar count = 1)\n"
                "- Systems with biological signals in body data\n"
                "- Both confirmed scans and potential signals\n\n"
                "Output includes:\n"
                "- System and stellar characteristics\n"
                "- Body characteristics for bio-signal bodies\n"
                "- Biological signal types and genera\n"
                "- Atmospheric, gravitational, and thermal data\n\n"
                "Purpose: Build dataset for Stratum Tectonicas vs bacteria differentiation."
            )
        )

    def count_main_stars(self, system_data: Dict[str, Any]) -> int:
        """Count the number of main stars in the system."""
        bodies = system_data.get('bodies', [])
        main_star_count = 0

        for body in bodies:
            if body.get('type') == 'Star' and body.get('mainStar', False):
                main_star_count += 1

        return main_star_count

    def extract_biological_signals(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Extract biological signal information from a body."""
        signals = body.get('signals', {})
        bio_signals = {}

        # Check for biological signals
        if 'signals' in signals:
            signal_types = signals['signals']
            for signal_type, count in signal_types.items():
                if 'biological' in signal_type.lower() or 'codex' in signal_type.lower():
                    bio_signals[signal_type] = count

        # Check for genera information
        if 'genuses' in signals:
            bio_signals['genuses'] = signals['genuses']

        return bio_signals

    def extract_body_characteristics(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relevant body characteristics for analysis."""
        return {
            'body_id': body.get('bodyId'),
            'name': body.get('name'),
            'type': body.get('type'),
            'sub_type': body.get('subType'),
            'distance_to_arrival': body.get('distanceToArrival'),
            'is_landable': body.get('isLandable'),
            'gravity': body.get('gravity'),
            'earth_masses': body.get('earthMasses'),
            'radius': body.get('radius'),
            'surface_temperature': body.get('surfaceTemperature'),
            'surface_pressure': body.get('surfacePressure'),
            'atmosphere_type': body.get('atmosphereType'),
            'atmosphere_composition': body.get('atmosphereComposition', {}),
            'volcanism_type': body.get('volcanismType'),
            'terraforming_state': body.get('terraformingState'),
            'solid_composition': body.get('solidComposition', {}),
            'materials': body.get('materials', {}),
            'parents': body.get('parents', []),
            'orbital_period': body.get('orbitalPeriod'),
            'semi_major_axis': body.get('semiMajorAxis'),
            'orbital_eccentricity': body.get('orbitalEccentricity'),
            'orbital_inclination': body.get('orbitalInclination'),
            'rotational_period': body.get('rotationalPeriod'),
            'rotational_period_tidally_locked': body.get('rotationalPeriodTidallyLocked'),
            'axial_tilt': body.get('axialTilt')
        }

    def extract_stellar_characteristics(self, system_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract stellar characteristics from the main star."""
        bodies = system_data.get('bodies', [])

        for body in bodies:
            if body.get('type') == 'Star' and body.get('mainStar', False):
                return {
                    'stellar_type': body.get('type'),
                    'stellar_sub_type': body.get('subType'),
                    'spectral_class': body.get('spectralClass'),
                    'luminosity': body.get('luminosity'),
                    'stellar_age': body.get('age'),
                    'stellar_mass': body.get('solarMasses'),
                    'stellar_radius': body.get('solarRadius'),
                    'surface_temperature': body.get('surfaceTemperature'),
                    'absolute_magnitude': body.get('absoluteMagnitude'),
                    'rotational_period': body.get('rotationalPeriod'),
                    'axial_tilt': body.get('axialTilt')
                }

        return {}

    def filter_system(self, system_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Filter systems for biological landmarks analysis."""

        # Requirement 1: Single-star systems only
        main_star_count = self.count_main_stars(system_data)
        if main_star_count != 1:
            return None

        # Requirement 2: Must have bodies with biological signals
        bodies = system_data.get('bodies', [])
        biological_bodies = []

        for body in bodies:
            bio_signals = self.extract_biological_signals(body)
            if bio_signals:  # Body has biological signals
                # For JSONL output, include full body data with biological signals
                full_body = body.copy()
                full_body['biological_signals'] = bio_signals
                biological_bodies.append(full_body)

        if not biological_bodies:
            return None

        # For JSONL output, return the complete system data with enhanced biological info
        result = system_data.copy()
        result['biological_bodies_count'] = len(biological_bodies)
        result['biological_bodies'] = biological_bodies

        # Add main star characteristics at top level for easy access
        stellar_characteristics = self.extract_stellar_characteristics(system_data)
        result['main_star'] = stellar_characteristics

        return result

    def get_output_columns(self) -> List[str]:
        """Define output columns for TSV format."""
        return [
            'system_name', 'system_id64', 'coords_x', 'coords_y', 'coords_z',
            'allegiance', 'government', 'economy', 'secondary_economy', 'security', 'population', 'body_count',
            'stellar_type', 'stellar_sub_type', 'spectral_class', 'luminosity', 'stellar_age',
            'stellar_mass', 'stellar_radius', 'surface_temperature', 'absolute_magnitude',
            'biological_bodies_count', 'date', 'update_time'
        ]