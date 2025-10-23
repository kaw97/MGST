"""Binary Body and Moon Search Configuration

This configuration identifies systems with binary bodies and moons that meet specific criteria.
It can precisely filter each body according to any characteristic in the database.

Initial Implementation:
- Binary pairs OR moon-planet pairs where one body is a ringless ammonia world OR earth-like world
- The other body must be landable rocky body with 0-0.1 atm and no volcanism

Architecture supports extensible filtering for any body characteristics:
- Physical properties (mass, radius, gravity, temperature)
- Composition (metal/rock/ice content, atmosphere)
- Geological features (volcanism, rings, terraforming state)
- World types and subtypes
"""

from typing import Dict, Any, List, Optional, Tuple
from .base import BaseConfig


class BinaryBodySearchConfig(BaseConfig):
    """Binary Body and Moon Search Configuration"""
    
    def __init__(self):
        super().__init__(
            name="binary-body-search",
            description=(
            "Binary Body and Moon Search Configuration\n\n"
            "Identifies systems with binary bodies and moons that meet specific criteria.\n"
            "Can precisely filter each body according to any characteristic in the database.\n\n"
            "Current Search Criteria:\n"
            "- Binary pairs OR moon-planet pairs where one body is a ringless ammonia world OR earth-like world\n"
            "- The other body must be landable rocky body with 0-0.1 atm and no volcanism\n\n"
            "Features:\n"
            "- Detects binary relationships through shared Null parents\n"
            "- Detects moon relationships through Planet parents\n"
            "- Supports complex body characteristic filtering\n"
            "- Extensible architecture for any filtering criteria\n\n"
            "Output includes detailed information about binary pairs, moons, and orbital characteristics."
            )
        )
    
    def detect_binary_pairs(self, bodies: List[Dict]) -> List[Tuple[Dict, Dict]]:
        """Detect binary pairs by finding bodies that share the same Null parent."""
        # Group bodies by their Null parent ID
        null_parent_groups = {}
        
        for body in bodies:
            if body['type'] != 'Planet':
                continue
                
            parents = body.get('parents', [])
            null_parent_id = None
            
            # Find the Null parent ID
            for parent in parents:
                if 'Null' in parent and parent['Null'] != 0:  # Exclude root null
                    null_parent_id = parent['Null']
                    break
            
            if null_parent_id is not None:
                if null_parent_id not in null_parent_groups:
                    null_parent_groups[null_parent_id] = []
                null_parent_groups[null_parent_id].append(body)
        
        # Extract binary pairs (groups with exactly 2 bodies)
        binary_pairs = []
        for group in null_parent_groups.values():
            if len(group) == 2:
                binary_pairs.append((group[0], group[1]))
        
        return binary_pairs
    
    def detect_moons(self, bodies: List[Dict]) -> List[Tuple[Dict, Dict]]:
        """Detect moon-planet relationships by finding bodies with Planet parents."""
        moon_planet_pairs = []
        
        # Create a lookup for bodies by their bodyId
        body_lookup = {body['bodyId']: body for body in bodies if body['type'] == 'Planet'}
        
        for body in bodies:
            if body['type'] != 'Planet':
                continue
                
            parents = body.get('parents', [])
            
            # Check if this body has a Planet parent (making it a moon)
            for parent in parents:
                if 'Planet' in parent:
                    parent_body_id = parent['Planet']
                    if parent_body_id in body_lookup:
                        parent_body = body_lookup[parent_body_id]
                        moon_planet_pairs.append((body, parent_body))  # (moon, planet)
                    break
        
        return moon_planet_pairs
    
    def has_rings(self, body: Dict) -> bool:
        """Check if a body has rings."""
        return bool(body.get('rings', []))
    
    def matches_partner_a_criteria(self, body: Dict) -> bool:
        """Check if body matches Partner A criteria: ringless ammonia world OR earth-like world."""
        sub_type = body.get('subType', '')
        
        if sub_type == 'Earth-like world':
            return True
        elif sub_type == 'Ammonia world':
            return not self.has_rings(body)
        
        return False
    
    def matches_partner_b_criteria(self, body: Dict) -> bool:
        """Check if body matches Partner B criteria: landable rocky body with 0-0.1 atm AND no volcanism."""
        sub_type = body.get('subType', '')
        volcanism = body.get('volcanismType', '')
        atmosphere_pressure = body.get('surfacePressure', 0)
        is_landable = body.get('isLandable', False)

        return (sub_type == 'Rocky body' and
                volcanism == 'No volcanism' and
                0 <= atmosphere_pressure <= 0.1 and
                is_landable)
    
    def matches_binary_criteria(self, body1: Dict, body2: Dict) -> bool:
        """Check if binary pair matches search criteria."""
        # Check both combinations: A-B and B-A
        combo1 = (self.matches_partner_a_criteria(body1) and 
                 self.matches_partner_b_criteria(body2))
        combo2 = (self.matches_partner_a_criteria(body2) and 
                 self.matches_partner_b_criteria(body1))
        
        return combo1 or combo2
    
    def get_body_summary(self, body: Dict) -> Dict[str, Any]:
        """Extract key characteristics of a body for output."""
        return {
            'name': body.get('name', ''),
            'type': body.get('subType', ''),
            'mass': body.get('earthMasses', 0),
            'radius': body.get('radius', 0),
            'gravity': body.get('gravity', 0),
            'temperature': body.get('surfaceTemperature', 0),
            'pressure': body.get('surfacePressure', 0),
            'atmosphere': body.get('atmosphereType', ''),
            'volcanism': body.get('volcanismType', ''),
            'terraforming': body.get('terraformingState', ''),
            'rings': len(body.get('rings', [])),
            'orbital_period': body.get('orbitalPeriod', 0),
            'semi_major_axis': body.get('semiMajorAxis', 0),
            'distance_to_arrival': body.get('distanceToArrival', 0)
        }
    
    def filter_system(self, system_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Filter systems containing binary bodies or moon-planet pairs matching criteria."""
        if 'bodies' not in system_data:
            return None

        bodies = system_data['bodies']

        # Detect binary pairs and moons
        binary_pairs = self.detect_binary_pairs(bodies)
        moon_pairs = self.detect_moons(bodies)

        # Filter binary pairs that match our criteria
        matching_pairs = []
        for pair in binary_pairs:
            if self.matches_binary_criteria(pair[0], pair[1]):
                matching_pairs.append(pair)

        # Also check moon-planet pairs that match our criteria
        for moon, planet in moon_pairs:
            if self.matches_binary_criteria(moon, planet):
                matching_pairs.append((moon, planet))

        # System qualifies if it has at least one matching pair (binary or moon-planet)
        if not matching_pairs:
            return None
        
        # Build result data
        coords = system_data.get('coords', {})
        result = {
            'system_name': system_data.get('name', ''),
            'coords_x': coords.get('x', 0),
            'coords_y': coords.get('y', 0),
            'coords_z': coords.get('z', 0),
            'binary_pair_count': len(matching_pairs),
            'moon_count': len(moon_pairs),
            'total_planets': len([b for b in bodies if b['type'] == 'Planet'])
        }
        
        # Add details for up to 3 binary pairs
        for i, (body1, body2) in enumerate(matching_pairs[:3]):
            pair_num = i + 1
            
            # Determine which is Partner A and which is Partner B
            if self.matches_partner_a_criteria(body1):
                partner_a, partner_b = body1, body2
            else:
                partner_a, partner_b = body2, body1
            
            a_summary = self.get_body_summary(partner_a)
            b_summary = self.get_body_summary(partner_b)
            
            result.update({
                f'pair_{pair_num}_body_a_name': a_summary['name'],
                f'pair_{pair_num}_body_a_type': a_summary['type'],
                f'pair_{pair_num}_body_a_mass': a_summary['mass'],
                f'pair_{pair_num}_body_a_gravity': a_summary['gravity'],
                f'pair_{pair_num}_body_a_temperature': a_summary['temperature'],
                f'pair_{pair_num}_body_a_atmosphere': a_summary['atmosphere'],
                f'pair_{pair_num}_body_a_rings': a_summary['rings'],
                f'pair_{pair_num}_body_a_distance': a_summary['distance_to_arrival'],
                
                f'pair_{pair_num}_body_b_name': b_summary['name'],
                f'pair_{pair_num}_body_b_type': b_summary['type'],
                f'pair_{pair_num}_body_b_mass': b_summary['mass'],
                f'pair_{pair_num}_body_b_gravity': b_summary['gravity'],
                f'pair_{pair_num}_body_b_temperature': b_summary['temperature'],
                f'pair_{pair_num}_body_b_volcanism': b_summary['volcanism'],
                f'pair_{pair_num}_body_b_terraforming': b_summary['terraforming'],
                f'pair_{pair_num}_body_b_distance': b_summary['distance_to_arrival'],
                
                f'pair_{pair_num}_orbital_period': max(a_summary['orbital_period'], b_summary['orbital_period']),
                f'pair_{pair_num}_semi_major_axis': max(a_summary['semi_major_axis'], b_summary['semi_major_axis'])
            })
        
        # Add details for up to 3 moons
        for i, (moon, planet) in enumerate(moon_pairs[:3]):
            moon_num = i + 1
            moon_summary = self.get_body_summary(moon)
            planet_summary = self.get_body_summary(planet)
            
            result.update({
                f'moon_{moon_num}_name': moon_summary['name'],
                f'moon_{moon_num}_type': moon_summary['type'],
                f'moon_{moon_num}_mass': moon_summary['mass'],
                f'moon_{moon_num}_parent_name': planet_summary['name'],
                f'moon_{moon_num}_parent_type': planet_summary['type'],
                f'moon_{moon_num}_orbital_period': moon_summary['orbital_period'],
                f'moon_{moon_num}_distance': moon_summary['distance_to_arrival']
            })
        
        return result
    
    def get_output_columns(self) -> List[str]:
        """Define output columns for TSV format."""
        base_columns = [
            'system_name', 'coords_x', 'coords_y', 'coords_z',
            'binary_pair_count', 'moon_count', 'total_planets'
        ]
        
        # Binary pair columns (up to 3 pairs)
        pair_columns = []
        for i in range(1, 4):
            pair_columns.extend([
                f'pair_{i}_body_a_name', f'pair_{i}_body_a_type', f'pair_{i}_body_a_mass',
                f'pair_{i}_body_a_gravity', f'pair_{i}_body_a_temperature', 
                f'pair_{i}_body_a_atmosphere', f'pair_{i}_body_a_rings', f'pair_{i}_body_a_distance',
                
                f'pair_{i}_body_b_name', f'pair_{i}_body_b_type', f'pair_{i}_body_b_mass',
                f'pair_{i}_body_b_gravity', f'pair_{i}_body_b_temperature',
                f'pair_{i}_body_b_volcanism', f'pair_{i}_body_b_terraforming', f'pair_{i}_body_b_distance',
                
                f'pair_{i}_orbital_period', f'pair_{i}_semi_major_axis'
            ])
        
        # Moon columns (up to 3 moons)
        moon_columns = []
        for i in range(1, 4):
            moon_columns.extend([
                f'moon_{i}_name', f'moon_{i}_type', f'moon_{i}_mass',
                f'moon_{i}_parent_name', f'moon_{i}_parent_type',
                f'moon_{i}_orbital_period', f'moon_{i}_distance'
            ])
        
        return base_columns + pair_columns + moon_columns