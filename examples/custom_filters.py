#!/usr/bin/env python3
"""
Custom filter configuration example.

This example shows how to create your own research configuration
by extending the BaseConfig class.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from mgst.configs.base import BaseConfig

class HighGravityWorldsConfig(BaseConfig):
    """Find systems with high-gravity worlds suitable for engineering materials."""
    
    def __init__(self):
        super().__init__(
            name="high-gravity-worlds",
            description="Systems with high-gravity worlds (>2G) for engineering materials"
        )
    
    def filter_system(self, system_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Filter systems with high-gravity worlds."""
        bodies = system_data.get('bodies', [])
        high_gravity_bodies = []
        
        for body in bodies:
            gravity = body.get('gravity', 0)
            body_type = body.get('subType', '')
            
            # Look for rocky worlds with >2G gravity
            if (gravity > 2.0 and 
                body_type in ['Rocky body', 'High metal content body'] and
                self.has_landable_surface(body)):
                
                high_gravity_bodies.append({
                    'name': body.get('bodyName', 'Unknown'),
                    'gravity': gravity,
                    'type': body_type,
                    'materials': self.predict_materials(body)
                })
        
        # Require at least 1 high-gravity world
        if len(high_gravity_bodies) == 0:
            return None
        
        # Calculate system metrics
        coords = self.extract_system_coordinates(system_data)
        max_gravity = max(body['gravity'] for body in high_gravity_bodies)
        
        return {
            'system_name': self.get_system_name(system_data),
            'high_gravity_bodies': len(high_gravity_bodies),
            'max_gravity': max_gravity,
            'coords_x': coords[0],
            'coords_y': coords[1], 
            'coords_z': coords[2],
            'body_details': high_gravity_bodies[:3]  # Limit to first 3
        }
    
    def has_landable_surface(self, body: Dict) -> bool:
        """Check if body has a landable surface."""
        # Check for atmosphere compatibility and surface features
        atmosphere = body.get('atmosphereType', '')
        return (
            atmosphere in ['No atmosphere', 'Thin atmosphere'] or
            'Thin' in atmosphere
        )
    
    def predict_materials(self, body: Dict) -> List[str]:
        """Predict likely engineering materials based on body properties."""
        materials = []
        gravity = body.get('gravity', 0)
        body_type = body.get('subType', '')
        
        if gravity > 3.0:
            materials.append('High-density materials')
        if body_type == 'High metal content body':
            materials.extend(['Iron', 'Nickel', 'Chromium'])
        if gravity > 2.5 and body_type == 'Rocky body':
            materials.extend(['Silicon', 'Sulphur'])
            
        return materials
    
    def get_output_columns(self) -> List[str]:
        """Define output columns for this configuration."""
        return [
            'system_name',
            'high_gravity_bodies',
            'max_gravity', 
            'coords_x',
            'coords_y',
            'coords_z',
            'predicted_materials'
        ]

def main():
    """Example usage of custom configuration."""
    config = HighGravityWorldsConfig()
    
    # Example system data
    example_system = {
        'name': 'Test System',
        'coords': {'x': 100.0, 'y': -50.0, 'z': 25.0},
        'bodies': [
            {
                'bodyName': 'Test World A',
                'gravity': 2.5,
                'subType': 'High metal content body',
                'atmosphereType': 'Thin atmosphere'
            },
            {
                'bodyName': 'Test World B', 
                'gravity': 0.8,
                'subType': 'Rocky body',
                'atmosphereType': 'No atmosphere'
            }
        ]
    }
    
    result = config.filter_system(example_system)
    if result:
        print("✅ System qualifies!")
        print(f"System: {result['system_name']}")
        print(f"High-gravity bodies: {result['high_gravity_bodies']}")
        print(f"Max gravity: {result['max_gravity']:.1f}G")
    else:
        print("❌ System does not qualify")

if __name__ == "__main__":
    main()