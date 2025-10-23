"""High-value exobiology co-occurrence search configuration.

This implementation uses the same genus-level minimum value analysis as the selective
configuration, but with a lower threshold - requiring "high value" (10-15M credits)
instead of "extremely high value" (>15M credits).

This should significantly increase the number of qualifying systems while still
ensuring guaranteed high-value returns.
"""

import os
import importlib.util
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime
from pathlib import Path

from .base import BaseConfig


class HighValueExobiologyConfig(BaseConfig):
    """High-value exobiology configuration using genus-level minimum value analysis with 10M+ threshold."""
    
    def __init__(self):
        super().__init__(
            name="high-value-exobiology",
            description="""High-Value Rule-Based Exobiology Configuration
Uses genus-level minimum value analysis to ensure guaranteed high-value returns with 10M+ threshold.

Selection Criteria:
1. 3+ bodies with at least 1 genus whose minimum species value is high (10M+), OR
2. 2+ bodies with high genus + moderate minimum value genus (5-10M)

Additional Requirements:
- Bodies must have atmospheric pressure 0-0.1 atmospheres
- No bodies updated after November 29, 2021
- Bodies with bacterium must have additional non-bacterium genus
- Analysis based on minimum possible species value per genus (worst-case scenario)

Value Tiers (Minimum Guaranteed):
- Low: 1-5M credits, Moderate: 5-10M credits, High: 10-15M credits, Extremely High: >15M credits
"""
        )
        
        # Value tier thresholds
        self.LOW_VALUE_MIN = 1_000_000
        self.LOW_VALUE_MAX = 5_000_000
        self.MODERATE_VALUE_MIN = 5_000_000
        self.MODERATE_VALUE_MAX = 10_000_000
        self.HIGH_VALUE_MIN = 10_000_000
        self.HIGH_VALUE_MAX = 15_000_000
        self.EXTREMELY_HIGH_VALUE_MIN = 15_000_000
        
        # Date threshold for filtering out recently updated bodies
        self.date_threshold = datetime(2021, 11, 29)
        
        # Load species rulesets
        self.species_rulesets = self._load_all_rulesets()
        
    def _load_all_rulesets(self) -> Dict[str, Dict]:
        """Load all species rulesets from the rulesets directory."""
        rulesets_dir = Path(__file__).parent.parent.parent.parent / "rulesets"
        species_data = {}
        
        if not rulesets_dir.exists():
            raise FileNotFoundError(f"Rulesets directory not found: {rulesets_dir}")
        
        for ruleset_file in rulesets_dir.glob("*.py"):
            if ruleset_file.name.startswith("__"):
                continue
                
            try:
                # Load the module dynamically
                spec = importlib.util.spec_from_file_location("ruleset", ruleset_file)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                if hasattr(module, 'catalog'):
                    catalog = module.catalog
                    
                    # Extract species from catalog
                    for genus_key, genus_data in catalog.items():
                        for species_key, species_info in genus_data.items():
                            species_name = species_info.get('name', 'Unknown')
                            value = species_info.get('value', 0)
                            rulesets = species_info.get('rulesets', [])
                            
                            # Store species with all its ruleset data
                            species_data[species_name] = {
                                'genus': genus_key,
                                'value': value,
                                'rulesets': rulesets
                            }
                            
            except Exception as e:
                print(f"Warning: Could not load ruleset {ruleset_file}: {e}")
                continue
        
        print(f"Loaded {len(species_data)} species from {len(list(rulesets_dir.glob('*.py')))} ruleset files")
        return species_data
    
    def _normalize_atmosphere(self, atmosphere_type: str) -> str:
        """Normalize atmosphere type string."""
        if not atmosphere_type:
            return ""
        return atmosphere_type.replace(" atmosphere", "").replace("_", "").strip()
    
    def _check_ruleset_match(self, body: Dict, ruleset: Dict) -> bool:
        """Check if a body matches a specific species ruleset."""
        # Extract body characteristics
        atmosphere_type = self._normalize_atmosphere(body.get('atmosphereType', '') or '')
        surface_temp = body.get('surfaceTemperature', 0) or 0
        gravity = body.get('gravity', 0) or 0
        body_type = body.get('subType', '') or ''
        surface_pressure = body.get('surfacePressure', 0) or 0
        volcanism = body.get('volcanism', '') or ''
        
        # Check atmosphere requirement
        if 'atmosphere' in ruleset:
            required_atmospheres = ruleset['atmosphere']
            if isinstance(required_atmospheres, list):
                if atmosphere_type not in required_atmospheres:
                    return False
            else:
                if atmosphere_type != required_atmospheres:
                    return False
        
        # Check body type requirement
        if 'body_type' in ruleset:
            required_body_types = ruleset['body_type']
            if isinstance(required_body_types, list):
                if body_type not in required_body_types:
                    return False
            else:
                if body_type != required_body_types:
                    return False
        
        # Check gravity range
        if 'min_gravity' in ruleset and gravity < ruleset['min_gravity']:
            return False
        if 'max_gravity' in ruleset and gravity > ruleset['max_gravity']:
            return False
        
        # Check temperature range
        if 'min_temperature' in ruleset and surface_temp < ruleset['min_temperature']:
            return False
        if 'max_temperature' in ruleset and surface_temp > ruleset['max_temperature']:
            return False
        
        # Check pressure range
        if 'min_pressure' in ruleset and surface_pressure < ruleset['min_pressure']:
            return False
        if 'max_pressure' in ruleset and surface_pressure > ruleset['max_pressure']:
            return False
        
        # Check volcanism requirement
        if 'volcanism' in ruleset:
            volcanism_req = ruleset['volcanism']
            if volcanism_req == 'Any':
                # Any volcanism including none is acceptable
                pass
            elif volcanism_req == 'None':
                if volcanism and volcanism.lower() != 'none':
                    return False
            elif isinstance(volcanism_req, list):
                # Check if any of the required volcanism types are present
                volcanism_lower = volcanism.lower()
                if not any(vol_type.lower() in volcanism_lower for vol_type in volcanism_req):
                    return False
            else:
                # Single volcanism type requirement
                if volcanism_req.lower() not in volcanism.lower():
                    return False
        
        return True
    
    def detect_species_on_body(self, body: Dict) -> List[Dict]:
        """Detect all possible species on a body based on its characteristics."""
        detected_species = []
        
        for species_name, species_info in self.species_rulesets.items():
            rulesets = species_info.get('rulesets', [])
            
            # Check if any ruleset matches this body
            for ruleset in rulesets:
                if self._check_ruleset_match(body, ruleset):
                    detected_species.append({
                        'name': species_name,
                        'genus': species_info.get('genus', 'Unknown'),
                        'value': species_info.get('value', 0)
                    })
                    break  # Only need one matching ruleset per species
                
        return detected_species
    
    def has_suitable_atmosphere(self, body: Dict) -> bool:
        """Check if body has suitable atmospheric pressure (0-0.1 atm)."""
        surface_pressure = body.get('surfacePressure', 0) or 0
        return 0.0 <= surface_pressure <= 0.1
    
    def passes_date_filter(self, body: Dict, date_threshold: datetime) -> bool:
        """Check if body hasn't been updated after the threshold date."""
        update_time = body.get('updateTime')
        if not update_time:
            return True
            
        try:
            if isinstance(update_time, str):
                body_date = datetime.fromisoformat(update_time.replace('Z', '+00:00'))
            else:
                body_date = update_time
            return body_date <= date_threshold
        except (ValueError, TypeError):
            return True
    
    def get_genus_minimum_values(self, detected_species: List[Dict]) -> Dict[str, int]:
        """Get the minimum value species for each genus."""
        genus_min_values = {}
        
        for species in detected_species:
            genus = species['genus']
            value = species['value']
            
            if genus not in genus_min_values:
                genus_min_values[genus] = value
            else:
                genus_min_values[genus] = min(genus_min_values[genus], value)
        
        return genus_min_values
    
    def categorize_genera_by_min_value(self, genus_min_values: Dict[str, int]) -> Dict[str, List[str]]:
        """Categorize genera by their minimum species value."""
        categories = {
            'extremely_high_min': [],  # Minimum species in genus is >15M
            'high_min': [],            # Minimum species in genus is 10-15M
            'moderate_min': [],        # Minimum species in genus is 5-10M
            'low_min': []              # Minimum species in genus is 1-5M
        }
        
        for genus, min_value in genus_min_values.items():
            if min_value >= self.EXTREMELY_HIGH_VALUE_MIN:
                categories['extremely_high_min'].append(genus)
            elif min_value >= self.HIGH_VALUE_MIN:
                categories['high_min'].append(genus)
            elif min_value >= self.MODERATE_VALUE_MIN:
                categories['moderate_min'].append(genus)
            elif min_value >= self.LOW_VALUE_MIN:
                categories['low_min'].append(genus)
        
        return categories
    
    def has_valuable_cooccurrence(self, detected_species: List[Dict], has_bacterium: bool) -> bool:
        """Check if body meets the high-value criteria (lowered threshold)."""
        genus_min_values = self.get_genus_minimum_values(detected_species)
        genus_categories = self.categorize_genera_by_min_value(genus_min_values)
        
        # Count genera with high minimum values (10M+ guaranteed) - THIS IS THE KEY CHANGE
        high_or_extremely_high_genera = len(genus_categories['high_min']) + len(genus_categories['extremely_high_min'])
        
        # Count genera with moderate minimum values (5-10M guaranteed)  
        moderate_min_genera = len(genus_categories['moderate_min'])
        
        # If body has bacterium, it needs an additional non-bacterium genus
        required_genera = 2 if has_bacterium else 1
        
        # Check if this body has at least 1 genus with high minimum value (10M+)
        has_high_genus = high_or_extremely_high_genera >= 1
        
        # Body qualifies if it has:
        # - At least 1 genus with high minimum value (10M+ guaranteed)
        # - If bacterium present, must have additional genus beyond bacterium
        return (has_high_genus and 
                len(genus_min_values) >= required_genera)
    
    def has_competing_bacterium(self, body: Dict) -> bool:
        """Check if body is eligible for bacterium aurasus or cerbrus."""
        atmosphere_type = self._normalize_atmosphere(body.get('atmosphereType', '') or '')
        surface_temp = body.get('surfaceTemperature', 0) or 0
        gravity = body.get('gravity', 0) or 0
        body_type = body.get('subType', '') or ''
        
        # Check Bacterium Aurasus eligibility
        if (atmosphere_type == 'CarbonDioxide' and 
            body_type in ['Rocky body', 'High metal content body', 'Rocky ice body'] and
            0.039 <= gravity <= 0.608 and
            145.0 <= surface_temp <= 400.0):
            return True
        
        # Check Bacterium Cerbrus eligibility  
        if (atmosphere_type == 'SulphurDioxide' and
            body_type in ['Rocky body', 'High metal content body', 'Rocky ice body'] and
            0.042 <= gravity <= 0.605 and
            132.0 <= surface_temp <= 500.0):
            return True
            
        return False
    
    def meets_system_criteria(self, qualifying_bodies: List[Dict]) -> bool:
        """Check if system meets the high-value criteria (lowered thresholds)."""
        if len(qualifying_bodies) < 2:
            return False
        
        # Analyze each body's genus minimum values
        body_analyses = []
        for body in qualifying_bodies:
            genus_min_values = self.get_genus_minimum_values(body['detected_species'])
            genus_categories = self.categorize_genera_by_min_value(genus_min_values)
            
            body_analyses.append({
                'body': body,
                'genus_min_values': genus_min_values,
                'genus_categories': genus_categories,
                'high_or_extremely_high_genera': len(genus_categories['high_min']) + len(genus_categories['extremely_high_min']),
                'moderate_min_genera': len(genus_categories['moderate_min']),
                'has_bacterium': body.get('has_bacterium', False)
            })
        
        # Condition 1: 3 bodies with at least 1 genus whose minimum is high value (10M+) - LOWERED THRESHOLD
        high_value_bodies = [
            body_analysis for body_analysis in body_analyses 
            if body_analysis['high_or_extremely_high_genera'] >= 1
        ]
        
        if len(high_value_bodies) >= 3:
            return True
        
        # Condition 2: 2 bodies with high genus + moderate minimum value genus - LOWERED THRESHOLD  
        condition2_bodies = []
        for body_analysis in body_analyses:
            has_high = body_analysis['high_or_extremely_high_genera'] >= 1
            has_moderate_min = body_analysis['moderate_min_genera'] >= 1
            
            if has_high and has_moderate_min:
                condition2_bodies.append(body_analysis)
        
        if len(condition2_bodies) >= 2:
            return True
        
        return False
    
    def filter_system(self, system_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Filter systems based on rule-based exobiology criteria."""
        bodies = system_data.get('bodies', [])
        qualifying_bodies = []
        
        for body in bodies:
            # Check if body has suitable atmosphere (0-0.1 atm)
            if not self.has_suitable_atmosphere(body):
                continue
                
            # Check date filter
            if not self.passes_date_filter(body, self.date_threshold):
                continue
            
            # Detect species on this body
            detected_species = self.detect_species_on_body(body)
            if not detected_species:
                continue
            
            # Check if body has competing bacterium
            has_bacterium = self.has_competing_bacterium(body)
            
            # Check for valuable co-occurrence using high-value criteria (lowered threshold)
            if not self.has_valuable_cooccurrence(detected_species, has_bacterium):
                continue
            
            # Calculate total value and prepare body info
            total_value = sum(species['value'] for species in detected_species)
            unique_genera = set(species['genus'] for species in detected_species)
            
            body_info = {
                'body_name': body.get('bodyName', body.get('name', 'Unknown')),
                'detected_species': detected_species,
                'genus_count': len(unique_genera),
                'species_count': len(detected_species),
                'total_value': total_value,
                'has_bacterium': has_bacterium,
                'atmosphere_type': body.get('atmosphereType', ''),
                'surface_pressure': body.get('surfacePressure', 0),
                'surface_temperature': body.get('surfaceTemperature', 0),
                'gravity': body.get('gravity', 0),
                'body_type': body.get('subType', '')
            }
            
            qualifying_bodies.append(body_info)
        
        # Check if system meets the criteria
        if not self.meets_system_criteria(qualifying_bodies):
            return None
        
        # System qualifies - prepare detailed output
        result = {
            'system_name': system_data.get('name', 'Unknown'),
            'qualifying_bodies': len(qualifying_bodies),
            'coords_x': round(system_data.get('coords', {}).get('x', 0), 1),
            'coords_y': round(system_data.get('coords', {}).get('y', 0), 1),
            'coords_z': round(system_data.get('coords', {}).get('z', 0), 1)
        }
        
        # Calculate system-level statistics
        total_genera = set()
        total_species = []
        total_system_value = 0
        
        for body_info in qualifying_bodies:
            for species in body_info['detected_species']:
                total_genera.add(species['genus'])
                total_species.append(species)
                total_system_value += species['value']
        
        result.update({
            'total_genera': len(total_genera),
            'total_species': len(total_species),
            'total_system_value': total_system_value
        })
        
        # Add detailed body information for up to 3 bodies
        for i, body_info in enumerate(qualifying_bodies[:3]):
            body_num = i + 1
            
            # Calculate genus categories for this body
            genus_min_values = self.get_genus_minimum_values(body_info['detected_species'])
            genus_categories = self.categorize_genera_by_min_value(genus_min_values)
            
            # Find minimum guaranteed value (worst case)
            min_guaranteed_value = min(genus_min_values.values()) if genus_min_values else 0
            
            # Get top genera for this body (by minimum value)
            top_genera = sorted(genus_min_values.items(), key=lambda x: x[1], reverse=True)[:3]
            top_genera_str = ', '.join([f"{genus}({value//1000000}M)" for genus, value in top_genera])
            
            result.update({
                f'body_{body_num}_name': body_info['body_name'],
                f'body_{body_num}_atmosphere': body_info['atmosphere_type'],
                f'body_{body_num}_pressure': round(body_info['surface_pressure'], 3),
                f'body_{body_num}_temperature': round(body_info['surface_temperature'], 1),
                f'body_{body_num}_gravity': round(body_info['gravity'], 3),
                f'body_{body_num}_body_type': body_info['body_type'],
                f'body_{body_num}_species_count': body_info['species_count'],
                f'body_{body_num}_genus_count': body_info['genus_count'],
                f'body_{body_num}_value': body_info['total_value'],
                f'body_{body_num}_high_or_extremely_high_genera': len(genus_categories['high_min']) + len(genus_categories['extremely_high_min']),
                f'body_{body_num}_moderate_min_genera': len(genus_categories['moderate_min']),
                f'body_{body_num}_low_min_genera': len(genus_categories['low_min']),
                f'body_{body_num}_has_bacterium': body_info['has_bacterium'],
                f'body_{body_num}_min_guaranteed_value': min_guaranteed_value,
                f'body_{body_num}_top_genera': top_genera_str
            })
        
        return result
    
    def get_output_columns(self) -> List[str]:
        """Define output columns for the TSV file."""
        base_columns = [
            'system_name', 'qualifying_bodies', 'total_genera', 'total_species', 'total_system_value',
            'coords_x', 'coords_y', 'coords_z'
        ]
        
        body_columns = []
        for i in range(1, 4):  # Up to 3 bodies
            body_columns.extend([
                f'body_{i}_name', f'body_{i}_atmosphere', f'body_{i}_pressure', 
                f'body_{i}_temperature', f'body_{i}_gravity', f'body_{i}_body_type',
                f'body_{i}_species_count', f'body_{i}_genus_count', f'body_{i}_value',
                f'body_{i}_high_or_extremely_high_genera', f'body_{i}_moderate_min_genera', f'body_{i}_low_min_genera',
                f'body_{i}_has_bacterium', f'body_{i}_min_guaranteed_value', f'body_{i}_top_genera'
            ])
        
        return base_columns + body_columns