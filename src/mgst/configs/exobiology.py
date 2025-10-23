"""Exobiology co-occurrence expansion research configuration.

Expands the original dataset by adding systems where bodies have both ultra-high value (≥15M) 
and moderate value (12-15M) species on the same body, reducing the 3+ body requirement.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime

from .base import BaseConfig


class ExobiologyConfig(BaseConfig):
    """Co-occurrence expansion exobiology research configuration."""
    
    def __init__(self):
        super().__init__(
            name="exobiology-cooccurrence",
            description="""Co-Occurrence Expansion Exobiology Research Configuration
Expands the original dataset by adding systems where bodies have both ultra-high value (≥15M) 
and moderate value (12-15M) species on the same body, reducing the 3+ body requirement.

Criteria: 
- Systems with 3+ bodies, each having 1+ genera where ALL predicted species of that genus ≥15 million credits (ORIGINAL)
  OR
- Systems with 2+ bodies having 1+ genera where ALL species ≥15M (REDUCED REQUIREMENT)
  PLUS at least 1 body that has BOTH ultra-high (≥15M) AND moderate value (12-15M) species
- At least 2-3 different qualifying bodies (0-0.1 atm atmosphere with high-value species)
- No bodies updated after November 29, 2021 (to exclude newer discoveries)
- Bodies must have atmospheric pressure between 0 and 0.1 atmospheres (suitable for biology)
- SPECIAL RULE: If a body is eligible for Bacterium Aurasus or Cerbrus, it must have at least 3 total genera
  (bacterium + valuable genus + at least 1 additional genus) to increase spawn diversity"""
        )
        # Date threshold: November 29, 2021
        self.date_threshold = datetime(2021, 11, 29)
    
    def has_competing_bacterium(self, body: Dict) -> bool:
        """Check if body is eligible for bacterium aurasus or cerbrus (species that compete with valuable ones)."""
        # Extract body conditions - handle None values
        atmosphere_type = body.get('atmosphereType', '') or ''
        surface_temp = body.get('surfaceTemperature', 0) or 0
        gravity = body.get('gravity', 0) or 0
        body_type = body.get('subType', '') or ''
        
        # Normalize atmosphere name (remove "Thin " prefix)
        normalized_atmosphere = atmosphere_type.replace('Thin ', '').replace('Thick ', '')
        
        # Check Bacterium Aurasus eligibility
        # Criteria: CarbonDioxide atmosphere, Rocky/High metal/Rocky ice body, gravity 0.039-0.608, temp 145-400K
        if (normalized_atmosphere == 'Carbon dioxide' and 
            body_type in ['Rocky body', 'High metal content body', 'Rocky ice body'] and
            0.039 <= gravity <= 0.608 and
            145.0 <= surface_temp <= 400.0):
            return True
        
        # Check Bacterium Cerbrus eligibility  
        # Criteria: SulphurDioxide atmosphere, Rocky/High metal/Rocky ice body, gravity 0.042-0.605, temp 132-500K
        if (normalized_atmosphere == 'Sulphur dioxide' and
            body_type in ['Rocky body', 'High metal content body', 'Rocky ice body'] and
            0.042 <= gravity <= 0.605 and
            132.0 <= surface_temp <= 500.0):
            return True
            
        return False
    
    def has_cooccurrence_species(self, body: Dict) -> bool:
        """Check if body has both ultra-high value (≥15M) and moderate value (12-15M) species."""
        bioscan_predictions = body.get('bioscan_predictions')
        if not bioscan_predictions:
            return False
        
        predicted_species = bioscan_predictions.get('predicted_species', [])
        if not predicted_species:
            return False
        
        has_ultra_high = False
        has_moderate = False
        
        for species in predicted_species:
            value = species.get('value', 0)
            if value >= 15000000:
                has_ultra_high = True
            elif 12000000 <= value < 15000000:
                has_moderate = True
        
        return has_ultra_high and has_moderate
    
    def filter_system(self, system_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Filter systems based on exobiology criteria."""
        bodies = system_data.get('bodies', [])
        qualifying_bodies = []
        cooccurrence_bodies = []
        
        for body in bodies:
            # Check if body has suitable atmosphere
            if not self.has_suitable_atmosphere(body):
                continue
                
            # Check date filter
            if not self.passes_date_filter(body, self.date_threshold):
                continue
                
            # Check for bioscan predictions
            bioscan_predictions = body.get('bioscan_predictions')
            if not bioscan_predictions:
                continue
                
            predicted_species = bioscan_predictions.get('predicted_species', [])
            if not predicted_species:
                continue
                
            # Group species by genus and check if ALL species in genus meet threshold
            genus_species = self.group_species_by_genus(predicted_species)
            body_species_detail = []
            ultra_high_species = []
            moderate_species = []
            
            for species in predicted_species:
                genus = species.get('genus', 'Unknown')
                value = species.get('value', 0)
                species_name = species.get('species', 'Unknown')
                
                species_info = {
                    'species': species_name,
                    'value': value,
                    'full_name': species.get('name', f"{genus} {species_name}")
                }
                
                # Track ultra-high and moderate value species
                if value >= 15000000:
                    ultra_high_species.append(species_info)
                elif 12000000 <= value < 15000000:
                    moderate_species.append(species_info)
            
            # Only count genera where ALL species meet the 15M threshold (for qualifying bodies)
            qualifying_genera = {}
            for genus, species_list in genus_species.items():
                # Check if ALL species in this genus meet the threshold
                all_species_qualify = all(species['value'] >= 15000000 for species in species_list)
                
                if all_species_qualify:
                    # Calculate total value for this genus (all species combined)
                    total_genus_value = sum(species['value'] for species in species_list)
                    qualifying_genera[genus] = {
                        'species_count': len(species_list),
                        'total_value': total_genus_value,
                        'species_list': species_list
                    }
                    
                    # Add to body species detail
                    for species in species_list:
                        body_species_detail.append({
                            'genus': genus,
                            'species': species['species'],
                            'value': species['value'],
                            'full_name': species['full_name']
                        })
            
            # Check if body qualifies and has co-occurrence
            has_cooccurrence = len(ultra_high_species) > 0 and len(moderate_species) > 0
            
            # Body must have at least 1 qualifying genus (where ALL species ≥15M)
            if len(qualifying_genera) >= 1:
                # Apply genus diversity rule for bodies with competing bacterium
                if self.has_competing_bacterium(body):
                    # Count all genera (including those that don't meet 15M threshold)
                    total_genera_count = len(genus_species)
                    # Require at least 3 total genera if competing bacterium present
                    if total_genera_count < 3:
                        continue
                        
                body_info = {
                    'body_name': body.get('bodyName', body.get('name', 'Unknown')),
                    'genus_count': len(qualifying_genera),
                    'total_value': sum(g['total_value'] for g in qualifying_genera.values()),
                    'genera_details': qualifying_genera,
                    'species_detail': body_species_detail,
                    'atmosphere': body.get('atmosphereType', ''),
                    'pressure': body.get('surfacePressure', 0),
                    'has_cooccurrence': has_cooccurrence,
                    'ultra_high_species': ultra_high_species,
                    'moderate_species': moderate_species
                }
                
                qualifying_bodies.append(body_info)
                
                if has_cooccurrence:
                    cooccurrence_bodies.append(body_info)
        
        # Apply expansion logic:
        # 1. 3+ qualifying bodies (original rule - always qualifies)
        # 2. 2+ qualifying bodies AND at least 1 body with co-occurrence (expansion rule)
        qualification_type = ""
        
        if len(qualifying_bodies) >= 3:
            # Original 3+ body rule - always qualifies
            qualification_type = "3+ bodies (original)"
        elif len(qualifying_bodies) >= 2 and len(cooccurrence_bodies) >= 1:
            # 2+ body expansion rule: requires co-occurrence
            qualification_type = f"{len(qualifying_bodies)}-body + co-occurrence (expansion)"
        else:
            # Doesn't qualify
            return None
        
        # Calculate system totals
        total_genera = set()
        total_value = 0
        all_species_details = []
        cooccurrence_count = len(cooccurrence_bodies)
        
        for body in qualifying_bodies:
            total_genera.update(body['genera_details'].keys())
            total_value += body['total_value']
            all_species_details.extend(body['species_detail'])
        
        coords = self.extract_system_coordinates(system_data)
        
        # Prepare detailed body information for up to 3 bodies
        result = {
            'system_name': self.get_system_name(system_data),
            'qualifying_bodies': len(qualifying_bodies),
            'total_genera': len(total_genera),
            'total_value': total_value,
            'cooccurrence_bodies': cooccurrence_count,
            'qualification_type': qualification_type,
            'coords_x': coords[0],
            'coords_y': coords[1], 
            'coords_z': coords[2],
            'body_details': qualifying_bodies,
            'species_summary': f"{len(total_genera)} genera, {len(all_species_details)} species (all ≥15M), {total_value:,} credits",
            'genera_list': ', '.join(sorted(total_genera))
        }
        
        # Add detailed body information for up to 3 bodies
        for i in range(3):
            body_prefix = f'body_{i+1}'
            if i < len(qualifying_bodies):
                body = qualifying_bodies[i]
                result[f'{body_prefix}_name'] = body['body_name']
                result[f'{body_prefix}_atmosphere'] = body['atmosphere']
                result[f'{body_prefix}_pressure'] = body['pressure']
                result[f'{body_prefix}_genera'] = ', '.join(sorted(body['genera_details'].keys()))
                result[f'{body_prefix}_species_count'] = sum(g['species_count'] for g in body['genera_details'].values())
                result[f'{body_prefix}_value'] = body['total_value']
                result[f'{body_prefix}_cooccurrence'] = 'Yes' if body['has_cooccurrence'] else 'No'
            else:
                result[f'{body_prefix}_name'] = ''
                result[f'{body_prefix}_atmosphere'] = ''
                result[f'{body_prefix}_pressure'] = ''
                result[f'{body_prefix}_genera'] = ''
                result[f'{body_prefix}_species_count'] = ''
                result[f'{body_prefix}_value'] = ''
                result[f'{body_prefix}_cooccurrence'] = ''
        
        return result
    
    def get_output_columns(self) -> List[str]:
        """Return the list of columns for the output CSV."""
        return [
            'system_name',
            'qualifying_bodies',
            'total_genera',
            'total_value',
            'cooccurrence_bodies',
            'qualification_type',
            'coords_x',
            'coords_y',
            'coords_z',
            'species_summary',
            'genera_list',
            'body_1_name',
            'body_1_atmosphere',
            'body_1_pressure',
            'body_1_genera',
            'body_1_species_count',
            'body_1_value',
            'body_1_cooccurrence',
            'body_2_name',
            'body_2_atmosphere',
            'body_2_pressure',
            'body_2_genera',
            'body_2_species_count',
            'body_2_value',
            'body_2_cooccurrence',
            'body_3_name',
            'body_3_atmosphere',
            'body_3_pressure',
            'body_3_genera',
            'body_3_species_count',
            'body_3_value',
            'body_3_cooccurrence'
        ]