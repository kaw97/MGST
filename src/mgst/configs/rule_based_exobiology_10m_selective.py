"""High-Value Rule-Based Exobiology Configuration - SELECTIVE (10M+ Threshold)

This configuration uses genus-level minimum value analysis to ensure guaranteed high-value 
returns with selective criteria that require either multiple high-value genera per body 
OR multiple qualifying bodies.

Selection Criteria:
1. 1+ bodies where each body has at least 2 genera whose minimum species value is >10M
   OR
2. 3+ bodies where each body has at least 1 genus whose minimum species value is >10M

This approach ensures systems provide substantial value by requiring either diversity 
within a single body or multiple qualifying bodies across the system.
"""

from typing import Dict, Any, List, Optional
from .high_value_exobiology import HighValueExobiologyConfig


class RuleBasedExobiologySelectiveConfig(HighValueExobiologyConfig):
    """High-Value Rule-Based Exobiology Configuration - SELECTIVE (10M+ Threshold)"""
    
    def __init__(self):
        super().__init__()
        
        # Override the parent configuration details
        self._name = "rule-based-exobiology-10m-selective"
        self._description = (
            "High-Value Rule-Based Exobiology Configuration - SELECTIVE (10M+ Threshold)\n"
            "Uses genus-level minimum value analysis to ensure guaranteed high-value returns with selective criteria.\n\n"
            "SELECTIVE Criteria:\n"
            "1. 1+ bodies where each body has at least 2 genera whose minimum species value is >10M\n"
            "   OR\n"
            "2. 3+ bodies where each body has at least 1 genus whose minimum species value is >10M\n\n"
            "Additional Requirements:\n"
            "- Bodies must have atmospheric pressure 0-0.1 atmospheres\n"
            "- No bodies updated after November 29, 2021\n"
            "- Analysis based on minimum possible species value per genus (worst-case scenario)\n"
            "- Systems with single high-value genus per body require 3+ such bodies\n\n"
            "Value Tiers (Minimum Guaranteed):\n"
            "- Low: 1-5M credits, Moderate: 5-10M credits, High: 10-15M credits, Extremely High: >10M credits\n\n"
            "This selective version ensures high returns by requiring multiple high-value genera OR multiple qualifying bodies."
        )
    
    def has_valuable_cooccurrence(self, detected_species: List[Dict], has_bacterium: bool) -> bool:
        """Check if body meets the selective criteria."""
        genus_min_values = self.get_genus_minimum_values(detected_species)
        genus_categories = self.categorize_genera_by_min_value(genus_min_values)
        
        # Count genera with extremely high minimum values (>10M guaranteed)
        extremely_high_genera = len(genus_categories['extremely_high_min'])
        
        # SELECTIVE CRITERIA: Body qualifies if it has 1+ genera with >10M minimum values
        # System-level logic will determine final qualification criteria
        return extremely_high_genera >= 1
    
    def meets_system_criteria(self, qualifying_bodies: List[Dict]) -> bool:
        """Check if system meets the selective criteria."""
        if len(qualifying_bodies) < 1:
            return False
        
        # SELECTIVE LOGIC: System qualifies if it has:
        # 1. 1+ bodies where each body has 2+ genera with >10M minimum values
        #    OR
        # 2. 3+ bodies where each body has 1+ genus with >10M minimum values
        
        # Count bodies by their high-value genus count
        multi_genus_bodies = 0  # Bodies with 2+ high-value genera
        qualifying_bodies_count = 0  # Bodies with at least 1 high-value genus
        
        for body_data in qualifying_bodies:
            # Use already calculated detected_species to avoid expensive recalculation
            detected_species = body_data.get('detected_species', [])
            genus_min_values = self.get_genus_minimum_values(detected_species)
            genus_categories = self.categorize_genera_by_min_value(genus_min_values)
            extremely_high_count = len(genus_categories['extremely_high_min'])
            
            if extremely_high_count >= 2:
                multi_genus_bodies += 1
                qualifying_bodies_count += 1
            elif extremely_high_count >= 1:
                qualifying_bodies_count += 1
        
        # Criteria 1: Any body with 2+ high-value genera qualifies the system
        if multi_genus_bodies >= 1:
            return True
        
        # Criteria 2: 3+ bodies each with at least 1 high-value genus
        if qualifying_bodies_count >= 3:
            return True
        
        return False