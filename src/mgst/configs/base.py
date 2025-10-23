"""Base configuration system for galaxy filtering."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Callable
from datetime import datetime

class BaseConfig(ABC):
    """Base class for all galaxy filter configurations."""
    
    def __init__(self, name: str, description: str):
        """Initialize base configuration.
        
        Args:
            name: Configuration name
            description: Human-readable description
        """
        self.name = name
        self.description = description
        self._output_columns: List[Tuple[str, Callable]] = []
        self._last_filter_result: Dict = {}
    
    @abstractmethod
    def filter_system(self, system_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Filter a single system based on configuration criteria.
        
        Args:
            system_data: Dictionary containing system data
            
        Returns:
            Filtered system data if system qualifies, None otherwise
        """
        pass
    
    @abstractmethod
    def get_output_columns(self) -> List[str]:
        """Get list of output column names for CSV export.
        
        Returns:
            List of column names
        """
        pass
    
    def get_description(self) -> str:
        """Get configuration description.
        
        Returns:
            Configuration description string
        """
        return self.description
    
    def passes_date_filter(self, body: Dict, date_threshold: datetime) -> bool:
        """Check if body passes the date filter (not updated after threshold).
        
        Args:
            body: Body data dictionary
            date_threshold: Cutoff date for filtering
            
        Returns:
            True if body passes filter, False otherwise
        """
        update_time = body.get('updateTime')
        if update_time:
            try:
                # Parse various date formats that might be present
                if isinstance(update_time, str):
                    # Handle ISO format with timezone: "2021-05-18 22:11:16+00:00"
                    if '+' in update_time:
                        update_time = update_time.split('+')[0]
                    elif 'T' in update_time:
                        update_time = update_time.split('T')[0] + ' ' + update_time.split('T')[1].split('+')[0].split('Z')[0]
                    
                    # Parse the datetime
                    try:
                        update_date = datetime.fromisoformat(update_time.replace('Z', ''))
                    except:
                        # Try alternative parsing
                        update_date = datetime.strptime(update_time[:19], '%Y-%m-%d %H:%M:%S')
                    
                    # Exclude if updated after threshold
                    if update_date > date_threshold:
                        return False
                        
            except (ValueError, TypeError):
                pass
        
        return True
    
    def has_suitable_atmosphere(self, body: Dict, min_pressure: float = 0.0, max_pressure: float = 0.1) -> bool:
        """Check if body has an atmosphere suitable for biology.
        
        Args:
            body: Body data dictionary
            min_pressure: Minimum atmospheric pressure
            max_pressure: Maximum atmospheric pressure
            
        Returns:
            True if atmosphere is suitable, False otherwise
        """
        # Must have an atmosphere (not "No atmosphere")
        atmosphere_type = body.get('atmosphereType', '')
        if atmosphere_type == 'No atmosphere':
            return False
        
        # Surface pressure must be within specified range
        surface_pressure = body.get('surfacePressure', 0)
        if surface_pressure <= min_pressure or surface_pressure > max_pressure:
            return False
        
        return True
    
    def extract_system_coordinates(self, system_data: Dict) -> Tuple[float, float, float]:
        """Extract system coordinates from system data.
        
        Args:
            system_data: System data dictionary
            
        Returns:
            Tuple of (x, y, z) coordinates
        """
        coords = system_data.get('coords', {})
        return (
            coords.get('x', 0),
            coords.get('y', 0), 
            coords.get('z', 0)
        )
    
    def get_system_name(self, system_data: Dict) -> str:
        """Extract system name from system data.
        
        Args:
            system_data: System data dictionary
            
        Returns:
            System name string
        """
        return system_data.get('name', 'Unknown')
    
    def filter_bodies_by_criteria(self, bodies: List[Dict], criteria_func: Callable) -> List[Dict]:
        """Filter bodies using a criteria function.
        
        Args:
            bodies: List of body data dictionaries
            criteria_func: Function that takes a body dict and returns bool
            
        Returns:
            List of bodies that pass the criteria
        """
        return [body for body in bodies if criteria_func(body)]
    
    def group_species_by_genus(self, predicted_species: List[Dict]) -> Dict[str, List[Dict]]:
        """Group predicted species by genus.
        
        Args:
            predicted_species: List of predicted species data
            
        Returns:
            Dictionary mapping genus names to species lists
        """
        genus_species = {}
        
        for species in predicted_species:
            genus = species.get('genus', 'Unknown')
            value = species.get('value', 0)
            species_name = species.get('species', 'Unknown')
            
            if genus not in genus_species:
                genus_species[genus] = []
            
            species_info = {
                'species': species_name,
                'value': value,
                'full_name': species.get('name', f"{genus} {species_name}")
            }
            
            genus_species[genus].append(species_info)
        
        return genus_species