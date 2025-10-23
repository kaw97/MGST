"""Efficient change detection for galaxy database updates."""

import json
import hashlib
from typing import Dict, Any, Optional, Set, List, Tuple
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class ChangeDetector:
    """Efficiently detects changes between system and station data."""
    
    def __init__(self, influence_threshold: float = 0.001, 
                 price_threshold: float = 0.05):
        """Initialize change detector.
        
        Args:
            influence_threshold: Minimum faction influence change to track (0.1%)
            price_threshold: Minimum price change to track (5%)
        """
        self.influence_threshold = influence_threshold
        self.price_threshold = price_threshold
        
        # Cache for system hashes to speed up change detection
        self._system_hash_cache: Dict[int, str] = {}
        self._station_hash_cache: Dict[int, str] = {}
        
    def detect_system_changes(self, old_system: Optional[Dict[str, Any]], 
                            new_system: Dict[str, Any]) -> Dict[str, Any]:
        """Detect meaningful changes in system data.
        
        Args:
            old_system: Previous system state (None if new)
            new_system: Current system state
            
        Returns:
            Change detection results
        """
        result = {
            'has_changes': False,
            'change_types': set(),
            'significant_changes': {}
        }
        
        system_id = new_system.get('id64')
        if not system_id:
            return result
            
        # If system is new, everything is a change
        if old_system is None:
            result['has_changes'] = True
            result['change_types'].add('system_discovered')
            result['significant_changes']['discovered'] = True
            return result
            
        # Quick hash check first
        old_hash = self._calculate_system_hash(old_system)
        new_hash = self._calculate_system_hash(new_system)
        
        if old_hash == new_hash:
            return result  # No changes
            
        result['has_changes'] = True
        
        # Detailed change detection
        changes = {}
        
        # Check faction changes
        faction_changes = self._detect_faction_changes(old_system, new_system)
        if faction_changes:
            result['change_types'].add('faction_influence')
            changes['factions'] = faction_changes
            
        # Check powerplay changes
        powerplay_changes = self._detect_powerplay_changes(old_system, new_system)
        if powerplay_changes:
            result['change_types'].add('powerplay')
            changes['powerplay'] = powerplay_changes
            
        # Check economy changes  
        economy_changes = self._detect_economy_changes(old_system, new_system)
        if economy_changes:
            result['change_types'].add('economy')
            changes['economy'] = economy_changes
            
        # Check station changes
        station_changes = self._detect_system_station_changes(old_system, new_system)
        if station_changes:
            result['change_types'].add('stations')
            changes['stations'] = station_changes
            
        # Check population changes
        population_changes = self._detect_population_changes(old_system, new_system)
        if population_changes:
            result['change_types'].add('population')
            changes['population'] = population_changes
            
        result['significant_changes'] = changes
        
        # Cache the new hash
        self._system_hash_cache[system_id] = new_hash
        
        return result
    
    def detect_station_changes(self, old_station: Optional[Dict[str, Any]],
                             new_station: Dict[str, Any]) -> Dict[str, Any]:
        """Detect meaningful changes in station data.
        
        Args:
            old_station: Previous station state (None if new)  
            new_station: Current station state
            
        Returns:
            Change detection results
        """
        result = {
            'has_changes': False,
            'change_types': set(),
            'significant_changes': {}
        }
        
        station_id = new_station.get('id')
        if not station_id:
            return result
            
        # If station is new, everything is a change
        if old_station is None:
            result['has_changes'] = True
            result['change_types'].add('station_discovered')
            result['significant_changes']['discovered'] = True
            return result
            
        # Quick hash check first
        old_hash = self._calculate_station_hash(old_station)
        new_hash = self._calculate_station_hash(new_station)
        
        if old_hash == new_hash:
            return result  # No changes
            
        result['has_changes'] = True
        
        # Detailed change detection
        changes = {}
        
        # Check faction control changes
        faction_changes = self._detect_station_faction_changes(old_station, new_station)
        if faction_changes:
            result['change_types'].add('faction_control')
            changes['faction_control'] = faction_changes
            
        # Check service changes
        service_changes = self._detect_service_changes(old_station, new_station)
        if service_changes:
            result['change_types'].add('services')
            changes['services'] = service_changes
            
        # Check market changes (high-level)
        market_changes = self._detect_market_changes(old_station, new_station)
        if market_changes:
            result['change_types'].add('market')
            changes['market'] = market_changes
            
        # Check shipyard changes
        shipyard_changes = self._detect_shipyard_changes(old_station, new_station)
        if shipyard_changes:
            result['change_types'].add('shipyard')
            changes['shipyard'] = shipyard_changes
            
        # Check outfitting changes
        outfitting_changes = self._detect_outfitting_changes(old_station, new_station)
        if outfitting_changes:
            result['change_types'].add('outfitting')
            changes['outfitting'] = outfitting_changes
            
        result['significant_changes'] = changes
        
        # Cache the new hash
        self._station_hash_cache[station_id] = new_hash
        
        return result
    
    def _calculate_system_hash(self, system: Dict[str, Any]) -> str:
        """Calculate hash of trackable system fields."""
        # Only hash fields we care about for change detection
        trackable_data = {}
        
        for field in ['allegiance', 'government', 'primaryEconomy', 'secondaryEconomy',
                     'security', 'population', 'controllingFaction', 'powerState', 'powers']:
            if field in system:
                trackable_data[field] = system[field]
                
        # Hash faction data (influences and states)
        if 'factions' in system:
            factions_data = []
            for faction in system['factions']:
                faction_data = {
                    'name': faction.get('name'),
                    'influence': round(faction.get('influence', 0), 4),  # Round to avoid floating point issues
                    'state': faction.get('state'),
                    'government': faction.get('government'),
                    'allegiance': faction.get('allegiance')
                }
                factions_data.append(faction_data)
            trackable_data['factions'] = sorted(factions_data, key=lambda x: x['name'])
            
        # Hash basic station info (not detailed market data)
        if 'stations' in system:
            stations_data = []
            for station in system['stations']:
                station_data = {
                    'name': station.get('name'),
                    'id': station.get('id'),
                    'type': station.get('type'),
                    'controllingFaction': station.get('controllingFaction'),
                    'services': sorted(station.get('services', []))
                }
                stations_data.append(station_data)
            trackable_data['stations'] = sorted(stations_data, key=lambda x: x.get('id', 0))
        
        # Create deterministic hash
        json_str = json.dumps(trackable_data, sort_keys=True, separators=(',', ':'))
        return hashlib.md5(json_str.encode('utf-8')).hexdigest()
    
    def _calculate_station_hash(self, station: Dict[str, Any]) -> str:
        """Calculate hash of trackable station fields."""
        trackable_data = {}
        
        # Basic station info
        for field in ['name', 'type', 'controllingFaction', 'controllingFactionState',
                     'allegiance', 'government', 'primaryEconomy']:
            if field in station:
                trackable_data[field] = station[field]
                
        # Services
        if 'services' in station:
            trackable_data['services'] = sorted(station['services'])
            
        # Market summary (count and categories, not detailed prices)
        if 'market' in station and 'commodities' in station['market']:
            commodities = station['market']['commodities']
            trackable_data['market_commodity_count'] = len(commodities)
            
            # Track categories of commodities, not prices
            categories = set()
            for commodity in commodities:
                if 'category' in commodity:
                    categories.add(commodity['category'])
            trackable_data['market_categories'] = sorted(list(categories))
            
        # Shipyard summary
        if 'shipyard' in station and 'ships' in station['shipyard']:
            ships = station['shipyard']['ships']
            trackable_data['shipyard_count'] = len(ships)
            ship_names = [ship.get('name') for ship in ships if 'name' in ship]
            trackable_data['shipyard_ships'] = sorted(ship_names)
            
        # Outfitting summary
        if 'outfitting' in station and 'modules' in station['outfitting']:
            modules = station['outfitting']['modules']
            trackable_data['outfitting_count'] = len(modules)
        
        # Create deterministic hash
        json_str = json.dumps(trackable_data, sort_keys=True, separators=(',', ':'))
        return hashlib.md5(json_str.encode('utf-8')).hexdigest()
    
    def _detect_faction_changes(self, old_sys: Dict[str, Any], 
                              new_sys: Dict[str, Any]) -> Dict[str, Any]:
        """Detect faction influence and state changes."""
        changes = {}
        
        old_factions = {f['name']: f for f in old_sys.get('factions', [])}
        new_factions = {f['name']: f for f in new_sys.get('factions', [])}
        
        # New factions
        new_faction_names = set(new_factions.keys()) - set(old_factions.keys())
        if new_faction_names:
            changes['new_factions'] = list(new_faction_names)
            
        # Removed factions
        removed_faction_names = set(old_factions.keys()) - set(new_factions.keys())
        if removed_faction_names:
            changes['removed_factions'] = list(removed_faction_names)
            
        # Influence changes
        influence_changes = {}
        state_changes = {}
        
        for faction_name in set(old_factions.keys()) & set(new_factions.keys()):
            old_faction = old_factions[faction_name]
            new_faction = new_factions[faction_name]
            
            # Check influence changes
            old_influence = old_faction.get('influence', 0)
            new_influence = new_faction.get('influence', 0)
            
            if abs(old_influence - new_influence) > self.influence_threshold:
                influence_changes[faction_name] = {
                    'old': old_influence,
                    'new': new_influence,
                    'change': new_influence - old_influence
                }
                
            # Check state changes
            old_state = old_faction.get('state')
            new_state = new_faction.get('state')
            
            if old_state != new_state:
                state_changes[faction_name] = {
                    'old': old_state,
                    'new': new_state
                }
                
        if influence_changes:
            changes['influence_changes'] = influence_changes
        if state_changes:
            changes['state_changes'] = state_changes
            
        return changes if changes else None
    
    def _detect_powerplay_changes(self, old_sys: Dict[str, Any], 
                                new_sys: Dict[str, Any]) -> Dict[str, Any]:
        """Detect powerplay changes."""
        changes = {}
        
        old_power_state = old_sys.get('powerState')
        new_power_state = new_sys.get('powerState')
        
        if old_power_state != new_power_state:
            changes['power_state'] = {
                'old': old_power_state,
                'new': new_power_state
            }
            
        old_powers = set(old_sys.get('powers', []))
        new_powers = set(new_sys.get('powers', []))
        
        if old_powers != new_powers:
            changes['powers'] = {
                'added': list(new_powers - old_powers),
                'removed': list(old_powers - new_powers)
            }
            
        return changes if changes else None
    
    def _detect_economy_changes(self, old_sys: Dict[str, Any], 
                              new_sys: Dict[str, Any]) -> Dict[str, Any]:
        """Detect economy changes."""
        changes = {}
        
        for field in ['primaryEconomy', 'secondaryEconomy']:
            old_value = old_sys.get(field)
            new_value = new_sys.get(field)
            
            if old_value != new_value:
                changes[field] = {
                    'old': old_value,
                    'new': new_value
                }
                
        return changes if changes else None
    
    def _detect_system_station_changes(self, old_sys: Dict[str, Any], 
                                     new_sys: Dict[str, Any]) -> Dict[str, Any]:
        """Detect station addition/removal in system."""
        changes = {}
        
        old_stations = {s.get('id'): s.get('name') for s in old_sys.get('stations', [])}
        new_stations = {s.get('id'): s.get('name') for s in new_sys.get('stations', [])}
        
        # New stations
        new_station_ids = set(new_stations.keys()) - set(old_stations.keys())
        if new_station_ids:
            changes['new_stations'] = [{'id': sid, 'name': new_stations[sid]} 
                                     for sid in new_station_ids]
            
        # Removed stations
        removed_station_ids = set(old_stations.keys()) - set(new_stations.keys())
        if removed_station_ids:
            changes['removed_stations'] = [{'id': sid, 'name': old_stations[sid]} 
                                         for sid in removed_station_ids]
            
        return changes if changes else None
    
    def _detect_population_changes(self, old_sys: Dict[str, Any], 
                                 new_sys: Dict[str, Any]) -> Dict[str, Any]:
        """Detect population changes."""
        old_pop = old_sys.get('population', 0)
        new_pop = new_sys.get('population', 0)
        
        if old_pop != new_pop:
            return {
                'old': old_pop,
                'new': new_pop,
                'change': new_pop - old_pop
            }
            
        return None
    
    def _detect_station_faction_changes(self, old_station: Dict[str, Any],
                                      new_station: Dict[str, Any]) -> Dict[str, Any]:
        """Detect station faction control changes."""
        changes = {}
        
        old_faction = old_station.get('controllingFaction')
        new_faction = new_station.get('controllingFaction')
        
        if old_faction != new_faction:
            changes['controlling_faction'] = {
                'old': old_faction,
                'new': new_faction
            }
            
        old_state = old_station.get('controllingFactionState')
        new_state = new_station.get('controllingFactionState')
        
        if old_state != new_state:
            changes['controlling_faction_state'] = {
                'old': old_state,
                'new': new_state
            }
            
        return changes if changes else None
    
    def _detect_service_changes(self, old_station: Dict[str, Any],
                              new_station: Dict[str, Any]) -> Dict[str, Any]:
        """Detect station service changes."""
        old_services = set(old_station.get('services', []))
        new_services = set(new_station.get('services', []))
        
        if old_services != new_services:
            return {
                'added': list(new_services - old_services),
                'removed': list(old_services - new_services)
            }
            
        return None
    
    def _detect_market_changes(self, old_station: Dict[str, Any],
                             new_station: Dict[str, Any]) -> Dict[str, Any]:
        """Detect high-level market changes."""
        changes = {}
        
        old_market = old_station.get('market', {})
        new_market = new_station.get('market', {})
        
        old_commodities = old_market.get('commodities', [])
        new_commodities = new_market.get('commodities', [])
        
        old_count = len(old_commodities)
        new_count = len(new_commodities)
        
        if old_count != new_count:
            changes['commodity_count_change'] = new_count - old_count
            
        return changes if changes else None
    
    def _detect_shipyard_changes(self, old_station: Dict[str, Any],
                               new_station: Dict[str, Any]) -> Dict[str, Any]:
        """Detect shipyard changes."""
        changes = {}
        
        old_shipyard = old_station.get('shipyard', {})
        new_shipyard = new_station.get('shipyard', {})
        
        old_ships = old_shipyard.get('ships', [])
        new_ships = new_shipyard.get('ships', [])
        
        old_ship_names = {s.get('name') for s in old_ships}
        new_ship_names = {s.get('name') for s in new_ships}
        
        if old_ship_names != new_ship_names:
            changes['ship_availability'] = {
                'added': list(new_ship_names - old_ship_names),
                'removed': list(old_ship_names - new_ship_names)
            }
            
        return changes if changes else None
    
    def _detect_outfitting_changes(self, old_station: Dict[str, Any],
                                 new_station: Dict[str, Any]) -> Dict[str, Any]:
        """Detect outfitting changes."""
        changes = {}
        
        old_outfitting = old_station.get('outfitting', {})
        new_outfitting = new_station.get('outfitting', {})
        
        old_modules = old_outfitting.get('modules', [])
        new_modules = new_outfitting.get('modules', [])
        
        old_count = len(old_modules)
        new_count = len(new_modules)
        
        if old_count != new_count:
            changes['module_count_change'] = new_count - old_count
            
        return changes if changes else None