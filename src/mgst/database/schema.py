"""Database schema definitions for time-series galaxy data."""

import json
import gzip
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional, List, Union
from pathlib import Path


@dataclass
class TimeSeriesRecord:
    """Base class for time-series records."""
    timestamp: str
    change_type: str
    previous_state: Optional[Dict[str, Any]] = None
    current_state: Optional[Dict[str, Any]] = None
    delta: Optional[Dict[str, Any]] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimeSeriesRecord':
        """Create record from dictionary."""
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert record to JSON string."""
        return json.dumps(self.to_dict(), separators=(',', ':'))


@dataclass 
class SystemChangeRecord:
    """Time-series record for system-level changes."""
    id64: int
    name: str
    timestamp: str
    change_type: str
    coords: Optional[Dict[str, float]] = None
    previous_state: Optional[Dict[str, Any]] = None
    current_state: Optional[Dict[str, Any]] = None
    delta: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert record to JSON string."""
        return json.dumps(self.to_dict(), separators=(',', ':'))
    
    @classmethod
    def from_system_diff(cls, system_id64: int, system_name: str, 
                        old_system: Optional[Dict[str, Any]], 
                        new_system: Dict[str, Any],
                        timestamp: Optional[str] = None) -> 'SystemChangeRecord':
        """Create system change record from before/after system data."""
        if timestamp is None:
            timestamp = datetime.utcnow().isoformat() + 'Z'
            
        # Detect change types
        change_types = []
        
        if old_system is None:
            change_types.append("system_discovered")
        else:
            # Check faction changes
            if cls._factions_changed(old_system, new_system):
                change_types.append("faction_influence")
                
            # Check powerplay changes  
            if cls._powerplay_changed(old_system, new_system):
                change_types.append("powerplay")
                
            # Check economy changes
            if cls._economy_changed(old_system, new_system):
                change_types.append("economy")
                
            # Check station changes
            if cls._stations_changed(old_system, new_system):
                change_types.append("stations")
        
        change_type = "|".join(change_types) if change_types else "unknown"
        
        # Calculate delta
        delta = cls._calculate_delta(old_system, new_system) if old_system else None
        
        return cls(
            id64=system_id64,
            name=system_name,
            coords=new_system.get('coords'),
            timestamp=timestamp,
            change_type=change_type,
            previous_state=cls._extract_trackable_fields(old_system) if old_system else None,
            current_state=cls._extract_trackable_fields(new_system),
            delta=delta
        )
    
    @staticmethod
    def _extract_trackable_fields(system: Dict[str, Any]) -> Dict[str, Any]:
        """Extract only fields we want to track for changes."""
        trackable = {}
        
        # System-level faction/political data
        for field in ['allegiance', 'government', 'primaryEconomy', 'secondaryEconomy', 
                     'security', 'population', 'controllingFaction', 'powerState', 'powers']:
            if field in system:
                trackable[field] = system[field]
                
        # Faction influence data
        if 'factions' in system:
            trackable['factions'] = system['factions']
            
        # Station count and basic info
        if 'stations' in system:
            trackable['station_count'] = len(system['stations'])
            trackable['station_names'] = [s.get('name') for s in system['stations']]
            
        return trackable
    
    @staticmethod
    def _factions_changed(old_sys: Dict[str, Any], new_sys: Dict[str, Any]) -> bool:
        """Check if faction data has changed significantly."""
        old_factions = old_sys.get('factions', [])
        new_factions = new_sys.get('factions', [])
        
        if len(old_factions) != len(new_factions):
            return True
            
        # Check faction influence changes > 0.1%
        old_faction_map = {f['name']: f for f in old_factions}
        new_faction_map = {f['name']: f for f in new_factions}
        
        for faction_name in new_faction_map:
            if faction_name not in old_faction_map:
                return True
                
            old_influence = old_faction_map[faction_name].get('influence', 0)
            new_influence = new_faction_map[faction_name].get('influence', 0)
            
            if abs(old_influence - new_influence) > 0.001:  # 0.1% threshold
                return True
                
            # Check state changes
            if (old_faction_map[faction_name].get('state') != 
                new_faction_map[faction_name].get('state')):
                return True
                
        return False
    
    @staticmethod
    def _powerplay_changed(old_sys: Dict[str, Any], new_sys: Dict[str, Any]) -> bool:
        """Check if powerplay data has changed."""
        return (old_sys.get('powerState') != new_sys.get('powerState') or
                old_sys.get('powers') != new_sys.get('powers'))
    
    @staticmethod 
    def _economy_changed(old_sys: Dict[str, Any], new_sys: Dict[str, Any]) -> bool:
        """Check if economic data has changed."""
        return (old_sys.get('primaryEconomy') != new_sys.get('primaryEconomy') or
                old_sys.get('secondaryEconomy') != new_sys.get('secondaryEconomy'))
    
    @staticmethod
    def _stations_changed(old_sys: Dict[str, Any], new_sys: Dict[str, Any]) -> bool:
        """Check if station data has changed."""
        old_stations = old_sys.get('stations', [])
        new_stations = new_sys.get('stations', [])
        
        if len(old_stations) != len(new_stations):
            return True
            
        old_station_names = {s.get('name') for s in old_stations}
        new_station_names = {s.get('name') for s in new_stations}
        
        return old_station_names != new_station_names
    
    @staticmethod
    def _calculate_delta(old_sys: Dict[str, Any], new_sys: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate meaningful deltas between system states."""
        delta = {}
        
        # Population change
        old_pop = old_sys.get('population', 0)
        new_pop = new_sys.get('population', 0)
        if old_pop != new_pop:
            delta['population_change'] = new_pop - old_pop
            
        # Faction influence changes
        old_factions = {f['name']: f for f in old_sys.get('factions', [])}
        new_factions = {f['name']: f for f in new_sys.get('factions', [])}
        
        faction_changes = {}
        for faction_name in set(old_factions.keys()) | set(new_factions.keys()):
            old_influence = old_factions.get(faction_name, {}).get('influence', 0)
            new_influence = new_factions.get(faction_name, {}).get('influence', 0)
            
            if abs(old_influence - new_influence) > 0.001:
                faction_changes[faction_name] = {
                    'influence_change': new_influence - old_influence,
                    'old_influence': old_influence,
                    'new_influence': new_influence
                }
                
        if faction_changes:
            delta['faction_influence_changes'] = faction_changes
            
        return delta if delta else None


@dataclass
class StationChangeRecord:
    """Time-series record for station-level changes."""
    station_id: int
    system_id64: int
    station_name: str
    system_name: str
    timestamp: str
    change_type: str
    previous_state: Optional[Dict[str, Any]] = None
    current_state: Optional[Dict[str, Any]] = None
    delta: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert record to JSON string."""
        return json.dumps(self.to_dict(), separators=(',', ':'))
    
    @classmethod
    def from_station_diff(cls, station_id: int, system_id64: int,
                         station_name: str, system_name: str,
                         old_station: Optional[Dict[str, Any]],
                         new_station: Dict[str, Any],
                         timestamp: Optional[str] = None) -> 'StationChangeRecord':
        """Create station change record from before/after station data."""
        if timestamp is None:
            timestamp = datetime.utcnow().isoformat() + 'Z'
            
        # Detect change types
        change_types = []
        
        if old_station is None:
            change_types.append("station_discovered")
        else:
            # Check market changes
            if cls._market_changed(old_station, new_station):
                change_types.append("market")
                
            # Check shipyard changes
            if cls._shipyard_changed(old_station, new_station):
                change_types.append("shipyard")
                
            # Check outfitting changes
            if cls._outfitting_changed(old_station, new_station):
                change_types.append("outfitting")
                
            # Check faction control changes
            if cls._faction_control_changed(old_station, new_station):
                change_types.append("faction_control")
                
            # Check services changes
            if cls._services_changed(old_station, new_station):
                change_types.append("services")
        
        change_type = "|".join(change_types) if change_types else "unknown"
        
        # Calculate delta
        delta = cls._calculate_delta(old_station, new_station) if old_station else None
        
        return cls(
            station_id=station_id,
            system_id64=system_id64,
            station_name=station_name,
            system_name=system_name,
            timestamp=timestamp,
            change_type=change_type,
            previous_state=cls._extract_trackable_fields(old_station) if old_station else None,
            current_state=cls._extract_trackable_fields(new_station),
            delta=delta
        )
    
    @staticmethod
    def _extract_trackable_fields(station: Dict[str, Any]) -> Dict[str, Any]:
        """Extract only fields we want to track for changes."""
        trackable = {}
        
        # Basic station info
        for field in ['name', 'type', 'controllingFaction', 'controllingFactionState',
                     'allegiance', 'government', 'primaryEconomy', 'economies', 'services']:
            if field in station:
                trackable[field] = station[field]
                
        # Market summary
        if 'market' in station and 'commodities' in station['market']:
            commodities = station['market']['commodities']
            trackable['commodity_count'] = len(commodities)
            
        # Shipyard summary  
        if 'shipyard' in station and 'ships' in station['shipyard']:
            ships = station['shipyard']['ships']
            trackable['ship_count'] = len(ships)
            
        # Outfitting summary
        if 'outfitting' in station and 'modules' in station['outfitting']:
            modules = station['outfitting']['modules']
            trackable['module_count'] = len(modules)
            
        return trackable
    
    @staticmethod
    def _market_changed(old_station: Dict[str, Any], new_station: Dict[str, Any]) -> bool:
        """Check if market data has changed significantly."""
        old_market = old_station.get('market', {})
        new_market = new_station.get('market', {})
        
        old_commodities = old_market.get('commodities', [])
        new_commodities = new_market.get('commodities', [])
        
        return len(old_commodities) != len(new_commodities)
    
    @staticmethod
    def _shipyard_changed(old_station: Dict[str, Any], new_station: Dict[str, Any]) -> bool:
        """Check if shipyard data has changed."""
        old_shipyard = old_station.get('shipyard', {})
        new_shipyard = new_station.get('shipyard', {})
        
        old_ships = old_shipyard.get('ships', [])
        new_ships = new_shipyard.get('ships', [])
        
        return len(old_ships) != len(new_ships)
    
    @staticmethod
    def _outfitting_changed(old_station: Dict[str, Any], new_station: Dict[str, Any]) -> bool:
        """Check if outfitting data has changed."""
        old_outfitting = old_station.get('outfitting', {})
        new_outfitting = new_station.get('outfitting', {})
        
        old_modules = old_outfitting.get('modules', [])
        new_modules = new_outfitting.get('modules', [])
        
        return len(old_modules) != len(new_modules)
    
    @staticmethod
    def _faction_control_changed(old_station: Dict[str, Any], new_station: Dict[str, Any]) -> bool:
        """Check if faction control has changed."""
        return (old_station.get('controllingFaction') != new_station.get('controllingFaction') or
                old_station.get('controllingFactionState') != new_station.get('controllingFactionState'))
    
    @staticmethod
    def _services_changed(old_station: Dict[str, Any], new_station: Dict[str, Any]) -> bool:
        """Check if services have changed."""
        old_services = set(old_station.get('services', []))
        new_services = set(new_station.get('services', []))
        return old_services != new_services
    
    @staticmethod
    def _calculate_delta(old_station: Dict[str, Any], new_station: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate meaningful deltas between station states."""
        delta = {}
        
        # Service changes
        old_services = set(old_station.get('services', []))
        new_services = set(new_station.get('services', []))
        
        if old_services != new_services:
            delta['services_added'] = list(new_services - old_services)
            delta['services_removed'] = list(old_services - new_services)
            
        # Market size changes
        old_market_size = len(old_station.get('market', {}).get('commodities', []))
        new_market_size = len(new_station.get('market', {}).get('commodities', []))
        
        if old_market_size != new_market_size:
            delta['market_size_change'] = new_market_size - old_market_size
            
        return delta if delta else None


class TimeSeriesWriter:
    """High-performance writer for time-series records."""
    
    def __init__(self, base_path: Path):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
    def write_system_changes(self, records: List[SystemChangeRecord], 
                           partition: str) -> None:
        """Write system change records to monthly partition."""
        partition_dir = self.base_path / "systems" / partition
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        # Group by sector for efficient storage
        sector_records = {}
        for record in records:
            if record.coords:
                sector = self._get_sector_name(record.coords)
                if sector not in sector_records:
                    sector_records[sector] = []
                sector_records[sector].append(record)
        
        # Write to compressed sector files
        for sector, sector_records_list in sector_records.items():
            sector_file = partition_dir / f"{sector}_changes.jsonl.gz"
            
            with gzip.open(sector_file, 'at', encoding='utf-8') as f:
                for record in sector_records_list:
                    f.write(record.to_json() + '\n')
    
    def write_station_changes(self, records: List[StationChangeRecord],
                            partition: str) -> None:
        """Write station change records to monthly partition."""
        partition_dir = self.base_path / "stations" / partition
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        # For now, write all station changes to a single file per partition
        # Could be optimized by system location in future
        station_file = partition_dir / "station_changes.jsonl.gz"
        
        with gzip.open(station_file, 'at', encoding='utf-8') as f:
            for record in records:
                f.write(record.to_json() + '\n')
    
    @staticmethod
    def _get_sector_name(coords: Dict[str, float]) -> str:
        """Convert coordinates to sector name."""
        # Use 1000 LY sectors for organization
        sector_x = int(coords['x'] // 1000)
        sector_y = int(coords['y'] // 1000)  
        sector_z = int(coords['z'] // 1000)
        return f"sector_{sector_x}_{sector_y}_{sector_z}"