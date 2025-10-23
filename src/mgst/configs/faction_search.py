"""Faction Search Configuration

This configuration finds systems with at least one station containing a specific faction.
It supports multiple output formats and can search for any faction presence.

Initial Implementation:
- Searches for "Dukes of Mikunn" faction by default
- Checks both controllingFaction and factions array at system level
- Provides three output options: full JSONL, TSV summary, or both
- Includes system coordinates and faction influence data

Architecture supports extensible faction searching:
- Configurable target faction name
- Multiple output format options
- Detailed faction information extraction
- Station-level faction analysis
"""

from typing import Dict, Any, List, Optional
from .base import BaseConfig


class FactionSearchConfig(BaseConfig):
    """Faction Search Configuration"""

    def __init__(self, target_faction: str = "The Dukes of Mikunn", output_format: str = "both"):
        self.target_faction = target_faction
        self.output_format = output_format  # "jsonl", "tsv", or "both"

        super().__init__(
            name="faction-search",
            description=(
                f"Faction Search Configuration\\n\\n"
                f"Finds systems with at least one station containing the '{target_faction}' faction.\\n"
                f"Searches both controllingFaction and factions array at system level.\\n\\n"
                f"Output Format: {output_format}\\n"
                f"- 'jsonl': Full system data in JSONL format\\n"
                f"- 'tsv': System coordinates and faction summary in TSV format\\n"
                f"- 'both': Both JSONL and TSV outputs\\n\\n"
                f"Features:\\n"
                f"- Detects faction presence in controllingFaction field\\n"
                f"- Detects faction presence in factions array\\n"
                f"- Extracts faction influence and state information\\n"
                f"- Supports configurable target faction\\n\\n"
                f"Output includes system coordinates, faction details, and station information."
            )
        )

    def has_target_faction(self, system_data: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        """Check if system has the target faction and return faction details."""
        faction_info = {
            'is_controlling': False,
            'influence': 0.0,
            'state': '',
            'allegiance': '',
            'government': '',
            'total_factions': 0
        }

        # Check controllingFaction
        controlling_faction = system_data.get('controllingFaction', {})
        if controlling_faction.get('name') == self.target_faction:
            faction_info.update({
                'is_controlling': True,
                'influence': 1.0,  # Controlling faction has 100% control
                'allegiance': controlling_faction.get('allegiance', ''),
                'government': controlling_faction.get('government', '')
            })
            return True, faction_info

        # Check factions array
        factions = system_data.get('factions', [])
        faction_info['total_factions'] = len(factions)

        for faction in factions:
            if faction.get('name') == self.target_faction:
                faction_info.update({
                    'is_controlling': False,
                    'influence': faction.get('influence', 0.0),
                    'state': faction.get('state', ''),
                    'allegiance': faction.get('allegiance', ''),
                    'government': faction.get('government', '')
                })
                return True, faction_info

        return False, faction_info

    def get_station_summary(self, system_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract station summary information."""
        stations = system_data.get('stations', [])

        station_summary = {
            'total_stations': len(stations),
            'starports': 0,
            'outposts': 0,
            'planetary': 0,
            'faction_controlled_stations': 0
        }

        for station in stations:
            station_type = station.get('type', '').lower()
            if 'coriolis' in station_type or 'orbis' in station_type or 'ocellus' in station_type:
                station_summary['starports'] += 1
            elif 'outpost' in station_type:
                station_summary['outposts'] += 1
            elif 'planetary' in station_type:
                station_summary['planetary'] += 1

            # Check if station is controlled by target faction
            station_controlling = station.get('controllingFaction', {})
            if station_controlling.get('name') == self.target_faction:
                station_summary['faction_controlled_stations'] += 1

        return station_summary

    def filter_system(self, system_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Filter systems containing the target faction."""
        # Check if system has the target faction
        has_faction, faction_info = self.has_target_faction(system_data)

        if not has_faction:
            return None

        # If output format is "jsonl", return full system data
        if self.output_format == "jsonl":
            return system_data

        # For TSV or both formats, create summary data
        coords = system_data.get('coords', {})
        station_summary = self.get_station_summary(system_data)

        result = {
            'system_name': system_data.get('name', ''),
            'coords_x': coords.get('x', 0),
            'coords_y': coords.get('y', 0),
            'coords_z': coords.get('z', 0),
            'faction_name': self.target_faction,
            'is_controlling_faction': faction_info['is_controlling'],
            'faction_influence': faction_info['influence'],
            'faction_state': faction_info['state'],
            'faction_allegiance': faction_info['allegiance'],
            'faction_government': faction_info['government'],
            'total_factions': faction_info['total_factions'],
            'total_stations': station_summary['total_stations'],
            'starports': station_summary['starports'],
            'outposts': station_summary['outposts'],
            'planetary_stations': station_summary['planetary'],
            'faction_controlled_stations': station_summary['faction_controlled_stations'],
            'population': system_data.get('population', 0),
            'security': system_data.get('security', ''),
            'economy': system_data.get('economy', ''),
            'government': system_data.get('government', ''),
            'allegiance': system_data.get('allegiance', '')
        }

        return result

    def get_output_columns(self) -> List[str]:
        """Define output columns for TSV format."""
        if self.output_format == "jsonl":
            # For JSONL format, return empty list as we output full JSON
            return []

        return [
            'system_name', 'coords_x', 'coords_y', 'coords_z',
            'faction_name', 'is_controlling_faction', 'faction_influence',
            'faction_state', 'faction_allegiance', 'faction_government',
            'total_factions', 'total_stations', 'starports', 'outposts',
            'planetary_stations', 'faction_controlled_stations',
            'population', 'security', 'economy', 'government', 'allegiance'
        ]