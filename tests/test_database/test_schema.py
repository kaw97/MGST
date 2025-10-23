"""Tests for database schema classes."""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile
import gzip
import json

from mgst.database.schema import (
    SystemChangeRecord, StationChangeRecord, TimeSeriesWriter
)


class TestSystemChangeRecord:
    """Test SystemChangeRecord functionality."""
    
    def test_create_from_new_system(self):
        """Test creating change record for newly discovered system."""
        new_system = {
            'id64': 123456789,
            'name': 'Test System',
            'coords': {'x': 100.0, 'y': 200.0, 'z': 300.0},
            'population': 1000000,
            'factions': [
                {'name': 'Test Faction', 'influence': 0.5, 'state': 'None'}
            ]
        }
        
        record = SystemChangeRecord.from_system_diff(
            123456789, 'Test System', None, new_system
        )
        
        assert record.id64 == 123456789
        assert record.name == 'Test System'
        assert 'system_discovered' in record.change_type
        assert record.previous_state is None
        assert record.current_state is not None
        assert record.delta is None
    
    def test_create_from_faction_change(self):
        """Test creating change record for faction influence change."""
        old_system = {
            'id64': 123456789,
            'name': 'Test System',
            'coords': {'x': 100.0, 'y': 200.0, 'z': 300.0},
            'factions': [
                {'name': 'Test Faction', 'influence': 0.5, 'state': 'None'}
            ]
        }
        
        new_system = {
            'id64': 123456789,
            'name': 'Test System',
            'coords': {'x': 100.0, 'y': 200.0, 'z': 300.0},
            'factions': [
                {'name': 'Test Faction', 'influence': 0.6, 'state': 'Boom'}
            ]
        }
        
        record = SystemChangeRecord.from_system_diff(
            123456789, 'Test System', old_system, new_system
        )
        
        assert record.id64 == 123456789
        assert 'faction_influence' in record.change_type
        assert record.delta is not None
        assert 'faction_influence_changes' in record.delta
    
    def test_factions_changed_threshold(self):
        """Test faction change detection with influence threshold."""
        old_system = {'factions': [{'name': 'Test', 'influence': 0.5}]}
        new_system = {'factions': [{'name': 'Test', 'influence': 0.5005}]}  # 0.05% change
        
        # Small change should not be detected
        assert not SystemChangeRecord._factions_changed(old_system, new_system)
        
        new_system = {'factions': [{'name': 'Test', 'influence': 0.502}]}  # 0.2% change
        
        # Larger change should be detected
        assert SystemChangeRecord._factions_changed(old_system, new_system)
    
    def test_to_json(self):
        """Test JSON serialization."""
        record = SystemChangeRecord(
            id64=123456789,
            name='Test System',
            timestamp='2025-01-01T00:00:00Z',
            change_type='test'
        )
        
        json_str = record.to_json()
        data = json.loads(json_str)
        
        assert data['id64'] == 123456789
        assert data['name'] == 'Test System'
        assert data['timestamp'] == '2025-01-01T00:00:00Z'


class TestStationChangeRecord:
    """Test StationChangeRecord functionality."""
    
    def test_create_from_new_station(self):
        """Test creating change record for newly discovered station."""
        new_station = {
            'id': 987654321,
            'name': 'Test Station',
            'type': 'Coriolis Starport',
            'controllingFaction': 'Test Faction',
            'services': ['Market', 'Shipyard']
        }
        
        record = StationChangeRecord.from_station_diff(
            987654321, 123456789, 'Test Station', 'Test System',
            None, new_station
        )
        
        assert record.station_id == 987654321
        assert record.system_id64 == 123456789
        assert record.station_name == 'Test Station'
        assert 'station_discovered' in record.change_type
    
    def test_services_changed(self):
        """Test service change detection."""
        old_station = {'services': ['Market', 'Shipyard']}
        new_station = {'services': ['Market', 'Shipyard', 'Outfitting']}
        
        assert StationChangeRecord._services_changed(old_station, new_station)
    
    def test_faction_control_changed(self):
        """Test faction control change detection."""
        old_station = {'controllingFaction': 'Old Faction'}
        new_station = {'controllingFaction': 'New Faction'}
        
        assert StationChangeRecord._faction_control_changed(old_station, new_station)


class TestTimeSeriesWriter:
    """Test TimeSeriesWriter functionality."""
    
    def test_write_system_changes(self):
        """Test writing system change records."""
        with tempfile.TemporaryDirectory() as temp_dir:
            base_path = Path(temp_dir)
            writer = TimeSeriesWriter(base_path)
            
            # Create test records
            records = [
                SystemChangeRecord(
                    id64=123456789,
                    name='Test System 1',
                    coords={'x': 1000.0, 'y': 2000.0, 'z': 3000.0},
                    timestamp='2025-01-01T00:00:00Z',
                    change_type='test'
                ),
                SystemChangeRecord(
                    id64=987654321,
                    name='Test System 2',
                    coords={'x': 1500.0, 'y': 2500.0, 'z': 3500.0},
                    timestamp='2025-01-01T01:00:00Z',
                    change_type='test'
                )
            ]
            
            # Write records
            writer.write_system_changes(records, '202501')
            
            # Verify files were created
            partition_dir = base_path / "systems" / "202501"
            assert partition_dir.exists()
            
            # Check that sector files were created
            sector_files = list(partition_dir.glob("*_changes.jsonl.gz"))
            assert len(sector_files) > 0
            
            # Verify content
            for sector_file in sector_files:
                with gzip.open(sector_file, 'rt', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            record_data = json.loads(line)
                            assert 'id64' in record_data
                            assert 'name' in record_data
                            assert 'timestamp' in record_data
    
    def test_write_station_changes(self):
        """Test writing station change records."""
        with tempfile.TemporaryDirectory() as temp_dir:
            base_path = Path(temp_dir)
            writer = TimeSeriesWriter(base_path)
            
            # Create test records
            records = [
                StationChangeRecord(
                    station_id=111111111,
                    system_id64=123456789,
                    station_name='Test Station',
                    system_name='Test System',
                    timestamp='2025-01-01T00:00:00Z',
                    change_type='test'
                )
            ]
            
            # Write records
            writer.write_station_changes(records, '202501')
            
            # Verify files were created
            partition_dir = base_path / "stations" / "202501"
            assert partition_dir.exists()
            
            station_file = partition_dir / "station_changes.jsonl.gz"
            assert station_file.exists()
            
            # Verify content
            with gzip.open(station_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        record_data = json.loads(line)
                        assert 'station_id' in record_data
                        assert 'system_id64' in record_data
    
    def test_get_sector_name(self):
        """Test sector name generation."""
        coords = {'x': 1234.5, 'y': -567.8, 'z': 890.1}
        sector_name = TimeSeriesWriter._get_sector_name(coords)
        
        # 1234.5 // 1000 = 1, -567.8 // 1000 = -1, 890.1 // 1000 = 0
        assert sector_name == "sector_1_-1_0"