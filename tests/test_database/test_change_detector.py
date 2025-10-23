"""Tests for change detection functionality."""

import pytest
from mgst.database.change_detector import ChangeDetector


class TestChangeDetector:
    """Test ChangeDetector functionality."""
    
    def test_detect_new_system(self):
        """Test detection of newly discovered system."""
        detector = ChangeDetector()
        
        new_system = {
            'id64': 123456789,
            'name': 'New System',
            'coords': {'x': 100.0, 'y': 200.0, 'z': 300.0},
            'population': 1000000
        }
        
        result = detector.detect_system_changes(None, new_system)
        
        assert result['has_changes'] is True
        assert 'system_discovered' in result['change_types']
        assert result['significant_changes']['discovered'] is True
    
    def test_no_changes(self):
        """Test when no changes are detected."""
        detector = ChangeDetector()
        
        system = {
            'id64': 123456789,
            'name': 'Test System',
            'allegiance': 'Federation',
            'population': 1000000,
            'factions': [
                {'name': 'Test Faction', 'influence': 0.5, 'state': 'None'}
            ]
        }
        
        result = detector.detect_system_changes(system, system)
        
        assert result['has_changes'] is False
        assert len(result['change_types']) == 0
    
    def test_faction_influence_change(self):
        """Test detection of faction influence changes."""
        detector = ChangeDetector(influence_threshold=0.001)  # 0.1% threshold
        
        old_system = {
            'id64': 123456789,
            'factions': [
                {'name': 'Test Faction', 'influence': 0.5, 'state': 'None'}
            ]
        }
        
        new_system = {
            'id64': 123456789,
            'factions': [
                {'name': 'Test Faction', 'influence': 0.52, 'state': 'None'}  # 2% change
            ]
        }
        
        result = detector.detect_system_changes(old_system, new_system)
        
        assert result['has_changes'] is True
        assert 'faction_influence' in result['change_types']
        
        changes = result['significant_changes']['factions']
        assert 'influence_changes' in changes
        assert 'Test Faction' in changes['influence_changes']
        assert changes['influence_changes']['Test Faction']['change'] == 0.02
    
    def test_faction_state_change(self):
        """Test detection of faction state changes."""
        detector = ChangeDetector()
        
        old_system = {
            'id64': 123456789,
            'factions': [
                {'name': 'Test Faction', 'influence': 0.5, 'state': 'None'}
            ]
        }
        
        new_system = {
            'id64': 123456789,
            'factions': [
                {'name': 'Test Faction', 'influence': 0.5, 'state': 'War'}
            ]
        }
        
        result = detector.detect_system_changes(old_system, new_system)
        
        assert result['has_changes'] is True
        assert 'faction_influence' in result['change_types']
        
        changes = result['significant_changes']['factions']
        assert 'state_changes' in changes
        assert 'Test Faction' in changes['state_changes']
        assert changes['state_changes']['Test Faction']['old'] == 'None'
        assert changes['state_changes']['Test Faction']['new'] == 'War'
    
    def test_new_faction_appears(self):
        """Test detection of new faction appearing in system."""
        detector = ChangeDetector()
        
        old_system = {
            'id64': 123456789,
            'factions': [
                {'name': 'Old Faction', 'influence': 0.6}
            ]
        }
        
        new_system = {
            'id64': 123456789,
            'factions': [
                {'name': 'Old Faction', 'influence': 0.5},
                {'name': 'New Faction', 'influence': 0.1}
            ]
        }
        
        result = detector.detect_system_changes(old_system, new_system)
        
        assert result['has_changes'] is True
        changes = result['significant_changes']['factions']
        assert 'new_factions' in changes
        assert 'New Faction' in changes['new_factions']
    
    def test_powerplay_changes(self):
        """Test detection of powerplay changes."""
        detector = ChangeDetector()
        
        old_system = {
            'id64': 123456789,
            'powerState': 'Contested',
            'powers': ['Edmund Mahon']
        }
        
        new_system = {
            'id64': 123456789,
            'powerState': 'Controlled',
            'powers': ['Edmund Mahon', 'Felicia Winters']
        }
        
        result = detector.detect_system_changes(old_system, new_system)
        
        assert result['has_changes'] is True
        assert 'powerplay' in result['change_types']
        
        changes = result['significant_changes']['powerplay']
        assert changes['power_state']['old'] == 'Contested'
        assert changes['power_state']['new'] == 'Controlled'
        assert 'Felicia Winters' in changes['powers']['added']
    
    def test_station_changes(self):
        """Test detection of station additions/removals."""
        detector = ChangeDetector()
        
        old_system = {
            'id64': 123456789,
            'stations': [
                {'id': 111, 'name': 'Old Station'}
            ]
        }
        
        new_system = {
            'id64': 123456789,
            'stations': [
                {'id': 111, 'name': 'Old Station'},
                {'id': 222, 'name': 'New Station'}
            ]
        }
        
        result = detector.detect_system_changes(old_system, new_system)
        
        assert result['has_changes'] is True
        assert 'stations' in result['change_types']
        
        changes = result['significant_changes']['stations']
        assert 'new_stations' in changes
        assert len(changes['new_stations']) == 1
        assert changes['new_stations'][0]['id'] == 222
    
    def test_station_service_changes(self):
        """Test detection of station service changes."""
        detector = ChangeDetector()
        
        old_station = {
            'id': 111,
            'services': ['Market', 'Shipyard']
        }
        
        new_station = {
            'id': 111,
            'services': ['Market', 'Shipyard', 'Outfitting']
        }
        
        result = detector.detect_station_changes(old_station, new_station)
        
        assert result['has_changes'] is True
        assert 'services' in result['change_types']
        
        changes = result['significant_changes']['services']
        assert 'Outfitting' in changes['added']
        assert len(changes['removed']) == 0
    
    def test_station_faction_control_change(self):
        """Test detection of station faction control changes."""
        detector = ChangeDetector()
        
        old_station = {
            'id': 111,
            'controllingFaction': 'Old Faction',
            'controllingFactionState': 'None'
        }
        
        new_station = {
            'id': 111,
            'controllingFaction': 'New Faction', 
            'controllingFactionState': 'Expansion'
        }
        
        result = detector.detect_station_changes(old_station, new_station)
        
        assert result['has_changes'] is True
        assert 'faction_control' in result['change_types']
        
        changes = result['significant_changes']['faction_control']
        assert changes['controlling_faction']['old'] == 'Old Faction'
        assert changes['controlling_faction']['new'] == 'New Faction'
        assert changes['controlling_faction_state']['new'] == 'Expansion'
    
    def test_hash_caching(self):
        """Test that hash caching improves performance."""
        detector = ChangeDetector()
        
        system = {
            'id64': 123456789,
            'name': 'Test System',
            'allegiance': 'Federation'
        }
        
        # First call should calculate hash
        result1 = detector.detect_system_changes(system, system)
        
        # Second call should use cached hash
        result2 = detector.detect_system_changes(system, system)
        
        assert result1['has_changes'] is False
        assert result2['has_changes'] is False
        
        # Verify system hash is cached
        assert 123456789 in detector._system_hash_cache
    
    def test_influence_threshold_configuration(self):
        """Test that influence threshold can be configured."""
        # Strict threshold
        strict_detector = ChangeDetector(influence_threshold=0.01)  # 1%
        
        # Loose threshold  
        loose_detector = ChangeDetector(influence_threshold=0.0001)  # 0.01%
        
        old_system = {'factions': [{'name': 'Test', 'influence': 0.5}]}
        new_system = {'factions': [{'name': 'Test', 'influence': 0.505}]}  # 0.5% change
        
        strict_result = strict_detector.detect_system_changes(old_system, new_system)
        loose_result = loose_detector.detect_system_changes(old_system, new_system)
        
        # Strict detector should not detect small change
        assert strict_result['has_changes'] is False
        
        # Loose detector should detect small change
        assert loose_result['has_changes'] is True