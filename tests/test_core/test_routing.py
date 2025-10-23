"""Tests for routing functionality."""

import pytest
import pandas as pd
import numpy as np

from mgst.core.routing import nearest_neighbor_route, calculate_route_metrics


class TestRouting:
    """Test routing algorithms."""
    
    def test_nearest_neighbor_empty_df(self):
        """Test routing with empty DataFrame."""
        empty_df = pd.DataFrame(columns=['system_name', 'coords_x', 'coords_y', 'coords_z'])
        result = nearest_neighbor_route(empty_df)
        
        assert len(result) == 0
        assert list(result.columns) == list(empty_df.columns)
    
    def test_nearest_neighbor_single_system(self):
        """Test routing with single system."""
        single_system = pd.DataFrame({
            'system_name': ['Test System'],
            'coords_x': [10.0],
            'coords_y': [20.0],
            'coords_z': [30.0]
        })
        
        result = nearest_neighbor_route(single_system)
        
        assert len(result) == 1
        assert result.iloc[0]['system_name'] == 'Test System'
    
    def test_nearest_neighbor_multiple_systems(self, sample_systems_df):
        """Test routing with multiple systems."""
        result = nearest_neighbor_route(sample_systems_df)
        
        # Should have same number of systems
        assert len(result) == len(sample_systems_df)
        
        # Should contain all original systems (possibly reordered)
        original_names = set(sample_systems_df['system_name'])
        result_names = set(result['system_name'])
        assert original_names == result_names
        
        # First system should be closest to origin (0,0,0)
        first_system = result.iloc[0]
        origin_distance = np.sqrt(first_system['coords_x']**2 + 
                                 first_system['coords_y']**2 + 
                                 first_system['coords_z']**2)
        
        # Sol should be first (it's at origin)
        assert first_system['system_name'] == 'Sol'
        assert origin_distance == 0.0
    
    def test_nearest_neighbor_missing_columns(self):
        """Test routing with missing coordinate columns."""
        bad_df = pd.DataFrame({
            'system_name': ['Test System'],
            'x': [10.0],  # Wrong column name
            'y': [20.0],  # Wrong column name
            'z': [30.0]   # Wrong column name
        })
        
        with pytest.raises(ValueError, match="must contain.*coords_x.*coords_y.*coords_z"):
            nearest_neighbor_route(bad_df)


class TestRouteMetrics:
    """Test route metrics calculation."""
    
    def test_calculate_route_metrics_empty(self):
        """Test metrics with empty DataFrame."""
        empty_df = pd.DataFrame(columns=['system_name', 'coords_x', 'coords_y', 'coords_z'])
        metrics = calculate_route_metrics(empty_df)
        
        assert metrics['system_count'] == 0
        assert metrics['total_distance'] == 0
        assert metrics['avg_distance_per_jump'] == 0
        assert metrics['route_efficiency'] == 0
    
    def test_calculate_route_metrics_single_system(self):
        """Test metrics with single system."""
        single_system = pd.DataFrame({
            'system_name': ['Test System'],
            'coords_x': [10.0],
            'coords_y': [20.0],
            'coords_z': [30.0]
        })
        
        metrics = calculate_route_metrics(single_system)
        
        assert metrics['system_count'] == 1
        assert metrics['total_distance'] == 0
        assert metrics['avg_distance_per_jump'] == 0
        assert metrics['route_efficiency'] == 1.0
    
    def test_calculate_route_metrics_two_systems(self):
        """Test metrics with two systems."""
        two_systems = pd.DataFrame({
            'system_name': ['System A', 'System B'],
            'coords_x': [0.0, 3.0],
            'coords_y': [0.0, 4.0],
            'coords_z': [0.0, 0.0]
        })
        
        metrics = calculate_route_metrics(two_systems)
        
        assert metrics['system_count'] == 2
        assert metrics['total_distance'] == 5.0  # 3-4-5 triangle
        assert metrics['avg_distance_per_jump'] == 5.0
        assert metrics['route_efficiency'] == 1.0  # Direct route
    
    def test_calculate_route_metrics_multiple_systems(self, sample_systems_df):
        """Test metrics with multiple systems."""
        # First route the systems
        routed_df = nearest_neighbor_route(sample_systems_df)
        metrics = calculate_route_metrics(routed_df)
        
        assert metrics['system_count'] == len(sample_systems_df)
        assert metrics['total_distance'] > 0
        assert metrics['avg_distance_per_jump'] > 0
        assert 0 < metrics['route_efficiency'] <= 1.0


class TestRoutingIntegration:
    """Integration tests for routing with clustering."""
    
    def test_routing_preserves_data(self, sample_systems_df):
        """Test that routing preserves all original data."""
        # Add some extra columns to test data preservation
        test_df = sample_systems_df.copy()
        test_df['extra_data'] = ['A', 'B', 'C', 'D']
        test_df['numeric_data'] = [1.1, 2.2, 3.3, 4.4]
        
        result = nearest_neighbor_route(test_df)
        
        # Should preserve all columns
        assert set(result.columns) == set(test_df.columns)
        
        # Should preserve all data (though reordered)
        assert set(result['extra_data']) == set(test_df['extra_data'])
        assert set(result['numeric_data']) == set(test_df['numeric_data'])