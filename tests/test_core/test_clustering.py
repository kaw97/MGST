"""Tests for clustering functionality."""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from mgst.core.clustering import find_optimal_k, cluster_systems, process_cluster
from mgst.core.routing import nearest_neighbor_route


class TestClustering:
    """Test clustering algorithms."""
    
    def test_find_optimal_k_small_dataset(self, sample_systems_df):
        """Test optimal k finding with small dataset."""
        coords = sample_systems_df[['coords_x', 'coords_y', 'coords_z']].values
        
        # With only 4 systems, should return a small k
        k = find_optimal_k(coords, k_range=[2, 3], sample_size=100)
        assert k in [2, 3]
    
    def test_find_optimal_k_custom_range(self, sample_systems_df):
        """Test optimal k with custom range."""
        coords = sample_systems_df[['coords_x', 'coords_y', 'coords_z']].values
        
        k = find_optimal_k(coords, k_range=[2, 3, 4])
        assert 2 <= k <= 4
    
    def test_cluster_systems_basic(self, sample_systems_df):
        """Test basic system clustering."""
        cluster_labels, clustering_info = cluster_systems(sample_systems_df, k=2)
        
        assert len(cluster_labels) == len(sample_systems_df)
        assert clustering_info['n_clusters'] <= 2  # May be less if empty clusters
        assert clustering_info['optimal_k'] == 2
        assert 'cluster_sizes' in clustering_info
    
    def test_cluster_systems_auto_k(self, sample_systems_df):
        """Test clustering with automatic k determination."""
        cluster_labels, clustering_info = cluster_systems(sample_systems_df, k=None)
        
        assert len(cluster_labels) == len(sample_systems_df)
        assert clustering_info['n_clusters'] >= 1
        assert clustering_info['optimal_k'] > 0
    
    def test_process_cluster_empty(self, temp_dir):
        """Test processing empty cluster."""
        empty_df = pd.DataFrame(columns=['system_name', 'coords_x', 'coords_y', 'coords_z'])
        result = process_cluster(empty_df, 0, temp_dir)
        
        assert 'error' in result
        assert 'Empty cluster' in result['error']
    
    def test_process_cluster_single_system(self, temp_dir):
        """Test processing cluster with single system."""
        single_system = pd.DataFrame({
            'system_name': ['Test System'],
            'coords_x': [10.0],
            'coords_y': [20.0], 
            'coords_z': [30.0]
        })
        
        result = process_cluster(single_system, 0, temp_dir)
        
        assert 'error' not in result
        assert result['system_count'] == 1
        assert result['total_distance'] == 0
        assert result['representative_system'] == 'Test System'
        
        # Check file was created
        expected_file = temp_dir / result['filename']
        assert expected_file.exists()
    
    def test_process_cluster_multiple_systems(self, temp_dir, sample_systems_df):
        """Test processing cluster with multiple systems."""
        result = process_cluster(sample_systems_df, 0, temp_dir)
        
        assert 'error' not in result
        assert result['system_count'] == len(sample_systems_df)
        assert result['total_distance'] > 0
        assert result['avg_distance_per_jump'] > 0
        
        # Check file was created
        expected_file = temp_dir / result['filename']
        assert expected_file.exists()
        
        # Verify file content
        saved_df = pd.read_csv(expected_file, sep='\t')
        assert len(saved_df) == len(sample_systems_df)
        assert list(saved_df.columns) == list(sample_systems_df.columns)


class TestClusteringIntegration:
    """Integration tests for clustering pipeline."""
    
    def test_full_clustering_pipeline(self, sample_tsv_file, temp_dir):
        """Test the complete clustering pipeline."""
        from mgst.core.clustering import cluster_and_route_systems
        
        output_dir = temp_dir / "clusters"
        
        results = cluster_and_route_systems(
            input_file=sample_tsv_file,
            output_dir=output_dir,
            k=2,
            workers=1  # Use single worker for testing
        )
        
        assert 'clustering_info' in results
        assert 'cluster_summaries' in results
        assert output_dir.exists()
        
        # Check summary file was created
        summary_file = output_dir / "auto_cluster_summary.tsv"
        if 'summary_file' in results:
            assert summary_file.exists()
            summary_df = pd.read_csv(summary_file, sep='\t')
            assert len(summary_df) > 0