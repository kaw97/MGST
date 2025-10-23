"""
Test suite for spatial prefiltering enhancements.
Tests TSV parsing, performance optimizations, and CLI integration.
"""

import pytest
import tempfile
import json
from pathlib import Path
from io import StringIO

from mgst.core.spatial import SpatialRange, SpatialPrefilter, SectorIndex, calculate_distance


class TestEnhancedTSVParser:
    """Test enhanced TSV parser with flexible column detection."""
    
    def test_csv_format_detection(self):
        """Test auto-detection of CSV vs TSV format."""
        # Create test CSV file
        csv_content = "system_name,x,y,z\nSol,0,0,0\nAlpha Centauri,1.34,-0.78,0.12"
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(csv_content)
            f.flush()
            
            spatial_range = SpatialRange.from_tsv(f.name, 50.0)
            
            assert len(spatial_range.target_coords) == 2
            assert spatial_range.target_coords[0] == (0.0, 0.0, 0.0)
            assert spatial_range.target_coords[1] == (1.34, -0.78, 0.12)
            assert spatial_range.target_systems[0]['system_name'] == 'Sol'
            assert spatial_range.target_systems[1]['system_name'] == 'Alpha Centauri'
    
    def test_fuzzy_column_matching(self):
        """Test fuzzy column name matching."""
        # Test various column name variations
        variations = [
            "System Name\tGalactic X\tGalactic Y\tGalactic Z\n",
            "star_system\tpos_x\tpos_y\tpos_z\n", 
            "name\tcoord_x\tcoord_y\tcoord_z\n",
            "systemname\tx_ly\ty_ly\tz_ly\n"
        ]
        
        for i, header in enumerate(variations):
            test_content = header + f"TestSys{i}\t100\t200\t300"
            
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv') as f:
                f.write(test_content)
                f.flush()
                
                spatial_range = SpatialRange.from_tsv(f.name, 50.0)
                
                assert len(spatial_range.target_coords) == 1
                assert spatial_range.target_coords[0] == (100.0, 200.0, 300.0)
                if 'name' in header.lower():
                    assert spatial_range.target_systems[0]['system_name'] == f'TestSys{i}'
    
    def test_invalid_coordinate_handling(self):
        """Test handling of invalid coordinates."""
        # Mix of valid and invalid data
        test_content = (
            "system_name\tx\ty\tz\n"
            "ValidSys1\t10.5\t20.5\t30.5\n"
            "InvalidSys\tabc\tdef\tghi\n"  # Invalid coordinates
            "ValidSys2\t40.5\t50.5\t60.5\n"
            "EmptySys\t\t\t\n"  # Empty coordinates
        )
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv') as f:
            f.write(test_content)
            f.flush()
            
            spatial_range = SpatialRange.from_tsv(f.name, 50.0)
            
            # Should only have 2 valid systems
            assert len(spatial_range.target_coords) == 2
            assert spatial_range.target_coords[0] == (10.5, 20.5, 30.5)
            assert spatial_range.target_coords[1] == (40.5, 50.5, 60.5)


class TestSpatialPrefilterEnhancements:
    """Test enhanced spatial prefiltering functionality."""
    
    @pytest.fixture
    def mock_sector_index(self, tmp_path):
        """Create mock sector index for testing."""
        # Create test sectors
        sectors_data = {
            "metadata": {
                "total_systems": 1000,
                "total_sectors": 5
            },
            "sectors": {
                "TestSector1": {
                    "filename": "TestSector1.jsonl",
                    "system_count": 250,
                    "center_coords": {"x": 0.0, "y": 0.0, "z": 0.0}
                },
                "TestSector2": {
                    "filename": "TestSector2.jsonl", 
                    "system_count": 300,
                    "center_coords": {"x": 100.0, "y": 0.0, "z": 0.0}
                },
                "TestSector3": {
                    "filename": "TestSector3.jsonl",
                    "system_count": 200,
                    "center_coords": {"x": 0.0, "y": 100.0, "z": 0.0}
                },
                "EmptySector": {
                    "filename": "EmptySector.jsonl",
                    "system_count": 5,  # Below minimum threshold
                    "center_coords": {"x": 1000.0, "y": 1000.0, "z": 1000.0}
                },
                "FarSector": {
                    "filename": "FarSector.jsonl", 
                    "system_count": 245,
                    "center_coords": {"x": 10000.0, "y": 10000.0, "z": 10000.0}  # Very far away
                }
            }
        }
        
        # Write sector index
        index_path = tmp_path / "sector_index.json"
        with open(index_path, 'w') as f:
            json.dump(sectors_data, f)
        
        # Create mock sector files
        for sector_name, sector_data in sectors_data["sectors"].items():
            sector_file = tmp_path / sector_data["filename"]
            # Create minimal JSONL content
            with open(sector_file, 'w') as f:
                for i in range(min(sector_data["system_count"], 3)):  # Just write a few entries
                    system = {
                        "name": f"{sector_name}_System_{i}",
                        "coords": sector_data["center_coords"]
                    }
                    f.write(json.dumps(system) + '\n')
        
        return tmp_path
    
    @pytest.fixture 
    def sample_targets_tsv(self, tmp_path):
        """Create sample targets TSV file."""
        targets_content = (
            "system_name\tx\ty\tz\n"
            "Origin\t0\t0\t0\n"
            "Nearby1\t50\t50\t50\n"
        )
        
        targets_file = tmp_path / "targets.tsv"
        with open(targets_file, 'w') as f:
            f.write(targets_content)
        
        return targets_file
    
    def test_optimized_sector_filtering(self, mock_sector_index, sample_targets_tsv):
        """Test optimized sector filtering with minimum system thresholds."""
        prefilter = SpatialPrefilter(
            sector_db_path=str(mock_sector_index),
            target_tsv_path=str(sample_targets_tsv),
            range_ly=150.0,  # Should include first 3 sectors but not far ones
            enable_system_filtering=True,
            min_sector_systems=10  # Should exclude EmptySector
        )
        
        stats = prefilter.get_stats()
        
        # Should include TestSector1, TestSector2, TestSector3 (all within range and above minimum)
        assert stats['filtered_sectors'] == 3
        assert stats['excluded_empty_sectors'] == 1  # EmptySector
        
        # Far sector should be excluded by distance
        assert 'FarSector' not in prefilter.sectors_in_range
        assert 'EmptySector' not in prefilter.sectors_in_range
    
    def test_system_level_filtering(self, mock_sector_index, sample_targets_tsv):
        """Test individual system distance filtering."""
        prefilter = SpatialPrefilter(
            sector_db_path=str(mock_sector_index),
            target_tsv_path=str(sample_targets_tsv),
            range_ly=75.0,
            enable_system_filtering=True,
            min_sector_systems=1
        )
        
        # Test system within range
        close_system = {
            "name": "CloseSystem",
            "coords": {"x": 10.0, "y": 10.0, "z": 10.0}  # Close to origin
        }
        assert prefilter.should_process_system(close_system) == True
        
        # Test system outside range
        far_system = {
            "name": "FarSystem", 
            "coords": {"x": 1000.0, "y": 1000.0, "z": 1000.0}  # Very far
        }
        assert prefilter.should_process_system(far_system) == False
        
        # Test system without coordinates (should be processed)
        no_coords_system = {"name": "NoCoords"}
        assert prefilter.should_process_system(no_coords_system) == True
    
    def test_distance_calculation_performance(self, mock_sector_index, sample_targets_tsv):
        """Test that squared distance optimization is working."""
        prefilter = SpatialPrefilter(
            sector_db_path=str(mock_sector_index),
            target_tsv_path=str(sample_targets_tsv), 
            range_ly=100.0,
            enable_system_filtering=True
        )
        
        # Should use squared distance internally for performance
        # We can verify this by checking that the distance calculation works correctly
        distance = prefilter.get_closest_target_distance({
            "coords": {"x": 0, "y": 0, "z": 100}  # Should be 100 ly from origin
        })
        
        assert abs(distance - 100.0) < 0.001  # Should be exactly 100 ly
    
    def test_enhanced_statistics(self, mock_sector_index, sample_targets_tsv):
        """Test enhanced statistics collection."""
        prefilter = SpatialPrefilter(
            sector_db_path=str(mock_sector_index),
            target_tsv_path=str(sample_targets_tsv),
            range_ly=200.0,
            enable_system_filtering=True,
            min_sector_systems=10
        )
        
        stats = prefilter.get_stats()
        
        # Check all expected statistics are present
        required_fields = [
            'target_systems_count', 'range_ly', 'total_sectors', 'filtered_sectors',
            'excluded_empty_sectors', 'sector_reduction', 'total_systems', 
            'filtered_systems', 'system_reduction', 'min_sector_systems',
            'enable_system_filtering', 'target_distances'
        ]
        
        for field in required_fields:
            assert field in stats
        
        # Verify values make sense
        assert stats['target_systems_count'] == 2  # Origin + Nearby1
        assert stats['range_ly'] == 200.0
        assert stats['enable_system_filtering'] == True
        assert stats['min_sector_systems'] == 10


class TestDistanceCalculations:
    """Test distance calculation utilities."""
    
    def test_euclidean_distance(self):
        """Test 3D Euclidean distance calculation."""
        # Test simple distances
        assert calculate_distance((0, 0, 0), (3, 4, 0)) == 5.0  # 3-4-5 triangle
        assert calculate_distance((0, 0, 0), (0, 0, 10)) == 10.0  # Simple z-axis
        assert calculate_distance((1, 1, 1), (1, 1, 1)) == 0.0  # Same point
    
    def test_distance_symmetry(self):
        """Test that distance calculation is symmetric."""
        coord1 = (10.5, 20.3, 30.7)
        coord2 = (-5.2, 15.8, -12.1)
        
        dist1 = calculate_distance(coord1, coord2)
        dist2 = calculate_distance(coord2, coord1)
        
        assert abs(dist1 - dist2) < 0.001  # Should be identical


if __name__ == "__main__":
    pytest.main([__file__])