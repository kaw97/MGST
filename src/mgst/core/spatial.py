"""
Spatial prefiltering system for galaxy data processing.
Enables filtering based on sector proximity to target coordinates.
"""
import json
import math
import csv
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional, Any
from dataclasses import dataclass


@dataclass
class SpatialRange:
    """Configuration for spatial range filtering."""
    range_ly: float
    target_coords: List[Tuple[float, float, float]]
    target_systems: List[Dict[str, Any]]  # Store full system info including names
    
    @classmethod
    def from_tsv(cls, tsv_path: str, range_ly: float) -> 'SpatialRange':
        """Load target coordinates from TSV file with enhanced column detection."""
        target_coords = []
        target_systems = []
        
        # Enhanced column name mapping for different Elite Dangerous export formats
        coordinate_mappings = {
            'x': ['x', 'coord_x', 'x_coord', 'pos_x', 'position_x', 'x_ly', 'galactic_x'],
            'y': ['y', 'coord_y', 'y_coord', 'pos_y', 'position_y', 'y_ly', 'galactic_y'], 
            'z': ['z', 'coord_z', 'z_coord', 'pos_z', 'position_z', 'z_ly', 'galactic_z']
        }
        
        system_name_mappings = ['system_name', 'name', 'system', 'star_system', 'systemname', 'starname']
        
        # Auto-detect CSV vs TSV format by checking first line
        with open(tsv_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            delimiter = '\t' if '\t' in first_line else ','
            
        with open(tsv_path, 'r', encoding='utf-8') as f:
            # Try to detect encoding issues
            try:
                content = f.read()
                f.seek(0)
            except UnicodeDecodeError:
                # Try with different encodings
                f.close()
                encodings = ['utf-8', 'latin1', 'cp1252']
                for encoding in encodings:
                    try:
                        with open(tsv_path, 'r', encoding=encoding) as f:
                            content = f.read()
                            f.seek(0)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise ValueError(f"Could not decode TSV file with any common encoding: {tsv_path}")
            
            reader = csv.DictReader(f, delimiter=delimiter)
            headers = [col.lower().strip() for col in reader.fieldnames] if reader.fieldnames else []
            
            # Find coordinate columns using fuzzy matching
            x_col = cls._find_column_match(reader.fieldnames, coordinate_mappings['x'])
            y_col = cls._find_column_match(reader.fieldnames, coordinate_mappings['y'])
            z_col = cls._find_column_match(reader.fieldnames, coordinate_mappings['z'])
            name_col = cls._find_column_match(reader.fieldnames, system_name_mappings)
            
            
            if not (x_col and y_col and z_col):
                available_cols = ', '.join(reader.fieldnames) if reader.fieldnames else 'none'
                raise ValueError(
                    f"Could not find coordinate columns in TSV file: {tsv_path}\n"
                    f"Available columns: {available_cols}\n"
                    f"Expected coordinate columns matching: {coordinate_mappings}"
                )
            
            print(f"Detected coordinate columns: X='{x_col}', Y='{y_col}', Z='{z_col}'")
            if name_col:
                print(f"Detected system name column: '{name_col}'")
            
            line_count = 0
            valid_coords = 0
            
            for row in reader:
                line_count += 1
                try:
                    x = float(row[x_col])
                    y = float(row[y_col]) 
                    z = float(row[z_col])
                    
                    target_coords.append((x, y, z))
                    
                    # Store full system info
                    system_info = {
                        'coordinates': (x, y, z),
                        'x': x, 'y': y, 'z': z,
                        'line_number': line_count
                    }
                    
                    if name_col and row.get(name_col):
                        system_info['system_name'] = row[name_col].strip()
                    
                    # Store all original row data for debugging
                    system_info['original_row'] = dict(row)
                    
                    target_systems.append(system_info)
                    valid_coords += 1
                    
                except (ValueError, KeyError, TypeError) as e:
                    # Track invalid entries for debugging
                    print(f"Warning: Skipping invalid coordinate on line {line_count}: {e}")
                    continue
        
        if not target_coords:
            raise ValueError(f"No valid coordinates found in TSV file: {tsv_path}")
        
        print(f"Loaded {valid_coords} target systems from {line_count} lines in TSV file")
        
        return cls(range_ly=range_ly, target_coords=target_coords, target_systems=target_systems)
    
    @staticmethod
    def _find_column_match(available_columns: List[str], possible_names: List[str]) -> Optional[str]:
        """Find matching column name using fuzzy matching."""
        if not available_columns:
            return None
            
        available_lower = {col.lower().strip(): col for col in available_columns}
        
        # First try exact matches
        for name in possible_names:
            if name.lower() in available_lower:
                return available_lower[name.lower()]
        
        # Then try partial matches - but be more careful about specificity
        for name in possible_names:
            # Find all columns that contain this name
            matches = []
            for available_key, original_col in available_lower.items():
                # For coordinate columns, prioritize columns that end with the coordinate letter
                if name in ['x', 'y', 'z']:
                    # Look for columns ending with the coordinate (e.g., "galactic_x", "coord_x")
                    if available_key.endswith('_' + name) or available_key.endswith(' ' + name) or available_key == name:
                        matches.append((original_col, 10))  # High priority for exact suffix match
                    elif name in available_key:
                        matches.append((original_col, 5))  # Lower priority for contains
                else:
                    # For non-coordinate columns, use contains matching
                    if name.lower() in available_key or available_key in name.lower():
                        matches.append((original_col, 1))
            
            # Return the highest priority match
            if matches:
                matches.sort(key=lambda x: x[1], reverse=True)
                return matches[0][0]
        
        return None


class SectorIndex:
    """Index of galaxy sectors with spatial coordinates."""
    
    def __init__(self, index_path: str):
        """Load sector index from JSON file."""
        self.index_path = Path(index_path)
        self.sectors = {}
        self.metadata = {}
        
        if self.index_path.exists():
            with open(self.index_path, 'r') as f:
                data = json.load(f)
                self.metadata = data.get('metadata', {})
                self.sectors = data.get('sectors', {})
        else:
            raise FileNotFoundError(f"Sector index not found: {index_path}")
    
    def get_sector_center(self, sector_name: str) -> Optional[Tuple[float, float, float]]:
        """Get center coordinates for a sector."""
        sector_data = self.sectors.get(sector_name)
        if sector_data and 'center_coords' in sector_data:
            coords = sector_data['center_coords']
            return (coords['x'], coords['y'], coords['z'])
        return None
    
    def get_all_sectors(self) -> List[str]:
        """Get list of all sector names."""
        return list(self.sectors.keys())
    
    def get_sector_filename(self, sector_name: str) -> Optional[str]:
        """Get JSONL filename for a sector."""
        sector_data = self.sectors.get(sector_name)
        return sector_data.get('filename') if sector_data else None


def calculate_distance(coord1: Tuple[float, float, float], 
                      coord2: Tuple[float, float, float]) -> float:
    """Calculate 3D Euclidean distance between two coordinates."""
    return math.sqrt(
        (coord1[0] - coord2[0])**2 + 
        (coord1[1] - coord2[1])**2 + 
        (coord1[2] - coord2[2])**2
    )


def find_sectors_in_range(sector_index: SectorIndex, 
                         spatial_range: SpatialRange) -> Set[str]:
    """
    Find all sectors whose centers are within range of any target coordinate.
    
    Args:
        sector_index: Index of galaxy sectors
        spatial_range: Range specification with targets and distance
        
    Returns:
        Set of sector names within range
    """
    sectors_in_range = set()
    
    for sector_name in sector_index.get_all_sectors():
        sector_center = sector_index.get_sector_center(sector_name)
        if not sector_center:
            continue
        
        # Check if sector is within range of any target coordinate
        for target_coord in spatial_range.target_coords:
            distance = calculate_distance(sector_center, target_coord)
            if distance <= spatial_range.range_ly:
                sectors_in_range.add(sector_name)
                break  # No need to check other targets for this sector
    
    return sectors_in_range


def get_sector_files_for_filtering(sectors_dir: str, 
                                  sector_names: Set[str], 
                                  sector_index: SectorIndex) -> List[str]:
    """
    Get list of sector JSONL files to process for filtering.
    Supports both compressed (.jsonl.gz) and uncompressed (.jsonl) files.
    
    Args:
        sectors_dir: Directory containing sector JSONL files
        sector_names: Set of sector names to include
        sector_index: Sector index for filename lookup
        
    Returns:
        List of absolute paths to sector files that exist
    """
    sectors_path = Path(sectors_dir)
    sector_files = []
    
    for sector_name in sector_names:
        filename = sector_index.get_sector_filename(sector_name)
        if filename:
            # First try compressed version
            compressed_file_path = sectors_path / (filename + '.gz')
            if compressed_file_path.exists():
                sector_files.append(str(compressed_file_path))
                continue
            
            # Fall back to uncompressed version
            uncompressed_file_path = sectors_path / filename
            if uncompressed_file_path.exists():
                sector_files.append(str(uncompressed_file_path))
    
    return sector_files


def calculate_spatial_statistics(sector_index: SectorIndex) -> Dict[str, Any]:
    """
    Calculate statistics about sector distribution for default range suggestions.
    
    Returns:
        Dictionary with spatial statistics including suggested default ranges
    """
    centers = []
    for sector_name in sector_index.get_all_sectors():
        center = sector_index.get_sector_center(sector_name)
        if center:
            centers.append(center)
    
    if not centers:
        return {"error": "No sector centers found"}
    
    # Calculate distances between all sector pairs
    distances = []
    for i, center1 in enumerate(centers):
        for center2 in centers[i+1:]:
            distance = calculate_distance(center1, center2)
            distances.append(distance)
    
    if not distances:
        return {"error": "Could not calculate inter-sector distances"}
    
    distances.sort()
    
    # Calculate statistics
    stats = {
        "total_sectors": len(centers),
        "min_distance": min(distances),
        "max_distance": max(distances),
        "median_distance": distances[len(distances) // 2],
        "avg_distance": sum(distances) / len(distances),
        "percentile_25": distances[int(len(distances) * 0.25)],
        "percentile_75": distances[int(len(distances) * 0.75)],
    }
    
    # Suggest default ranges based on statistics
    stats["suggested_ranges"] = {
        "tight": round(stats["percentile_25"] / 2, 0),  # Half of 25th percentile
        "normal": round(stats["median_distance"] / 2, 0),  # Half of median
        "wide": round(stats["percentile_75"], 0),  # 75th percentile
        "very_wide": round(stats["avg_distance"], 0),  # Average distance
    }
    
    return stats


class SpatialPrefilter:
    """Main spatial prefiltering interface for galaxy filtering system."""
    
    def __init__(self, sector_db_path: str, target_tsv_path: str, 
                 range_ly: float, enable_system_filtering: bool = True,
                 min_sector_systems: int = 1):
        """Initialize spatial prefilter.
        
        Args:
            sector_db_path: Path to sector database directory
            target_tsv_path: Path to TSV file with target coordinates
            range_ly: Range in light years for filtering
            enable_system_filtering: Whether to pre-filter individual systems by distance
            min_sector_systems: Minimum systems required to include a sector
        """
        sector_index_path = Path(sector_db_path) / "sector_index.json"
        self.sector_index = SectorIndex(str(sector_index_path))
        self.sectors_dir = sector_db_path
        self.spatial_range = SpatialRange.from_tsv(target_tsv_path, range_ly)
        self.enable_system_filtering = enable_system_filtering
        self.min_sector_systems = min_sector_systems
        
        # Find sectors in range with optimizations
        self.sectors_in_range = self._find_optimized_sectors_in_range()
        
        # Get input files for these sectors
        self.input_files = get_sector_files_for_filtering(
            sector_db_path, self.sectors_in_range, self.sector_index
        )
        
        # Calculate statistics
        self._calculate_stats()
    
    def _find_optimized_sectors_in_range(self) -> Set[str]:
        """Find sectors in range with performance optimizations."""
        sectors_in_range = set()
        
        # Pre-calculate squared distances for performance
        range_squared = self.spatial_range.range_ly ** 2
        
        for sector_name in self.sector_index.get_all_sectors():
            sector_data = self.sector_index.sectors.get(sector_name, {})
            
            # Skip sectors with too few systems
            if sector_data.get('system_count', 0) < self.min_sector_systems:
                continue
                
            sector_center = self.sector_index.get_sector_center(sector_name)
            if not sector_center:
                continue
            
            # Check if sector is within range of any target coordinate
            # Use squared distance to avoid sqrt calculation
            for target_coord in self.spatial_range.target_coords:
                distance_squared = (
                    (sector_center[0] - target_coord[0])**2 + 
                    (sector_center[1] - target_coord[1])**2 + 
                    (sector_center[2] - target_coord[2])**2
                )
                
                if distance_squared <= range_squared:
                    sectors_in_range.add(sector_name)
                    break  # No need to check other targets for this sector
        
        return sectors_in_range
    
    def should_process_system(self, system_data: Dict[str, Any]) -> bool:
        """
        Check if individual system should be processed based on distance.
        Only used if enable_system_filtering is True.
        
        Args:
            system_data: System data dictionary with coordinates
            
        Returns:
            True if system is within range of any target
        """
        if not self.enable_system_filtering:
            return True
            
        coords = system_data.get('coords', {})
        if not coords:
            return True  # Process systems without coordinates
            
        try:
            system_coord = (
                float(coords.get('x', 0)),
                float(coords.get('y', 0)), 
                float(coords.get('z', 0))
            )
        except (ValueError, TypeError):
            return True  # Process systems with invalid coordinates
        
        # Check distance to any target system
        range_squared = self.spatial_range.range_ly ** 2
        
        for target_coord in self.spatial_range.target_coords:
            distance_squared = (
                (system_coord[0] - target_coord[0])**2 + 
                (system_coord[1] - target_coord[1])**2 + 
                (system_coord[2] - target_coord[2])**2
            )
            
            if distance_squared <= range_squared:
                return True
        
        return False
    
    def get_input_files(self) -> List[str]:
        """Get list of sector files to process."""
        return [Path(f) for f in self.input_files]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get prefiltering statistics."""
        return self._stats
    
    def get_closest_target_distance(self, system_data: Dict[str, Any]) -> Optional[float]:
        """Get distance to closest target system for a given system."""
        coords = system_data.get('coords', {})
        if not coords:
            return None
            
        try:
            system_coord = (
                float(coords.get('x', 0)),
                float(coords.get('y', 0)),
                float(coords.get('z', 0))
            )
        except (ValueError, TypeError):
            return None
        
        min_distance = float('inf')
        for target_coord in self.spatial_range.target_coords:
            distance = calculate_distance(system_coord, target_coord)
            min_distance = min(min_distance, distance)
        
        return min_distance if min_distance != float('inf') else None
    
    def _calculate_stats(self):
        """Calculate prefiltering statistics with enhanced metrics."""
        total_sectors = len(self.sector_index.get_all_sectors())
        filtered_sectors = len(self.sectors_in_range)
        
        # Calculate system counts
        total_systems = 0
        filtered_systems = 0
        excluded_empty_sectors = 0
        
        for sector_name in self.sector_index.get_all_sectors():
            sector_data = self.sector_index.sectors.get(sector_name, {})
            count = sector_data.get('system_count', 0)
            total_systems += count
            
            if count < self.min_sector_systems:
                excluded_empty_sectors += 1
            elif sector_name in self.sectors_in_range:
                filtered_systems += count
        
        # Calculate target system statistics
        target_distances = []
        if len(self.spatial_range.target_coords) > 1:
            for i, coord1 in enumerate(self.spatial_range.target_coords):
                for coord2 in self.spatial_range.target_coords[i+1:]:
                    distance = calculate_distance(coord1, coord2)
                    target_distances.append(distance)
        
        self._stats = {
            'target_systems_count': len(self.spatial_range.target_coords),
            'range_ly': self.spatial_range.range_ly,
            'total_sectors': total_sectors,
            'filtered_sectors': filtered_sectors,
            'excluded_empty_sectors': excluded_empty_sectors,
            'sector_reduction': (1 - filtered_sectors / total_sectors) * 100 if total_sectors > 0 else 0,
            'total_systems': total_systems,
            'filtered_systems': filtered_systems,
            'system_reduction': (1 - filtered_systems / total_systems) * 100 if total_systems > 0 else 0,
            'min_sector_systems': self.min_sector_systems,
            'enable_system_filtering': self.enable_system_filtering,
            'target_distances': {
                'min': min(target_distances) if target_distances else 0,
                'max': max(target_distances) if target_distances else 0,
                'avg': sum(target_distances) / len(target_distances) if target_distances else 0
            }
        }