#!/usr/bin/env python3
"""
Calculate sector statistics and suggest default spatial prefiltering range.
"""

import json
import math
from pathlib import Path
import statistics
from collections import defaultdict

def calculate_distance_3d(coords1, coords2):
    """Calculate 3D Euclidean distance between two coordinate points."""
    x1, y1, z1 = coords1.get('x', 0), coords1.get('y', 0), coords1.get('z', 0)
    x2, y2, z2 = coords2.get('x', 0), coords2.get('y', 0), coords2.get('z', 0)
    
    return math.sqrt((x2-x1)**2 + (y2-y1)**2 + (z2-z1)**2)

def analyze_sector_statistics(sector_db_path: Path):
    """Analyze sector statistics from a sector database."""
    
    index_file = sector_db_path / "sector_index.json"
    if not index_file.exists():
        raise FileNotFoundError(f"Sector index not found: {index_file}")
    
    # Load sector index
    with open(index_file, 'r', encoding='utf-8') as f:
        sector_index = json.load(f)
    
    print(f"=== Sector Database Statistics ===")
    print(f"Database: {sector_db_path}")
    print(f"Total sectors: {sector_index['metadata']['total_sectors']:,}")
    print(f"Total systems: {sector_index['metadata']['total_systems']:,}")
    print()
    
    # Collect sector statistics
    sector_centers = []
    sector_sizes = []
    sector_radii = []
    
    for sector_name, sector_info in sector_index['sectors'].items():
        center_coords = sector_info.get('center_coords', {})
        if center_coords and all(k in center_coords for k in ['x', 'y', 'z']):
            sector_centers.append(center_coords)
        
        system_count = sector_info.get('system_count', 0)
        sector_sizes.append(system_count)
        
        # Calculate effective radius if we have radius info
        avg_radius = sector_info.get('avg_radius', 0)
        max_radius = sector_info.get('max_radius', 0)
        if max_radius > 0:
            sector_radii.append(max_radius)
    
    # Calculate sector size statistics
    print(f"=== Sector Size Distribution ===")
    print(f"Min systems per sector: {min(sector_sizes):,}")
    print(f"Max systems per sector: {max(sector_sizes):,}")
    print(f"Average systems per sector: {statistics.mean(sector_sizes):,.1f}")
    print(f"Median systems per sector: {statistics.median(sector_sizes):,.0f}")
    print(f"Standard deviation: {statistics.stdev(sector_sizes):,.1f}")
    print()
    
    # Calculate sector radius statistics if available
    if sector_radii:
        print(f"=== Sector Radius Distribution (Light Years) ===")
        print(f"Min sector radius: {min(sector_radii):.1f} ly")
        print(f"Max sector radius: {max(sector_radii):.1f} ly")
        print(f"Average sector radius: {statistics.mean(sector_radii):.1f} ly")
        print(f"Median sector radius: {statistics.median(sector_radii):.1f} ly")
        print(f"95th percentile: {statistics.quantiles(sector_radii, n=20)[-1]:.1f} ly")
        print()
    
    # Calculate inter-sector distances for nearby sectors
    print(f"=== Inter-Sector Distance Analysis ===")
    print("Calculating distances between sector centers...")
    
    distances_to_neighbors = []
    sample_size = min(1000, len(sector_centers))  # Sample to avoid O(n²) complexity
    
    for i, center1 in enumerate(sector_centers[:sample_size]):
        nearby_distances = []
        for j, center2 in enumerate(sector_centers):
            if i != j:
                distance = calculate_distance_3d(center1, center2)
                nearby_distances.append(distance)
        
        # Find closest neighbors
        nearby_distances.sort()
        if len(nearby_distances) >= 5:
            # Average distance to 5 closest neighbors
            avg_to_neighbors = statistics.mean(nearby_distances[:5])
            distances_to_neighbors.append(avg_to_neighbors)
    
    if distances_to_neighbors:
        print(f"Average distance to 5 nearest sectors: {statistics.mean(distances_to_neighbors):.1f} ly")
        print(f"Median distance to 5 nearest sectors: {statistics.median(distances_to_neighbors):.1f} ly")
        print(f"90th percentile distance: {statistics.quantiles(distances_to_neighbors, n=10)[-1]:.1f} ly")
        print()
        
        # Suggest prefiltering ranges
        print(f"=== Suggested Spatial Prefiltering Ranges ===")
        
        median_neighbor_distance = statistics.median(distances_to_neighbors)
        
        ranges = {
            "Conservative (same sector only)": statistics.median(sector_radii) if sector_radii else 100,
            "Local (adjacent sectors)": median_neighbor_distance * 1.5,
            "Regional (nearby sectors)": median_neighbor_distance * 3.0,
            "Extended (wide area)": median_neighbor_distance * 5.0
        }
        
        for description, range_ly in ranges.items():
            print(f"{description:.<35} {range_ly:>8.0f} ly")
        
        print()
        print(f"🎯 RECOMMENDED DEFAULT: {ranges['Regional (nearby sectors)']:,.0f} ly")
        print("   (Captures most adjacent sectors while maintaining good performance)")
        
        return ranges['Regional (nearby sectors)']
    else:
        print("Could not calculate inter-sector distances.")
        return 1000  # Default fallback

def main():
    """Analyze both test and full sector databases if available."""
    
    databases_to_check = [
        ("Test Sectors", Path("Databases/test_streaming_sectors")),
        ("Test Sectors (Alternative)", Path("Databases/test_sectors")),
        ("Full Galaxy by Sector", Path("Databases/galaxy_by_sector"))
    ]
    
    recommended_ranges = []
    
    for name, path in databases_to_check:
        if path.exists() and (path / "sector_index.json").exists():
            print(f"\n{'='*60}")
            print(f"Analyzing: {name}")
            print(f"{'='*60}")
            try:
                recommended_range = analyze_sector_statistics(path)
                recommended_ranges.append(recommended_range)
            except Exception as e:
                print(f"Error analyzing {name}: {e}")
        else:
            print(f"\nSkipping {name} - not found: {path}")
    
    if recommended_ranges:
        print(f"\n{'='*60}")
        print("FINAL RECOMMENDATIONS")
        print(f"{'='*60}")
        
        avg_recommended = statistics.mean(recommended_ranges)
        print(f"Average recommended range: {avg_recommended:,.0f} ly")
        print(f"Ranges from different databases: {[f'{r:,.0f}' for r in recommended_ranges]}")
        
        print(f"\n🚀 SUGGESTED COMMAND LINE DEFAULT: --spatial-range {avg_recommended:,.0f}")

if __name__ == "__main__":
    main()