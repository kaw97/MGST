#!/usr/bin/env python3
"""
Production script to reorganize the entire galaxy database by sectors.
Creates sector-organized JSONL files and a comprehensive sector index.
Uses streaming processing for memory efficiency with massive datasets.
"""
import json
import os
import re
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional

def parse_system_name(system_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse system name to extract sector and mass code.
    Returns (sector_name, mass_code) or (None, None) if non-standard.
    """
    mass_code_pattern = r'\b([A-Z]{2}-[A-Z])\b'
    match = re.search(mass_code_pattern, system_name)
    
    if match:
        mass_code = match.group(1)
        mass_code_start = match.start()
        sector_name = system_name[:mass_code_start].strip()
        return sector_name, mass_code
    
    return None, None

def calculate_distance(coord1: Tuple[float, float, float], coord2: Tuple[float, float, float]) -> float:
    """Calculate 3D Euclidean distance between two coordinates."""
    return ((coord1[0] - coord2[0])**2 + (coord1[1] - coord2[1])**2 + (coord1[2] - coord2[2])**2)**0.5

def find_nearest_sector(system_coords: Tuple[float, float, float], 
                       sector_centers: Dict[str, Tuple[float, float, float]]) -> str:
    """Find the nearest sector to a system's coordinates."""
    if not sector_centers:
        return "Unknown"
    
    min_distance = float('inf')
    nearest_sector = None
    
    for sector_name, center_coords in sector_centers.items():
        distance = calculate_distance(system_coords, center_coords)
        if distance < min_distance:
            min_distance = distance
            nearest_sector = sector_name
    
    return nearest_sector or "Unknown"

def main():
    input_dir = Path("Databases/galaxy_chunks_annotated")
    output_dir = Path("Databases/galaxy_sectors")
    
    if not input_dir.exists():
        print(f"Error: Input directory {input_dir} does not exist")
        return
    
    # Create output directory
    output_dir.mkdir(exist_ok=True)
    
    print("Building sector-organized galaxy database...")
    print("=" * 60)
    
    # Phase 1: Collect sector statistics from all files
    print("Phase 1: Collecting sector statistics from all files...")
    sector_stats = defaultdict(lambda: {'count': 0, 'coords': []})
    non_standard_systems = []
    total_systems = 0
    files_processed = 0
    
    jsonl_files = list(input_dir.glob("*.jsonl"))
    print(f"Found {len(jsonl_files)} JSONL files to process")
    
    for jsonl_file in jsonl_files:
        print(f"  Processing: {jsonl_file.name}")
        files_processed += 1
        
        with open(jsonl_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                if line_num % 100000 == 0:
                    print(f"    Processed {line_num:,} systems")
                
                try:
                    system = json.loads(line.strip())
                    if 'name' not in system:
                        continue
                    
                    total_systems += 1
                    system_name = system['name']
                    coords = (system.get('coords', {}).get('x', 0),
                             system.get('coords', {}).get('y', 0), 
                             system.get('coords', {}).get('z', 0))
                    
                    sector_name, mass_code = parse_system_name(system_name)
                    
                    if sector_name:
                        sector_stats[sector_name]['count'] += 1
                        sector_stats[sector_name]['coords'].append(coords)
                    else:
                        non_standard_systems.append((system_name, coords))
                        
                except json.JSONDecodeError:
                    continue
    
    print(f"Statistics collection complete:")
    print(f"  Total systems processed: {total_systems:,}")
    print(f"  Files processed: {files_processed}")
    print(f"  Unique sectors found: {len(sector_stats):,}")
    print(f"  Non-standard systems: {len(non_standard_systems):,}")
    
    # Calculate sector centers and filter by minimum size
    min_systems_per_sector = 10
    sector_centers = {}
    valid_sectors = {}
    
    print(f"\nCalculating sector centers (minimum {min_systems_per_sector} systems per sector)...")
    for sector_name, stats in sector_stats.items():
        if stats['count'] >= min_systems_per_sector:
            coords_list = stats['coords']
            center_x = sum(c[0] for c in coords_list) / len(coords_list)
            center_y = sum(c[1] for c in coords_list) / len(coords_list)
            center_z = sum(c[2] for c in coords_list) / len(coords_list)
            
            sector_centers[sector_name] = (center_x, center_y, center_z)
            valid_sectors[sector_name] = stats['count']
    
    print(f"Valid sectors (>= {min_systems_per_sector} systems): {len(valid_sectors):,}")
    
    # Create sector index
    sector_index = {
        "metadata": {
            "total_systems": total_systems,
            "total_sectors": len(valid_sectors),
            "min_systems_per_sector": min_systems_per_sector,
            "source_files": [f.name for f in jsonl_files],
            "non_standard_systems": len(non_standard_systems)
        },
        "sectors": {}
    }
    
    for sector_name in valid_sectors:
        center_coords = sector_centers[sector_name]
        sector_index["sectors"][sector_name] = {
            "filename": f"{sector_name.replace(' ', '_')}.jsonl",
            "system_count": valid_sectors[sector_name],
            "center_coords": {
                "x": center_coords[0],
                "y": center_coords[1], 
                "z": center_coords[2]
            }
        }
    
    # Write sector index
    index_file = output_dir / "sector_index.json"
    with open(index_file, 'w') as f:
        json.dump(sector_index, f, indent=2)
    
    print(f"Created sector index: {index_file}")
    
    # Phase 2: Stream systems into sector files
    print(f"\nPhase 2: Streaming systems into sector files...")
    sector_files = {}  # sector_name -> file_handle
    systems_written = 0
    non_standard_assigned = 0
    
    # Open all sector files for writing
    for sector_name in valid_sectors:
        filename = sector_name.replace(' ', '_') + '.jsonl'
        file_path = output_dir / filename
        sector_files[sector_name] = open(file_path, 'w')
    
    try:
        # Process all files again, streaming to appropriate sector files
        for jsonl_file in jsonl_files:
            print(f"  Streaming from: {jsonl_file.name}")
            
            with open(jsonl_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    if line_num % 100000 == 0:
                        print(f"    Streamed {line_num:,} systems")
                    
                    try:
                        system = json.loads(line.strip())
                        if 'name' not in system:
                            continue
                        
                        system_name = system['name']
                        coords = (system.get('coords', {}).get('x', 0),
                                 system.get('coords', {}).get('y', 0),
                                 system.get('coords', {}).get('z', 0))
                        
                        sector_name, mass_code = parse_system_name(system_name)
                        
                        # Assign to sector or find nearest
                        target_sector = None
                        if sector_name and sector_name in valid_sectors:
                            target_sector = sector_name
                        else:
                            # Find nearest sector for non-standard systems
                            target_sector = find_nearest_sector(coords, sector_centers)
                            if target_sector != "Unknown":
                                non_standard_assigned += 1
                        
                        if target_sector and target_sector in sector_files:
                            sector_files[target_sector].write(line)
                            systems_written += 1
                            
                    except json.JSONDecodeError:
                        continue
    
    finally:
        # Close all sector files
        for f in sector_files.values():
            f.close()
    
    print(f"\nDatabase reorganization complete!")
    print(f"  Systems written to sector files: {systems_written:,}")
    print(f"  Non-standard systems assigned to nearest sectors: {non_standard_assigned:,}")
    print(f"  Sector files created: {len(valid_sectors):,}")
    print(f"  Output directory: {output_dir}")
    
    # Show top sectors by system count
    print(f"\nTop 10 largest sectors:")
    sorted_sectors = sorted(valid_sectors.items(), key=lambda x: x[1], reverse=True)
    for i, (sector_name, count) in enumerate(sorted_sectors[:10]):
        center = sector_centers[sector_name]
        print(f"  {i+1:2d}. {sector_name:<25} {count:>6,} systems | Center: ({center[0]:>8.1f}, {center[1]:>8.1f}, {center[2]:>8.1f})")

if __name__ == "__main__":
    main()