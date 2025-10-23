#!/usr/bin/env python3
"""
Create a test sector database from the first JSONL file for testing spatial prefiltering.
"""

import json
import re
import math
from collections import defaultdict
from pathlib import Path
import statistics

def parse_system_name(name: str):
    """Parse a system name to extract sector and mass code."""
    mass_code_pattern = r'\b([A-Z]{2}-[A-Z])\b'
    
    match = re.search(mass_code_pattern, name)
    if match:
        mass_code = match.group(1)
        mass_code_start = match.start()
        sector = name[:mass_code_start].strip()
        rest = name[match.end():].strip()
        return sector, mass_code, rest
    else:
        return None, None, name

def calculate_sector_center(systems):
    """Calculate the center coordinates of a sector."""
    if not systems:
        return {'x': 0, 'y': 0, 'z': 0}
    
    total_x = total_y = total_z = 0
    count = 0
    
    for system in systems:
        coords = system.get('coords', {})
        if coords and all(k in coords for k in ['x', 'y', 'z']):
            total_x += coords['x']
            total_y += coords['y'] 
            total_z += coords['z']
            count += 1
    
    if count == 0:
        return {'x': 0, 'y': 0, 'z': 0}
    
    return {
        'x': total_x / count,
        'y': total_y / count,
        'z': total_z / count
    }

def calculate_distance(coords1, coords2):
    """Calculate 3D Euclidean distance between two coordinate points."""
    x1, y1, z1 = coords1.get('x', 0), coords1.get('y', 0), coords1.get('z', 0)
    x2, y2, z2 = coords2.get('x', 0), coords2.get('y', 0), coords2.get('z', 0)
    
    return math.sqrt((x2-x1)**2 + (y2-y1)**2 + (z2-z1)**2)

def create_test_sectors():
    """Create test sector database from first JSONL file."""
    
    input_dir = Path("Databases/galaxy_chunks_annotated")
    output_dir = Path("Databases/test_sectors")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    first_file = list(input_dir.glob("*.jsonl"))[0]
    sector_systems = defaultdict(list)
    non_standard_systems = []
    total_systems = 0
    
    print(f"Creating test sector database from {first_file.name}")
    print("=" * 60)
    
    # Process the first file
    with open(first_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            try:
                system_data = json.loads(line)
                system_name = system_data.get('name', '')
                
                if not system_name:
                    continue
                
                total_systems += 1
                
                # Parse the system name
                sector, mass_code, rest = parse_system_name(system_name)
                
                if sector is not None and mass_code is not None:
                    sector_systems[sector].append(system_data)
                else:
                    non_standard_systems.append(system_data)
            
            except json.JSONDecodeError:
                continue
    
    print(f"Processed {total_systems:,} systems")
    print(f"Found {len(sector_systems):,} unique sectors")
    print(f"Non-standard systems: {len(non_standard_systems):,}")
    
    # Calculate sector centers and stats
    sector_centers = {}
    sector_stats = {}
    
    for sector_name, systems in sector_systems.items():
        center = calculate_sector_center(systems)
        sector_centers[sector_name] = center
        
        # Calculate sector spread
        coords_list = []
        for system in systems:
            coords = system.get('coords', {})
            if coords and all(k in coords for k in ['x', 'y', 'z']):
                coords_list.append(coords)
        
        if coords_list:
            distances = [calculate_distance(center, coords) for coords in coords_list]
            avg_distance = statistics.mean(distances) if distances else 0
            max_distance = max(distances) if distances else 0
        else:
            avg_distance = max_distance = 0
        
        sector_stats[sector_name] = {
            'system_count': len(systems),
            'center': center,
            'avg_distance_from_center': avg_distance,
            'max_distance_from_center': max_distance
        }
    
    # Assign non-standard systems to nearest sectors
    for system_data in non_standard_systems:
        coords = system_data.get('coords', {})
        if coords and all(k in coords for k in ['x', 'y', 'z']):
            min_distance = float('inf')
            nearest_sector = "Unknown"
            
            for sector_name, center_coords in sector_centers.items():
                distance = calculate_distance(coords, center_coords)
                if distance < min_distance:
                    min_distance = distance
                    nearest_sector = sector_name
            
            sector_systems[nearest_sector].append(system_data)
        else:
            sector_systems["Unknown"].append(system_data)
    
    # Write sector files (only write sectors with >= 5 systems to keep manageable)
    sectors_written = 0
    min_systems_per_sector = 5
    
    print(f"\nWriting sector files (minimum {min_systems_per_sector} systems per sector)...")
    
    for sector_name, systems in sector_systems.items():
        if len(systems) < min_systems_per_sector:
            continue
        
        # Create safe filename
        safe_sector_name = re.sub(r'[^a-zA-Z0-9_-]', '_', sector_name)
        safe_sector_name = re.sub(r'_+', '_', safe_sector_name).strip('_')
        if not safe_sector_name:
            safe_sector_name = f"sector_{sectors_written}"
        
        output_file = output_dir / f"{safe_sector_name}.jsonl"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for system in systems:
                json.dump(system, f, separators=(',', ':'))
                f.write('\n')
        
        sectors_written += 1
    
    print(f"Written {sectors_written} sector files")
    
    # Create sector index
    index_file = output_dir / "sector_index.json"
    
    index_data = {
        'metadata': {
            'total_systems': sum(len(systems) for systems in sector_systems.values()),
            'total_sectors': len([s for s in sector_systems.values() if len(s) >= min_systems_per_sector]),
            'min_systems_per_sector': min_systems_per_sector,
            'source_file': str(first_file)
        },
        'sectors': {}
    }
    
    for sector_name, stats in sector_stats.items():
        if len(sector_systems[sector_name]) >= min_systems_per_sector:
            safe_sector_name = re.sub(r'[^a-zA-Z0-9_-]', '_', sector_name)
            safe_sector_name = re.sub(r'_+', '_', safe_sector_name).strip('_')
            if not safe_sector_name:
                safe_sector_name = f"sector_{len(index_data['sectors'])}"
            
            index_data['sectors'][sector_name] = {
                'filename': f"{safe_sector_name}.jsonl",
                'system_count': len(sector_systems[sector_name]),
                'center_coords': stats['center'],
                'avg_radius': stats['avg_distance_from_center'],
                'max_radius': stats['max_distance_from_center']
            }
    
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, indent=2)
    
    print(f"Created sector index: {index_file}")
    
    # Calculate statistics for prefiltering recommendation
    avg_radii = [stats['avg_distance_from_center'] for stats in sector_stats.values() 
                 if stats['avg_distance_from_center'] > 0 and len(sector_systems[sector_name]) >= min_systems_per_sector]
    max_radii = [stats['max_distance_from_center'] for stats in sector_stats.values() 
                 if stats['max_distance_from_center'] > 0 and len(sector_systems[sector_name]) >= min_systems_per_sector]
    
    if max_radii:
        print(f"\nSector statistics:")
        print(f"  Average sector radius: {statistics.mean(avg_radii):.1f} ly")
        print(f"  Median sector radius: {statistics.median(avg_radii):.1f} ly")
        print(f"  Average max radius: {statistics.mean(max_radii):.1f} ly")
        print(f"  Median max radius: {statistics.median(max_radii):.1f} ly")
        
        suggested_range = statistics.median(max_radii) * 3
        print(f"\n  Suggested prefiltering range: {suggested_range:.0f} ly")
    
    print(f"\nTest sector database created in: {output_dir}")
    return index_data

if __name__ == "__main__":
    create_test_sectors()