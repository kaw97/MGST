#!/usr/bin/env python3
"""
Streaming sector reorganization - process files without loading everything into memory.
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

def create_streaming_sector_reorganization():
    """First pass: stream through data to collect sector statistics without storing systems."""
    
    input_dir = Path("Databases/galaxy_chunks_annotated")
    output_dir = Path("Databases/galaxy_by_sector")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=== Phase 1: Streaming analysis to collect sector statistics ===")
    
    # Collect statistics without storing systems
    sector_coords_sum = defaultdict(lambda: {'x': 0, 'y': 0, 'z': 0, 'count': 0})
    sector_system_count = defaultdict(int)
    non_standard_systems = []  # Keep these since they're small in number
    total_systems = 0
    
    # First pass - collect statistics
    for jsonl_file in sorted(input_dir.glob("*.jsonl")):
        print(f"Pass 1 - Analyzing {jsonl_file.name}...")
        
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    system_data = json.loads(line)
                    system_name = system_data.get('name', '')
                    coords = system_data.get('coords', {})
                    
                    if not system_name:
                        continue
                    
                    total_systems += 1
                    
                    # Parse the system name
                    sector, mass_code, rest = parse_system_name(system_name)
                    
                    if sector is not None and mass_code is not None:
                        # Standard naming - accumulate coordinates for center calculation
                        sector_system_count[sector] += 1
                        
                        if coords and all(k in coords for k in ['x', 'y', 'z']):
                            sector_coords_sum[sector]['x'] += coords['x']
                            sector_coords_sum[sector]['y'] += coords['y']
                            sector_coords_sum[sector]['z'] += coords['z']
                            sector_coords_sum[sector]['count'] += 1
                    else:
                        # Non-standard naming - store for nearest sector assignment
                        non_standard_systems.append(system_data)
                
                except (json.JSONDecodeError, Exception) as e:
                    continue
        
        # Progress update
        if total_systems % 100000 == 0:
            print(f"  Analyzed {total_systems:,} systems, found {len(sector_system_count):,} sectors so far")
    
    print(f"Pass 1 complete:")
    print(f"  Total systems: {total_systems:,}")
    print(f"  Unique sectors: {len(sector_system_count):,}")
    print(f"  Non-standard systems: {len(non_standard_systems):,}")
    
    # Calculate sector centers
    print(f"\n=== Phase 2: Calculating sector centers ===")
    sector_centers = {}
    
    for sector_name, coord_data in sector_coords_sum.items():
        if coord_data['count'] > 0:
            sector_centers[sector_name] = {
                'x': coord_data['x'] / coord_data['count'],
                'y': coord_data['y'] / coord_data['count'],
                'z': coord_data['z'] / coord_data['count']
            }
        else:
            sector_centers[sector_name] = {'x': 0, 'y': 0, 'z': 0}
    
    print(f"Calculated centers for {len(sector_centers):,} sectors")
    
    # Create sector index first
    print(f"\n=== Phase 3: Creating sector index ===")
    index_file = output_dir / "sector_index.json"
    
    index_data = {
        'metadata': {
            'total_systems': total_systems,
            'total_sectors': len(sector_system_count),
            'non_standard_systems': len(non_standard_systems),
            'source_directory': str(input_dir)
        },
        'sectors': {}
    }
    
    for sector_name, system_count in sector_system_count.items():
        # Create safe filename
        safe_sector_name = re.sub(r'[^a-zA-Z0-9_-]', '_', sector_name)
        safe_sector_name = re.sub(r'_+', '_', safe_sector_name).strip('_')
        if not safe_sector_name or safe_sector_name in index_data['sectors']:
            safe_sector_name = f"sector_{len(index_data['sectors'])}"
        
        index_data['sectors'][sector_name] = {
            'filename': f"{safe_sector_name}.jsonl",
            'system_count': system_count,
            'center_coords': sector_centers.get(sector_name, {'x': 0, 'y': 0, 'z': 0})
        }
    
    # Add non-standard systems to index (they'll be in separate files by nearest sector)
    for system_data in non_standard_systems:
        coords = system_data.get('coords', {})
        if coords and all(k in coords for k in ['x', 'y', 'z']):
            # Find nearest sector
            min_distance = float('inf')
            nearest_sector = "Unknown"
            
            for sector_name, center_coords in sector_centers.items():
                distance = math.sqrt(
                    (coords['x'] - center_coords['x'])**2 +
                    (coords['y'] - center_coords['y'])**2 +
                    (coords['z'] - center_coords['z'])**2
                )
                if distance < min_distance:
                    min_distance = distance
                    nearest_sector = sector_name
            
            if nearest_sector in index_data['sectors']:
                index_data['sectors'][nearest_sector]['system_count'] += 1
        else:
            # No coordinates - add to Unknown sector
            if "Unknown" not in index_data['sectors']:
                index_data['sectors']["Unknown"] = {
                    'filename': "Unknown.jsonl",
                    'system_count': 0,
                    'center_coords': {'x': 0, 'y': 0, 'z': 0}
                }
            index_data['sectors']["Unknown"]['system_count'] += 1
    
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, indent=2)
    
    print(f"Created sector index: {index_file}")
    
    print(f"\n=== Phase 4: Second pass - streaming reorganization into sector files ===")
    
    # Open all sector files for writing (streaming approach)
    sector_files = {}
    
    try:
        # Create file handles for all sectors
        for sector_name, sector_info in index_data['sectors'].items():
            filename = sector_info['filename']
            file_path = output_dir / filename
            sector_files[sector_name] = open(file_path, 'w', encoding='utf-8')
        
        sectors_processed = 0
        
        # Second pass - stream systems into appropriate sector files
        for jsonl_file in sorted(input_dir.glob("*.jsonl")):
            print(f"Pass 2 - Reorganizing {jsonl_file.name}...")
            
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        system_data = json.loads(line)
                        system_name = system_data.get('name', '')
                        
                        if not system_name:
                            continue
                        
                        # Parse the system name
                        sector, mass_code, rest = parse_system_name(system_name)
                        
                        if sector is not None and mass_code is not None:
                            # Standard naming - write to sector file
                            if sector in sector_files:
                                json.dump(system_data, sector_files[sector], separators=(',', ':'))
                                sector_files[sector].write('\n')
                                sectors_processed += 1
                    
                    except (json.JSONDecodeError, Exception) as e:
                        continue
        
        # Process non-standard systems
        print("Processing non-standard systems...")
        for system_data in non_standard_systems:
            coords = system_data.get('coords', {})
            target_sector = "Unknown"
            
            if coords and all(k in coords for k in ['x', 'y', 'z']):
                # Find nearest sector
                min_distance = float('inf')
                
                for sector_name, center_coords in sector_centers.items():
                    distance = math.sqrt(
                        (coords['x'] - center_coords['x'])**2 +
                        (coords['y'] - center_coords['y'])**2 +
                        (coords['z'] - center_coords['z'])**2
                    )
                    if distance < min_distance:
                        min_distance = distance
                        target_sector = sector_name
            
            if target_sector in sector_files:
                json.dump(system_data, sector_files[target_sector], separators=(',', ':'))
                sector_files[target_sector].write('\n')
        
    finally:
        # Close all files
        for f in sector_files.values():
            f.close()
    
    print(f"\\nReorganization complete!")
    print(f"Created {len(sector_files):,} sector files in {output_dir}")
    print(f"Processed {sectors_processed:,} systems with standard naming")
    print(f"Assigned {len(non_standard_systems):,} non-standard systems to nearest sectors")
    
    return index_data

if __name__ == "__main__":
    create_streaming_sector_reorganization()