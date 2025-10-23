#!/usr/bin/env python3
"""
Test streaming sector reorganization on one file.
"""

import json
import re
import math
from collections import defaultdict
from pathlib import Path

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

def test_streaming_reorganization():
    """Test streaming reorganization on first 50,000 systems from one file."""
    
    input_dir = Path("Databases/galaxy_chunks_annotated")
    output_dir = Path("Databases/test_streaming_sectors")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    first_file = list(input_dir.glob("*.jsonl"))[0]
    
    print(f"Testing streaming reorganization with {first_file.name}")
    print("=" * 60)
    
    # Phase 1: Collect sector statistics
    sector_coords_sum = defaultdict(lambda: {'x': 0, 'y': 0, 'z': 0, 'count': 0})
    sector_system_count = defaultdict(int)
    non_standard_systems = []
    total_systems = 0
    max_test_systems = 50000
    
    print("Phase 1: Collecting sector statistics...")
    
    with open(first_file, 'r', encoding='utf-8') as f:
        for line in f:
            if total_systems >= max_test_systems:
                break
                
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
                    sector_system_count[sector] += 1
                    
                    if coords and all(k in coords for k in ['x', 'y', 'z']):
                        sector_coords_sum[sector]['x'] += coords['x']
                        sector_coords_sum[sector]['y'] += coords['y']
                        sector_coords_sum[sector]['z'] += coords['z']
                        sector_coords_sum[sector]['count'] += 1
                else:
                    non_standard_systems.append(system_data)
            
            except (json.JSONDecodeError, Exception):
                continue
    
    print(f"  Processed {total_systems:,} systems")
    print(f"  Found {len(sector_system_count):,} unique sectors")
    print(f"  Non-standard systems: {len(non_standard_systems):,}")
    
    # Calculate sector centers
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
    
    # Only process sectors with >= 10 systems for manageable test
    min_systems_per_sector = 10
    large_sectors = {k: v for k, v in sector_system_count.items() if v >= min_systems_per_sector}
    
    print(f"  Large sectors (>={min_systems_per_sector} systems): {len(large_sectors):,}")
    
    # Create index for large sectors only
    index_data = {
        'metadata': {
            'total_systems': sum(large_sectors.values()),
            'total_sectors': len(large_sectors),
            'min_systems_per_sector': min_systems_per_sector,
            'test_file': str(first_file)
        },
        'sectors': {}
    }
    
    for sector_name, system_count in large_sectors.items():
        safe_sector_name = re.sub(r'[^a-zA-Z0-9_-]', '_', sector_name)
        safe_sector_name = re.sub(r'_+', '_', safe_sector_name).strip('_')
        if not safe_sector_name:
            safe_sector_name = f"sector_{len(index_data['sectors'])}"
        
        index_data['sectors'][sector_name] = {
            'filename': f"{safe_sector_name}.jsonl",
            'system_count': system_count,
            'center_coords': sector_centers.get(sector_name, {'x': 0, 'y': 0, 'z': 0})
        }
    
    # Write index
    index_file = output_dir / "sector_index.json"
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, indent=2)
    
    print(f"Created sector index: {index_file}")
    
    # Phase 2: Stream systems into sector files
    print("\\nPhase 2: Streaming systems into sector files...")
    
    sector_files = {}
    
    try:
        # Open files for large sectors
        for sector_name, sector_info in index_data['sectors'].items():
            filename = sector_info['filename']
            file_path = output_dir / filename
            sector_files[sector_name] = open(file_path, 'w', encoding='utf-8')
        
        # Stream systems from original file
        systems_written = 0
        
        with open(first_file, 'r', encoding='utf-8') as f:
            current_system = 0
            
            for line in f:
                if current_system >= max_test_systems:
                    break
                
                line = line.strip()
                if not line:
                    continue
                
                try:
                    system_data = json.loads(line)
                    system_name = system_data.get('name', '')
                    
                    if not system_name:
                        continue
                    
                    current_system += 1
                    
                    # Parse the system name
                    sector, mass_code, rest = parse_system_name(system_name)
                    
                    if sector is not None and sector in sector_files:
                        json.dump(system_data, sector_files[sector], separators=(',', ':'))
                        sector_files[sector].write('\n')
                        systems_written += 1
                
                except (json.JSONDecodeError, Exception):
                    continue
    
    finally:
        # Close all files
        for f in sector_files.values():
            f.close()
    
    print(f"  Streamed {systems_written:,} systems into {len(sector_files):,} sector files")
    
    # Show some results
    print("\\nResults:")
    for sector_name, sector_info in list(index_data['sectors'].items())[:5]:
        center = sector_info['center_coords']
        print(f"  {sector_name:<30} {sector_info['system_count']:>4} systems | "
              f"Center: ({center['x']:>8.1f}, {center['y']:>8.1f}, {center['z']:>8.1f})")
    
    print(f"\\nTest complete! Files created in: {output_dir}")
    return index_data

if __name__ == "__main__":
    test_streaming_reorganization()