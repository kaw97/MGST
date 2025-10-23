#!/usr/bin/env python3
"""
Test the reorganization logic on a small subset.
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

def test_reorganization():
    """Test reorganization on first 10,000 systems."""
    
    input_dir = Path("Databases/galaxy_chunks_annotated")
    first_file = list(input_dir.glob("*.jsonl"))[0]
    
    sector_systems = defaultdict(list)
    non_standard_systems = []
    total_processed = 0
    
    print(f"Testing reorganization with first 10,000 systems from {first_file.name}")
    print("=" * 80)
    
    with open(first_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if total_processed >= 10000:
                break
                
            line = line.strip()
            if not line:
                continue
            
            try:
                system_data = json.loads(line)
                system_name = system_data.get('name', '')
                
                if not system_name:
                    continue
                
                total_processed += 1
                
                # Parse the system name
                sector, mass_code, rest = parse_system_name(system_name)
                
                if sector is not None and mass_code is not None:
                    sector_systems[sector].append(system_data)
                else:
                    non_standard_systems.append(system_data)
            
            except json.JSONDecodeError:
                continue
    
    print(f"Processed {total_processed:,} systems")
    print(f"Standard systems: {total_processed - len(non_standard_systems):,}")
    print(f"Non-standard systems: {len(non_standard_systems):,}")
    print(f"Unique sectors found: {len(sector_systems):,}")
    
    # Show top sectors by system count
    print(f"\nTop 10 sectors by system count:")
    sorted_sectors = sorted(sector_systems.items(), key=lambda x: len(x[1]), reverse=True)
    
    for i, (sector_name, systems) in enumerate(sorted_sectors[:10], 1):
        center = calculate_sector_center(systems)
        print(f"  {i:2d}. {sector_name:<30} {len(systems):>4} systems | Center: ({center['x']:>8.1f}, {center['y']:>8.1f}, {center['z']:>8.1f})")
    
    # Show some non-standard systems
    print(f"\nSample non-standard systems:")
    for i, system in enumerate(non_standard_systems[:5], 1):
        coords = system.get('coords', {})
        x, y, z = coords.get('x', 0), coords.get('y', 0), coords.get('z', 0)
        print(f"  {i}. {system['name']:<30} | Coords: ({x:>8.1f}, {y:>8.1f}, {z:>8.1f})")

if __name__ == "__main__":
    test_reorganization()