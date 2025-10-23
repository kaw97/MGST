#!/usr/bin/env python3
"""
Analyze system naming patterns to understand sector structure.
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
import statistics

def parse_system_name(name):
    """Parse a system name to extract sector and mass code.
    
    Pattern: "Hypoae Aihm SG-E c12-5"
    - Sector: "Hypoae Aihm" (everything before mass code)
    - Mass Code: "SG-E" (2 letters, dash, letter)
    - Rest: "c12-5" (remaining part)
    """
    # Mass code pattern: 2 letters, dash, letter (e.g., SG-E, AB-C, XY-Z)
    mass_code_pattern = r'\b([A-Z]{2}-[A-Z])\b'
    
    match = re.search(mass_code_pattern, name)
    if match:
        mass_code = match.group(1)
        mass_code_start = match.start()
        
        # Sector is everything before the mass code (trimmed)
        sector = name[:mass_code_start].strip()
        
        # Rest is everything after the mass code
        rest = name[match.end():].strip()
        
        return sector, mass_code, rest
    else:
        # Doesn't follow standard pattern
        return None, None, name

def analyze_database():
    """Analyze the entire database for naming patterns."""
    
    database_dir = Path("Databases/galaxy_chunks_annotated")
    
    # Counters and collections
    sector_systems = defaultdict(list)
    mass_code_counts = Counter()
    non_standard_systems = []
    
    total_systems = 0
    standard_systems = 0
    
    print("Analyzing system naming patterns...")
    
    # Process each JSONL file
    for jsonl_file in database_dir.glob("*.jsonl"):
        print(f"Processing {jsonl_file.name}...")
        
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
                        # Standard naming pattern
                        standard_systems += 1
                        
                        sector_systems[sector].append({
                            'name': system_name,
                            'mass_code': mass_code,
                            'rest': rest,
                            'coords': coords
                        })
                        
                        mass_code_counts[mass_code] += 1
                    else:
                        # Non-standard naming
                        non_standard_systems.append({
                            'name': system_name,
                            'coords': coords
                        })
                
                except json.JSONDecodeError as e:
                    print(f"JSON error in {jsonl_file.name} line {line_num}: {e}")
                    continue
                except Exception as e:
                    print(f"Error processing {jsonl_file.name} line {line_num}: {e}")
                    continue
        
        # Progress update
        if total_systems % 50000 == 0:
            print(f"Processed {total_systems:,} systems so far...")
    
    print(f"\n=== Analysis Results ===")
    print(f"Total systems: {total_systems:,}")
    print(f"Standard naming pattern: {standard_systems:,} ({standard_systems/total_systems*100:.1f}%)")
    print(f"Non-standard naming: {len(non_standard_systems):,} ({len(non_standard_systems)/total_systems*100:.1f}%)")
    print(f"Unique sectors: {len(sector_systems):,}")
    print(f"Unique mass codes: {len(mass_code_counts):,}")
    
    # Sector statistics
    sector_sizes = [len(systems) for systems in sector_systems.values()]
    print(f"\nSector size statistics:")
    print(f"  Min systems per sector: {min(sector_sizes):,}")
    print(f"  Max systems per sector: {max(sector_sizes):,}")
    print(f"  Avg systems per sector: {statistics.mean(sector_sizes):,.1f}")
    print(f"  Median systems per sector: {statistics.median(sector_sizes):,.1f}")
    
    # Top sectors by system count
    print(f"\nTop 10 sectors by system count:")
    sorted_sectors = sorted(sector_systems.items(), key=lambda x: len(x[1]), reverse=True)
    for i, (sector, systems) in enumerate(sorted_sectors[:10], 1):
        print(f"  {i:2d}. {sector:<30} {len(systems):>6,} systems")
    
    # Top mass codes
    print(f"\nTop 10 mass codes:")
    for i, (mass_code, count) in enumerate(mass_code_counts.most_common(10), 1):
        print(f"  {i:2d}. {mass_code:<6} {count:>8,} systems")
    
    # Sample non-standard systems
    print(f"\nSample non-standard system names:")
    for i, system in enumerate(non_standard_systems[:10], 1):
        print(f"  {i:2d}. {system['name']}")
    
    return {
        'sector_systems': dict(sector_systems),
        'mass_code_counts': dict(mass_code_counts),
        'non_standard_systems': non_standard_systems,
        'total_systems': total_systems,
        'standard_systems': standard_systems
    }

if __name__ == "__main__":
    results = analyze_database()