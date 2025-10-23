#!/usr/bin/env python3
"""
Reorganize galaxy database by sectors and create sector index.
"""

import json
import re
import math
from collections import defaultdict
from pathlib import Path
import statistics
from typing import Dict, List, Tuple, Optional

def parse_system_name(name: str) -> Tuple[Optional[str], Optional[str], str]:
    """Parse a system name to extract sector and mass code."""
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

def calculate_distance(coords1: Dict, coords2: Dict) -> float:
    """Calculate 3D Euclidean distance between two coordinate points."""
    x1, y1, z1 = coords1.get('x', 0), coords1.get('y', 0), coords1.get('z', 0)
    x2, y2, z2 = coords2.get('x', 0), coords2.get('y', 0), coords2.get('z', 0)
    
    return math.sqrt((x2-x1)**2 + (y2-y1)**2 + (z2-z1)**2)

def calculate_sector_center(systems: List[Dict]) -> Dict[str, float]:
    """Calculate the center coordinates of a sector based on its systems."""
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

def find_nearest_sector(system_coords: Dict, sector_centers: Dict[str, Dict]) -> str:
    """Find the nearest sector for a non-standard system."""
    if not system_coords or not sector_centers:
        return "Unknown"
    
    min_distance = float('inf')
    nearest_sector = "Unknown"
    
    for sector_name, center_coords in sector_centers.items():
        distance = calculate_distance(system_coords, center_coords)
        if distance < min_distance:
            min_distance = distance
            nearest_sector = sector_name
    
    return nearest_sector

def reorganize_database():
    """Reorganize the database by sectors."""
    
    input_dir = Path("Databases/galaxy_chunks_annotated")
    output_dir = Path("Databases/galaxy_by_sector")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Data structures
    sector_systems = defaultdict(list)
    non_standard_systems = []
    total_systems = 0
    
    print("=== Phase 1: Processing and categorizing systems ===")
    
    # Process all JSONL files
    for jsonl_file in sorted(input_dir.glob("*.jsonl")):
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
                        # Standard naming pattern - add to sector
                        sector_systems[sector].append(system_data)
                    else:
                        # Non-standard naming - store for later assignment
                        non_standard_systems.append(system_data)
                
                except json.JSONDecodeError as e:
                    print(f"JSON error in {jsonl_file.name} line {line_num}: {e}")
                    continue
                except Exception as e:
                    print(f"Error processing {jsonl_file.name} line {line_num}: {e}")
                    continue
        
        # Progress update
        if total_systems % 100000 == 0:
            print(f"  Processed {total_systems:,} systems, found {len(sector_systems):,} sectors")
    
    print(f"\nPhase 1 complete:")
    print(f"  Total systems: {total_systems:,}")
    print(f"  Standard systems: {total_systems - len(non_standard_systems):,}")
    print(f"  Non-standard systems: {len(non_standard_systems):,}")
    print(f"  Unique sectors: {len(sector_systems):,}")
    
    # Calculate sector centers
    print(f"\n=== Phase 2: Calculating sector centers ===")
    sector_centers = {}
    sector_stats = {}
    
    for sector_name, systems in sector_systems.items():
        center = calculate_sector_center(systems)
        sector_centers[sector_name] = center
        
        # Calculate sector statistics
        coords_list = []
        for system in systems:
            coords = system.get('coords', {})
            if coords and all(k in coords for k in ['x', 'y', 'z']):
                coords_list.append(coords)
        
        if coords_list:
            # Calculate spread (standard deviation from center)
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
    
    print(f"Calculated centers for {len(sector_centers):,} sectors")
    
    # Assign non-standard systems to nearest sectors
    print(f"\n=== Phase 3: Assigning non-standard systems to nearest sectors ===")
    assigned_count = 0
    
    for system_data in non_standard_systems:
        coords = system_data.get('coords', {})
        if coords and all(k in coords for k in ['x', 'y', 'z']):
            nearest_sector = find_nearest_sector(coords, sector_centers)
            sector_systems[nearest_sector].append(system_data)
            assigned_count += 1
        else:
            # No coordinates - assign to "Unknown" sector
            sector_systems["Unknown"].append(system_data)
    
    print(f"Assigned {assigned_count:,} non-standard systems to nearest sectors")
    print(f"Assigned {len(non_standard_systems) - assigned_count:,} systems without coordinates to 'Unknown' sector")
    
    # Write sector files
    print(f"\n=== Phase 4: Writing sector files ===")
    sectors_written = 0
    
    for sector_name, systems in sector_systems.items():
        if not systems:
            continue
        
        # Create safe filename from sector name
        safe_sector_name = re.sub(r'[^a-zA-Z0-9_-]', '_', sector_name)
        safe_sector_name = re.sub(r'_+', '_', safe_sector_name).strip('_')
        if not safe_sector_name:
            safe_sector_name = f"sector_{len(sectors_written)}"
        
        output_file = output_dir / f"{safe_sector_name}.jsonl"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for system in systems:
                json.dump(system, f, separators=(',', ':'))
                f.write('\n')
        
        sectors_written += 1
        if sectors_written % 100 == 0:
            print(f"  Written {sectors_written:,} sector files")
    
    print(f"Written {sectors_written:,} sector files to {output_dir}")
    
    # Create sector index
    print(f"\n=== Phase 5: Creating sector index ===")
    index_file = output_dir / "sector_index.json"
    
    index_data = {
        'metadata': {
            'total_systems': total_systems,
            'total_sectors': len(sector_systems),
            'sectors_with_files': sectors_written,
            'creation_time': str(Path().cwd()),
            'source_directory': str(input_dir)
        },
        'sectors': {}
    }
    
    for sector_name, stats in sector_stats.items():
        if sector_name in sector_systems and sector_systems[sector_name]:
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
    
    # Add Unknown sector if it exists
    if "Unknown" in sector_systems and sector_systems["Unknown"]:
        index_data['sectors']["Unknown"] = {
            'filename': "Unknown.jsonl",
            'system_count': len(sector_systems["Unknown"]),
            'center_coords': {'x': 0, 'y': 0, 'z': 0},
            'avg_radius': 0,
            'max_radius': 0
        }
    
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, indent=2)
    
    print(f"Created sector index: {index_file}")
    
    # Generate statistics
    print(f"\n=== Final Statistics ===")
    sector_sizes = [len(systems) for systems in sector_systems.values()]
    avg_radii = [stats['avg_distance_from_center'] for stats in sector_stats.values() if stats['avg_distance_from_center'] > 0]
    max_radii = [stats['max_distance_from_center'] for stats in sector_stats.values() if stats['max_distance_from_center'] > 0]
    
    print(f"Sector count statistics:")
    print(f"  Total sectors: {len(sector_systems):,}")
    print(f"  Min systems per sector: {min(sector_sizes):,}")
    print(f"  Max systems per sector: {max(sector_sizes):,}")
    print(f"  Avg systems per sector: {statistics.mean(sector_sizes):,.1f}")
    print(f"  Median systems per sector: {statistics.median(sector_sizes):,.1f}")
    
    if avg_radii:
        print(f"\nSector size statistics (light years):")
        print(f"  Min avg radius: {min(avg_radii):.1f} ly")
        print(f"  Max avg radius: {max(avg_radii):.1f} ly")
        print(f"  Avg sector radius: {statistics.mean(avg_radii):.1f} ly")
        print(f"  Median sector radius: {statistics.median(avg_radii):.1f} ly")
        
        print(f"\n  Min max radius: {min(max_radii):.1f} ly")
        print(f"  Max max radius: {max(max_radii):.1f} ly")
        print(f"  Avg max radius: {statistics.mean(max_radii):.1f} ly")
        print(f"  Median max radius: {statistics.median(max_radii):.1f} ly")
        
        # Suggest prefiltering range
        suggested_range = statistics.median(max_radii) * 3  # 3x median max radius to capture adjacent sectors
        print(f"\nSuggested spatial prefiltering range: {suggested_range:.0f} ly")
        print(f"(3x median max sector radius - should capture most adjacent sectors)")
    
    return index_data

if __name__ == "__main__":
    reorganize_database()