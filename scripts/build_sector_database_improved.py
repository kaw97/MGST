#!/usr/bin/env python3
"""
Improved sector database reorganization with better progress reporting.
Handles massive files efficiently with regular progress updates.
"""
import json
import os
import re
import sys
import time
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional

def parse_system_name(system_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse system name to extract sector and mass code."""
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

def process_jsonl_file_for_stats(file_path: Path, progress_interval: int = 50000) -> Tuple[Dict, List, int]:
    """Process a single JSONL file and extract sector statistics."""
    sector_stats = defaultdict(lambda: {'count': 0, 'coords': []})
    non_standard_systems = []
    total_systems = 0
    
    print(f"  📂 Processing {file_path.name} ({file_path.stat().st_size / (1024**3):.1f} GB)", flush=True)
    start_time = time.time()
    
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % progress_interval == 0:
                elapsed = time.time() - start_time
                rate = line_num / elapsed if elapsed > 0 else 0
                print(f"    📊 Processed {line_num:,} systems ({rate:,.0f} systems/sec)", flush=True)
            
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
    
    elapsed = time.time() - start_time
    print(f"    ✅ Completed {file_path.name}: {total_systems:,} systems in {elapsed:.1f}s ({total_systems/elapsed:.0f} systems/sec)", flush=True)
    
    return dict(sector_stats), non_standard_systems, total_systems

def stream_jsonl_file_to_sectors(file_path: Path, sector_files: Dict[str, Any], 
                                valid_sectors: set, sector_centers: Dict[str, Tuple],
                                progress_interval: int = 50000) -> Tuple[int, int]:
    """Stream systems from JSONL file into appropriate sector files."""
    systems_written = 0
    non_standard_assigned = 0
    
    print(f"  📂 Streaming {file_path.name}")
    start_time = time.time()
    
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % progress_interval == 0:
                elapsed = time.time() - start_time
                rate = line_num / elapsed if elapsed > 0 else 0
                print(f"    💾 Streamed {line_num:,} systems ({rate:,.0f} systems/sec)", flush=True)
            
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
    
    elapsed = time.time() - start_time
    print(f"    ✅ Streamed {systems_written:,} systems in {elapsed:.1f}s")
    
    return systems_written, non_standard_assigned

def main():
    input_dir = Path("Databases/galaxy_chunks_annotated")
    output_dir = Path("Databases/galaxy_sectors")
    
    if not input_dir.exists():
        print(f"❌ Error: Input directory {input_dir} does not exist")
        return
    
    # Create output directory
    output_dir.mkdir(exist_ok=True)
    
    print("🌌 Building sector-organized galaxy database...")
    print("=" * 60)
    
    # Get list of JSONL files
    jsonl_files = list(input_dir.glob("*.jsonl"))
    print(f"📊 Found {len(jsonl_files)} JSONL files to process")
    total_size = sum(f.stat().st_size for f in jsonl_files) / (1024**3)
    print(f"📦 Total data size: {total_size:.1f} GB")
    
    # Phase 1: Collect sector statistics from all files
    print(f"\n📈 Phase 1: Collecting sector statistics...")
    all_sector_stats = defaultdict(lambda: {'count': 0, 'coords': []})
    all_non_standard = []
    total_systems = 0
    
    start_time = time.time()
    
    for i, jsonl_file in enumerate(jsonl_files, 1):
        print(f"\n🔄 Processing file {i}/{len(jsonl_files)}")
        
        file_sector_stats, file_non_standard, file_systems = process_jsonl_file_for_stats(jsonl_file)
        
        # Merge stats
        for sector_name, stats in file_sector_stats.items():
            all_sector_stats[sector_name]['count'] += stats['count']
            all_sector_stats[sector_name]['coords'].extend(stats['coords'])
        
        all_non_standard.extend(file_non_standard)
        total_systems += file_systems
        
        # Progress summary
        elapsed = time.time() - start_time
        avg_rate = total_systems / elapsed if elapsed > 0 else 0
        print(f"    📊 Progress: {i}/{len(jsonl_files)} files, {total_systems:,} total systems ({avg_rate:,.0f} systems/sec)")
    
    print(f"\n✅ Phase 1 complete:")
    print(f"  📊 Total systems: {total_systems:,}")
    print(f"  🏢 Unique sectors found: {len(all_sector_stats):,}")
    print(f"  ❓ Non-standard systems: {len(all_non_standard):,}")
    
    # Calculate sector centers and filter by minimum size
    min_systems_per_sector = 10
    sector_centers = {}
    valid_sectors = {}
    
    print(f"\n📐 Calculating sector centers (minimum {min_systems_per_sector} systems)...")
    for sector_name, stats in all_sector_stats.items():
        if stats['count'] >= min_systems_per_sector:
            coords_list = stats['coords']
            center_x = sum(c[0] for c in coords_list) / len(coords_list)
            center_y = sum(c[1] for c in coords_list) / len(coords_list)
            center_z = sum(c[2] for c in coords_list) / len(coords_list)
            
            sector_centers[sector_name] = (center_x, center_y, center_z)
            valid_sectors[sector_name] = stats['count']
    
    print(f"✅ Valid sectors: {len(valid_sectors):,}")
    
    # Create sector index
    sector_index = {
        "metadata": {
            "total_systems": total_systems,
            "total_sectors": len(valid_sectors),
            "min_systems_per_sector": min_systems_per_sector,
            "source_files": [f.name for f in jsonl_files],
            "non_standard_systems": len(all_non_standard),
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
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
    
    print(f"✅ Created sector index: {index_file}")
    
    # Phase 2: Stream systems into sector files
    print(f"\n💾 Phase 2: Streaming systems into {len(valid_sectors):,} sector files...")
    
    # Open all sector files for writing
    sector_files = {}
    for sector_name in valid_sectors:
        filename = sector_name.replace(' ', '_') + '.jsonl'
        file_path = output_dir / filename
        sector_files[sector_name] = open(file_path, 'w')
    
    total_written = 0
    total_non_standard_assigned = 0
    
    try:
        for i, jsonl_file in enumerate(jsonl_files, 1):
            print(f"\n🔄 Streaming file {i}/{len(jsonl_files)}")
            
            written, non_standard = stream_jsonl_file_to_sectors(
                jsonl_file, sector_files, valid_sectors, sector_centers
            )
            
            total_written += written
            total_non_standard_assigned += non_standard
            
            print(f"    📊 Running totals: {total_written:,} systems written, {total_non_standard_assigned:,} non-standard assigned")
    
    finally:
        # Close all sector files
        for f in sector_files.values():
            f.close()
    
    print(f"\n🎉 Database reorganization complete!")
    print(f"  📊 Systems written: {total_written:,}")
    print(f"  🔄 Non-standard assigned: {total_non_standard_assigned:,}")
    print(f"  📁 Sector files created: {len(valid_sectors):,}")
    print(f"  📂 Output directory: {output_dir}")
    
    # Show top sectors by system count
    print(f"\n🏆 Top 10 largest sectors:")
    sorted_sectors = sorted(valid_sectors.items(), key=lambda x: x[1], reverse=True)
    for i, (sector_name, count) in enumerate(sorted_sectors[:10]):
        center = sector_centers[sector_name]
        print(f"  {i+1:2d}. {sector_name:<25} {count:>8,} systems | Center: ({center[0]:>8.1f}, {center[1]:>8.1f}, {center[2]:>8.1f})")

if __name__ == "__main__":
    main()