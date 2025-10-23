#!/usr/bin/env python3
"""
Parallel sector database reorganization using 12 processors.
Uses ProcessPoolExecutor to handle multiple files simultaneously.
"""
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ProcessPoolExecutor, as_completed
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

def sanitize_filename(sector_name: str) -> str:
    """Convert sector name to safe filename."""
    return sector_name.replace(' ', '_').replace('/', '_').replace('\\', '_').replace(':', '_')

def process_file_pass1(file_path: Path) -> Tuple[Dict, int, int]:
    """
    Process a single file for Pass 1: collect sector statistics.
    Returns (sector_stats, standard_count, non_standard_count)
    """
    sector_stats = defaultdict(lambda: {'count': 0, 'sum_x': 0.0, 'sum_y': 0.0, 'sum_z': 0.0})
    standard_count = 0
    non_standard_count = 0
    progress_interval = 50000
    
    print(f"  🔄 Worker processing: {file_path.name} ({file_path.stat().st_size / (1024**3):.1f} GB)", flush=True)
    start_time = time.time()
    
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % progress_interval == 0:
                elapsed = time.time() - start_time
                rate = line_num / elapsed if elapsed > 0 else 0
                print(f"    📊 {file_path.name}: {line_num:,} systems ({rate:,.0f}/sec)", flush=True)
            
            try:
                system = json.loads(line.strip())
                if 'name' not in system:
                    continue
                
                system_name = system['name']
                coords = (system.get('coords', {}).get('x', 0),
                         system.get('coords', {}).get('y', 0), 
                         system.get('coords', {}).get('z', 0))
                
                sector_name, mass_code = parse_system_name(system_name)
                
                if sector_name:
                    standard_count += 1
                    # Track statistics with running sums
                    stats = sector_stats[sector_name]
                    stats['count'] += 1
                    stats['sum_x'] += coords[0]
                    stats['sum_y'] += coords[1]
                    stats['sum_z'] += coords[2]
                else:
                    non_standard_count += 1
                    
            except json.JSONDecodeError:
                continue
    
    elapsed = time.time() - start_time
    total_systems = standard_count + non_standard_count
    print(f"    ✅ {file_path.name}: {total_systems:,} systems in {elapsed:.1f}s", flush=True)
    
    return dict(sector_stats), standard_count, non_standard_count

def process_file_pass2(args: Tuple) -> Tuple[int, str]:
    """
    Process a single file for Pass 2: write standard systems to sector files.
    Returns (systems_written, file_name)
    """
    file_path, output_dir, valid_sectors = args
    systems_written = 0
    progress_interval = 50000
    
    # Track which sector files this worker has opened
    worker_sector_files = {}
    
    print(f"  🔄 Worker streaming: {file_path.name}", flush=True)
    start_time = time.time()
    
    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                if line_num % progress_interval == 0:
                    elapsed = time.time() - start_time
                    rate = line_num / elapsed if elapsed > 0 else 0
                    print(f"    💾 {file_path.name}: {line_num:,} systems ({rate:,.0f}/sec)", flush=True)
                
                try:
                    system = json.loads(line.strip())
                    if 'name' not in system:
                        continue
                    
                    system_name = system['name']
                    sector_name, mass_code = parse_system_name(system_name)
                    
                    if sector_name and sector_name in valid_sectors:
                        # Open sector file if not already opened by this worker
                        if sector_name not in worker_sector_files:
                            filename = sanitize_filename(sector_name) + '.jsonl'
                            file_path_sector = Path(output_dir) / filename
                            # Open in append mode to allow multiple workers
                            worker_sector_files[sector_name] = open(file_path_sector, 'a')
                        
                        # Write system to sector file
                        worker_sector_files[sector_name].write(line)
                        systems_written += 1
                        
                except json.JSONDecodeError:
                    continue
    
    finally:
        # Close all sector files opened by this worker
        for f in worker_sector_files.values():
            f.close()
    
    elapsed = time.time() - start_time
    print(f"    ✅ {file_path.name}: {systems_written:,} systems streamed in {elapsed:.1f}s", flush=True)
    
    return systems_written, file_path.name

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

def process_file_pass3(args: Tuple) -> Tuple[int, str]:
    """
    Process a single file for Pass 3: assign non-standard systems.
    Returns (systems_assigned, file_name)
    """
    file_path, output_dir, valid_sectors, sector_centers = args
    systems_assigned = 0
    progress_interval = 50000
    
    # Track which sector files this worker has opened
    worker_sector_files = {}
    
    print(f"  🎯 Worker assigning: {file_path.name}", flush=True)
    start_time = time.time()
    
    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                if line_num % progress_interval == 0:
                    elapsed = time.time() - start_time
                    rate = line_num / elapsed if elapsed > 0 else 0
                    print(f"    🎯 {file_path.name}: {line_num:,} systems ({rate:,.0f}/sec)", flush=True)
                
                try:
                    system = json.loads(line.strip())
                    if 'name' not in system:
                        continue
                    
                    system_name = system['name']
                    sector_name, mass_code = parse_system_name(system_name)
                    
                    # Only process non-standard systems
                    if not sector_name:
                        coords = (system.get('coords', {}).get('x', 0),
                                 system.get('coords', {}).get('y', 0),
                                 system.get('coords', {}).get('z', 0))
                        
                        # Find nearest valid sector
                        nearest_sector = find_nearest_sector(coords, sector_centers)
                        if nearest_sector in valid_sectors:
                            # Open sector file if not already opened by this worker
                            if nearest_sector not in worker_sector_files:
                                filename = sanitize_filename(nearest_sector) + '.jsonl'
                                file_path_sector = Path(output_dir) / filename
                                worker_sector_files[nearest_sector] = open(file_path_sector, 'a')
                            
                            # Write system to nearest sector file
                            worker_sector_files[nearest_sector].write(line)
                            systems_assigned += 1
                        
                except json.JSONDecodeError:
                    continue
    
    finally:
        # Close all sector files opened by this worker
        for f in worker_sector_files.values():
            f.close()
    
    elapsed = time.time() - start_time
    print(f"    ✅ {file_path.name}: {systems_assigned:,} non-standard assigned in {elapsed:.1f}s", flush=True)
    
    return systems_assigned, file_path.name

def main():
    input_dir = Path("Databases/galaxy_chunks_annotated")
    output_dir = Path("Databases/galaxy_sectors")
    workers = 12  # Use 12 processors as requested
    
    if not input_dir.exists():
        print(f"❌ Error: Input directory {input_dir} does not exist", flush=True)
        return
    
    # Create output directory
    output_dir.mkdir(exist_ok=True)
    
    print("🌌 Building sector-organized galaxy database (12 Processors)...", flush=True)
    print("=" * 60, flush=True)
    
    # Get list of JSONL files
    jsonl_files = list(input_dir.glob("*.jsonl"))
    print(f"📊 Found {len(jsonl_files)} JSONL files to process", flush=True)
    total_size = sum(f.stat().st_size for f in jsonl_files) / (1024**3)
    print(f"📦 Total data size: {total_size:.1f} GB", flush=True)
    print(f"🚀 Using {workers} worker processes", flush=True)
    
    # PASS 1: Collect sector statistics in parallel
    print(f"\n🚀 Pass 1: Collecting sector statistics with {workers} workers...", flush=True)
    all_sector_stats = defaultdict(lambda: {'count': 0, 'sum_x': 0.0, 'sum_y': 0.0, 'sum_z': 0.0})
    total_standard = 0
    total_non_standard = 0
    
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit all files for processing
        future_to_file = {executor.submit(process_file_pass1, file_path): file_path 
                         for file_path in jsonl_files}
        
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                file_stats, standard_count, non_standard_count = future.result()
                
                # Merge statistics
                for sector_name, stats in file_stats.items():
                    all_stats = all_sector_stats[sector_name]
                    all_stats['count'] += stats['count']
                    all_stats['sum_x'] += stats['sum_x']
                    all_stats['sum_y'] += stats['sum_y']
                    all_stats['sum_z'] += stats['sum_z']
                
                total_standard += standard_count
                total_non_standard += non_standard_count
                
                print(f"✅ Completed statistics for: {file_path.name}", flush=True)
                
            except Exception as e:
                print(f"❌ Error processing {file_path}: {e}", flush=True)
    
    total_systems = total_standard + total_non_standard
    
    print(f"\n✅ Pass 1 complete:", flush=True)
    print(f"  📊 Total systems: {total_systems:,}", flush=True)
    print(f"  ✨ Standard systems: {total_standard:,}", flush=True)
    print(f"  ❓ Non-standard systems: {total_non_standard:,}", flush=True)
    print(f"  🏢 Sectors discovered: {len(all_sector_stats):,}", flush=True)
    
    # Calculate sector centers and filter by minimum size
    min_systems_per_sector = 10
    sector_centers = {}
    valid_sectors = {}
    
    print(f"\n📐 Pass 2: Calculating sector centers (min {min_systems_per_sector} systems)...", flush=True)
    for sector_name, stats in all_sector_stats.items():
        count = stats['count']
        if count >= min_systems_per_sector:
            # Calculate center from running sums
            center_x = stats['sum_x'] / count
            center_y = stats['sum_y'] / count  
            center_z = stats['sum_z'] / count
            
            sector_centers[sector_name] = (center_x, center_y, center_z)
            valid_sectors[sector_name] = count
    
    print(f"✅ Valid sectors: {len(valid_sectors):,}", flush=True)
    
    # Create empty sector files
    print(f"📁 Creating {len(valid_sectors):,} empty sector files...", flush=True)
    for sector_name in valid_sectors:
        filename = sanitize_filename(sector_name) + '.jsonl'
        file_path = output_dir / filename
        # Create empty file
        with open(file_path, 'w') as f:
            pass
    
    # PASS 2: Stream standard systems to sector files in parallel
    print(f"\n💾 Pass 2: Streaming standard systems with {workers} workers...", flush=True)
    
    total_written = 0
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit all files for processing
        future_to_file = {executor.submit(process_file_pass2, (file_path, output_dir, valid_sectors)): file_path 
                         for file_path in jsonl_files}
        
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                systems_written, file_name = future.result()
                total_written += systems_written
                print(f"✅ Completed streaming: {file_name} ({systems_written:,} systems)", flush=True)
                
            except Exception as e:
                print(f"❌ Error streaming {file_path}: {e}", flush=True)
    
    # PASS 3: Assign non-standard systems in parallel
    print(f"\n🎯 Pass 3: Assigning non-standard systems with {workers} workers...", flush=True)
    
    total_assigned = 0
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit all files for processing
        future_to_file = {executor.submit(process_file_pass3, (file_path, output_dir, valid_sectors, sector_centers)): file_path 
                         for file_path in jsonl_files}
        
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                systems_assigned, file_name = future.result()
                total_assigned += systems_assigned
                print(f"✅ Completed assignment: {file_name} ({systems_assigned:,} systems)", flush=True)
                
            except Exception as e:
                print(f"❌ Error assigning {file_path}: {e}", flush=True)
    
    # Create sector index
    sector_index = {
        "metadata": {
            "total_systems": total_systems,
            "total_sectors": len(valid_sectors),
            "min_systems_per_sector": min_systems_per_sector,
            "source_files": [f.name for f in jsonl_files],
            "non_standard_systems": total_non_standard,
            "standard_systems": total_standard,
            "workers_used": workers,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
        },
        "sectors": {}
    }
    
    for sector_name in valid_sectors:
        center_coords = sector_centers[sector_name]
        sector_index["sectors"][sector_name] = {
            "filename": f"{sanitize_filename(sector_name)}.jsonl",
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
    
    total_elapsed = time.time() - start_time
    
    print(f"\n🎉 Database reorganization complete!", flush=True)
    print(f"  ⏱️  Total time: {total_elapsed:.1f}s", flush=True)
    print(f"  🚀 Workers used: {workers}", flush=True)
    print(f"  📊 Total systems: {total_systems:,}", flush=True)
    print(f"  ✨ Standard systems: {total_written:,}", flush=True)
    print(f"  🎯 Non-standard assigned: {total_assigned:,}", flush=True)
    print(f"  📁 Sector files created: {len(valid_sectors):,}", flush=True)
    print(f"  📂 Output directory: {output_dir}", flush=True)
    print(f"  📋 Sector index: {index_file}", flush=True)
    
    # Show top sectors by system count
    print(f"\n🏆 Top 10 largest sectors:", flush=True)
    sorted_sectors = sorted(valid_sectors.items(), key=lambda x: x[1], reverse=True)
    for i, (sector_name, count) in enumerate(sorted_sectors[:10]):
        center = sector_centers[sector_name]
        print(f"  {i+1:2d}. {sector_name:<25} {count:>8,} systems | Center: ({center[0]:>8.1f}, {center[1]:>8.1f}, {center[2]:>8.1f})", flush=True)

if __name__ == "__main__":
    main()