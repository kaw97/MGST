#!/usr/bin/env python3
"""
Final optimized sector database reorganization.
Single pass + temp file approach: stream standard systems directly while collecting statistics,
write non-standard to temp file, then process only temp file for assignments.
"""
import json
import os
import re
import sys
import time
import tempfile
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
import fcntl

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

def sanitize_filename(sector_name: str) -> str:
    """Convert sector name to safe filename."""
    # Replace problematic characters with underscores
    safe_chars = []
    for char in sector_name:
        if char.isalnum() or char in [' ', '-', '_']:
            safe_chars.append(char)
        else:
            safe_chars.append('_')
    
    # Replace spaces with underscores and collapse multiple underscores
    safe_name = ''.join(safe_chars).replace(' ', '_')
    while '__' in safe_name:
        safe_name = safe_name.replace('__', '_')
    
    return safe_name.strip('_')

def safe_write_to_sector_file(output_dir: Path, sector_name: str, line: str):
    """Safely write line to sector file with file locking."""
    filename = sanitize_filename(sector_name) + '.jsonl'
    file_path = output_dir / filename
    
    # Use append mode with file locking
    with open(file_path, 'a') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(line)
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)

def safe_write_to_temp_file(temp_file_path: Path, line: str):
    """Safely write line to temporary file with file locking."""
    with open(temp_file_path, 'a') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(line)
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)

def process_file_single_pass(args) -> Tuple[Dict, int, int, int]:
    """
    Single pass: stream standard systems to files while collecting statistics,
    write non-standard systems to temp file.
    Returns (sector_stats, total_systems, standard_written, non_standard_count)
    """
    jsonl_file, output_dir, temp_file_path, progress_interval = args
    
    sector_stats = defaultdict(lambda: {'count': 0, 'sum_x': 0.0, 'sum_y': 0.0, 'sum_z': 0.0})
    total_systems = 0
    standard_written = 0
    non_standard_count = 0
    
    print(f"🔄 Processing {jsonl_file.name} ({jsonl_file.stat().st_size / (1024**3):.1f} GB)", flush=True)
    start_time = time.time()
    
    with open(jsonl_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % progress_interval == 0:
                elapsed = time.time() - start_time
                rate = line_num / elapsed if elapsed > 0 else 0
                print(f"  📊 {jsonl_file.name}: {line_num:,} systems ({rate:,.0f}/sec)", flush=True)
            
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
                    # Standard system - stream directly to sector file
                    safe_write_to_sector_file(output_dir, sector_name, line)
                    standard_written += 1
                    
                    # Track statistics with running sums (for center calculation)
                    stats = sector_stats[sector_name]
                    stats['count'] += 1
                    stats['sum_x'] += coords[0]
                    stats['sum_y'] += coords[1]
                    stats['sum_z'] += coords[2]
                else:
                    # Non-standard system - write to temp file
                    safe_write_to_temp_file(temp_file_path, line)
                    non_standard_count += 1
                    
            except json.JSONDecodeError:
                continue
    
    elapsed = time.time() - start_time
    print(f"  ✅ {jsonl_file.name}: {total_systems:,} systems in {elapsed:.1f}s ({total_systems/elapsed:.0f}/sec)", flush=True)
    
    return dict(sector_stats), total_systems, standard_written, non_standard_count

def process_temp_file_for_assignment(temp_file_path: Path, output_dir: Path, 
                                   sector_centers: Dict[str, Tuple[float, float, float]],
                                   progress_interval: int = 50000) -> int:
    """
    Process temporary file to assign non-standard systems to nearest sectors.
    Returns count of non-standard systems assigned.
    """
    non_standard_assigned = 0
    
    if not temp_file_path.exists():
        print("  ℹ️  No temporary file found - no non-standard systems to assign", flush=True)
        return 0
    
    print(f"🎯 Processing temporary file: {temp_file_path}", flush=True)
    print(f"  📦 Temp file size: {temp_file_path.stat().st_size / (1024**2):.1f} MB", flush=True)
    start_time = time.time()
    
    with open(temp_file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % progress_interval == 0:
                elapsed = time.time() - start_time
                rate = line_num / elapsed if elapsed > 0 else 0
                print(f"  🎯 Processed {line_num:,} non-standard systems ({rate:,.0f}/sec)", flush=True)
            
            try:
                system = json.loads(line.strip())
                if 'name' not in system:
                    continue
                
                coords = (system.get('coords', {}).get('x', 0),
                         system.get('coords', {}).get('y', 0),
                         system.get('coords', {}).get('z', 0))
                
                # Find nearest valid sector
                nearest_sector = find_nearest_sector(coords, sector_centers)
                if nearest_sector != "Unknown":
                    safe_write_to_sector_file(output_dir, nearest_sector, line)
                    non_standard_assigned += 1
                        
            except json.JSONDecodeError:
                continue
    
    elapsed = time.time() - start_time
    print(f"  ✅ Temp file processed: {non_standard_assigned:,} non-standard assigned in {elapsed:.1f}s", flush=True)
    
    return non_standard_assigned

def main():
    input_dir = Path("Databases/galaxy_chunks_annotated")
    output_dir = Path("Databases/galaxy_sectors")
    
    if not input_dir.exists():
        print(f"❌ Error: Input directory {input_dir} does not exist", flush=True)
        return
    
    # Create output directory
    output_dir.mkdir(exist_ok=True)
    
    print("🌌 Building sector-organized galaxy database (Final Optimized)...", flush=True)
    print("=" * 60, flush=True)
    
    # Get list of JSONL files
    jsonl_files = list(input_dir.glob("*.jsonl"))
    print(f"📊 Found {len(jsonl_files)} JSONL files to process", flush=True)
    total_size = sum(f.stat().st_size for f in jsonl_files) / (1024**3)
    print(f"📦 Total data size: {total_size:.1f} GB", flush=True)
    
    # Create temporary file for non-standard systems
    temp_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.jsonl', delete=False, 
                                           dir=output_dir, prefix='non_standard_')
    temp_file_path = Path(temp_file.name)
    temp_file.close()  # Close but don't delete
    
    print(f"📝 Temporary file for non-standard systems: {temp_file_path.name}", flush=True)
    
    # PASS 1: Stream standard systems to sector files while collecting statistics,
    # write non-standard systems to temp file
    print(f"\n🚀 Pass 1: Processing all systems (standard → sector files, non-standard → temp file)...", flush=True)
    
    progress_interval = 50000
    start_time = time.time()
    
    # Prepare arguments for parallel processing
    pass1_args = [(jsonl_file, output_dir, temp_file_path, progress_interval) for jsonl_file in jsonl_files]
    
    # Process files in parallel (12 workers)
    all_sector_stats = defaultdict(lambda: {'count': 0, 'sum_x': 0.0, 'sum_y': 0.0, 'sum_z': 0.0})
    total_systems = 0
    standard_written = 0
    non_standard_count = 0
    
    with ProcessPoolExecutor(max_workers=12) as executor:
        future_to_file = {executor.submit(process_file_single_pass, args): args[0] for args in pass1_args}
        
        for future in as_completed(future_to_file):
            try:
                file_stats, file_systems, file_standard, file_non_standard = future.result()
                
                # Merge statistics
                for sector_name, stats in file_stats.items():
                    all_stats = all_sector_stats[sector_name]
                    all_stats['count'] += stats['count']
                    all_stats['sum_x'] += stats['sum_x']
                    all_stats['sum_y'] += stats['sum_y']
                    all_stats['sum_z'] += stats['sum_z']
                
                total_systems += file_systems
                standard_written += file_standard
                non_standard_count += file_non_standard
                
                # Show running totals
                elapsed = time.time() - start_time
                avg_rate = total_systems / elapsed if elapsed > 0 else 0
                print(f"📊 Running totals: {total_systems:,} systems, {standard_written:,} standard, {non_standard_count:,} non-standard ({avg_rate:,.0f} systems/sec)", flush=True)
                
            except Exception as e:
                file_path = future_to_file[future]
                print(f"❌ Error processing {file_path.name}: {e}", flush=True)
    
    print(f"\n✅ Pass 1 complete:", flush=True)
    print(f"  📊 Total systems processed: {total_systems:,}", flush=True)
    print(f"  ✨ Standard systems written: {standard_written:,}", flush=True)
    print(f"  ❓ Non-standard systems in temp file: {non_standard_count:,}", flush=True)
    print(f"  🏢 Sectors discovered: {len(all_sector_stats):,}", flush=True)
    print(f"  📝 Temp file size: {temp_file_path.stat().st_size / (1024**2):.1f} MB", flush=True)
    
    # Calculate sector centers and create index
    print(f"\n📐 Calculating sector centers and creating index...", flush=True)
    min_systems_per_sector = 10
    sector_centers = {}
    valid_sectors = {}
    
    for sector_name, stats in all_sector_stats.items():
        count = stats['count']
        if count >= min_systems_per_sector:
            # Calculate center from running sums
            center_x = stats['sum_x'] / count
            center_y = stats['sum_y'] / count  
            center_z = stats['sum_z'] / count
            
            sector_centers[sector_name] = (center_x, center_y, center_z)
            valid_sectors[sector_name] = count
        else:
            # Remove file for sectors below minimum threshold
            filename = sanitize_filename(sector_name) + '.jsonl'
            file_path = output_dir / filename
            if file_path.exists():
                file_path.unlink()
                print(f"  🗑️  Removed {filename} (only {count} systems, below minimum {min_systems_per_sector})", flush=True)
    
    print(f"✅ Valid sectors (>= {min_systems_per_sector} systems): {len(valid_sectors):,}", flush=True)
    
    # Create sector index
    sector_index = {
        "metadata": {
            "total_systems": total_systems,
            "total_sectors": len(valid_sectors),
            "min_systems_per_sector": min_systems_per_sector,
            "source_files": [f.name for f in jsonl_files],
            "non_standard_systems": non_standard_count,
            "standard_systems": standard_written,
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
    
    print(f"✅ Created sector index: {index_file}", flush=True)
    
    # PASS 2: Process only the temporary file to assign non-standard systems
    print(f"\n🎯 Pass 2: Assigning non-standard systems from temp file to nearest sectors...", flush=True)
    
    non_standard_assigned = process_temp_file_for_assignment(
        temp_file_path, output_dir, sector_centers, progress_interval
    )
    
    # Clean up temporary file
    if temp_file_path.exists():
        temp_file_path.unlink()
        print(f"🗑️  Cleaned up temporary file: {temp_file_path.name}", flush=True)
    
    total_elapsed = time.time() - start_time
    
    print(f"\n🎉 Database reorganization complete!", flush=True)
    print(f"  ⏱️  Total time: {total_elapsed:.1f}s", flush=True)
    print(f"  📊 Total systems: {total_systems:,}", flush=True)
    print(f"  ✨ Standard systems: {standard_written:,}", flush=True)
    print(f"  🎯 Non-standard assigned: {non_standard_assigned:,}", flush=True)
    print(f"  📁 Sector files created: {len(valid_sectors):,}", flush=True)
    print(f"  📂 Output directory: {output_dir}", flush=True)
    
    # Calculate efficiency improvements
    if total_systems > 0:
        temp_efficiency = (non_standard_count / total_systems) * 100
        print(f"\n📈 Efficiency improvements:", flush=True)
        print(f"  💾 Pass 2 only processed {temp_efficiency:.1f}% of total data (temp file)", flush=True)
        print(f"  🚀 Avoided re-reading {total_size * (1 - temp_efficiency/100):.1f} GB in Pass 2", flush=True)
    
    # Show top sectors by system count
    print(f"\n🏆 Top 10 largest sectors:", flush=True)
    sorted_sectors = sorted(valid_sectors.items(), key=lambda x: x[1], reverse=True)
    for i, (sector_name, count) in enumerate(sorted_sectors[:10]):
        center = sector_centers[sector_name]
        print(f"  {i+1:2d}. {sector_name:<25} {count:>8,} systems | Center: ({center[0]:>8.1f}, {center[1]:>8.1f}, {center[2]:>8.1f})", flush=True)

if __name__ == "__main__":
    main()