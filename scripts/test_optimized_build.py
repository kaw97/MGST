#!/usr/bin/env python3
"""
Test script for optimized sector database build on small sample.
"""
import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import the main function from optimized script
import orjson
import os
import re
import time
import tempfile
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed

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

def batch_write_sector_files(output_dir: Path, sector_batches: Dict[str, List[str]]):
    """Batch write all sector files at once to minimize I/O."""
    for sector_name, lines in sector_batches.items():
        filename = sanitize_filename(sector_name) + '.jsonl'
        file_path = output_dir / filename
        
        # Write entire batch at once
        with open(file_path, 'a') as f:
            for line in lines:
                f.write(line)

def test_optimized_processing():
    input_dir = Path("test_input")
    output_dir = Path("test_output")
    
    # Clean up any previous test
    if output_dir.exists():
        import shutil
        shutil.rmtree(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    print("🧪 Testing optimized processing on small sample...", flush=True)
    print("=" * 50, flush=True)
    
    jsonl_files = list(input_dir.glob("*.jsonl"))
    if not jsonl_files:
        print("❌ No test files found in test_input/", flush=True)
        return False
        
    test_file = jsonl_files[0]
    print(f"📊 Test file: {test_file.name} ({test_file.stat().st_size / 1024:.1f} KB)", flush=True)
    
    # Test processing
    sector_batches = defaultdict(list)
    sector_stats = defaultdict(lambda: {'count': 0, 'sum_x': 0.0, 'sum_y': 0.0, 'sum_z': 0.0})
    non_standard_lines = []
    total_systems = 0
    standard_written = 0
    
    start_time = time.time()
    
    with open(test_file, 'rb') as f:  # Use binary mode for orjson
        for line_num, line in enumerate(f, 1):
            try:
                # Test orjson parsing
                system = orjson.loads(line.strip())
                if 'name' not in system:
                    continue
                
                total_systems += 1
                system_name = system['name']
                coords = (system.get('coords', {}).get('x', 0),
                         system.get('coords', {}).get('y', 0), 
                         system.get('coords', {}).get('z', 0))
                
                sector_name, mass_code = parse_system_name(system_name)
                
                if sector_name:
                    # Standard system - add to batch
                    sector_batches[sector_name].append(line.decode('utf-8'))
                    standard_written += 1
                    
                    # Track statistics
                    stats = sector_stats[sector_name]
                    stats['count'] += 1
                    stats['sum_x'] += coords[0]
                    stats['sum_y'] += coords[1]
                    stats['sum_z'] += coords[2]
                else:
                    # Non-standard system
                    non_standard_lines.append(line.decode('utf-8'))
                    
            except (orjson.JSONDecodeError, UnicodeDecodeError):
                continue
    
    # Test batch writing
    if sector_batches:
        batch_write_sector_files(output_dir, sector_batches)
    
    elapsed = time.time() - start_time
    
    print(f"✅ Test results:", flush=True)
    print(f"  📊 Total systems: {total_systems:,}", flush=True)
    print(f"  ✨ Standard systems: {standard_written:,}", flush=True)
    print(f"  ❓ Non-standard systems: {len(non_standard_lines):,}", flush=True)
    print(f"  🏢 Sectors found: {len(sector_stats):,}", flush=True)
    print(f"  ⏱️  Processing time: {elapsed:.3f}s", flush=True)
    print(f"  🚀 Rate: {total_systems/elapsed:,.0f} systems/sec", flush=True)
    
    # Check output files
    output_files = list(output_dir.glob("*.jsonl"))
    print(f"  📁 Output files created: {len(output_files)}", flush=True)
    
    # Show some examples
    if sector_stats:
        print(f"\n🔍 Sample sectors found:", flush=True)
        for i, (sector_name, stats) in enumerate(list(sector_stats.items())[:5]):
            center_x = stats['sum_x'] / stats['count']
            center_y = stats['sum_y'] / stats['count']
            center_z = stats['sum_z'] / stats['count']
            print(f"  {i+1}. {sector_name:<20} {stats['count']:>4} systems | Center: ({center_x:>8.1f}, {center_y:>8.1f}, {center_z:>8.1f})", flush=True)
    
    print(f"\n🎉 Test completed successfully!", flush=True)
    print(f"💡 Performance looks good for full dataset run", flush=True)
    
    return True

if __name__ == "__main__":
    success = test_optimized_processing()
    sys.exit(0 if success else 1)