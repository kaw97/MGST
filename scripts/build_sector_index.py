#!/usr/bin/env python3
"""
Build sector index file from existing compressed database.
Creates sector_index.json with sector names and center coordinates.
"""

import json
import gzip
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

def parse_system_name(system_name: str):
    """Parse system name to extract sector and mass code."""
    mass_code_pattern = r'\b([A-Z]{2}-[A-Z])\b'
    match = re.search(mass_code_pattern, system_name)

    if match:
        mass_code = match.group(1)
        mass_code_start = match.start()
        sector_name = system_name[:mass_code_start].strip()
        return sector_name, mass_code

    return None, None

def build_sector_index(database_path: Path, output_path: Path, sample_size: int = 200):
    """Build sector index by sampling systems from each sector file."""

    print(f"Building sector index from: {database_path}")
    print(f"Output file: {output_path}")
    print(f"Sample size per sector: {sample_size}")
    print("=" * 60)

    sector_stats = defaultdict(lambda: {
        'count': 0,
        'sum_x': 0.0,
        'sum_y': 0.0,
        'sum_z': 0.0,
        'filename': '',
        'systems_sampled': 0
    })

    # Process each sector file
    sector_files = list(database_path.glob("*.jsonl.gz"))
    print(f"Found {len(sector_files)} sector files to process")

    for i, sector_file in enumerate(sector_files):
        sector_filename = sector_file.name
        # Convert filename to sector name (reverse of sanitization)
        sector_name = sector_file.stem.replace('.jsonl', '').replace('_', ' ')

        print(f"Processing [{i+1}/{len(sector_files)}]: {sector_name}")

        stats = sector_stats[sector_name]
        stats['filename'] = sector_filename

        try:
            with gzip.open(sector_file, 'rt', encoding='utf-8') as f:
                for line_num, line in enumerate(f):
                    if line_num >= sample_size:  # Sample first N systems
                        break

                    try:
                        system = json.loads(line.strip())
                        coords = system.get('coords', {})

                        if coords and all(k in coords for k in ['x', 'y', 'z']):
                            x, y, z = coords['x'], coords['y'], coords['z']
                            stats['count'] += 1
                            stats['sum_x'] += x
                            stats['sum_y'] += y
                            stats['sum_z'] += z
                            stats['systems_sampled'] += 1

                        # Also validate systematic naming for this sector
                        system_name = system.get('name', '')
                        if system_name:
                            parsed_sector, mass_code = parse_system_name(system_name)
                            if parsed_sector and parsed_sector != sector_name:
                                # File contains systems from different sectors
                                stats['mixed_content'] = True

                    except json.JSONDecodeError:
                        continue

        except Exception as e:
            print(f"  Error reading {sector_file}: {e}")
            continue

        if stats['systems_sampled'] > 0:
            print(f"  Sampled {stats['systems_sampled']} systems")
        else:
            print(f"  No valid coordinates found")

    # Calculate sector centers and build index
    print("\nCalculating sector centers...")

    sector_index = {
        "metadata": {
            "created_at": datetime.now().isoformat(),
            "source_database": str(database_path),
            "sample_size_per_sector": sample_size,
            "total_sectors": 0,
            "valid_sectors": 0
        },
        "sectors": {}
    }

    valid_sectors = 0

    for sector_name, stats in sector_stats.items():
        sector_index["metadata"]["total_sectors"] += 1

        if stats['count'] > 0:
            # Calculate center coordinates
            center_x = stats['sum_x'] / stats['count']
            center_y = stats['sum_y'] / stats['count']
            center_z = stats['sum_z'] / stats['count']

            sector_index["sectors"][sector_name] = {
                "filename": stats['filename'],
                "center_coords": {
                    "x": round(center_x, 2),
                    "y": round(center_y, 2),
                    "z": round(center_z, 2)
                },
                "systems_sampled": stats['systems_sampled'],
                "has_mixed_content": stats.get('mixed_content', False)
            }

            valid_sectors += 1
            print(f"  {sector_name}: ({center_x:.1f}, {center_y:.1f}, {center_z:.1f}) - {stats['systems_sampled']} systems")
        else:
            print(f"  {sector_name}: No valid coordinates - skipped")

    sector_index["metadata"]["valid_sectors"] = valid_sectors

    # Save the index
    print(f"\nSaving sector index...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(sector_index, f, indent=2, ensure_ascii=False)

    print(f"✅ Sector index saved: {output_path}")
    print(f"📊 Summary:")
    print(f"  Total sectors found: {sector_index['metadata']['total_sectors']}")
    print(f"  Valid sectors with coordinates: {valid_sectors}")
    print(f"  Index file size: {output_path.stat().st_size / 1024:.1f} KB")

    return output_path

def main():
    database_path = Path('Databases/galaxy_sectors_compressed')
    output_path = Path('Databases/galaxy_sectors_compressed/sector_index.json')

    if not database_path.exists():
        print(f"Database directory not found: {database_path}")
        return

    if output_path.exists():
        print(f"Sector index already exists: {output_path}")
        response = input("Overwrite? (y/N): ").strip().lower()
        if response != 'y':
            print("Aborted.")
            return

    build_sector_index(database_path, output_path, sample_size=200)

if __name__ == "__main__":
    main()