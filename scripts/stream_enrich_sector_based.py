#!/usr/bin/env python3
"""
Sector-based multithreaded enrichment for maximum efficiency.
Groups codex entries by sector, assigns each worker a sector to process.
Eliminates file contention and minimizes I/O operations.
"""

import json
import gzip
import re
import threading
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
import argparse
import time

@dataclass
class StellarData:
    """Container for stellar characteristics."""
    spectral_class: str = ""
    solar_masses: float = 0.0
    surface_temperature: float = 0.0
    luminosity: str = ""
    age: int = 0
    main_star_count: int = 0
    system_type: str = ""

@dataclass
class BodyData:
    """Container for specific body characteristics where discovery was made."""
    distance_to_arrival: float = 0.0
    body_type: str = ""
    planet_class: str = ""
    mass_em: float = 0.0
    radius: float = 0.0
    surface_gravity: float = 0.0
    surface_temperature: float = 0.0
    atmosphere: str = ""
    terraforming_state: str = ""
    orbital_period: float = 0.0
    semi_major_axis: float = 0.0
    body_found: bool = False

def parse_system_name(system_name: str) -> Optional[str]:
    """Extract sector name from systematic system naming."""
    mass_code_pattern = r'\b([A-Z]{2}-[A-Z])\b'
    match = re.search(mass_code_pattern, system_name)

    if match:
        mass_code_start = match.start()
        sector_name = system_name[:mass_code_start].strip()
        return sector_name
    return None

def sanitize_filename(sector_name: str) -> str:
    """Convert sector name to filename format."""
    return sector_name.replace(' ', '_')

def extract_stellar_characteristics(system_data: Dict[str, Any]) -> StellarData:
    """Extract stellar characteristics from system data."""
    stellar_data = StellarData()

    # Get primary star data
    stars = system_data.get('stars', [])
    if stars:
        primary_star = stars[0]
        stellar_data.spectral_class = primary_star.get('type', '')
        stellar_data.solar_masses = primary_star.get('solarMasses', 0.0)
        stellar_data.surface_temperature = primary_star.get('surfaceTemperature', 0.0)
        stellar_data.luminosity = primary_star.get('luminosity', '')
        stellar_data.age = primary_star.get('age', 0)

        # Determine system type
        stellar_data.main_star_count = len(stars)
        stellar_data.system_type = "Multi-star" if len(stars) > 1 else "Single-star"

    return stellar_data

def find_specific_body(system_data: Dict[str, Any], codex_entry: Dict[str, Any]) -> BodyData:
    """Find the specific body where the species was discovered."""
    body_data = BodyData()

    # Extract body name from codex entry
    codex_body_name = (codex_entry.get('body') or '').strip()
    if not codex_body_name:
        return body_data

    # Search through all bodies in the system
    bodies = system_data.get('bodies', [])
    for body in bodies:
        body_name = body.get('name', '')

        # Try exact match first
        if body_name == codex_body_name:
            body_data = extract_body_data(body)
            body_data.body_found = True
            break

        # Try partial match (body name might be truncated in codex)
        elif codex_body_name in body_name or body_name in codex_body_name:
            body_data = extract_body_data(body)
            body_data.body_found = True
            break

    return body_data

def extract_body_data(body: Dict[str, Any]) -> BodyData:
    """Extract body characteristics from body data."""
    return BodyData(
        distance_to_arrival=body.get('distanceToArrival', 0.0),
        body_type=body.get('type', ''),
        planet_class=body.get('planetClass', ''),
        mass_em=body.get('massEM', 0.0),
        radius=body.get('radius', 0.0),
        surface_gravity=body.get('surfaceGravity', 0.0),
        surface_temperature=body.get('surfaceTemperature', 0.0),
        atmosphere=body.get('atmosphere', ''),
        terraforming_state=body.get('terraformingState', ''),
        orbital_period=body.get('orbitalPeriod', 0.0),
        semi_major_axis=body.get('semiMajorAxis', 0.0),
        body_found=True
    )

def process_sector(sector_name: str, codex_entries: List[Dict[str, Any]],
                  database_path: Path, results: List[Dict[str, Any]],
                  results_lock: threading.Lock, stats: Dict[str, int],
                  stats_lock: threading.Lock) -> None:
    """Process all codex entries for a specific sector."""

    # Load sector file into memory once
    sector_filename = sanitize_filename(sector_name) + ".jsonl.gz"
    sector_file_path = database_path / sector_filename

    # Try .gz first, then .jsonl
    if not sector_file_path.exists():
        sector_file_path = database_path / (sanitize_filename(sector_name) + ".jsonl")
        if not sector_file_path.exists():
            with stats_lock:
                stats['sectors_not_found'] += 1
                stats['entries_skipped'] += len(codex_entries)
            return

    # Load all systems from sector into memory
    sector_systems = {}
    try:
        if sector_file_path.suffix == '.gz':
            with gzip.open(sector_file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        system = json.loads(line.strip())
                        system_name = system.get('name', '')
                        if system_name:
                            sector_systems[system_name] = system
                    except json.JSONDecodeError:
                        continue
        else:
            with open(sector_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        system = json.loads(line.strip())
                        system_name = system.get('name', '')
                        if system_name:
                            sector_systems[system_name] = system
                    except json.JSONDecodeError:
                        continue

        with stats_lock:
            stats['sectors_loaded'] += 1
            stats['systems_loaded'] += len(sector_systems)

    except Exception as e:
        with stats_lock:
            stats['sectors_failed'] += 1
            stats['entries_skipped'] += len(codex_entries)
        return

    # Process all codex entries for this sector
    sector_results = []
    for codex_entry in codex_entries:
        system_name = codex_entry.get('system', '')

        if system_name in sector_systems:
            system_data = sector_systems[system_name]

            # Extract stellar characteristics
            stellar_data = extract_stellar_characteristics(system_data)

            # Find specific body data
            body_data = find_specific_body(system_data, codex_entry)

            # Create enriched entry
            enriched_entry = {
                **codex_entry,
                'stellar_spectral_class': stellar_data.spectral_class,
                'stellar_solar_masses': stellar_data.solar_masses,
                'stellar_surface_temperature': stellar_data.surface_temperature,
                'stellar_luminosity': stellar_data.luminosity,
                'stellar_age': stellar_data.age,
                'stellar_main_star_count': stellar_data.main_star_count,
                'stellar_system_type': stellar_data.system_type,
                'body_distance_to_arrival': body_data.distance_to_arrival,
                'body_type': body_data.body_type,
                'body_planet_class': body_data.planet_class,
                'body_mass_em': body_data.mass_em,
                'body_radius': body_data.radius,
                'body_surface_gravity': body_data.surface_gravity,
                'body_surface_temperature': body_data.surface_temperature,
                'body_atmosphere': body_data.atmosphere,
                'body_terraforming_state': body_data.terraforming_state,
                'body_orbital_period': body_data.orbital_period,
                'body_semi_major_axis': body_data.semi_major_axis,
                'body_found': body_data.body_found,
                'enrichment_timestamp': datetime.now().isoformat()
            }

            sector_results.append(enriched_entry)

            with stats_lock:
                stats['entries_enriched'] += 1
                if body_data.body_found:
                    stats['bodies_matched'] += 1
        else:
            with stats_lock:
                stats['systems_not_found'] += 1

    # Add results to shared list
    with results_lock:
        results.extend(sector_results)

    with stats_lock:
        stats['entries_processed'] += len(codex_entries)

def group_codex_by_sector(codex_file: Path, max_entries: Optional[int] = None) -> Dict[str, List[Dict[str, Any]]]:
    """Group codex entries by their sector names."""
    print("📊 Grouping codex entries by sector...")

    sector_groups = defaultdict(list)
    total_entries = 0
    systematic_entries = 0

    with open(codex_file, 'r', encoding='utf-8') as f:
        for line in f:
            if max_entries and total_entries >= max_entries:
                break

            try:
                entry = json.loads(line.strip())
                total_entries += 1

                system_name = entry.get('system', '')
                sector_name = parse_system_name(system_name)

                if sector_name:
                    sector_groups[sector_name].append(entry)
                    systematic_entries += 1

            except json.JSONDecodeError:
                continue

    print(f"✅ Grouped {systematic_entries:,} systematic entries into {len(sector_groups):,} sectors")
    print(f"📈 {systematic_entries/total_entries*100:.1f}% of entries are systematic")

    return dict(sector_groups)

def main():
    parser = argparse.ArgumentParser(description='Sector-based multithreaded codex enrichment')
    parser.add_argument('--max-entries', type=int, help='Limit number of codex entries to process')
    parser.add_argument('--workers', type=int, default=8, help='Number of worker threads')
    args = parser.parse_args()

    codex_file = Path('Databases/codex.json/codex.jsonl')
    database_path = Path('Databases/galaxy_sectors_compressed')
    output_dir = Path(f'output/sector_enriched_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / 'enriched_codex.jsonl'

    print("🚀 Starting sector-based enrichment...")
    print(f"📂 Codex file: {codex_file}")
    print(f"🗄️ Database: {database_path}")
    print(f"📤 Output: {output_file}")
    print(f"👥 Workers: {args.workers}")
    if args.max_entries:
        print(f"🔢 Max entries: {args.max_entries:,}")
    print("=" * 60)

    start_time = time.time()

    # Group codex entries by sector
    sector_groups = group_codex_by_sector(codex_file, args.max_entries)

    if not sector_groups:
        print("❌ No systematic entries found!")
        return

    # Prepare shared data structures
    results = []
    results_lock = threading.Lock()
    stats = {
        'sectors_loaded': 0,
        'sectors_not_found': 0,
        'sectors_failed': 0,
        'systems_loaded': 0,
        'entries_processed': 0,
        'entries_enriched': 0,
        'entries_skipped': 0,
        'systems_not_found': 0,
        'bodies_matched': 0
    }
    stats_lock = threading.Lock()

    # Create and start worker threads
    threads = []
    sectors = list(sector_groups.keys())
    sectors_per_worker = len(sectors) // args.workers + (1 if len(sectors) % args.workers else 0)

    print(f"🏗️ Distributing {len(sectors):,} sectors across {args.workers} workers...")
    print(f"📦 ~{sectors_per_worker} sectors per worker")

    for i in range(args.workers):
        start_idx = i * sectors_per_worker
        end_idx = min((i + 1) * sectors_per_worker, len(sectors))
        worker_sectors = sectors[start_idx:end_idx]

        def worker_func(sector_list=worker_sectors, worker_id=i):
            for sector_name in sector_list:
                codex_entries = sector_groups[sector_name]
                process_sector(sector_name, codex_entries, database_path,
                             results, results_lock, stats, stats_lock)

                # Progress update
                with stats_lock:
                    if stats['sectors_loaded'] % 50 == 0:
                        elapsed = time.time() - start_time
                        print(f"🔄 Worker {worker_id}: {stats['sectors_loaded']:,} sectors processed, "
                              f"{stats['entries_enriched']:,} entries enriched ({elapsed:.1f}s)")

        thread = threading.Thread(target=worker_func)
        threads.append(thread)
        thread.start()

    # Wait for all workers to complete
    for thread in threads:
        thread.join()

    total_time = time.time() - start_time

    # Write results
    print(f"\n💾 Writing {len(results):,} enriched entries...")
    with open(output_file, 'w', encoding='utf-8') as f:
        for entry in results:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')

    # Final statistics
    print(f"\n✅ Sector-based enrichment complete!")
    print(f"📊 Final Statistics:")
    print(f"   • Total processing time: {total_time/60:.1f} minutes")
    print(f"   • Sectors processed: {stats['sectors_loaded']:,}")
    print(f"   • Sectors not found: {stats['sectors_not_found']:,}")
    print(f"   • Systems loaded: {stats['systems_loaded']:,}")
    print(f"   • Entries enriched: {stats['entries_enriched']:,}")
    print(f"   • Bodies matched: {stats['bodies_matched']:,}")
    print(f"   • Body match rate: {stats['bodies_matched']/stats['entries_enriched']*100:.1f}%")
    print(f"   • Processing rate: {stats['entries_enriched']/total_time*60:.0f} entries/minute")
    print(f"📁 Output saved to: {output_file}")

if __name__ == "__main__":
    main()