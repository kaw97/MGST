#!/usr/bin/env python3
"""
Phase 2: Enrich codex entries from identified sectors only.
Processes only sectors that contain systematic codex entries for maximum efficiency.
"""

import json
import gzip
import threading
import time
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from collections import defaultdict
import argparse

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

def sanitize_filename(sector_name: str) -> str:
    """Convert sector name to filename format."""
    return sector_name.replace(' ', '_').replace('/', '_')

def extract_stellar_characteristics(system_data: Dict[str, Any]) -> StellarData:
    """Extract stellar characteristics from system data."""
    stellar_data = StellarData()

    # Get star bodies (stars are stored as bodies with type="Star")
    bodies = system_data.get('bodies', [])
    star_bodies = [body for body in bodies if body.get('type') == 'Star']

    if star_bodies:
        # Find primary star (first one with mainStar=True, or just first star)
        primary_star = None
        for star in star_bodies:
            if star.get('mainStar', False):
                primary_star = star
                break
        if not primary_star:
            primary_star = star_bodies[0]  # Fallback to first star

        stellar_data.spectral_class = primary_star.get('spectralClass', '') or primary_star.get('subType', '')
        stellar_data.solar_masses = primary_star.get('solarMasses', 0.0)
        stellar_data.surface_temperature = primary_star.get('surfaceTemperature', 0.0)
        stellar_data.luminosity = primary_star.get('luminosity', '')
        stellar_data.age = primary_star.get('age', 0)

        # Determine system type
        stellar_data.main_star_count = len(star_bodies)
        stellar_data.system_type = "Multi-star" if len(star_bodies) > 1 else "Single-star"

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

    # First try exact match
    for body in bodies:
        body_name = body.get('name', '')
        if body_name == codex_body_name:
            body_data = extract_body_data(body)
            body_data.body_found = True
            return body_data

    # If no exact match, try more sophisticated matching
    # Filter to non-star bodies first (planets, moons, etc.)
    non_star_bodies = [body for body in bodies if body.get('type') != 'Star']

    for body in non_star_bodies:
        body_name = body.get('name', '')

        # Check if codex body name is contained in database body name
        # This handles cases where codex might have truncated names
        if codex_body_name in body_name:
            body_data = extract_body_data(body)
            body_data.body_found = True
            return body_data

    # Last resort: check all bodies (including stars) but prefer closest match
    best_match = None
    best_score = 0

    for body in bodies:
        body_name = body.get('name', '')

        # Simple matching score based on common characters
        if codex_body_name in body_name or body_name in codex_body_name:
            score = len(set(codex_body_name.split()) & set(body_name.split()))
            # Prefer non-star bodies
            if body.get('type') != 'Star':
                score += 10

            if score > best_score:
                best_score = score
                best_match = body

    if best_match:
        body_data = extract_body_data(best_match)
        body_data.body_found = True

    return body_data

def extract_body_data(body: Dict[str, Any]) -> BodyData:
    """Extract body characteristics from body data."""
    return BodyData(
        distance_to_arrival=body.get('distanceToArrival', 0.0),
        body_type=body.get('type', ''),
        planet_class=body.get('subType', ''),  # subType contains the planet class
        mass_em=body.get('earthMasses', 0.0),  # earthMasses not massEM
        radius=body.get('radius', 0.0),
        surface_gravity=body.get('gravity', 0.0),  # gravity not surfaceGravity
        surface_temperature=body.get('surfaceTemperature', 0.0),
        atmosphere=body.get('atmosphereType', ''),  # atmosphereType not atmosphere
        terraforming_state=body.get('terraformingState', ''),
        orbital_period=body.get('orbitalPeriod', 0.0),
        semi_major_axis=body.get('semiMajorAxis', 0.0),
        body_found=True
    )

def process_sector_file(sector_file_path: Path, database_path: Path,
                       output_file: Path, output_lock: threading.Lock,
                       stats: Dict[str, int], stats_lock: threading.Lock) -> None:
    """Process a single sector codex file."""
    sector_name = sector_file_path.stem  # Remove .jsonl extension

    # Convert back from sanitized filename to actual sector name
    actual_sector_name = sector_name.replace('_', ' ')

    # Load corresponding galaxy database sector file
    galaxy_sector_filename = sector_name + ".jsonl.gz"
    galaxy_sector_path = database_path / galaxy_sector_filename

    # Try .gz first, then .jsonl
    if not galaxy_sector_path.exists():
        galaxy_sector_path = database_path / (sector_name + ".jsonl")
        if not galaxy_sector_path.exists():
            with stats_lock:
                stats['sectors_not_found'] += 1
            return

    # Get systems needed from codex entries first (memory efficient)
    needed_systems = set()
    try:
        with open(sector_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    codex_entry = json.loads(line.strip())
                    system_name = codex_entry.get('system', '')
                    if system_name:
                        needed_systems.add(system_name)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        with stats_lock:
            stats['sectors_failed'] += 1
        return

    # Load only needed systems from galaxy sector (memory efficient)
    sector_systems = {}
    systems_loaded_count = 0
    try:
        if galaxy_sector_path.suffix == '.gz':
            with gzip.open(galaxy_sector_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        system = json.loads(line.strip())
                        system_name = system.get('name', '')
                        if system_name in needed_systems:
                            sector_systems[system_name] = system
                            systems_loaded_count += 1
                            # Early exit if we found all needed systems
                            if len(sector_systems) == len(needed_systems):
                                break
                    except json.JSONDecodeError:
                        continue
        else:
            with open(galaxy_sector_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        system = json.loads(line.strip())
                        system_name = system.get('name', '')
                        if system_name in needed_systems:
                            sector_systems[system_name] = system
                            systems_loaded_count += 1
                            # Early exit if we found all needed systems
                            if len(sector_systems) == len(needed_systems):
                                break
                    except json.JSONDecodeError:
                        continue

        with stats_lock:
            stats['sectors_loaded'] += 1
            stats['systems_loaded'] += systems_loaded_count

    except Exception as e:
        with stats_lock:
            stats['sectors_failed'] += 1
        return

    # Process all codex entries for this sector and stream output
    entries_processed = 0
    try:
        with open(sector_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    codex_entry = json.loads(line.strip())
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

                        # Write directly to output file (thread-safe)
                        with output_lock:
                            with open(output_file, 'a', encoding='utf-8') as out_f:
                                out_f.write(json.dumps(enriched_entry, ensure_ascii=False) + '\n')

                        entries_processed += 1

                        with stats_lock:
                            stats['entries_enriched'] += 1
                            if body_data.body_found:
                                stats['bodies_matched'] += 1
                    else:
                        with stats_lock:
                            stats['systems_not_found'] += 1

                except json.JSONDecodeError:
                    continue

        with stats_lock:
            stats['entries_processed'] += entries_processed

    except Exception as e:
        with stats_lock:
            stats['files_failed'] += 1
        return

def main():
    parser = argparse.ArgumentParser(description='Enrich codex entries from identified sectors only')
    parser.add_argument('--codex-sectors-dir', required=True,
                       help='Directory containing extracted systematic codex sector files')
    parser.add_argument('--workers', type=int, default=8, help='Number of worker threads')
    args = parser.parse_args()

    # Paths
    codex_sectors_dir = Path(args.codex_sectors_dir)
    database_path = Path('Databases/galaxy_sectors_compressed')
    output_dir = Path(f'output/identified_sectors_enriched_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / 'enriched_codex.jsonl'

    print("🚀 Starting identified sectors enrichment...")
    print(f"📂 Codex sectors: {codex_sectors_dir}")
    print(f"🗄️ Galaxy database: {database_path}")
    print(f"📤 Output: {output_file}")
    print(f"👥 Workers: {args.workers}")
    print("=" * 60)

    if not codex_sectors_dir.exists():
        print(f"❌ Codex sectors directory not found: {codex_sectors_dir}")
        return

    if not database_path.exists():
        print(f"❌ Galaxy database not found: {database_path}")
        return

    # Get all sector files to process
    sector_files = list(codex_sectors_dir.glob("*.jsonl"))
    if not sector_files:
        print(f"❌ No sector files found in: {codex_sectors_dir}")
        return

    print(f"📊 Found {len(sector_files):,} sector files to process")

    start_time = time.time()

    # Prepare shared data structures
    output_lock = threading.Lock()
    stats = {
        'sectors_loaded': 0,
        'sectors_not_found': 0,
        'sectors_failed': 0,
        'files_failed': 0,
        'systems_loaded': 0,
        'entries_processed': 0,
        'entries_enriched': 0,
        'systems_not_found': 0,
        'bodies_matched': 0
    }
    stats_lock = threading.Lock()

    # Create empty output file
    with open(output_file, 'w', encoding='utf-8') as f:
        pass  # Create empty file

    # Distribute sector files across workers
    files_per_worker = len(sector_files) // args.workers + (1 if len(sector_files) % args.workers else 0)

    print(f"🏗️ Distributing {len(sector_files):,} files across {args.workers} workers...")
    print(f"📦 ~{files_per_worker} files per worker")

    # Create and start worker threads
    threads = []
    for i in range(args.workers):
        start_idx = i * files_per_worker
        end_idx = min((i + 1) * files_per_worker, len(sector_files))
        worker_files = sector_files[start_idx:end_idx]

        def worker_func(file_list=worker_files, worker_id=i):
            for sector_file in file_list:
                process_sector_file(sector_file, database_path, output_file, output_lock, stats, stats_lock)

                # Progress update
                with stats_lock:
                    if stats['sectors_loaded'] % 50 == 0 and stats['sectors_loaded'] > 0:
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

    # Count final results
    result_count = stats['entries_enriched']
    print(f"\n💾 Streaming output complete: {result_count:,} enriched entries written")

    # Create processing summary
    summary = {
        "processing_timestamp": datetime.now().isoformat(),
        "codex_sectors_directory": str(codex_sectors_dir),
        "galaxy_database_path": str(database_path),
        "total_processing_time_minutes": total_time / 60,
        "sector_files_found": len(sector_files),
        "sectors_successfully_loaded": stats['sectors_loaded'],
        "sectors_not_found_in_database": stats['sectors_not_found'],
        "sectors_failed_to_load": stats['sectors_failed'],
        "codex_files_failed": stats['files_failed'],
        "total_systems_loaded": stats['systems_loaded'],
        "total_entries_processed": stats['entries_processed'],
        "total_entries_enriched": stats['entries_enriched'],
        "systems_not_found": stats['systems_not_found'],
        "bodies_successfully_matched": stats['bodies_matched'],
        "body_match_rate_percent": stats['bodies_matched'] / stats['entries_enriched'] * 100 if stats['entries_enriched'] > 0 else 0,
        "processing_rate_entries_per_minute": stats['entries_enriched'] / total_time * 60 if total_time > 0 else 0
    }

    summary_file = output_dir / 'processing_summary.json'
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # Final statistics
    print(f"\n✅ Identified sectors enrichment complete!")
    print(f"📊 Final Statistics:")
    print(f"   • Total processing time: {total_time/60:.1f} minutes")
    print(f"   • Sector files processed: {len(sector_files):,}")
    print(f"   • Sectors loaded from database: {stats['sectors_loaded']:,}")
    print(f"   • Sectors not found: {stats['sectors_not_found']:,}")
    print(f"   • Total systems loaded: {stats['systems_loaded']:,}")
    print(f"   • Entries processed: {stats['entries_processed']:,}")
    print(f"   • Entries enriched: {stats['entries_enriched']:,}")
    print(f"   • Bodies matched: {stats['bodies_matched']:,}")
    print(f"   • Body match rate: {stats['bodies_matched']/stats['entries_enriched']*100:.1f}%")
    print(f"   • Processing rate: {stats['entries_enriched']/total_time*60:.0f} entries/minute")
    print(f"📁 Output saved to: {output_file}")
    print(f"📄 Summary saved to: {summary_file}")

if __name__ == "__main__":
    main()