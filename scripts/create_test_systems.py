#!/usr/bin/env python3
"""
Create test systems from enriched codex data for validating enhanced species prediction rules.
Extracts known positive examples where species actually occur.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any
import argparse
import random

def extract_test_systems(enriched_codex_file: Path, target_species: List[str], samples_per_species: int = 10) -> Dict[str, List[Dict]]:
    """Extract test systems where target species are known to occur."""
    test_systems = {species: [] for species in target_species}

    print(f"Extracting test systems from {enriched_codex_file}")
    print(f"Target species: {target_species}")
    print(f"Samples per species: {samples_per_species}")
    print()

    processed = 0
    found_species = {species: 0 for species in target_species}

    with open(enriched_codex_file, 'r') as f:
        for line in f:
            processed += 1
            if processed % 100000 == 0:
                print(f"  Processed {processed:,} entries...")

            try:
                entry = json.loads(line.strip())
                english_name = entry.get('english_name', '')

                # Check if this entry matches any target species
                for species in target_species:
                    if species in english_name and len(test_systems[species]) < samples_per_species:

                        # Create test system structure
                        test_system = {
                            'system_name': entry.get('system', 'Unknown'),
                            'body_name': entry.get('body', 'Unknown'),
                            'species_found': english_name,
                            'stellar_class': entry.get('stellar_spectral_class', 'Unknown'),
                            'stellar_temperature': entry.get('stellar_surface_temperature', 0),
                            'stellar_system_type': entry.get('stellar_system_type', 'Unknown'),
                            'body_data': {
                                'subType': entry.get('body_type', ''),
                                'atmosphereType': entry.get('body_atmosphere', ''),
                                'surfacePressure': 0.05,  # Default suitable pressure
                                'surfaceTemperature': entry.get('body_surface_temperature', 0),
                                'gravity': entry.get('body_surface_gravity', 0.1),
                                'distanceToArrival': entry.get('body_distance_to_arrival', 1000),
                                'updateTime': '2021-01-01T00:00:00Z'  # Before threshold
                            },
                            'coordinates': {
                                'x': float(entry.get('x', 0)),
                                'y': float(entry.get('y', 0)),
                                'z': float(entry.get('z', 0))
                            }
                        }

                        test_systems[species].append(test_system)
                        found_species[species] += 1

                        if all(len(systems) >= samples_per_species for systems in test_systems.values()):
                            print(f"Found enough samples for all species at entry {processed:,}")
                            break

            except json.JSONDecodeError:
                continue

    print(f"\nExtraction complete. Processed {processed:,} entries")
    for species, count in found_species.items():
        print(f"  {species}: {count} test systems")

    return test_systems

def create_galaxy_system_format(test_data: Dict) -> Dict:
    """Convert test data to galaxy database system format."""
    stellar_class = test_data['stellar_class']
    stellar_temp = test_data['stellar_temperature']

    return {
        'name': test_data['system_name'],
        'id64': hash(test_data['system_name']) % (2**63),
        'coords': test_data['coordinates'],
        'stars': [{
            'name': f"{test_data['system_name']} A",
            'mainStar': True,
            'spectralClass': stellar_class,
            'surfaceTemperature': stellar_temp,
            'solarMasses': 1.0  # Default
        }] if stellar_class != 'Unknown' else [],
        'bodies': [{
            'name': test_data['body_name'],
            'bodyName': test_data['body_name'],
            **test_data['body_data'],
            # Add stellar data to body for compatibility
            'stellar_spectral_class': stellar_class,
            'stellar_surface_temperature': stellar_temp,
            'stellar_system_type': test_data['stellar_system_type']
        }]
    }

def main():
    parser = argparse.ArgumentParser(description='Create test systems from enriched codex data')
    parser.add_argument('--enriched-codex', required=True, help='Path to enriched codex JSONL file')
    parser.add_argument('--output-dir', required=True, help='Directory to save test systems')
    parser.add_argument('--samples', type=int, default=5, help='Samples per species')

    args = parser.parse_args()

    # Define target species for testing (major species with clear stellar preferences)
    target_species = [
        'Stratum Tectonicas',
        'Bacterium Vesicula',
        'Bacterium Acies',
        'Fonticulua Campestris',
        'Stratum Paleas',
        'Bacterium Aurasus',
        'Osseus Spiralis'
    ]

    enriched_file = Path(args.enriched_codex)
    output_dir = Path(args.output_dir)

    if not enriched_file.exists():
        print(f"Error: Enriched codex file not found: {enriched_file}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Extract test systems
    test_systems = extract_test_systems(enriched_file, target_species, args.samples)

    # Save raw test data
    with open(output_dir / 'test_systems_raw.json', 'w') as f:
        json.dump(test_systems, f, indent=2)

    # Convert to galaxy system format for testing
    galaxy_test_systems = {}

    for species, systems in test_systems.items():
        galaxy_systems = []
        for system_data in systems:
            galaxy_system = create_galaxy_system_format(system_data)
            galaxy_systems.append(galaxy_system)
        galaxy_test_systems[species] = galaxy_systems

    # Save galaxy-format test systems
    with open(output_dir / 'test_systems_galaxy_format.json', 'w') as f:
        json.dump(galaxy_test_systems, f, indent=2)

    print(f"\nTest systems saved to:")
    print(f"  Raw data: {output_dir / 'test_systems_raw.json'}")
    print(f"  Galaxy format: {output_dir / 'test_systems_galaxy_format.json'}")

    # Display sample
    print(f"\nSample test system (Stratum Tectonicas):")
    if 'Stratum Tectonicas' in galaxy_test_systems and galaxy_test_systems['Stratum Tectonicas']:
        sample = galaxy_test_systems['Stratum Tectonicas'][0]
        print(f"  System: {sample['name']}")
        print(f"  Stellar class: {sample['stars'][0]['spectralClass'] if sample['stars'] else 'Unknown'}")
        print(f"  Stellar temp: {sample['stars'][0]['surfaceTemperature'] if sample['stars'] else 'Unknown'}K")
        print(f"  Body type: {sample['bodies'][0]['subType']}")
        print(f"  Atmosphere: {sample['bodies'][0]['atmosphereType']}")
        print(f"  Body temp: {sample['bodies'][0]['surfaceTemperature']}K")

if __name__ == "__main__":
    main()