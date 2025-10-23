#!/usr/bin/env python3
"""
Analyze database structure to identify available fields for bodies and stars.
This script examines a small sample of systems to determine what fields are available
for distance-to-star filtering and stellar characteristics.
"""

import json
import gzip
from pathlib import Path
from collections import defaultdict, Counter
import sys

def analyze_system_sample(file_path, max_systems=50):
    """Analyze a small sample of systems to identify available fields."""

    body_fields = Counter()
    star_fields = Counter()
    planet_fields = Counter()
    distance_fields = set()
    stellar_characteristics = set()

    systems_analyzed = 0

    try:
        if file_path.suffix == '.gz':
            file_handle = gzip.open(file_path, 'rt', encoding='utf-8')
        else:
            file_handle = open(file_path, 'r', encoding='utf-8')

        with file_handle as f:
            for line_num, line in enumerate(f):
                if systems_analyzed >= max_systems:
                    break

                try:
                    system = json.loads(line.strip())
                    systems_analyzed += 1

                    # Analyze bodies
                    for body in system.get('bodies', []):
                        # Count all body fields
                        for field in body.keys():
                            body_fields[field] += 1

                        # Specifically analyze stars
                        if body.get('type') == 'Star':
                            for field in body.keys():
                                star_fields[field] += 1
                                stellar_characteristics.add(field)

                        # Specifically analyze planets
                        elif body.get('type') == 'Planet':
                            for field in body.keys():
                                planet_fields[field] += 1

                        # Check for distance-related fields
                        for field in body.keys():
                            if 'distance' in field.lower() or 'semi' in field.lower() or 'orbital' in field.lower():
                                distance_fields.add(field)

                    if systems_analyzed % 10 == 0:
                        print(f"Analyzed {systems_analyzed} systems...", file=sys.stderr)

                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"Error processing line {line_num}: {e}", file=sys.stderr)
                    continue

    except Exception as e:
        print(f"Error reading file {file_path}: {e}", file=sys.stderr)
        return None

    return {
        'systems_analyzed': systems_analyzed,
        'body_fields': dict(body_fields.most_common()),
        'star_fields': dict(star_fields.most_common()),
        'planet_fields': dict(planet_fields.most_common()),
        'distance_fields': sorted(distance_fields),
        'stellar_characteristics': sorted(stellar_characteristics)
    }

def main():
    # Find a sample database file
    database_dirs = [
        Path('Databases/galaxy_sectors_compressed'),
        Path('Databases/galaxy_chunks_annotated'),
        Path('output/enhanced_exobiology_full_jsonl_20250920_0950')
    ]

    sample_file = None
    for db_dir in database_dirs:
        if db_dir.exists():
            for file_path in db_dir.glob('*.jsonl*'):
                sample_file = file_path
                break
            if sample_file:
                break

    if not sample_file:
        print("No database files found to analyze")
        return

    print(f"Analyzing database structure from: {sample_file}")
    print("=" * 60)

    results = analyze_system_sample(sample_file, max_systems=100)

    if not results:
        print("Failed to analyze database structure")
        return

    print(f"Analysis Results (from {results['systems_analyzed']} systems):")
    print()

    print("DISTANCE/ORBITAL RELATED FIELDS:")
    print("-" * 40)
    for field in results['distance_fields']:
        print(f"  {field}")
    print()

    print("STELLAR CHARACTERISTICS (Star bodies):")
    print("-" * 40)
    for field in results['stellar_characteristics']:
        print(f"  {field}")
    print()

    print("MOST COMMON BODY FIELDS:")
    print("-" * 40)
    for field, count in list(results['body_fields'].items())[:20]:
        print(f"  {field}: {count}")
    print()

    print("PLANET-SPECIFIC FIELDS:")
    print("-" * 40)
    planet_only = set(results['planet_fields'].keys()) - set(results['star_fields'].keys())
    for field in sorted(planet_only)[:15]:
        print(f"  {field}")
    print()

    # Look for specific fields we're interested in
    key_fields = ['distanceToArrival', 'semiMajorAxis', 'orbitalPeriod', 'parents',
                  'spectralClass', 'solarMasses', 'surfaceTemperature', 'luminosity']

    print("KEY FIELDS FOR DISTANCE/STELLAR ANALYSIS:")
    print("-" * 40)
    for field in key_fields:
        if field in results['body_fields']:
            count = results['body_fields'][field]
            percentage = (count / results['systems_analyzed']) * 100
            print(f"  {field}: Present in {count} bodies ({percentage:.1f}% of systems)")
        else:
            print(f"  {field}: NOT FOUND")

if __name__ == "__main__":
    main()