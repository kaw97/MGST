#!/usr/bin/env python3
"""
Extract star-specific temperature and distance ranges for species from the stellar analysis.
Creates detailed range specifications for enhanced prediction rules.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Tuple
import argparse

def extract_species_stellar_ranges(stellar_analysis: Dict[str, Any], min_observations: int = 100) -> Dict[str, Dict]:
    """Extract detailed stellar ranges for each species."""
    species_ranges = {}

    for species_key, species_data in stellar_analysis.items():
        if species_data.get('single_star_entries', 0) < min_observations:
            continue

        species_name = species_key.replace('_', ' ')
        star_analysis = species_data.get('star_type_analysis', {})

        stellar_class_ranges = {}

        # Group by stellar class and extract ranges
        for star_type, analysis in star_analysis.items():
            if analysis.get('count', 0) < 50:  # Minimum per star type
                continue

            stellar_class = star_type[0] if star_type else 'Unknown'

            if stellar_class not in stellar_class_ranges:
                stellar_class_ranges[stellar_class] = {
                    'star_types': [],
                    'observations': 0,
                    'stellar_temp_range': None,
                    'body_temp_range': None,
                    'distance_range': None,
                    'thermal_regulation_quality': 'unknown'
                }

            class_data = stellar_class_ranges[stellar_class]
            class_data['star_types'].append(star_type)
            class_data['observations'] += analysis['count']

            # Extract temperature and distance data
            stellar_temp = analysis.get('stellar_temperature', {})
            body_temp = analysis.get('body_temperature', {})
            distance = analysis.get('distance_to_arrival', {})

            # Stellar temperature range (mean ± 2 std dev for ~95% coverage)
            if stellar_temp.get('count', 0) > 0 and 'std_dev' in stellar_temp:
                mean = stellar_temp['mean']
                std = stellar_temp['std_dev']
                temp_range = (max(0, mean - 2*std), mean + 2*std)

                if class_data['stellar_temp_range'] is None:
                    class_data['stellar_temp_range'] = temp_range
                else:
                    # Expand range to include this star type
                    current_min, current_max = class_data['stellar_temp_range']
                    class_data['stellar_temp_range'] = (
                        min(current_min, temp_range[0]),
                        max(current_max, temp_range[1])
                    )

            # Body temperature range (mean ± 2 std dev)
            if body_temp.get('count', 0) > 0 and 'std_dev' in body_temp:
                mean = body_temp['mean']
                std = body_temp['std_dev']
                temp_range = (mean - 2*std, mean + 2*std)

                if class_data['body_temp_range'] is None:
                    class_data['body_temp_range'] = temp_range
                else:
                    current_min, current_max = class_data['body_temp_range']
                    class_data['body_temp_range'] = (
                        min(current_min, temp_range[0]),
                        max(current_max, temp_range[1])
                    )

            # Distance range (mean ± 2 std dev)
            if distance.get('count', 0) > 0 and 'std_dev' in distance:
                mean = distance['mean']
                std = distance['std_dev']
                dist_range = (max(0, mean - 2*std), mean + 2*std)

                if class_data['distance_range'] is None:
                    class_data['distance_range'] = dist_range
                else:
                    current_min, current_max = class_data['distance_range']
                    class_data['distance_range'] = (
                        min(current_min, dist_range[0]),
                        max(current_max, dist_range[1])
                    )

            # Thermal regulation assessment (using body temp std dev)
            if body_temp.get('count', 0) > 0 and 'std_dev' in body_temp:
                temp_variation = body_temp['std_dev']
                if temp_variation < 15:
                    thermal_quality = 'excellent'
                elif temp_variation < 30:
                    thermal_quality = 'good'
                elif temp_variation < 50:
                    thermal_quality = 'moderate'
                else:
                    thermal_quality = 'poor'

                class_data['thermal_regulation_quality'] = thermal_quality

        # Only include species with significant stellar class data
        if stellar_class_ranges:
            species_ranges[species_name] = {
                'total_observations': species_data.get('single_star_entries', 0),
                'stellar_class_ranges': stellar_class_ranges
            }

    return species_ranges

def format_range_for_display(range_tuple, unit=""):
    """Format a range tuple for display."""
    if range_tuple is None:
        return "No data"
    min_val, max_val = range_tuple
    return f"{min_val:.0f}{unit} - {max_val:.0f}{unit}"

def main():
    parser = argparse.ArgumentParser(description='Extract species stellar ranges for enhanced prediction')
    parser.add_argument('--stellar-analysis', required=True, help='Path to stellar preferences detailed JSON')
    parser.add_argument('--output-file', required=True, help='Path to save species ranges JSON')
    parser.add_argument('--min-observations', type=int, default=1000, help='Minimum observations per species')

    args = parser.parse_args()

    # Load stellar analysis
    with open(args.stellar_analysis, 'r') as f:
        stellar_analysis = json.load(f)

    print("=== EXTRACTING SPECIES STELLAR RANGES ===")
    print(f"Input: {args.stellar_analysis}")
    print(f"Minimum observations: {args.min_observations}")
    print()

    # Extract ranges
    species_ranges = extract_species_stellar_ranges(stellar_analysis, args.min_observations)

    print(f"Extracted ranges for {len(species_ranges)} species")
    print()

    # Display sample ranges
    print("SAMPLE SPECIES RANGES:")
    print("=" * 80)

    for species_name in list(species_ranges.keys())[:5]:  # Show first 5
        data = species_ranges[species_name]
        print(f"{species_name} ({data['total_observations']:,} observations):")

        for stellar_class, class_data in data['stellar_class_ranges'].items():
            print(f"  {stellar_class}-class ({class_data['observations']:,} obs):")
            print(f"    Stellar temp: {format_range_for_display(class_data['stellar_temp_range'], 'K')}")
            print(f"    Body temp:    {format_range_for_display(class_data['body_temp_range'], 'K')}")
            print(f"    Distance:     {format_range_for_display(class_data['distance_range'], ' ls')}")
            print(f"    Thermal reg:  {class_data['thermal_regulation_quality']}")
            print()
        print()

    # Save results
    output_file = Path(args.output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump(species_ranges, f, indent=2)

    print(f"Species ranges saved to: {output_file}")

if __name__ == "__main__":
    main()