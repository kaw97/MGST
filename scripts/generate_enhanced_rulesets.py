#!/usr/bin/env python3
"""
Generate enhanced species rulesets incorporating stellar adaptation data.
Creates star-class specific rules based on empirical analysis of 3.45M codex entries.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Tuple
import argparse

def load_stellar_analysis(analysis_file: Path) -> Dict[str, Any]:
    """Load the stellar analysis data."""
    with open(analysis_file, 'r') as f:
        return json.load(f)

def load_existing_rulesets(rulesets_dir: Path) -> Dict[str, Any]:
    """Load existing species rulesets."""
    import importlib.util
    species_data = {}

    for ruleset_file in rulesets_dir.glob("*.py"):
        if ruleset_file.name.startswith("__"):
            continue

        try:
            # Load the module dynamically
            spec = importlib.util.spec_from_file_location("ruleset", ruleset_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            if hasattr(module, 'catalog'):
                catalog = module.catalog

                # Extract species from catalog
                for genus_key, genus_data in catalog.items():
                    for species_key, species_info in genus_data.items():
                        species_name = species_info.get('name', 'Unknown')
                        value = species_info.get('value', 0)
                        rulesets = species_info.get('rulesets', [])

                        species_data[species_name] = {
                            'genus': genus_key,
                            'genus_name': genus_key,
                            'species_key': species_key,
                            'value': value,
                            'rulesets': rulesets
                        }

        except Exception as e:
            print(f"Warning: Could not load ruleset {ruleset_file}: {e}")
            continue

    return species_data

def calculate_stellar_class_ranges(star_analysis: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Calculate optimal ranges for each stellar class."""
    stellar_classes = {}

    for star_type, analysis in star_analysis.items():
        if analysis.get('count', 0) < 50:  # Skip types with insufficient data
            continue

        stellar_class = star_type[0] if star_type else 'Unknown'

        if stellar_class not in stellar_classes:
            stellar_classes[stellar_class] = {
                'entries': 0,
                'stellar_temps': [],
                'body_temps': [],
                'distances': [],
                'star_types': []
            }

        stellar_classes[stellar_class]['entries'] += analysis['count']
        stellar_classes[stellar_class]['star_types'].append(star_type)

        # Collect temperature and distance data
        stellar_temp = analysis.get('stellar_temperature', {})
        body_temp = analysis.get('body_temperature', {})
        distance = analysis.get('distance_to_arrival', {})

        if stellar_temp.get('count', 0) > 0:
            stellar_classes[stellar_class]['stellar_temps'].append(stellar_temp['mean'])
        if body_temp.get('count', 0) > 0:
            stellar_classes[stellar_class]['body_temps'].append(body_temp['mean'])
        if distance.get('count', 0) > 0:
            stellar_classes[stellar_class]['distances'].append(distance['mean'])

    # Calculate aggregate statistics for each class
    for stellar_class, data in stellar_classes.items():
        if data['stellar_temps']:
            data['stellar_temp_range'] = (min(data['stellar_temps']), max(data['stellar_temps']))
            data['avg_stellar_temp'] = sum(data['stellar_temps']) / len(data['stellar_temps'])
        if data['body_temps']:
            data['body_temp_range'] = (min(data['body_temps']), max(data['body_temps']))
            data['avg_body_temp'] = sum(data['body_temps']) / len(data['body_temps'])
        if data['distances']:
            data['distance_range'] = (min(data['distances']), max(data['distances']))
            data['avg_distance'] = sum(data['distances']) / len(data['distances'])

    return stellar_classes

def generate_enhanced_ruleset(species_name: str, original_data: Dict[str, Any],
                             stellar_data: Dict[str, Any]) -> Dict[str, Any]:
    """Generate enhanced ruleset for a species incorporating stellar data."""
    species_key = species_name.replace(' ', '_')

    enhanced_ruleset = {
        'name': species_name,
        'genus': original_data.get('genus', 'Unknown'),
        'value': original_data.get('value', 0),
        'original_rulesets': original_data.get('rulesets', []),
        'stellar_enhanced_rulesets': [],
        'stellar_analysis_available': species_key in stellar_data
    }

    if species_key not in stellar_data:
        # No stellar data available, use original rules
        enhanced_ruleset['stellar_enhanced_rulesets'] = original_data.get('rulesets', [])
        enhanced_ruleset['enhancement_note'] = "No stellar analysis data available - using original rules"
        return enhanced_ruleset

    species_stellar = stellar_data[species_key]
    star_analysis = species_stellar.get('star_type_analysis', {})

    # Group by stellar class and create enhanced rules
    stellar_class_data = calculate_stellar_class_ranges(star_analysis)

    for stellar_class, class_data in stellar_class_data.items():
        if class_data['entries'] < 100:  # Skip classes with insufficient data
            continue

        # Create enhanced ruleset for this stellar class
        enhanced_rule = {
            'stellar_class': stellar_class,
            'confidence_level': 'high' if class_data['entries'] > 1000 else 'moderate',
            'sample_size': class_data['entries'],
            'enhancement_source': 'empirical_analysis_3.45M_entries'
        }

        # Add stellar temperature constraints
        if 'stellar_temp_range' in class_data:
            enhanced_rule['stellar_temperature_range'] = class_data['stellar_temp_range']
            enhanced_rule['optimal_stellar_temperature'] = class_data['avg_stellar_temp']

        # Add body temperature expectations
        if 'body_temp_range' in class_data:
            enhanced_rule['expected_body_temperature_range'] = class_data['body_temp_range']
            enhanced_rule['optimal_body_temperature'] = class_data['avg_body_temp']

        # Add orbital distance constraints
        if 'distance_range' in class_data:
            enhanced_rule['orbital_distance_range'] = class_data['distance_range']
            enhanced_rule['optimal_orbital_distance'] = class_data['avg_distance']

        # Include star types for this class
        enhanced_rule['common_star_types'] = class_data['star_types']

        # Calculate thermal regulation quality
        body_temp_span = class_data['body_temp_range'][1] - class_data['body_temp_range'][0] if 'body_temp_range' in class_data else 0
        if body_temp_span < 15:
            thermal_regulation = 'excellent'
        elif body_temp_span < 30:
            thermal_regulation = 'good'
        elif body_temp_span < 50:
            thermal_regulation = 'moderate'
        else:
            thermal_regulation = 'poor'

        enhanced_rule['thermal_regulation_quality'] = thermal_regulation
        enhanced_rule['body_temperature_span'] = body_temp_span

        # Merge with original atmospheric and body type constraints
        for original_rule in original_data.get('rulesets', []):
            merged_rule = {**enhanced_rule}  # Copy stellar enhancements

            # Add original constraints
            for key, value in original_rule.items():
                if key not in ['regions']:  # Skip region constraints for now
                    merged_rule[f'original_{key}'] = value

            enhanced_ruleset['stellar_enhanced_rulesets'].append(merged_rule)

    # Add enhancement metadata
    enhanced_ruleset['enhancement_note'] = f"Enhanced with stellar analysis from {species_stellar.get('single_star_entries', 0)} single-star observations"
    enhanced_ruleset['preferred_stellar_classes'] = list(stellar_class_data.keys())
    enhanced_ruleset['total_observations'] = species_stellar.get('single_star_entries', 0)

    return enhanced_ruleset

def main():
    parser = argparse.ArgumentParser(description='Generate enhanced species rulesets with stellar adaptation data')
    parser.add_argument('--stellar-analysis', required=True, help='Path to stellar preferences detailed JSON file')
    parser.add_argument('--rulesets-dir', required=True, help='Path to existing rulesets directory')
    parser.add_argument('--output-dir', required=True, help='Path to save enhanced rulesets')
    parser.add_argument('--species-filter', help='Only process specific species (comma-separated)')

    args = parser.parse_args()

    stellar_file = Path(args.stellar_analysis)
    rulesets_dir = Path(args.rulesets_dir)
    output_dir = Path(args.output_dir)

    if not stellar_file.exists():
        print(f"Error: Stellar analysis file {stellar_file} does not exist")
        sys.exit(1)

    if not rulesets_dir.exists():
        print(f"Error: Rulesets directory {rulesets_dir} does not exist")
        sys.exit(1)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=== GENERATING ENHANCED SPECIES RULESETS ===")
    print(f"Loading stellar analysis from: {stellar_file}")
    print(f"Loading existing rulesets from: {rulesets_dir}")
    print(f"Output directory: {output_dir}")
    print()

    # Load data
    stellar_analysis = load_stellar_analysis(stellar_file)
    existing_rulesets = load_existing_rulesets(rulesets_dir)

    print(f"Loaded stellar analysis for {len(stellar_analysis)} species")
    print(f"Loaded existing rulesets for {len(existing_rulesets)} species")

    # Filter species if requested
    if args.species_filter:
        filter_species = [s.strip() for s in args.species_filter.split(',')]
        existing_rulesets = {k: v for k, v in existing_rulesets.items() if k in filter_species}
        print(f"Filtered to {len(existing_rulesets)} species: {', '.join(existing_rulesets.keys())}")

    # Generate enhanced rulesets
    enhanced_rulesets = {}
    processed = 0

    for species_name, original_data in existing_rulesets.items():
        enhanced_ruleset = generate_enhanced_ruleset(species_name, original_data, stellar_analysis)
        enhanced_rulesets[species_name] = enhanced_ruleset

        processed += 1
        if processed % 50 == 0:
            print(f"  Enhanced {processed} species...")

    print(f"Enhanced {processed} species rulesets")

    # Save results
    output_file = output_dir / "enhanced_species_rulesets.json"
    with open(output_file, 'w') as f:
        json.dump(enhanced_rulesets, f, indent=2)

    # Generate summary
    with_stellar_data = sum(1 for r in enhanced_rulesets.values() if r['stellar_analysis_available'])

    summary = {
        'total_species': len(enhanced_rulesets),
        'species_with_stellar_data': with_stellar_data,
        'species_without_stellar_data': len(enhanced_rulesets) - with_stellar_data,
        'enhancement_coverage': f"{with_stellar_data/len(enhanced_rulesets)*100:.1f}%",
        'generation_timestamp': str(datetime.now()),
        'source_stellar_analysis': str(stellar_file),
        'source_rulesets': str(rulesets_dir)
    }

    summary_file = output_dir / "enhancement_summary.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"\nEnhancement complete!")
    print(f"Enhanced rulesets: {output_file}")
    print(f"Summary: {summary_file}")
    print(f"Coverage: {summary['enhancement_coverage']} of species have stellar analysis data")

if __name__ == "__main__":
    from datetime import datetime
    main()