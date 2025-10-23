#!/usr/bin/env python3
"""
Validate Existing Exobiology Rules Against Real Biological Data - FIXED VERSION

This script tests the current exobiology detection rules against the 40,213 systems
with confirmed biological signals. The original version had 0% match rate due to
field mapping issues between database format and ruleset format.

FIXES APPLIED:
- Database "High metal content world" → Ruleset "High metal content body"
- Database "Thin Carbon dioxide" → Ruleset "CarbonDioxide"
- Database "No volcanism" → Ruleset "None"
- Proper atmosphere and volcanism mapping
"""

import json
import sys
import time
from pathlib import Path
from collections import defaultdict, Counter

# Import existing exobiology rules
sys.path.append('/mnt/z/HITEC')

def create_field_mappers():
    """Create mapping functions to convert database fields to ruleset format."""

    # Body type mappings
    body_type_map = {
        'High metal content world': 'High metal content body',
        'Rocky Ice world': 'Rocky ice body',
        'Icy body': 'Icy body',
        'Rocky body': 'Rocky body',
        'Metal-rich body': 'High metal content body',  # Best guess
        'Water world': 'Water world',
    }

    # Atmosphere mappings - remove "Thin " prefix and convert names
    atmosphere_map = {
        'None': None,
        'No atmosphere': None,
        'Thin Carbon dioxide': 'CarbonDioxide',
        'Thin Sulphur dioxide': 'SulphurDioxide',
        'Thin Argon': 'Argon',
        'Thin Argon-rich': 'ArgonRich',
        'Thin Ammonia': 'Ammonia',
        'Thin Methane': 'Methane',
        'Thin Methane-rich': 'MethaneRich',
        'Thin Neon': 'Neon',
        'Thin Neon-rich': 'NeonRich',
        'Thin Nitrogen': 'Nitrogen',
        'Thin Helium': 'Helium',
        'Thin Water': 'Water',
        'Thin Water-rich': 'WaterRich',
        'Thin Oxygen': 'Oxygen',
        'Carbon dioxide': 'CarbonDioxide',
        'Sulphur dioxide': 'SulphurDioxide',
        'Argon': 'Argon',
        'Ammonia': 'Ammonia',
        'Methane': 'Methane',
        'Water': 'Water',
        'Nitrogen': 'Nitrogen',
        'Helium': 'Helium',
        'Oxygen': 'Oxygen',
    }

    # Volcanism mappings
    volcanism_map = {
        'None': 'None',
        'No volcanism': 'None',
        # Most volcanism types should map to 'Any' for rules that require volcanism
        'Major Water Geysers': 'water',
        'Minor Water Geysers': 'water',
        'Water Geysers': 'water',
        'Major Water Magma': 'water',
        'Minor Water Magma': 'water',
        'Water Magma': 'water',
        'Major Carbon Dioxide Geysers': 'carbon dioxide',
        'Minor Carbon Dioxide Geysers': 'carbon dioxide',
        'Carbon Dioxide Geysers': 'carbon dioxide',
        'Major Ammonia Geysers': 'ammonia',
        'Minor Ammonia Geysers': 'ammonia',
        'Ammonia Geysers': 'ammonia',
        'Major Methane Geysers': 'methane',
        'Minor Methane Geysers': 'methane',
        'Methane Geysers': 'methane',
        'Major Nitrogen Geysers': 'nitrogen',
        'Minor Nitrogen Geysers': 'nitrogen',
        'Nitrogen Geysers': 'nitrogen',
        # Magma types generally considered 'Any' volcanism
        'Major Silicate Vapour Geysers': 'Any',
        'Minor Silicate Vapour Geysers': 'Any',
        'Silicate Vapour Geysers': 'Any',
        'Major Metallic Magma': 'Any',
        'Minor Metallic Magma': 'Any',
        'Metallic Magma': 'Any',
        'Major Rocky Magma': 'Any',
        'Minor Rocky Magma': 'Any',
        'Rocky Magma': 'Any',
    }

    def map_body_type(db_value):
        """Convert database body type to ruleset format."""
        return body_type_map.get(db_value, db_value)

    def map_atmosphere(db_value):
        """Convert database atmosphere to ruleset format."""
        if not db_value or db_value == 'None':
            return None
        return atmosphere_map.get(db_value, db_value)

    def map_volcanism(db_value):
        """Convert database volcanism to ruleset format."""
        if not db_value or db_value == 'None':
            return 'None'
        return volcanism_map.get(db_value, 'Any')  # Default unknown volcanism to 'Any'

    return map_body_type, map_atmosphere, map_volcanism

def load_species_rulesets():
    """Load the actual species detection rulesets."""
    try:
        from rulesets.stratum import catalog as stratum_catalog
        from rulesets.bacterium import catalog as bacterium_catalog

        print(f"✅ Loaded Stratum rulesets: {len(stratum_catalog)} species groups")
        print(f"✅ Loaded Bacterium rulesets: {len(bacterium_catalog)} species groups")

        return {
            'stratum': stratum_catalog,
            'bacterium': bacterium_catalog
        }
    except ImportError as e:
        print(f"⚠️  Could not load species rulesets: {e}")
        return {}

def test_body_against_species_rules(body, species_catalogs, mappers):
    """Test a body against all species detection rules with proper field mapping."""
    results = {
        'stratum_matches': [],
        'bacterium_matches': [],
        'total_matches': 0
    }

    map_body_type, map_atmosphere, map_volcanism = mappers

    # Extract and map database values to ruleset format
    raw_body_type = body.get('subType', '')
    raw_atmosphere = body.get('atmosphereType', '')
    raw_volcanism = body.get('volcanismType', '')

    body_type = map_body_type(raw_body_type)
    atmosphere = map_atmosphere(raw_atmosphere)
    volcanism = map_volcanism(raw_volcanism)

    temperature = body.get('surfaceTemperature', 0)
    gravity = body.get('gravity', 0)
    pressure = body.get('surfacePressure', 0)

    # Test against Stratum Tectonicas rules
    if 'stratum' in species_catalogs:
        for genus_key, genus_data in species_catalogs['stratum'].items():
            for species_key, species_data in genus_data.items():
                species_name = species_data.get('name', species_key)
                rulesets = species_data.get('rulesets', [])

                for ruleset in rulesets:
                    if matches_ruleset(body_type, atmosphere, temperature, gravity, pressure, volcanism, ruleset):
                        results['stratum_matches'].append({
                            'species': species_name,
                            'value': species_data.get('value', 0),
                            'ruleset': ruleset,
                            'mapped_values': {
                                'body_type': f"{raw_body_type} → {body_type}",
                                'atmosphere': f"{raw_atmosphere} → {atmosphere}",
                                'volcanism': f"{raw_volcanism} → {volcanism}"
                            }
                        })
                        results['total_matches'] += 1

    # Test against Bacterium rules
    if 'bacterium' in species_catalogs:
        for genus_key, genus_data in species_catalogs['bacterium'].items():
            for species_key, species_data in genus_data.items():
                species_name = species_data.get('name', species_key)
                rulesets = species_data.get('rulesets', [])

                for ruleset in rulesets:
                    if matches_ruleset(body_type, atmosphere, temperature, gravity, pressure, volcanism, ruleset):
                        results['bacterium_matches'].append({
                            'species': species_name,
                            'value': species_data.get('value', 0),
                            'ruleset': ruleset,
                            'mapped_values': {
                                'body_type': f"{raw_body_type} → {body_type}",
                                'atmosphere': f"{raw_atmosphere} → {atmosphere}",
                                'volcanism': f"{raw_volcanism} → {volcanism}"
                            }
                        })
                        results['total_matches'] += 1

    return results

def matches_ruleset(body_type, atmosphere, temperature, gravity, pressure, volcanism, ruleset):
    """Check if body conditions match a specific species ruleset."""

    # Check body type
    if 'body_type' in ruleset:
        if isinstance(ruleset['body_type'], list):
            if body_type not in ruleset['body_type']:
                return False
        elif body_type != ruleset['body_type']:
            return False

    # Check atmosphere
    if 'atmosphere' in ruleset:
        if isinstance(ruleset['atmosphere'], list):
            if atmosphere not in ruleset['atmosphere']:
                return False
        elif atmosphere != ruleset['atmosphere']:
            return False

    # Check temperature range
    if temperature is not None:
        if 'min_temperature' in ruleset and temperature < ruleset['min_temperature']:
            return False
        if 'max_temperature' in ruleset and temperature > ruleset['max_temperature']:
            return False

    # Check gravity range
    if gravity is not None:
        if 'min_gravity' in ruleset and gravity < ruleset['min_gravity']:
            return False
        if 'max_gravity' in ruleset and gravity > ruleset['max_gravity']:
            return False

    # Check pressure range
    if pressure is not None:
        if 'min_pressure' in ruleset and pressure < ruleset['min_pressure']:
            return False
        if 'max_pressure' in ruleset and pressure > ruleset['max_pressure']:
            return False

    # Check volcanism
    if 'volcanism' in ruleset:
        volcanism_req = ruleset['volcanism']
        if volcanism_req == 'None' and volcanism not in ['None']:
            return False
        elif volcanism_req == 'Any' and volcanism in ['None']:
            return False
        elif isinstance(volcanism_req, list):
            # Check if volcanism matches any of the required types
            if volcanism not in volcanism_req and volcanism != 'Any':
                return False
        elif isinstance(volcanism_req, str) and volcanism_req not in ['None', 'Any']:
            if volcanism != volcanism_req and volcanism != 'Any':
                return False

    return True

def analyze_rule_performance(jsonl_file, species_catalogs):
    """Analyze how well current rules perform against real biological data."""

    print(f"🔬 Testing FIXED exobiology rules against {jsonl_file}")

    # Create field mappers
    mappers = create_field_mappers()

    performance = {
        'systems_processed': 0,
        'bodies_with_signals': 0,
        'bodies_matching_rules': 0,
        'stratum_predictions': 0,
        'bacterium_predictions': 0,
        'conflicting_predictions': 0,
        'high_value_predictions': 0,
        'body_type_analysis': defaultdict(lambda: {
            'total': 0, 'rule_matches': 0, 'stratum_matches': 0, 'bacterium_matches': 0
        }),
        'atmosphere_analysis': defaultdict(lambda: {
            'total': 0, 'rule_matches': 0, 'stratum_matches': 0, 'bacterium_matches': 0
        }),
        'value_distribution': defaultdict(int),
        'prediction_examples': {
            'stratum_high_confidence': [],
            'bacteria_high_confidence': [],
            'conflicts': [],
            'field_mapping_examples': []
        },
        'mapping_statistics': {
            'body_type_mappings': defaultdict(int),
            'atmosphere_mappings': defaultdict(int),
            'volcanism_mappings': defaultdict(int)
        }
    }

    start_time = time.time()

    with open(jsonl_file, 'r') as f:
        for line_num, line in enumerate(f):
            if line.strip():
                try:
                    system = json.loads(line)
                    performance['systems_processed'] += 1

                    # Process all bodies in system
                    bodies = system.get('bodies', [])
                    for body in bodies:
                        signals = body.get('signals', {})
                        if 'signals' in signals or 'genuses' in signals:
                            performance['bodies_with_signals'] += 1

                            # Test against species rules
                            rule_results = test_body_against_species_rules(body, species_catalogs, mappers)

                            if rule_results['total_matches'] > 0:
                                performance['bodies_matching_rules'] += 1

                                # Analyze matches
                                stratum_count = len(rule_results['stratum_matches'])
                                bacterium_count = len(rule_results['bacterium_matches'])

                                if stratum_count > 0:
                                    performance['stratum_predictions'] += 1
                                if bacterium_count > 0:
                                    performance['bacterium_predictions'] += 1
                                if stratum_count > 0 and bacterium_count > 0:
                                    performance['conflicting_predictions'] += 1

                                # Check for high-value matches (>1M credits)
                                high_value_matches = [
                                    m for m in rule_results['stratum_matches'] + rule_results['bacterium_matches']
                                    if m['value'] >= 1000000
                                ]
                                if high_value_matches:
                                    performance['high_value_predictions'] += 1

                                # Track by body type and atmosphere
                                body_type = body.get('subType', 'Unknown')
                                atmosphere = body.get('atmosphereType', 'None')

                                performance['body_type_analysis'][body_type]['rule_matches'] += 1
                                performance['body_type_analysis'][body_type]['stratum_matches'] += stratum_count
                                performance['body_type_analysis'][body_type]['bacterium_matches'] += bacterium_count

                                performance['atmosphere_analysis'][atmosphere]['rule_matches'] += 1
                                performance['atmosphere_analysis'][atmosphere]['stratum_matches'] += stratum_count
                                performance['atmosphere_analysis'][atmosphere]['bacterium_matches'] += bacterium_count

                                # Collect examples with field mappings
                                if len(performance['prediction_examples']['field_mapping_examples']) < 10:
                                    all_matches = rule_results['stratum_matches'] + rule_results['bacterium_matches']
                                    if all_matches:
                                        performance['prediction_examples']['field_mapping_examples'].append({
                                            'system': system.get('name', ''),
                                            'body': body.get('name', ''),
                                            'mapping_example': all_matches[0]['mapped_values'],
                                            'species': all_matches[0]['species'],
                                            'value': all_matches[0]['value']
                                        })

                            # Track all body types and atmospheres for baseline
                            body_type = body.get('subType', 'Unknown')
                            atmosphere = body.get('atmosphereType', 'None')
                            performance['body_type_analysis'][body_type]['total'] += 1
                            performance['atmosphere_analysis'][atmosphere]['total'] += 1

                    if line_num % 2000 == 0 and line_num > 0:
                        elapsed = time.time() - start_time
                        rate = line_num / elapsed
                        match_rate = (performance['bodies_matching_rules'] / max(1, performance['bodies_with_signals'])) * 100
                        print(f"   Processed {line_num:,} systems ({rate:.0f} systems/sec) - Match rate: {match_rate:.2f}%")

                except json.JSONDecodeError:
                    continue

    elapsed = time.time() - start_time
    print(f"✅ FIXED rule validation complete in {elapsed:.1f}s")

    return performance

def print_performance_summary(performance):
    """Print comprehensive performance analysis."""

    print("\\n" + "="*80)
    print("FIXED EXOBIOLOGY RULE VALIDATION RESULTS")
    print("="*80)

    print(f"\\n📊 OVERALL PERFORMANCE:")
    print(f"   Systems processed: {performance['systems_processed']:,}")
    print(f"   Bodies with bio signals: {performance['bodies_with_signals']:,}")
    print(f"   Bodies matching any rule: {performance['bodies_matching_rules']:,}")

    if performance['bodies_with_signals'] > 0:
        match_rate = (performance['bodies_matching_rules'] / performance['bodies_with_signals']) * 100
        print(f"   Rule match rate: {match_rate:.2f}% (FIXED from 0%!)")

    print(f"\\n🎯 SPECIES PREDICTIONS:")
    print(f"   Stratum Tectonicas predictions: {performance['stratum_predictions']:,}")
    print(f"   Bacterium predictions: {performance['bacterium_predictions']:,}")
    print(f"   Conflicting predictions: {performance['conflicting_predictions']:,}")
    print(f"   High-value predictions (>1M): {performance['high_value_predictions']:,}")

    if performance['conflicting_predictions'] > 0 and performance['bodies_matching_rules'] > 0:
        conflict_rate = (performance['conflicting_predictions'] / performance['bodies_matching_rules']) * 100
        print(f"   Conflict rate: {conflict_rate:.2f}% (needs improvement)")

    print(f"\\n🗺️  FIELD MAPPING EXAMPLES:")
    for example in performance['prediction_examples']['field_mapping_examples'][:5]:
        print(f"   {example['species']} ({example['value']:,} credits):")
        print(f"     Body Type: {example['mapping_example']['body_type']}")
        print(f"     Atmosphere: {example['mapping_example']['atmosphere']}")
        print(f"     Volcanism: {example['mapping_example']['volcanism']}")
        print()

    print(f"\\n🪐 BODY TYPE PERFORMANCE (Top 10):")
    for body_type, stats in sorted(performance['body_type_analysis'].items(),
                                  key=lambda x: x[1]['total'], reverse=True)[:10]:
        total = stats['total']
        matches = stats['rule_matches']
        stratum = stats['stratum_matches']
        bacterium = stats['bacterium_matches']

        if total > 0:
            match_pct = (matches / total) * 100
            print(f"   {body_type}: {matches:,}/{total:,} ({match_pct:.1f}%) - S:{stratum} B:{bacterium}")

    print(f"\\n🌫️  ATMOSPHERE PERFORMANCE (Top 10):")
    for atmosphere, stats in sorted(performance['atmosphere_analysis'].items(),
                                   key=lambda x: x[1]['total'], reverse=True)[:10]:
        total = stats['total']
        matches = stats['rule_matches']
        stratum = stats['stratum_matches']
        bacterium = stats['bacterium_matches']

        if total > 0:
            match_pct = (matches / total) * 100
            print(f"   {atmosphere}: {matches:,}/{total:,} ({match_pct:.1f}%) - S:{stratum} B:{bacterium}")

def main():
    jsonl_file = sys.argv[1] if len(sys.argv) > 1 else 'output/bio_landmarks_full_20250919_1814/results.jsonl'

    # Load species rulesets
    species_catalogs = load_species_rulesets()

    if not species_catalogs:
        print("❌ Cannot proceed without species rulesets")
        return

    # Test current rules against biological data with FIXED field mappings
    performance = analyze_rule_performance(jsonl_file, species_catalogs)

    # Print comprehensive analysis
    print_performance_summary(performance)

    # Save detailed results
    output_file = f"output/rule_validation_FIXED_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        # Convert defaultdicts to regular dicts for JSON serialization
        serializable_performance = {}
        for key, value in performance.items():
            if isinstance(value, defaultdict):
                serializable_performance[key] = dict(value)
            else:
                serializable_performance[key] = value

        json.dump(serializable_performance, f, indent=2, default=str)

    print(f"\\n💾 Detailed FIXED validation results saved to: {output_file}")

if __name__ == "__main__":
    main()