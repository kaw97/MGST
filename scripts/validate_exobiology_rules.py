#!/usr/bin/env python3
"""
Validate Existing Exobiology Rules Against Real Biological Data

This script tests the current exobiology detection rules against the 40,213 systems
with confirmed biological signals to identify:
1. How well current rules predict actual biological occurrence
2. False positive patterns and rates
3. Specific rule improvements needed for Stratum Tectonicas vs bacteria differentiation
"""

import json
import sys
import time
from pathlib import Path
from collections import defaultdict, Counter

# Import existing exobiology rules
sys.path.append('/mnt/z/HITEC')

def load_exobiology_config():
    """Load the existing high-value exobiology configuration for testing."""
    try:
        from src.hitec_galaxy.configs.high_value_exobiology import HighValueExobiologyConfig
        config = HighValueExobiologyConfig()
        print("✅ Loaded existing high-value exobiology configuration")
        return config
    except ImportError as e:
        print(f"❌ Could not load exobiology config: {e}")
        return None

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

def test_body_against_species_rules(body, species_catalogs):
    """Test a body against all species detection rules."""
    results = {
        'stratum_matches': [],
        'bacterium_matches': [],
        'total_matches': 0
    }

    body_type = body.get('subType', '')
    atmosphere = body.get('atmosphereType', '')
    temperature = body.get('surfaceTemperature', 0)
    gravity = body.get('gravity', 0)
    pressure = body.get('surfacePressure', 0)
    volcanism = body.get('volcanismType', '')

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
                            'ruleset': ruleset
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
                            'ruleset': ruleset
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
    if 'min_temperature' in ruleset and temperature < ruleset['min_temperature']:
        return False
    if 'max_temperature' in ruleset and temperature > ruleset['max_temperature']:
        return False

    # Check gravity range
    if 'min_gravity' in ruleset and gravity < ruleset['min_gravity']:
        return False
    if 'max_gravity' in ruleset and gravity > ruleset['max_gravity']:
        return False

    # Check pressure range
    if 'min_pressure' in ruleset and pressure < ruleset['min_pressure']:
        return False
    if 'max_pressure' in ruleset and pressure > ruleset['max_pressure']:
        return False

    # Check volcanism
    if 'volcanism' in ruleset:
        volcanism_req = ruleset['volcanism']
        if volcanism_req == 'None' and volcanism not in ['None', 'No volcanism', '']:
            return False
        elif volcanism_req == 'Any' and volcanism in ['None', 'No volcanism', '']:
            return False
        elif isinstance(volcanism_req, list):
            # Check if volcanism contains any of the required types
            volcanism_lower = volcanism.lower() if volcanism else ''
            if not any(req.lower() in volcanism_lower for req in volcanism_req):
                return False
        elif isinstance(volcanism_req, str) and volcanism_req not in ['None', 'Any']:
            if volcanism_req.lower() not in volcanism.lower():
                return False

    return True

def analyze_rule_performance(jsonl_file, species_catalogs):
    """Analyze how well current rules perform against real biological data."""

    print(f"🔬 Testing current exobiology rules against {jsonl_file}")

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
            'missed_opportunities': []
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
                            rule_results = test_body_against_species_rules(body, species_catalogs)

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

                                # Check for high-value matches (>10M credits)
                                high_value_matches = [
                                    m for m in rule_results['stratum_matches'] + rule_results['bacterium_matches']
                                    if m['value'] >= 10000000
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

                                # Collect examples
                                if stratum_count > 0 and bacterium_count == 0:
                                    if len(performance['prediction_examples']['stratum_high_confidence']) < 10:
                                        performance['prediction_examples']['stratum_high_confidence'].append({
                                            'system': system.get('name', ''),
                                            'body': body.get('name', ''),
                                            'body_type': body_type,
                                            'atmosphere': atmosphere,
                                            'matches': rule_results['stratum_matches']
                                        })

                                if bacterium_count > 0 and stratum_count == 0:
                                    if len(performance['prediction_examples']['bacteria_high_confidence']) < 10:
                                        performance['prediction_examples']['bacteria_high_confidence'].append({
                                            'system': system.get('name', ''),
                                            'body': body.get('name', ''),
                                            'body_type': body_type,
                                            'atmosphere': atmosphere,
                                            'matches': rule_results['bacterium_matches']
                                        })

                                if stratum_count > 0 and bacterium_count > 0:
                                    if len(performance['prediction_examples']['conflicts']) < 10:
                                        performance['prediction_examples']['conflicts'].append({
                                            'system': system.get('name', ''),
                                            'body': body.get('name', ''),
                                            'body_type': body_type,
                                            'atmosphere': atmosphere,
                                            'stratum_matches': rule_results['stratum_matches'],
                                            'bacterium_matches': rule_results['bacterium_matches']
                                        })

                            # Track all body types and atmospheres for baseline
                            body_type = body.get('subType', 'Unknown')
                            atmosphere = body.get('atmosphereType', 'None')
                            performance['body_type_analysis'][body_type]['total'] += 1
                            performance['atmosphere_analysis'][atmosphere]['total'] += 1

                    if line_num % 5000 == 0 and line_num > 0:
                        elapsed = time.time() - start_time
                        rate = line_num / elapsed
                        print(f"   Processed {line_num:,} systems ({rate:.0f} systems/sec)")

                except json.JSONDecodeError:
                    continue

    elapsed = time.time() - start_time
    print(f"✅ Rule validation complete in {elapsed:.1f}s")

    return performance

def print_performance_summary(performance):
    """Print comprehensive performance analysis."""

    print("\n" + "="*80)
    print("EXOBIOLOGY RULE VALIDATION RESULTS")
    print("="*80)

    print(f"\n📊 OVERALL PERFORMANCE:")
    print(f"   Systems processed: {performance['systems_processed']:,}")
    print(f"   Bodies with bio signals: {performance['bodies_with_signals']:,}")
    print(f"   Bodies matching any rule: {performance['bodies_matching_rules']:,}")

    if performance['bodies_with_signals'] > 0:
        match_rate = (performance['bodies_matching_rules'] / performance['bodies_with_signals']) * 100
        print(f"   Rule match rate: {match_rate:.2f}%")

    print(f"\n🎯 SPECIES PREDICTIONS:")
    print(f"   Stratum Tectonicas predictions: {performance['stratum_predictions']:,}")
    print(f"   Bacterium predictions: {performance['bacterium_predictions']:,}")
    print(f"   Conflicting predictions: {performance['conflicting_predictions']:,}")
    print(f"   High-value predictions (>10M): {performance['high_value_predictions']:,}")

    if performance['conflicting_predictions'] > 0:
        conflict_rate = (performance['conflicting_predictions'] / performance['bodies_matching_rules']) * 100
        print(f"   Conflict rate: {conflict_rate:.2f}% (needs improvement)")

    print(f"\n🪐 BODY TYPE PERFORMANCE:")
    for body_type, stats in sorted(performance['body_type_analysis'].items(),
                                  key=lambda x: x[1]['total'], reverse=True)[:10]:
        total = stats['total']
        matches = stats['rule_matches']
        stratum = stats['stratum_matches']
        bacterium = stats['bacterium_matches']

        if total > 0:
            match_pct = (matches / total) * 100
            print(f"   {body_type}: {matches:,}/{total:,} ({match_pct:.1f}%) - S:{stratum} B:{bacterium}")

    print(f"\n🌫️  ATMOSPHERE PERFORMANCE:")
    for atmosphere, stats in sorted(performance['atmosphere_analysis'].items(),
                                   key=lambda x: x[1]['total'], reverse=True)[:10]:
        total = stats['total']
        matches = stats['rule_matches']
        stratum = stats['stratum_matches']
        bacterium = stats['bacterium_matches']

        if total > 0:
            match_pct = (matches / total) * 100
            print(f"   {atmosphere}: {matches:,}/{total:,} ({match_pct:.1f}%) - S:{stratum} B:{bacterium}")

    print(f"\n⭐ HIGH CONFIDENCE STRATUM PREDICTIONS:")
    for i, example in enumerate(performance['prediction_examples']['stratum_high_confidence'][:5]):
        print(f"   {i+1}. {example['body']} ({example['body_type']}, {example['atmosphere']})")
        for match in example['matches'][:2]:
            print(f"      → {match['species']}: {match['value']:,} credits")

    print(f"\n🦠 HIGH CONFIDENCE BACTERIA PREDICTIONS:")
    for i, example in enumerate(performance['prediction_examples']['bacteria_high_confidence'][:5]):
        print(f"   {i+1}. {example['body']} ({example['body_type']}, {example['atmosphere']})")
        for match in example['matches'][:2]:
            print(f"      → {match['species']}: {match['value']:,} credits")

    print(f"\n⚠️  CONFLICTING PREDICTIONS (Need Resolution):")
    for i, example in enumerate(performance['prediction_examples']['conflicts'][:5]):
        print(f"   {i+1}. {example['body']} ({example['body_type']}, {example['atmosphere']})")
        print(f"      Stratum: {len(example['stratum_matches'])} matches")
        print(f"      Bacteria: {len(example['bacterium_matches'])} matches")

def main():
    jsonl_file = sys.argv[1] if len(sys.argv) > 1 else 'output/bio_landmarks_full_20250919_1814/results.jsonl'

    # Load configurations and rulesets
    exobio_config = load_exobiology_config()
    species_catalogs = load_species_rulesets()

    if not species_catalogs:
        print("❌ Cannot proceed without species rulesets")
        return

    # Test current rules against biological data
    performance = analyze_rule_performance(jsonl_file, species_catalogs)

    # Print comprehensive analysis
    print_performance_summary(performance)

    # Save detailed results
    output_file = f"output/rule_validation_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        # Convert defaultdicts to regular dicts for JSON serialization
        serializable_performance = {}
        for key, value in performance.items():
            if isinstance(value, defaultdict):
                serializable_performance[key] = dict(value)
            else:
                serializable_performance[key] = value

        json.dump(serializable_performance, f, indent=2, default=str)

    print(f"\n💾 Detailed validation results saved to: {output_file}")

if __name__ == "__main__":
    main()