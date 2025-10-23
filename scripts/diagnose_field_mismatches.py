#!/usr/bin/env python3
"""
Diagnose Field Mismatches Between Database and Rulesets

This script analyzes the actual field values in the biological dataset
vs the expected values in the ruleset files to identify why rules aren't matching.
"""

import json
import sys
from pathlib import Path
from collections import Counter, defaultdict

def analyze_field_mismatches(jsonl_file):
    """Analyze actual vs expected field values."""

    print(f"🔍 Analyzing field mismatches in {jsonl_file}")

    # Load species rules for comparison
    sys.path.append('/mnt/z/HITEC')
    try:
        from rulesets.stratum import catalog as stratum_catalog
        from rulesets.bacterium import catalog as bacterium_catalog
        print(f"✅ Loaded {len(stratum_catalog)} Stratum species groups")
        print(f"✅ Loaded {len(bacterium_catalog)} Bacterium species groups")
    except ImportError as e:
        print(f"⚠️  Could not load species rules: {e}")
        return

    # Collect all expected values from rulesets
    expected_body_types = set()
    expected_atmospheres = set()
    expected_volcanism = set()

    for genus_data in stratum_catalog.values():
        for species_data in genus_data.values():
            for ruleset in species_data.get('rulesets', []):
                if 'body_type' in ruleset:
                    if isinstance(ruleset['body_type'], list):
                        expected_body_types.update(ruleset['body_type'])
                    else:
                        expected_body_types.add(ruleset['body_type'])

                if 'atmosphere' in ruleset:
                    if isinstance(ruleset['atmosphere'], list):
                        expected_atmospheres.update(ruleset['atmosphere'])
                    else:
                        expected_atmospheres.add(ruleset['atmosphere'])

                if 'volcanism' in ruleset:
                    volcanism_val = ruleset['volcanism']
                    if isinstance(volcanism_val, list):
                        expected_volcanism.update(volcanism_val)
                    else:
                        expected_volcanism.add(volcanism_val)

    for genus_data in bacterium_catalog.values():
        for species_data in genus_data.values():
            for ruleset in species_data.get('rulesets', []):
                if 'body_type' in ruleset:
                    if isinstance(ruleset['body_type'], list):
                        expected_body_types.update(ruleset['body_type'])
                    else:
                        expected_body_types.add(ruleset['body_type'])

                if 'atmosphere' in ruleset:
                    if isinstance(ruleset['atmosphere'], list):
                        expected_atmospheres.update(ruleset['atmosphere'])
                    else:
                        expected_atmospheres.add(ruleset['atmosphere'])

                if 'volcanism' in ruleset:
                    volcanism_val = ruleset['volcanism']
                    if isinstance(volcanism_val, list):
                        expected_volcanism.update(volcanism_val)
                    else:
                        expected_volcanism.add(volcanism_val)

    # Collect actual values from dataset
    actual_body_types = Counter()
    actual_atmospheres = Counter()
    actual_volcanism = Counter()
    bio_body_count = 0

    with open(jsonl_file, 'r') as f:
        for line_num, line in enumerate(f):
            if line.strip():
                try:
                    system = json.loads(line)

                    for body in system.get('bodies', []):
                        signals = body.get('signals', {})
                        if 'signals' in signals or 'genuses' in signals:
                            bio_body_count += 1

                            # Count actual field values
                            sub_type = body.get('subType', 'None')
                            atmosphere = body.get('atmosphereType', 'None')
                            volcanism = body.get('volcanismType', 'None')

                            actual_body_types[sub_type] += 1
                            actual_atmospheres[atmosphere] += 1
                            actual_volcanism[volcanism] += 1

                            if bio_body_count <= 5:  # Print first few examples
                                print(f"\nExample {bio_body_count}:")
                                print(f"  Database subType: '{sub_type}'")
                                print(f"  Database atmosphereType: '{atmosphere}'")
                                print(f"  Database volcanismType: '{volcanism}'")
                                print(f"  Temperature: {body.get('surfaceTemperature')}")
                                print(f"  Gravity: {body.get('gravity')}")
                                print(f"  Pressure: {body.get('surfacePressure')}")

                except json.JSONDecodeError:
                    continue

            if line_num % 1000 == 0 and line_num > 0:
                print(f"   Processed {line_num:,} systems, found {bio_body_count} bio bodies")

            # Quick analysis mode - stop after first 5000 systems
            if line_num > 5000:
                break

    print(f"\n{'='*80}")
    print("FIELD MISMATCH ANALYSIS")
    print(f"{'='*80}")

    print(f"\n📊 SAMPLE ANALYSIS: {bio_body_count} biological bodies from {line_num+1:,} systems")

    print(f"\n🪐 BODY TYPE MISMATCHES:")
    print(f"Expected in rulesets: {sorted(expected_body_types)}")
    print(f"Actual in database (top 10):")
    for body_type, count in actual_body_types.most_common(10):
        in_rules = body_type in expected_body_types
        print(f"  '{body_type}': {count:,} ({'✅' if in_rules else '❌ NOT IN RULES'})")

    print(f"\n🌫️  ATMOSPHERE MISMATCHES:")
    print(f"Expected in rulesets: {sorted(expected_atmospheres)}")
    print(f"Actual in database (top 15):")
    for atmosphere, count in actual_atmospheres.most_common(15):
        in_rules = atmosphere in expected_atmospheres
        print(f"  '{atmosphere}': {count:,} ({'✅' if in_rules else '❌ NOT IN RULES'})")

    print(f"\n🌋 VOLCANISM MISMATCHES:")
    print(f"Expected in rulesets: {sorted(expected_volcanism)}")
    print(f"Actual in database (top 10):")
    for volcanism, count in actual_volcanism.most_common(10):
        in_rules = volcanism in expected_volcanism
        print(f"  '{volcanism}': {count:,} ({'✅' if in_rules else '❌ NOT IN RULES'})")

    print(f"\n💡 FIELD MAPPING RECOMMENDATIONS:")
    print("Database → Ruleset mappings needed:")

    # Body type mappings
    print("\nBody Type Mappings:")
    for db_value, count in actual_body_types.most_common(5):
        if db_value not in expected_body_types:
            # Suggest mapping
            suggestions = []
            if 'metal content' in db_value.lower():
                suggestions.append('High metal content body')
            elif 'rocky' in db_value.lower():
                suggestions.append('Rocky body')
            elif 'icy' in db_value.lower():
                suggestions.append('Icy body')

            print(f"  '{db_value}' → {suggestions if suggestions else 'No clear mapping'}")

    # Atmosphere mappings
    print("\nAtmosphere Type Mappings:")
    for db_value, count in actual_atmospheres.most_common(5):
        if db_value not in expected_atmospheres:
            # Simple suggestion based on content
            suggestions = []
            if 'dioxide' in db_value.lower():
                if 'carbon' in db_value.lower():
                    suggestions.append('CarbonDioxide')
                elif 'sulphur' in db_value.lower():
                    suggestions.append('SulphurDioxide')
            elif 'ammonia' in db_value.lower():
                suggestions.append('Ammonia')
            elif 'argon' in db_value.lower():
                suggestions.append('Argon')

            print(f"  '{db_value}' → {suggestions if suggestions else 'No clear mapping'}")

def main():
    jsonl_file = sys.argv[1] if len(sys.argv) > 1 else 'output/bio_landmarks_full_20250919_1814/results.jsonl'
    analyze_field_mismatches(jsonl_file)

if __name__ == "__main__":
    main()