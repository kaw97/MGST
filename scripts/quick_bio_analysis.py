#!/usr/bin/env python3
"""
Quick biological analysis without multiprocessing complications
"""

import json
import sys
import time
from pathlib import Path
from collections import defaultdict, Counter

def analyze_bio_data(jsonl_file):
    """Analyze biological data from JSONL file."""

    print(f"🔬 Analyzing biological data from {jsonl_file}")

    # Load species rules
    sys.path.append('/mnt/z/HITEC')
    try:
        from rulesets.stratum import catalog as stratum_catalog
        from rulesets.bacterium import catalog as bacterium_catalog
        print(f"✅ Loaded {len(stratum_catalog)} Stratum species groups")
        print(f"✅ Loaded {len(bacterium_catalog)} Bacterium species groups")
    except ImportError as e:
        print(f"⚠️  Could not load species rules: {e}")
        stratum_catalog = {}
        bacterium_catalog = {}

    # Analysis containers
    analysis = {
        'body_types': defaultdict(int),
        'atmosphere_types': defaultdict(int),
        'temperature_ranges': [],
        'gravity_ranges': [],
        'pressure_ranges': [],
        'materials_by_body_type': defaultdict(lambda: defaultdict(list)),
        'solid_composition': defaultdict(lambda: defaultdict(list)),
        'volcanism_types': defaultdict(int),
        'landable_count': {'landable': 0, 'non_landable': 0},
        'stratum_candidates': [],
        'bacteria_candidates': [],
        'high_metal_content_bodies': [],
        'total_bio_bodies': 0,
        'systems_processed': 0
    }

    # Process JSONL file
    start_time = time.time()
    with open(jsonl_file, 'r') as f:
        for line_num, line in enumerate(f):
            if line.strip():
                try:
                    system = json.loads(line)
                    analysis['systems_processed'] += 1

                    # Process all bodies in system
                    bodies = system.get('bodies', [])
                    for body in bodies:
                        signals = body.get('signals', {})
                        if 'signals' in signals or 'genuses' in signals:
                            analysis['total_bio_bodies'] += 1
                            analyze_single_body(body, analysis)

                    if line_num % 5000 == 0 and line_num > 0:
                        elapsed = time.time() - start_time
                        rate = line_num / elapsed
                        print(f"   Processed {line_num:,} systems ({rate:.0f} systems/sec)")

                except json.JSONDecodeError:
                    continue

    elapsed = time.time() - start_time
    print(f"✅ Analysis complete: {analysis['systems_processed']:,} systems, {analysis['total_bio_bodies']:,} bio bodies in {elapsed:.1f}s")

    return analysis

def analyze_single_body(body, analysis):
    """Analyze a single body with biological signals."""

    # Basic characteristics
    sub_type = body.get('subType', 'Unknown')
    atmosphere = body.get('atmosphereType', 'None')
    temperature = body.get('surfaceTemperature')
    gravity = body.get('gravity')
    pressure = body.get('surfacePressure')
    volcanism = body.get('volcanismType', 'None')
    is_landable = body.get('isLandable', False)

    # Count characteristics
    analysis['body_types'][sub_type] += 1
    analysis['atmosphere_types'][atmosphere] += 1
    analysis['volcanism_types'][volcanism] += 1

    if is_landable:
        analysis['landable_count']['landable'] += 1
    else:
        analysis['landable_count']['non_landable'] += 1

    # Collect ranges
    if temperature is not None:
        analysis['temperature_ranges'].append(temperature)
    if gravity is not None:
        analysis['gravity_ranges'].append(gravity)
    if pressure is not None:
        analysis['pressure_ranges'].append(pressure)

    # Materials analysis
    materials = body.get('materials', {})
    for material, percentage in materials.items():
        analysis['materials_by_body_type'][sub_type][material].append(percentage)

    # Solid composition
    solid_comp = body.get('solidComposition', {})
    for component, percentage in solid_comp.items():
        analysis['solid_composition'][sub_type][component].append(percentage)

    # Special analysis for High metal content bodies
    if sub_type == 'High metal content body':
        hmc_data = {
            'system': body.get('name', '').split()[0] + ' ' + body.get('name', '').split()[1] if len(body.get('name', '').split()) > 1 else 'Unknown',
            'body_name': body.get('name', ''),
            'atmosphere': atmosphere,
            'temperature': temperature,
            'gravity': gravity,
            'pressure': pressure,
            'materials': materials,
            'solid_composition': solid_comp,
            'signals': body.get('signals', {})
        }
        analysis['high_metal_content_bodies'].append(hmc_data)

    # Check if matches Stratum Tectonicas conditions
    if matches_stratum_conditions(body):
        analysis['stratum_candidates'].append({
            'body_name': body.get('name', ''),
            'body_type': sub_type,
            'atmosphere': atmosphere,
            'temperature': temperature,
            'gravity': gravity,
            'materials': materials
        })

    # Check if matches bacteria conditions
    if matches_bacteria_conditions(body):
        analysis['bacteria_candidates'].append({
            'body_name': body.get('name', ''),
            'body_type': sub_type,
            'atmosphere': atmosphere,
            'temperature': temperature,
            'gravity': gravity,
            'materials': materials
        })

def matches_stratum_conditions(body):
    """Simple Stratum Tectonicas detection."""
    sub_type = body.get('subType', '')
    atmosphere = body.get('atmosphereType', '')
    temperature = body.get('surfaceTemperature', 0)
    gravity = body.get('gravity', 0)

    # High confidence: High metal content body
    if sub_type == 'High metal content body':
        stratum_atmospheres = ['CarbonDioxide', 'SulphurDioxide', 'Ammonia', 'Oxygen', 'CarbonDioxideRich', 'Argon', 'ArgonRich', 'Water']
        if atmosphere in stratum_atmospheres and 165 <= temperature <= 450 and 0.035 <= gravity <= 0.62:
            return True

    return False

def matches_bacteria_conditions(body):
    """Simple bacteria detection."""
    sub_type = body.get('subType', '')
    atmosphere = body.get('atmosphereType', '')
    temperature = body.get('surfaceTemperature', 0)

    # Strong bacteria indicators
    bacteria_body_types = ['Icy body', 'Rocky ice body']
    bacteria_atmospheres = ['Helium', 'Methane', 'Neon', 'NeonRich', 'Nitrogen']

    if sub_type in bacteria_body_types and atmosphere in bacteria_atmospheres and 20 <= temperature <= 150:
        return True

    return False

def print_summary(analysis):
    """Print analysis summary."""
    print("\n" + "="*60)
    print("BIOLOGICAL ANALYSIS SUMMARY")
    print("="*60)

    print(f"\n📊 DATASET OVERVIEW:")
    print(f"   Systems processed: {analysis['systems_processed']:,}")
    print(f"   Bodies with bio signals: {analysis['total_bio_bodies']:,}")

    print(f"\n🪐 BODY TYPES:")
    for body_type, count in Counter(analysis['body_types']).most_common(10):
        print(f"   {body_type}: {count:,}")

    print(f"\n🌫️  ATMOSPHERES:")
    for atmosphere, count in Counter(analysis['atmosphere_types']).most_common(10):
        print(f"   {atmosphere}: {count:,}")

    if analysis['temperature_ranges']:
        temps = analysis['temperature_ranges']
        print(f"\n🌡️  TEMPERATURE RANGE:")
        print(f"   Min: {min(temps):.1f}K, Max: {max(temps):.1f}K, Avg: {sum(temps)/len(temps):.1f}K")

    if analysis['gravity_ranges']:
        gravs = analysis['gravity_ranges']
        print(f"\n🌍 GRAVITY RANGE:")
        print(f"   Min: {min(gravs):.3f}g, Max: {max(gravs):.3f}g, Avg: {sum(gravs)/len(gravs):.3f}g")

    print(f"\n🔬 STRATUM vs BACTERIA ANALYSIS:")
    print(f"   Stratum candidates: {len(analysis['stratum_candidates'])}")
    print(f"   Bacteria candidates: {len(analysis['bacteria_candidates'])}")
    print(f"   High metal content bodies: {len(analysis['high_metal_content_bodies'])}")

    if analysis['high_metal_content_bodies']:
        print(f"\n⭐ HIGH METAL CONTENT BODIES (Top 5):")
        for i, hmc in enumerate(analysis['high_metal_content_bodies'][:5]):
            print(f"   {i+1}. {hmc['body_name']}")
            print(f"      Atmosphere: {hmc['atmosphere']}, T: {hmc.get('temperature', 'N/A')}K, G: {hmc.get('gravity', 'N/A')}g")
            if hmc['materials']:
                top_materials = sorted(hmc['materials'].items(), key=lambda x: x[1], reverse=True)[:3]
                print(f"      Materials: {', '.join([f'{m}: {p:.1f}%' for m, p in top_materials])}")

def main():
    jsonl_file = sys.argv[1] if len(sys.argv) > 1 else 'output/bio_landmarks_full_20250919_1814/results.jsonl'

    analysis = analyze_bio_data(jsonl_file)
    print_summary(analysis)

    # Save results
    output_file = f"output/quick_bio_analysis_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        # Convert to JSON-serializable format
        serializable_analysis = {}
        for key, value in analysis.items():
            if isinstance(value, defaultdict):
                serializable_analysis[key] = dict(value)
            else:
                serializable_analysis[key] = value

        json.dump(serializable_analysis, f, indent=2, default=str)

    print(f"\n💾 Detailed results saved to: {output_file}")

if __name__ == "__main__":
    main()