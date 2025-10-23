#!/usr/bin/env python3
"""
Analyze the structure and fields present in the codex.json file.
This script examines the codex file to understand what data is available
for enrichment with stellar characteristics.
"""

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

def analyze_codex_structure(codex_path, max_entries=2000):
    """Analyze the structure of the codex.json file."""

    field_counts = Counter()
    system_name_patterns = defaultdict(int)
    body_name_patterns = defaultdict(int)
    species_names = set()
    genus_names = set()
    hud_categories = Counter()

    entries_analyzed = 0
    total_entries = 0

    try:
        print("Loading codex data... (this may take a moment)", file=sys.stderr)
        with open(codex_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        total_entries = len(data)
        print(f"Loaded {total_entries:,} entries from codex", file=sys.stderr)

        for i, entry in enumerate(data):
            if entries_analyzed >= max_entries:
                break

            entries_analyzed += 1

            # Count all fields
            for field in entry.keys():
                field_counts[field] += 1

            # Count categories
            hud_category = entry.get('hud_category', '')
            if hud_category:
                hud_categories[hud_category] += 1

            # Analyze system names for patterns
            system_name = entry.get('system', '')
            if system_name:
                # Look for systematic naming patterns
                parts = system_name.split()
                if len(parts) >= 2:
                    # Pattern like "Sector AA-A h1" or "Sector YE-A g0"
                    if len(parts) >= 3 and '-' in parts[-2]:
                        system_name_patterns['systematic_multi_part'] += 1
                    elif len(parts) == 2 and '-' in parts[1]:
                        system_name_patterns['systematic_2_part'] += 1
                    else:
                        system_name_patterns['named_system'] += 1
                else:
                    system_name_patterns['single_word'] += 1

            # Analyze body names
            body_name = entry.get('body', '')
            if body_name:
                if system_name and body_name.startswith(system_name):
                    body_name_patterns['system_prefix'] += 1
                else:
                    body_name_patterns['no_system_prefix'] += 1

            # Collect species information from english_name
            english_name = entry.get('english_name', '')
            if english_name:
                species_names.add(english_name)
                # Try to extract genus (first word)
                genus = english_name.split()[0] if english_name else ''
                if genus:
                    genus_names.add(genus)

            if entries_analyzed % 500 == 0:
                print(f"Analyzed {entries_analyzed} entries...", file=sys.stderr)

    except Exception as e:
        print(f"Error reading codex file: {e}", file=sys.stderr)
        return None

    return {
        'total_entries': total_entries,
        'entries_analyzed': entries_analyzed,
        'field_counts': dict(field_counts.most_common()),
        'system_name_patterns': dict(system_name_patterns),
        'body_name_patterns': dict(body_name_patterns),
        'hud_categories': dict(hud_categories.most_common()),
        'unique_species': len(species_names),
        'unique_genera': len(genus_names),
        'sample_species': sorted(list(species_names))[:20],
        'sample_genera': sorted(list(genus_names))[:20]
    }

def analyze_systematic_naming(codex_path, max_entries=2000):
    """Analyze systematic naming patterns to understand sector extraction."""

    systematic_systems = []
    non_systematic = []

    entries_analyzed = 0

    try:
        with open(codex_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        for i, entry in enumerate(data):
            if entries_analyzed >= max_entries:
                break

            entries_analyzed += 1

            system_name = entry.get('system', '')
            if not system_name:
                continue

            # Check for systematic naming pattern
            # Pattern: "Sector Subsector Mass" where Mass is like "h1", "g0", etc.
            parts = system_name.split()

            if len(parts) >= 3:
                # Look for mass code pattern (letter followed by number)
                last_part = parts[-1]
                if len(last_part) >= 2 and last_part[0].isalpha() and last_part[1:].isdigit():
                    # This looks systematic
                    sector = ' '.join(parts[:-1])  # Everything before mass code
                    mass_code = last_part
                    systematic_systems.append({
                        'system': system_name,
                        'sector': sector,
                        'mass_code': mass_code,
                        'parts_count': len(parts)
                    })
                else:
                    non_systematic.append(system_name)
            else:
                non_systematic.append(system_name)

    except Exception as e:
        print(f"Error reading codex file: {e}", file=sys.stderr)
        return None

    return {
        'systematic_count': len(systematic_systems),
        'non_systematic_count': len(non_systematic),
        'systematic_examples': systematic_systems[:15],
        'non_systematic_examples': non_systematic[:15]
    }

def main():
    codex_path = Path('Databases/codex.json/codex.json')

    if not codex_path.exists():
        print(f"Codex file not found: {codex_path}")
        return

    print(f"Analyzing codex structure from: {codex_path}")
    print("=" * 60)

    # Analyze overall structure
    results = analyze_codex_structure(codex_path, max_entries=3000)

    if not results:
        print("Failed to analyze codex structure")
        return

    print(f"CODEX ANALYSIS RESULTS:")
    print(f"Total entries in file: {results['total_entries']:,}")
    print(f"Entries analyzed: {results['entries_analyzed']:,}")
    print()

    print("FIELD FREQUENCY:")
    print("-" * 40)
    for field, count in results['field_counts'].items():
        percentage = (count / results['entries_analyzed']) * 100
        print(f"  {field}: {count} ({percentage:.1f}%)")
    print()

    print("HUD CATEGORIES:")
    print("-" * 40)
    for category, count in results['hud_categories'].items():
        percentage = (count / results['entries_analyzed']) * 100
        print(f"  {category}: {count} ({percentage:.1f}%)")
    print()

    print("SYSTEM NAME PATTERNS:")
    print("-" * 40)
    for pattern, count in results['system_name_patterns'].items():
        percentage = (count / results['entries_analyzed']) * 100
        print(f"  {pattern}: {count} ({percentage:.1f}%)")
    print()

    print("BODY NAME PATTERNS:")
    print("-" * 40)
    for pattern, count in results['body_name_patterns'].items():
        percentage = (count / results['entries_analyzed']) * 100
        print(f"  {pattern}: {count} ({percentage:.1f}%)")
    print()

    print(f"BIOLOGICAL/GEOLOGICAL DATA:")
    print("-" * 40)
    print(f"  Unique discoveries: {results['unique_species']}")
    print(f"  Unique discovery types: {results['unique_genera']}")
    print()

    print("SAMPLE DISCOVERY TYPES:")
    print("-" * 40)
    for species in results['sample_species']:
        print(f"  {species}")
    print()

    # Analyze systematic naming
    print("SYSTEMATIC NAMING ANALYSIS:")
    print("=" * 60)

    naming_results = analyze_systematic_naming(codex_path, max_entries=3000)

    if naming_results:
        total = naming_results['systematic_count'] + naming_results['non_systematic_count']
        if total > 0:
            systematic_pct = (naming_results['systematic_count'] / total) * 100

            print(f"Systematic naming: {naming_results['systematic_count']} ({systematic_pct:.1f}%)")
            print(f"Non-systematic: {naming_results['non_systematic_count']} ({100-systematic_pct:.1f}%)")
            print()

            print("SYSTEMATIC EXAMPLES:")
            print("-" * 40)
            for example in naming_results['systematic_examples']:
                print(f"  {example['system']} -> Sector: '{example['sector']}', Mass: '{example['mass_code']}'")
            print()

            print("NON-SYSTEMATIC EXAMPLES:")
            print("-" * 40)
            for example in naming_results['non_systematic_examples']:
                print(f"  {example}")

if __name__ == "__main__":
    main()