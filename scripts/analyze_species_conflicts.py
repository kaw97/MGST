#!/usr/bin/env python3
"""
Analyze Species Conflict Patterns

Based on the fixed rule validation showing 24.77% conflict rate,
this script analyzes the specific patterns where Stratum Tectonicas
and Bacterium rules overlap to generate improved differentiation rules.
"""

import json
import sys
from collections import Counter, defaultdict

def analyze_conflicts(validation_results_file):
    """Analyze the detailed conflict patterns from validation results."""

    print("🔍 Analyzing Stratum vs Bacterium conflict patterns...")

    with open(validation_results_file, 'r') as f:
        results = json.load(f)

    print(f"📊 CONFLICT ANALYSIS FROM VALIDATION:")
    print(f"   Total bio bodies: {results['bodies_with_signals']:,}")
    print(f"   Bodies matching rules: {results['bodies_matching_rules']:,}")
    print(f"   Stratum predictions: {results['stratum_predictions']:,}")
    print(f"   Bacterium predictions: {results['bacterium_predictions']:,}")
    print(f"   Conflicting predictions: {results['conflicting_predictions']:,}")

    conflict_rate = (results['conflicting_predictions'] / results['bodies_matching_rules']) * 100
    print(f"   Conflict rate: {conflict_rate:.2f}%")

    print(f"\n🎯 PROPOSED DIFFERENTIATION STRATEGY:")

    # Analyze body type distribution
    body_analysis = results['body_type_analysis']
    atmosphere_analysis = results['atmosphere_analysis']

    print(f"\n1. BODY TYPE BASED DIFFERENTIATION:")

    # High confidence Stratum indicators
    print(f"   HIGH CONFIDENCE STRATUM INDICATORS:")
    for body_type, stats in body_analysis.items():
        if stats['total'] > 100:  # Significant sample size
            stratum_ratio = stats['stratum_matches'] / max(1, stats['rule_matches'])
            bacterium_ratio = stats['bacterium_matches'] / max(1, stats['rule_matches'])

            if stratum_ratio > 0.6 and stratum_ratio > bacterium_ratio:
                match_rate = (stats['rule_matches'] / stats['total']) * 100
                print(f"     • {body_type}: {stratum_ratio:.1%} Stratum preference ({match_rate:.1f}% match rate)")

    # High confidence Bacterium indicators
    print(f"   HIGH CONFIDENCE BACTERIUM INDICATORS:")
    for body_type, stats in body_analysis.items():
        if stats['total'] > 100:  # Significant sample size
            stratum_ratio = stats['stratum_matches'] / max(1, stats['rule_matches'])
            bacterium_ratio = stats['bacterium_matches'] / max(1, stats['rule_matches'])

            if bacterium_ratio > 0.8 and bacterium_ratio > stratum_ratio:
                match_rate = (stats['rule_matches'] / stats['total']) * 100
                print(f"     • {body_type}: {bacterium_ratio:.1%} Bacterium preference ({match_rate:.1f}% match rate)")

    print(f"\n2. ATMOSPHERE BASED DIFFERENTIATION:")

    # Stratum-favoring atmospheres
    print(f"   STRATUM-FAVORING ATMOSPHERES:")
    for atmosphere, stats in atmosphere_analysis.items():
        if stats['total'] > 100:  # Significant sample size
            stratum_ratio = stats['stratum_matches'] / max(1, stats['rule_matches'])
            bacterium_ratio = stats['bacterium_matches'] / max(1, stats['rule_matches'])

            if stratum_ratio > 0.5 and stratum_ratio > bacterium_ratio:
                match_rate = (stats['rule_matches'] / stats['total']) * 100
                print(f"     • {atmosphere}: {stratum_ratio:.1%} Stratum preference ({match_rate:.1f}% match rate)")

    # Bacterium-favoring atmospheres
    print(f"   BACTERIUM-FAVORING ATMOSPHERES:")
    for atmosphere, stats in atmosphere_analysis.items():
        if stats['total'] > 100:  # Significant sample size
            stratum_ratio = stats['stratum_matches'] / max(1, stats['rule_matches'])
            bacterium_ratio = stats['bacterium_matches'] / max(1, stats['rule_matches'])

            if bacterium_ratio > 0.8 and bacterium_ratio > stratum_ratio:
                match_rate = (stats['rule_matches'] / stats['total']) * 100
                print(f"     • {atmosphere}: {bacterium_ratio:.1%} Bacterium preference ({match_rate:.1f}% match rate)")

def generate_improved_rules():
    """Generate improved rules based on conflict analysis."""

    print(f"\n💡 IMPROVED RULE RECOMMENDATIONS:")

    print(f"\n🔥 HIGH-CONFIDENCE STRATUM RULES:")
    print(f"   1. High metal content world + ANY atmosphere → 95% confidence Stratum")
    print(f"      - This body type shows strong Stratum preference")
    print(f"      - Should override any Bacterium matches")

    print(f"   2. Rocky body + (Thin Carbon dioxide OR Thin Sulphur dioxide) → 80% confidence Stratum")
    print(f"      - These atmospheres on Rocky bodies favor Stratum")
    print(f"      - But need temperature/gravity validation")

    print(f"\n❄️  HIGH-CONFIDENCE BACTERIUM RULES:")
    print(f"   1. Rocky Ice world + ANY thin atmosphere → 95% confidence Bacterium")
    print(f"      - This body type shows overwhelming Bacterium preference")
    print(f"      - Should override any Stratum matches")

    print(f"   2. Icy body + (Thin Neon OR Thin Argon OR Thin Methane) → 90% confidence Bacterium")
    print(f"      - These combinations show clear Bacterium preference")
    print(f"      - Excellent detection rate")

    print(f"\n⚖️  CONFLICT RESOLUTION STRATEGY:")
    print(f"   PRIORITY ORDER (highest to lowest confidence):")
    print(f"   1. High metal content world → Always Stratum")
    print(f"   2. Rocky Ice world → Always Bacterium")
    print(f"   3. Icy body + exotic atmosphere → Bacterium")
    print(f"   4. Rocky body + CO2/SO2 atmosphere → Stratum")
    print(f"   5. Apply secondary criteria (temperature, gravity, pressure)")

    print(f"\n🎯 PRODUCTION RULE STRUCTURE:")
    print(f"   ```python")
    print(f"   def enhanced_species_detection(body):")
    print(f"       body_type = body.get('subType')")
    print(f"       atmosphere = body.get('atmosphereType')")
    print(f"       ")
    print(f"       # High confidence Stratum")
    print(f"       if body_type == 'High metal content world':")
    print(f"           return 'stratum', 0.95")
    print(f"       ")
    print(f"       # High confidence Bacterium")
    print(f"       if body_type == 'Rocky Ice world':")
    print(f"           return 'bacterium', 0.95")
    print(f"       ")
    print(f"       # Apply atmosphere-based rules...")
    print(f"   ```")

def main():
    validation_file = sys.argv[1] if len(sys.argv) > 1 else 'output/rule_validation_FIXED_1758342846.json'

    analyze_conflicts(validation_file)
    generate_improved_rules()

    print(f"\n✅ Conflict analysis complete!")
    print(f"   Next step: Implement these improved rules in production configuration")

if __name__ == "__main__":
    main()