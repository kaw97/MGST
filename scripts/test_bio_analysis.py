#!/usr/bin/env python3
"""
Quick test of biological analysis using existing test data
"""

import sys
sys.path.append('/mnt/z/HITEC')

from scripts.biological_analysis import BiologicalAnalyzer
import json

def test_with_snake_sector():
    """Test analysis with small dataset first."""
    print("🧪 Testing biological analysis with Snake Sector data...")

    analyzer = BiologicalAnalyzer()

    # Test with existing test result if available
    test_files = [
        'output/bio_landmarks_test_20250919_1809/results.tsv'
    ]

    for test_file in test_files:
        try:
            print(f"\n📊 Testing with {test_file}")
            analyzer.load_biological_landmarks(test_file)

            # Run basic analysis
            stellar_analysis = analyzer.analyze_stellar_correlations()
            print(f"Stellar analysis: {stellar_analysis}")

            overlap_analysis = analyzer.analyze_stratum_vs_bacteria_overlap()
            print(f"Species overlap analysis keys: {list(overlap_analysis.keys())}")

            # Generate test report
            analyzer.generate_report('output/test_bio_analysis')
            print(f"✅ Test report generated in output/test_bio_analysis/")

        except Exception as e:
            print(f"⚠️  Error with {test_file}: {e}")

    # Test species rules loading
    print(f"\n🔬 Species rules loaded:")
    if analyzer.species_rules:
        for genus, data in analyzer.species_rules.items():
            print(f"   {genus}: {len(data)} species groups")
    else:
        print("   No species rules loaded")

if __name__ == "__main__":
    test_with_snake_sector()