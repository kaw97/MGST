#!/usr/bin/env python3
"""
Iterative testing framework for temperature range-based exobiology configuration.
Tests against known positive systems and adjusts ranges as needed.
"""

import sys
import json
from pathlib import Path
from typing import Dict, List, Any, Tuple
import argparse
from datetime import datetime

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from hitec_galaxy.configs.temperature_range_exobiology import TemperatureRangeExobiologyConfig
from hitec_galaxy.configs.improved_exobiology import ImprovedExobiologyConfig

class TemperatureRangeValidator:
    """Validator for testing temperature range configuration against known systems."""

    def __init__(self, config: TemperatureRangeExobiologyConfig, test_systems: Dict[str, List[Dict]]):
        self.config = config
        self.test_systems = test_systems
        self.results = {}

    def test_species_detection(self, species_name: str, tolerance_factors: Tuple[float, float, float] = (1.0, 1.0, 1.0)) -> Dict[str, Any]:
        """Test species detection for a specific species with given tolerance factors."""
        stellar_tol, body_tol, dist_tol = tolerance_factors

        # Set tolerance factors
        self.config.set_tolerance_factors(stellar_tol, body_tol, dist_tol)

        if species_name not in self.test_systems:
            return {'error': f'No test systems for {species_name}'}

        test_systems = self.test_systems[species_name]
        results = {
            'species_name': species_name,
            'tolerance_factors': {
                'stellar_temp': stellar_tol,
                'body_temp': body_tol,
                'distance': dist_tol
            },
            'total_test_systems': len(test_systems),
            'successful_detections': 0,
            'failed_detections': 0,
            'detection_rate': 0.0,
            'system_results': []
        }

        for i, test_system in enumerate(test_systems):
            system_result = self._test_single_system(test_system, species_name)
            results['system_results'].append(system_result)

            if system_result['species_detected']:
                results['successful_detections'] += 1
            else:
                results['failed_detections'] += 1

        results['detection_rate'] = results['successful_detections'] / results['total_test_systems']

        return results

    def _test_single_system(self, test_system: Dict, expected_species: str) -> Dict[str, Any]:
        """Test detection on a single system."""
        system_name = test_system.get('name', 'Unknown')
        body = test_system['bodies'][0] if test_system.get('bodies') else {}

        # Detect species on the body
        detected_species = self.config.detect_species_on_body(body, test_system)

        # Check if expected species was detected
        species_detected = any(expected_species in species['name'] for species in detected_species)

        result = {
            'system_name': system_name,
            'stellar_class': test_system['stars'][0]['spectralClass'] if test_system.get('stars') else 'Unknown',
            'stellar_temp': test_system['stars'][0]['surfaceTemperature'] if test_system.get('stars') else 0,
            'body_temp': body.get('surfaceTemperature', 0),
            'distance': body.get('distanceToArrival', 0),
            'expected_species': expected_species,
            'species_detected': species_detected,
            'detected_species_count': len(detected_species),
            'all_detected_species': [s['name'] for s in detected_species]
        }

        return result

    def find_optimal_tolerance(self, species_name: str, min_detection_rate: float = 0.8) -> Dict[str, Any]:
        """Find optimal tolerance factors to achieve minimum detection rate."""
        print(f"Finding optimal tolerance for {species_name} (target: {min_detection_rate:.1%})")

        # Test different tolerance factor combinations
        tolerance_tests = [
            (1.0, 1.0, 1.0),  # Exact ranges
            (1.2, 1.2, 1.2),  # 20% expansion
            (1.5, 1.5, 1.5),  # 50% expansion
            (2.0, 2.0, 2.0),  # 100% expansion
            (1.0, 2.0, 1.0),  # Expand body temp only
            (2.0, 1.0, 1.0),  # Expand stellar temp only
            (1.0, 1.0, 2.0),  # Expand distance only
        ]

        best_result = None
        best_score = 0

        for tolerance in tolerance_tests:
            result = self.test_species_detection(species_name, tolerance)
            detection_rate = result['detection_rate']

            print(f"  Tolerance {tolerance}: {detection_rate:.1%} detection rate")

            if detection_rate >= min_detection_rate and detection_rate > best_score:
                best_result = result
                best_score = detection_rate

        if best_result:
            print(f"  ✅ Best tolerance: {best_result['tolerance_factors']}")
            print(f"  ✅ Detection rate: {best_result['detection_rate']:.1%}")
        else:
            print(f"  ❌ Could not achieve {min_detection_rate:.1%} detection rate")
            # Return the best we found
            all_results = [self.test_species_detection(species_name, tol) for tol in tolerance_tests]
            best_result = max(all_results, key=lambda x: x['detection_rate'])
            print(f"  📊 Best achieved: {best_result['detection_rate']:.1%}")

        return best_result

    def run_comprehensive_test(self) -> Dict[str, Any]:
        """Run comprehensive testing across all species."""
        print("=== COMPREHENSIVE TEMPERATURE RANGE TESTING ===")
        print()

        comprehensive_results = {
            'total_species_tested': len(self.test_systems),
            'species_results': {},
            'summary': {
                'excellent_detection': [],  # >90% detection rate
                'good_detection': [],       # 70-90% detection rate
                'poor_detection': [],       # <70% detection rate
            }
        }

        for species_name in self.test_systems.keys():
            print(f"Testing {species_name}...")

            # Test with exact ranges first
            exact_result = self.test_species_detection(species_name, (1.0, 1.0, 1.0))
            print(f"  Exact ranges: {exact_result['detection_rate']:.1%}")

            # Find optimal tolerance if needed
            if exact_result['detection_rate'] < 0.8:
                optimal_result = self.find_optimal_tolerance(species_name, 0.8)
                comprehensive_results['species_results'][species_name] = optimal_result
            else:
                comprehensive_results['species_results'][species_name] = exact_result

            # Categorize results
            final_rate = comprehensive_results['species_results'][species_name]['detection_rate']
            if final_rate >= 0.9:
                comprehensive_results['summary']['excellent_detection'].append(species_name)
            elif final_rate >= 0.7:
                comprehensive_results['summary']['good_detection'].append(species_name)
            else:
                comprehensive_results['summary']['poor_detection'].append(species_name)

            print()

        return comprehensive_results

    def analyze_failures(self, species_name: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze why certain systems failed detection."""
        failures = [sr for sr in result['system_results'] if not sr['species_detected']]

        if not failures:
            return {'message': 'No failures to analyze'}

        # Get species range info
        range_info = self.config.get_species_range_info(species_name)

        analysis = {
            'failed_systems': len(failures),
            'failure_reasons': {
                'stellar_temp_out_of_range': 0,
                'body_temp_out_of_range': 0,
                'distance_out_of_range': 0,
                'stellar_class_unsupported': 0
            },
            'failure_details': []
        }

        for failure in failures:
            stellar_class = failure['stellar_class']
            stellar_temp = failure['stellar_temp']
            body_temp = failure['body_temp']
            distance = failure['distance']

            failure_detail = {
                'system': failure['system_name'],
                'stellar_class': stellar_class,
                'issues': []
            }

            if stellar_class in range_info.get('stellar_classes', {}):
                class_ranges = range_info['stellar_classes'][stellar_class]

                # Check stellar temperature
                stellar_range = class_ranges.get('stellar_temp_range')
                if stellar_range and not (stellar_range[0] <= stellar_temp <= stellar_range[1]):
                    analysis['failure_reasons']['stellar_temp_out_of_range'] += 1
                    failure_detail['issues'].append(f'Stellar temp {stellar_temp}K outside {stellar_range[0]:.0f}-{stellar_range[1]:.0f}K')

                # Check body temperature
                body_range = class_ranges.get('body_temp_range')
                if body_range and not (body_range[0] <= body_temp <= body_range[1]):
                    analysis['failure_reasons']['body_temp_out_of_range'] += 1
                    failure_detail['issues'].append(f'Body temp {body_temp}K outside {body_range[0]:.0f}-{body_range[1]:.0f}K')

                # Check distance
                dist_range = class_ranges.get('distance_range')
                if dist_range and not (dist_range[0] <= distance <= dist_range[1]):
                    analysis['failure_reasons']['distance_out_of_range'] += 1
                    failure_detail['issues'].append(f'Distance {distance:.0f}ls outside {dist_range[0]:.0f}-{dist_range[1]:.0f}ls')
            else:
                analysis['failure_reasons']['stellar_class_unsupported'] += 1
                failure_detail['issues'].append(f'Stellar class {stellar_class} not in empirical data')

            analysis['failure_details'].append(failure_detail)

        return analysis

def main():
    parser = argparse.ArgumentParser(description='Test temperature range-based exobiology configuration')
    parser.add_argument('--test-systems', required=True, help='Path to test systems JSON file')
    parser.add_argument('--species', help='Test specific species only')
    parser.add_argument('--output-dir', help='Directory to save test results')
    parser.add_argument('--config-type', choices=['original', 'improved', 'both'], default='both',
                        help='Which configuration to test')

    args = parser.parse_args()

    # Load test systems
    with open(args.test_systems, 'r') as f:
        test_systems = json.load(f)

    print(f"Loaded {len(test_systems)} species with test systems")

    # Test configurations
    configs_to_test = []
    if args.config_type in ['original', 'both']:
        configs_to_test.append(('original', TemperatureRangeExobiologyConfig()))
    if args.config_type in ['improved', 'both']:
        configs_to_test.append(('improved', ImprovedExobiologyConfig()))

    for config_name, config in configs_to_test:
        print(f"\n=== TESTING {config_name.upper()} CONFIGURATION ===")
        validator = TemperatureRangeValidator(config, test_systems)

        if args.species:
            # Test specific species
            print(f"Testing {args.species} only...")
            result = validator.find_optimal_tolerance(args.species)

            # Analyze failures
            if result['detection_rate'] < 1.0:
                print(f"\\nAnalyzing failures for {args.species}:")
                failure_analysis = validator.analyze_failures(args.species, result)
                print(json.dumps(failure_analysis, indent=2))

        else:
            # Run comprehensive test
            results = validator.run_comprehensive_test()
            results['configuration_name'] = config_name

            # Print summary
            print(f"=== {config_name.upper()} TEST SUMMARY ===")
            summary = results['summary']
            print(f"Excellent detection (>90%): {len(summary['excellent_detection'])} species")
            for species in summary['excellent_detection']:
                rate = results['species_results'][species]['detection_rate']
                print(f"  ✅ {species}: {rate:.1%}")

            print(f"\\nGood detection (70-90%): {len(summary['good_detection'])} species")
            for species in summary['good_detection']:
                rate = results['species_results'][species]['detection_rate']
                print(f"  🟡 {species}: {rate:.1%}")

            print(f"\\nPoor detection (<70%): {len(summary['poor_detection'])} species")
            for species in summary['poor_detection']:
                rate = results['species_results'][species]['detection_rate']
                print(f"  ❌ {species}: {rate:.1%}")

            # Save results if output directory specified
            if args.output_dir:
                output_dir = Path(args.output_dir)
                output_dir.mkdir(parents=True, exist_ok=True)

                results_file = f'{config_name}_comprehensive_test_results.json'
                with open(output_dir / results_file, 'w') as f:
                    json.dump(results, f, indent=2)

                print(f"\\nDetailed results saved to: {output_dir / results_file}")

    # If testing both configurations, create comparison
    if args.config_type == 'both' and not args.species and args.output_dir:
        print(f"\\n=== CONFIGURATION COMPARISON ===")
        output_dir = Path(args.output_dir)

        # Load both result files
        original_file = output_dir / 'original_comprehensive_test_results.json'
        improved_file = output_dir / 'improved_comprehensive_test_results.json'

        if original_file.exists() and improved_file.exists():
            with open(original_file, 'r') as f:
                original_results = json.load(f)
            with open(improved_file, 'r') as f:
                improved_results = json.load(f)

            # Create comparison summary
            comparison = {
                'comparison_timestamp': datetime.now().isoformat(),
                'configurations_compared': ['original', 'improved'],
                'species_comparison': {}
            }

            for species in original_results['species_results'].keys():
                original_rate = original_results['species_results'][species]['detection_rate']
                improved_rate = improved_results['species_results'][species]['detection_rate']
                improvement = improved_rate - original_rate

                comparison['species_comparison'][species] = {
                    'original_detection_rate': original_rate,
                    'improved_detection_rate': improved_rate,
                    'improvement': improvement,
                    'improvement_percent': improvement * 100
                }

                print(f"{species}:")
                print(f"  Original: {original_rate:.1%}")
                print(f"  Improved: {improved_rate:.1%}")
                print(f"  Change: {improvement:+.1%} ({improvement * 100:+.1f} percentage points)")
                print()

            # Save comparison
            with open(output_dir / 'configuration_comparison.json', 'w') as f:
                json.dump(comparison, f, indent=2)

            print(f"Configuration comparison saved to: {output_dir / 'configuration_comparison.json'}")

if __name__ == "__main__":
    main()