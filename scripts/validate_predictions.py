#!/usr/bin/env python3
"""
Validation script that tests temperature range predictions against empirical data.

Uses enriched codex data (empirical observations) to validate predictions made
from the compressed galaxy database (Spansh/EDDB data).
"""

import sys
import json
import gzip
from pathlib import Path
from typing import Dict, List, Any, Set
from collections import defaultdict
import argparse
import random

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from hitec_galaxy.configs.temperature_range_exobiology import TemperatureRangeExobiologyConfig
from hitec_galaxy.configs.improved_exobiology import ImprovedExobiologyConfig

class PredictionValidator:
    """Validates exobiology predictions against empirical codex data."""

    def __init__(self, enriched_codex_path: str, galaxy_db_path: str):
        self.enriched_codex_path = enriched_codex_path
        self.galaxy_db_path = Path(galaxy_db_path)

        # Load configurations to test
        self.configs = {
            'original': TemperatureRangeExobiologyConfig(),
            'improved': ImprovedExobiologyConfig()
        }

    def extract_empirical_observations(self, max_samples: int = 1000) -> Dict[str, List[Dict]]:
        """Extract empirical observations by species from enriched codex data."""
        print(f"Extracting empirical observations from {self.enriched_codex_path}...")

        species_observations = defaultdict(list)
        total_processed = 0

        with open(self.enriched_codex_path, 'r', encoding='utf-8') as f:
            for line in f:
                if total_processed >= max_samples * 10:  # Process more to get diverse samples
                    break

                try:
                    entry = json.loads(line.strip())
                    # Parse species name from english_name (e.g., "Bacterium Cerbrus - Indigo" -> "Bacterium Cerbrus")
                    english_name = entry.get('english_name', '')
                    if english_name and ' - ' in english_name:
                        species_name = english_name.split(' - ')[0].strip()
                    else:
                        species_name = english_name

                    system_name = entry.get('system', '')

                    if species_name and system_name:
                        # Store observation with key data
                        observation = {
                            'system': system_name,
                            'species': species_name,
                            'body': entry.get('body', ''),
                            'stellar_spectral_class': entry.get('stellar_spectral_class', ''),
                            'stellar_surface_temperature': entry.get('stellar_surface_temperature', 0),
                            'body_surface_temperature': entry.get('body_surface_temperature', 0),
                            'body_distance_to_arrival': entry.get('body_distance_to_arrival', 0),
                            'system_type': entry.get('stellar_system_type', '')
                        }

                        species_observations[species_name].append(observation)

                    total_processed += 1

                except json.JSONDecodeError:
                    continue

        print(f"Processed {total_processed} entries, found observations for {len(species_observations)} species")

        # Sample observations per species to get manageable test set
        sampled_observations = {}
        for species, observations in species_observations.items():
            if len(observations) >= 1:  # Need at least 1 observation
                sample_size = min(max(1, max_samples // len(species_observations)), len(observations))
                sampled_observations[species] = random.sample(observations, sample_size)

        return sampled_observations

    def lookup_system_in_galaxy_db(self, system_name: str) -> Dict[str, Any]:
        """Look up a system in the compressed galaxy database."""
        # Determine sector from system name
        sector = self.parse_sector_name(system_name)
        if not sector:
            return {}

        # Try to find the sector file
        sector_file = self.galaxy_db_path / f"{sector}.jsonl.gz"
        if not sector_file.exists():
            # Try uncompressed version
            sector_file = self.galaxy_db_path / f"{sector}.jsonl"
            if not sector_file.exists():
                return {}

        # Search for the system in the sector file
        try:
            open_func = gzip.open if sector_file.suffix == '.gz' else open
            with open_func(sector_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        system_data = json.loads(line.strip())
                        if system_data.get('name') == system_name:
                            return system_data
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"Error reading sector file {sector_file}: {e}")

        return {}

    def parse_sector_name(self, system_name: str) -> str:
        """Parse sector name from system name using mass code pattern."""
        import re
        mass_code_pattern = r'\b([A-Z]{2}-[A-Z])\b'
        match = re.search(mass_code_pattern, system_name)
        if match:
            mass_code_start = match.start()
            sector_name = system_name[:mass_code_start].strip()
            return sector_name
        return ""

    def test_prediction_accuracy(self, observations: Dict[str, List[Dict]], config_name: str) -> Dict[str, Any]:
        """Test prediction accuracy for a configuration."""
        config = self.configs[config_name]
        results = {
            'config_name': config_name,
            'species_results': {},
            'overall_stats': {
                'total_predictions': 0,
                'correct_predictions': 0,
                'systems_found': 0,
                'systems_not_found': 0
            }
        }

        print(f"\nTesting {config_name} configuration...")

        for species_name, species_observations in observations.items():
            print(f"  Testing {species_name} ({len(species_observations)} observations)...")

            species_stats = {
                'total_observations': len(species_observations),
                'systems_found_in_db': 0,
                'correct_predictions': 0,
                'false_positives': 0,
                'false_negatives': 0,
                'prediction_accuracy': 0.0,
                'sample_results': []
            }

            for obs in species_observations:
                system_name = obs['system']

                # Look up system in galaxy database
                system_data = self.lookup_system_in_galaxy_db(system_name)
                results['overall_stats']['total_predictions'] += 1

                if not system_data:
                    results['overall_stats']['systems_not_found'] += 1
                    continue

                results['overall_stats']['systems_found'] += 1
                species_stats['systems_found_in_db'] += 1

                # Find the body that matches the observation
                matching_body = self.find_matching_body(system_data, obs)
                if not matching_body:
                    continue

                # Test prediction: does our config predict this species on this body?
                predicted_species = config.detect_species_on_body(matching_body, system_data)
                species_predicted = any(species_name.lower() in pred['name'].lower()
                                      for pred in predicted_species)

                # Record result
                sample_result = {
                    'system': system_name,
                    'body': obs['body'],
                    'empirical_species': species_name,
                    'prediction_correct': species_predicted,
                    'stellar_class': obs['stellar_spectral_class'],
                    'stellar_temp': obs['stellar_surface_temperature'],
                    'body_temp': obs['body_surface_temperature'],
                    'distance': obs['body_distance_to_arrival']
                }

                species_stats['sample_results'].append(sample_result)

                if species_predicted:
                    species_stats['correct_predictions'] += 1
                    results['overall_stats']['correct_predictions'] += 1

            # Calculate accuracy
            if species_stats['systems_found_in_db'] > 0:
                species_stats['prediction_accuracy'] = (
                    species_stats['correct_predictions'] / species_stats['systems_found_in_db']
                )

            results['species_results'][species_name] = species_stats

        # Calculate overall accuracy
        if results['overall_stats']['systems_found'] > 0:
            results['overall_stats']['accuracy'] = (
                results['overall_stats']['correct_predictions'] /
                results['overall_stats']['systems_found']
            )

        return results

    def find_matching_body(self, system_data: Dict, observation: Dict) -> Dict[str, Any]:
        """Find the body in system_data that matches the observation."""
        obs_body_name = observation.get('body', '')
        obs_temp = observation.get('body_surface_temperature', 0)
        obs_distance = observation.get('body_distance_to_arrival', 0)

        bodies = system_data.get('bodies', [])

        # First try exact name match
        for body in bodies:
            if body.get('name', '') == obs_body_name:
                return body

        # Then try partial name match
        for body in bodies:
            body_name = body.get('name', '') or ''
            if obs_body_name and body_name and (obs_body_name in body_name or body_name in obs_body_name):
                return body

        # Finally try temperature and distance matching
        best_match = None
        best_score = float('inf')

        for body in bodies:
            body_temp = body.get('surfaceTemperature', 0)
            body_distance = body.get('distanceToArrival', 0)

            # Calculate similarity score
            temp_diff = abs(body_temp - obs_temp) if obs_temp > 0 else 0
            dist_diff = abs(body_distance - obs_distance) if obs_distance > 0 else 0
            score = temp_diff + (dist_diff / 1000)  # Normalize distance

            if score < best_score:
                best_score = score
                best_match = body

        return best_match or {}

    def generate_report(self, results: Dict[str, Dict]) -> None:
        """Generate a validation report comparing configurations."""
        print("\n" + "="*60)
        print("PREDICTION VALIDATION REPORT")
        print("="*60)

        for config_name, result in results.items():
            overall = result['overall_stats']
            print(f"\n{config_name.upper()} Configuration:")
            print(f"  Systems found in DB: {overall['systems_found']}/{overall['total_predictions']}")
            print(f"  Overall accuracy: {overall.get('accuracy', 0):.1%}")
            print(f"  Correct predictions: {overall['correct_predictions']}")

            print(f"\n  Species-level results:")
            for species_name, stats in result['species_results'].items():
                accuracy = stats['prediction_accuracy']
                found = stats['systems_found_in_db']
                correct = stats['correct_predictions']
                print(f"    {species_name}: {accuracy:.1%} ({correct}/{found})")

        # Compare configurations
        if len(results) > 1:
            print(f"\nCOMPARISON:")
            config_names = list(results.keys())
            for i, config1 in enumerate(config_names):
                for config2 in config_names[i+1:]:
                    acc1 = results[config1]['overall_stats'].get('accuracy', 0)
                    acc2 = results[config2]['overall_stats'].get('accuracy', 0)
                    improvement = acc2 - acc1
                    print(f"  {config2} vs {config1}: {improvement:+.1%} ({acc2:.1%} vs {acc1:.1%})")

def main():
    parser = argparse.ArgumentParser(description='Validate exobiology predictions against empirical data')
    parser.add_argument('--enriched-codex', required=True,
                        help='Path to enriched codex JSONL file')
    parser.add_argument('--galaxy-db', required=True,
                        help='Path to compressed galaxy database directory')
    parser.add_argument('--max-samples', type=int, default=500,
                        help='Maximum samples to test per configuration')
    parser.add_argument('--output-dir', help='Directory to save results')
    parser.add_argument('--config', choices=['original', 'improved', 'both'], default='both',
                        help='Which configuration(s) to test')

    args = parser.parse_args()

    # Initialize validator
    validator = PredictionValidator(args.enriched_codex, args.galaxy_db)

    # Extract empirical observations
    observations = validator.extract_empirical_observations(args.max_samples)

    if not observations:
        print("No observations extracted. Check input files.")
        return

    print(f"Extracted observations for {len(observations)} species:")
    for species, obs_list in observations.items():
        print(f"  {species}: {len(obs_list)} observations")

    # Test configurations
    results = {}
    configs_to_test = []

    if args.config in ['original', 'both']:
        configs_to_test.append('original')
    if args.config in ['improved', 'both']:
        configs_to_test.append('improved')

    for config_name in configs_to_test:
        results[config_name] = validator.test_prediction_accuracy(observations, config_name)

    # Generate report
    validator.generate_report(results)

    # Save results
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        for config_name, result in results.items():
            output_file = output_dir / f'{config_name}_validation_results.json'
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            print(f"Saved {config_name} results to: {output_file}")

if __name__ == "__main__":
    main()