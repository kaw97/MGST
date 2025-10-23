#!/usr/bin/env python3
"""
Script to analyze species stellar preferences by analyzing occurrences in single-star systems.
Generates detailed statistics on star type preferences, temperatures, and orbital distances.
"""

import json
import os
import sys
from pathlib import Path
from collections import defaultdict
import argparse
from typing import Dict, List, Any, Tuple
import statistics
import time

class StellarAnalysis:
    def __init__(self):
        self.species_data = defaultdict(lambda: defaultdict(list))
        self.single_star_entries = 0
        self.multi_star_entries = 0
        self.total_entries = 0

    def analyze_species_file(self, species_file: Path, genus: str, species: str) -> Dict[str, Any]:
        """
        Analyze a single species file for stellar preferences.
        """
        local_stats = {
            'total_entries': 0,
            'single_star_entries': 0,
            'multi_star_entries': 0,
            'star_type_data': defaultdict(list)
        }

        with open(species_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    local_stats['total_entries'] += 1

                    # Check if single-star system
                    star_count = entry.get('stellar_main_star_count', 0)
                    system_type = entry.get('stellar_system_type', '')

                    if star_count == 1 or system_type == "Single-star":
                        local_stats['single_star_entries'] += 1

                        # Extract stellar data
                        spectral_class = entry.get('stellar_spectral_class', '')
                        stellar_temp = entry.get('stellar_surface_temperature', 0.0)
                        solar_masses = entry.get('stellar_solar_masses', 0.0)
                        luminosity = entry.get('stellar_luminosity', '')

                        # Extract body data
                        distance_to_arrival = entry.get('body_distance_to_arrival', 0.0)
                        body_temp = entry.get('body_surface_temperature', 0.0)

                        # Store data point
                        data_point = {
                            'stellar_temperature': stellar_temp,
                            'solar_masses': solar_masses,
                            'luminosity': luminosity,
                            'distance_to_arrival': distance_to_arrival,
                            'body_temperature': body_temp
                        }

                        local_stats['star_type_data'][spectral_class].append(data_point)
                    else:
                        local_stats['multi_star_entries'] += 1

                except json.JSONDecodeError:
                    continue

        return local_stats

    def calculate_statistics(self, data_points: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate statistical summaries for a list of data points.
        """
        if not data_points:
            return {}

        stats = {
            'count': len(data_points),
            'stellar_temperature': {},
            'solar_masses': {},
            'distance_to_arrival': {},
            'body_temperature': {}
        }

        # Extract values for each metric
        for metric in ['stellar_temperature', 'solar_masses', 'distance_to_arrival', 'body_temperature']:
            values = [dp[metric] for dp in data_points if dp[metric] > 0]

            if values:
                stats[metric] = {
                    'mean': statistics.mean(values),
                    'median': statistics.median(values),
                    'min': min(values),
                    'max': max(values),
                    'std_dev': statistics.stdev(values) if len(values) > 1 else 0.0,
                    'count': len(values)
                }
            else:
                stats[metric] = {'count': 0}

        # Luminosity distribution
        luminosities = [dp['luminosity'] for dp in data_points if dp['luminosity']]
        luminosity_counts = defaultdict(int)
        for lum in luminosities:
            luminosity_counts[lum] += 1
        stats['luminosity_distribution'] = dict(luminosity_counts)

        return stats

    def analyze_all_species(self, analysis_dir: Path) -> Dict[str, Dict[str, Any]]:
        """
        Analyze all species in the analysis directory.
        """
        all_results = {}
        processed_species = 0

        print(f"Scanning analysis directory: {analysis_dir}")

        for genus_dir in analysis_dir.iterdir():
            if not genus_dir.is_dir():
                continue

            genus = genus_dir.name
            print(f"Processing genus: {genus}")

            for species_dir in genus_dir.iterdir():
                if not species_dir.is_dir():
                    continue

                species = species_dir.name
                species_key = f"{genus}_{species}"

                # Find the species entry file
                entry_files = list(species_dir.glob("*.jsonl"))
                if not entry_files:
                    continue

                species_file = entry_files[0]

                print(f"  Analyzing {species_key}...")

                # Analyze this species
                species_stats = self.analyze_species_file(species_file, genus, species)

                # Calculate detailed statistics for each star type
                star_type_analysis = {}
                for star_type, data_points in species_stats['star_type_data'].items():
                    star_type_analysis[star_type] = self.calculate_statistics(data_points)

                all_results[species_key] = {
                    'genus': genus,
                    'species': species,
                    'total_entries': species_stats['total_entries'],
                    'single_star_entries': species_stats['single_star_entries'],
                    'multi_star_entries': species_stats['multi_star_entries'],
                    'single_star_percentage': (species_stats['single_star_entries'] / species_stats['total_entries'] * 100) if species_stats['total_entries'] > 0 else 0,
                    'star_type_analysis': star_type_analysis,
                    'preferred_star_types': sorted(star_type_analysis.keys(), key=lambda x: star_type_analysis[x]['count'], reverse=True)[:5]
                }

                processed_species += 1
                if processed_species % 10 == 0:
                    print(f"    Processed {processed_species} species...")

        return all_results

    def generate_summary_report(self, results: Dict[str, Dict[str, Any]]) -> str:
        """
        Generate a comprehensive summary report.
        """
        report = []
        report.append("=" * 80)
        report.append("STELLAR PREFERENCES ANALYSIS REPORT")
        report.append("=" * 80)
        report.append("")

        # Overall statistics
        total_species = len(results)
        total_entries = sum(r['total_entries'] for r in results.values())
        total_single_star = sum(r['single_star_entries'] for r in results.values())

        report.append(f"OVERVIEW:")
        report.append(f"  Species analyzed: {total_species}")
        report.append(f"  Total entries: {total_entries:,}")
        report.append(f"  Single-star entries: {total_single_star:,}")
        report.append(f"  Single-star percentage: {total_single_star/total_entries*100:.1f}%")
        report.append("")

        # Star type frequency across all species
        all_star_types = defaultdict(int)
        for species_data in results.values():
            for star_type, analysis in species_data['star_type_analysis'].items():
                all_star_types[star_type] += analysis['count']

        report.append("MOST COMMON STAR TYPES (across all species):")
        for star_type, count in sorted(all_star_types.items(), key=lambda x: x[1], reverse=True)[:15]:
            percentage = count / total_single_star * 100 if total_single_star > 0 else 0
            report.append(f"  {star_type:<8} {count:>8,} entries ({percentage:5.1f}%)")
        report.append("")

        # Top species by single-star entries
        report.append("TOP 15 SPECIES BY SINGLE-STAR ENTRIES:")
        species_by_single_star = sorted(results.items(), key=lambda x: x[1]['single_star_entries'], reverse=True)
        for species_key, data in species_by_single_star[:15]:
            report.append(f"  {species_key:<25} {data['single_star_entries']:>8,} entries ({data['single_star_percentage']:5.1f}% single-star)")
        report.append("")

        # Detailed analysis for top 5 species
        report.append("DETAILED ANALYSIS - TOP 5 SPECIES:")
        report.append("")

        for species_key, data in species_by_single_star[:5]:
            report.append(f"{species_key.upper()}:")
            report.append(f"  Total entries: {data['total_entries']:,}")
            report.append(f"  Single-star entries: {data['single_star_entries']:,} ({data['single_star_percentage']:.1f}%)")
            report.append(f"  Top star types:")

            for i, star_type in enumerate(data['preferred_star_types'][:5], 1):
                if star_type in data['star_type_analysis']:
                    analysis = data['star_type_analysis'][star_type]
                    count = analysis['count']
                    percentage = count / data['single_star_entries'] * 100 if data['single_star_entries'] > 0 else 0

                    report.append(f"    {i}. {star_type:<8} {count:>6,} entries ({percentage:5.1f}%)")

                    # Temperature and distance stats
                    if 'stellar_temperature' in analysis and 'count' in analysis['stellar_temperature'] and analysis['stellar_temperature']['count'] > 0:
                        temp_stats = analysis['stellar_temperature']
                        dist_stats = analysis['distance_to_arrival'] if 'distance_to_arrival' in analysis else {}

                        report.append(f"       Stellar temp: {temp_stats.get('mean', 0):.0f}K ± {temp_stats.get('std_dev', 0):.0f}K")
                        if 'count' in dist_stats and dist_stats['count'] > 0:
                            report.append(f"       Distance: {dist_stats.get('mean', 0):.0f} ± {dist_stats.get('std_dev', 0):.0f} ls")

            report.append("")

        return "\n".join(report)

    def save_detailed_results(self, results: Dict[str, Dict[str, Any]], output_file: Path):
        """
        Save detailed results to JSON file.
        """
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)

        print(f"Detailed results saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Analyze species stellar preferences in single-star systems')
    parser.add_argument('--analysis-dir', required=True, help='Path to species analysis directory')
    parser.add_argument('--output-dir', required=True, help='Path to save analysis results')

    args = parser.parse_args()

    analysis_dir = Path(args.analysis_dir)
    output_dir = Path(args.output_dir)

    if not analysis_dir.exists():
        print(f"Error: Analysis directory {analysis_dir} does not exist")
        sys.exit(1)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=== STELLAR PREFERENCES ANALYSIS ===")
    print(f"Analyzing species in: {analysis_dir}")
    print(f"Output directory: {output_dir}")
    print("Restricting analysis to single-star systems only")
    print()

    start_time = time.time()

    analyzer = StellarAnalysis()
    results = analyzer.analyze_all_species(analysis_dir)

    elapsed_time = time.time() - start_time

    print(f"\nAnalysis complete in {elapsed_time:.1f} seconds")

    # Generate and save summary report
    summary_report = analyzer.generate_summary_report(results)

    report_file = output_dir / "stellar_preferences_report.txt"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(summary_report)

    # Save detailed results
    detailed_file = output_dir / "stellar_preferences_detailed.json"
    analyzer.save_detailed_results(results, detailed_file)

    # Print summary to console
    print("\n" + summary_report)

    print(f"\nResults saved:")
    print(f"  Summary report: {report_file}")
    print(f"  Detailed data: {detailed_file}")

if __name__ == "__main__":
    main()