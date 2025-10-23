#!/usr/bin/env python3
"""
Script to generate comprehensive stellar adaptation summary files for all species.
Creates individual species reports and comparative analysis across stellar classes.
"""

import json
import os
import sys
from pathlib import Path
from collections import defaultdict
import argparse
from typing import Dict, List, Any, Tuple
import time

def calculate_percentile(values: List[float], percentile: float) -> float:
    """Calculate percentile of a list of values."""
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = (percentile / 100.0) * (len(sorted_values) - 1)
    lower_index = int(index)
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    weight = index - lower_index
    return sorted_values[lower_index] * (1 - weight) + sorted_values[upper_index] * weight

def calculate_stellar_class_stats(star_analysis: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Group star types by stellar class and calculate aggregate statistics with percentile bounds.
    """
    stellar_classes = defaultdict(list)

    # Group by stellar class (first letter)
    for star_type, analysis in star_analysis.items():
        if analysis.get('count', 0) > 10:  # Minimum threshold for meaningful data
            stellar_class = star_type[0] if star_type else 'Unknown'
            stellar_classes[stellar_class].append((star_type, analysis))

    class_stats = {}

    for stellar_class, star_types in stellar_classes.items():
        total_entries = sum(analysis['count'] for _, analysis in star_types)

        # Collect all individual data points (not weighted averages)
        all_stellar_temp_points = []
        all_body_temp_points = []
        all_distance_points = []

        detailed_types = []

        for star_type, analysis in star_types:
            count = analysis['count']

            # Extract statistics
            stellar_temp = analysis.get('stellar_temperature', {})
            body_temp = analysis.get('body_temperature', {})
            distance = analysis.get('distance_to_arrival', {})

            type_data = {
                'star_type': star_type,
                'count': count,
                'stellar_temp_mean': stellar_temp.get('mean', 0),
                'stellar_temp_std': stellar_temp.get('std_dev', 0),
                'body_temp_mean': body_temp.get('mean', 0),
                'body_temp_std': body_temp.get('std_dev', 0),
                'distance_mean': distance.get('mean', 0),
                'distance_std': distance.get('std_dev', 0)
            }
            detailed_types.append(type_data)

            # For percentile calculations, simulate individual data points using mean ± std
            # This is an approximation but gives us distribution bounds
            if stellar_temp.get('count', 0) > 0 and stellar_temp.get('std_dev', 0) > 0:
                mean = stellar_temp['mean']
                std = stellar_temp['std_dev']
                # Generate representative sample points assuming normal distribution
                sample_points = [mean + std * x for x in [-1.5, -1.0, -0.5, 0, 0.5, 1.0, 1.5]]
                all_stellar_temp_points.extend(sample_points * (count // 7 + 1))
            elif stellar_temp.get('count', 0) > 0:
                all_stellar_temp_points.extend([stellar_temp['mean']] * count)

            if body_temp.get('count', 0) > 0 and body_temp.get('std_dev', 0) > 0:
                mean = body_temp['mean']
                std = body_temp['std_dev']
                sample_points = [mean + std * x for x in [-1.5, -1.0, -0.5, 0, 0.5, 1.0, 1.5]]
                all_body_temp_points.extend(sample_points * (count // 7 + 1))
            elif body_temp.get('count', 0) > 0:
                all_body_temp_points.extend([body_temp['mean']] * count)

            if distance.get('count', 0) > 0 and distance.get('std_dev', 0) > 0:
                mean = distance['mean']
                std = distance['std_dev']
                sample_points = [max(0, mean + std * x) for x in [-1.5, -1.0, -0.5, 0, 0.5, 1.0, 1.5]]
                all_distance_points.extend(sample_points * (count // 7 + 1))
            elif distance.get('count', 0) > 0:
                all_distance_points.extend([distance['mean']] * count)

        # Calculate class-wide statistics with percentiles
        class_summary = {
            'stellar_class': stellar_class,
            'total_entries': total_entries,
            'star_types': sorted(detailed_types, key=lambda x: x['count'], reverse=True),
            'class_averages': {}
        }

        if all_stellar_temp_points:
            class_summary['class_averages']['stellar_temp'] = sum(all_stellar_temp_points) / len(all_stellar_temp_points)
            class_summary['class_averages']['stellar_temp_p25'] = calculate_percentile(all_stellar_temp_points, 25)
            class_summary['class_averages']['stellar_temp_p75'] = calculate_percentile(all_stellar_temp_points, 75)
            class_summary['class_averages']['stellar_temp_bounds'] = (
                calculate_percentile(all_stellar_temp_points, 12.5),
                calculate_percentile(all_stellar_temp_points, 87.5)
            )

        if all_body_temp_points:
            class_summary['class_averages']['body_temp'] = sum(all_body_temp_points) / len(all_body_temp_points)
            class_summary['class_averages']['body_temp_min'] = min(all_body_temp_points)
            class_summary['class_averages']['body_temp_max'] = max(all_body_temp_points)
            class_summary['class_averages']['body_temp_range'] = max(all_body_temp_points) - min(all_body_temp_points)
            class_summary['class_averages']['body_temp_p25'] = calculate_percentile(all_body_temp_points, 25)
            class_summary['class_averages']['body_temp_p75'] = calculate_percentile(all_body_temp_points, 75)
            class_summary['class_averages']['body_temp_bounds'] = (
                calculate_percentile(all_body_temp_points, 12.5),
                calculate_percentile(all_body_temp_points, 87.5)
            )

        if all_distance_points:
            class_summary['class_averages']['distance'] = sum(all_distance_points) / len(all_distance_points)
            class_summary['class_averages']['distance_p25'] = calculate_percentile(all_distance_points, 25)
            class_summary['class_averages']['distance_p75'] = calculate_percentile(all_distance_points, 75)
            class_summary['class_averages']['distance_bounds'] = (
                calculate_percentile(all_distance_points, 12.5),
                calculate_percentile(all_distance_points, 87.5)
            )

        class_stats[stellar_class] = class_summary

    return class_stats

def generate_species_report(species_key: str, species_data: Dict[str, Any]) -> str:
    """
    Generate a detailed report for a single species.
    """
    report = []

    # Header
    report.append("=" * 80)
    report.append(f"STELLAR ADAPTATION ANALYSIS: {species_key.replace('_', ' ').upper()}")
    report.append("=" * 80)
    report.append("")

    # Basic statistics
    report.append("OVERVIEW:")
    report.append(f"  Genus: {species_data.get('genus', 'Unknown')}")
    report.append(f"  Species: {species_data.get('species', 'Unknown')}")
    report.append(f"  Total entries: {species_data.get('total_entries', 0):,}")
    report.append(f"  Single-star entries: {species_data.get('single_star_entries', 0):,}")
    report.append(f"  Single-star percentage: {species_data.get('single_star_percentage', 0):.1f}%")
    report.append("")

    # Stellar class analysis
    star_analysis = species_data.get('star_type_analysis', {})
    class_stats = calculate_stellar_class_stats(star_analysis)

    if class_stats:
        report.append("STELLAR CLASS ADAPTATION SUMMARY:")
        report.append("")

        # Sort by total entries
        sorted_classes = sorted(class_stats.items(), key=lambda x: x[1]['total_entries'], reverse=True)

        for stellar_class, data in sorted_classes:
            total = data['total_entries']
            percentage = total / species_data.get('single_star_entries', 1) * 100

            report.append(f"{stellar_class}-CLASS STARS ({total:,} entries, {percentage:.1f}%):")

            averages = data['class_averages']
            if 'stellar_temp' in averages:
                report.append(f"  Average stellar temperature: {averages['stellar_temp']:.0f}K")
                if 'stellar_temp_bounds' in averages:
                    bounds = averages['stellar_temp_bounds']
                    report.append(f"  Stellar temp 75% range: {bounds[0]:.0f}K - {bounds[1]:.0f}K")
            if 'body_temp' in averages:
                report.append(f"  Average body temperature: {averages['body_temp']:.0f}K")
                report.append(f"  Body temperature range: {averages['body_temp_min']:.0f}K - {averages['body_temp_max']:.0f}K (span: {averages['body_temp_range']:.0f}K)")
                if 'body_temp_bounds' in averages:
                    bounds = averages['body_temp_bounds']
                    report.append(f"  Body temp 75% range: {bounds[0]:.0f}K - {bounds[1]:.0f}K")
            if 'distance' in averages:
                report.append(f"  Average distance to star: {averages['distance']:.0f} ls")
                if 'distance_bounds' in averages:
                    bounds = averages['distance_bounds']
                    report.append(f"  Distance 75% range: {bounds[0]:.0f} - {bounds[1]:.0f} ls")

            # Thermal regulation assessment
            if 'body_temp_range' in averages:
                temp_range = averages['body_temp_range']
                if temp_range < 15:
                    regulation = "Excellent"
                elif temp_range < 30:
                    regulation = "Good"
                elif temp_range < 50:
                    regulation = "Moderate"
                else:
                    regulation = "Poor"
                report.append(f"  Thermal regulation: {regulation} ({temp_range:.0f}K span)")

            report.append("")

        # Identify preferred stellar class
        if sorted_classes:
            preferred_class = sorted_classes[0][0]
            preferred_data = sorted_classes[0][1]

            report.append("ADAPTATION ANALYSIS:")
            report.append(f"  Preferred stellar class: {preferred_class}-class stars")
            report.append(f"  Primary habitat: {preferred_data['total_entries']:,} entries ({preferred_data['total_entries']/species_data.get('single_star_entries', 1)*100:.1f}%)")

            # Compare thermal regulation across classes
            if len(sorted_classes) > 1:
                temp_ranges = [(sc, data['class_averages'].get('body_temp_range', 0)) for sc, data in sorted_classes if 'body_temp_range' in data['class_averages']]
                if temp_ranges:
                    best_regulation = min(temp_ranges, key=lambda x: x[1])
                    worst_regulation = max(temp_ranges, key=lambda x: x[1])

                    report.append(f"  Best thermal regulation: {best_regulation[0]}-class ({best_regulation[1]:.0f}K span)")
                    report.append(f"  Worst thermal regulation: {worst_regulation[0]}-class ({worst_regulation[1]:.0f}K span)")
            report.append("")

    # Detailed star type breakdown
    if star_analysis:
        report.append("DETAILED STAR TYPE ANALYSIS:")
        report.append("")

        # Sort by count
        sorted_types = sorted(star_analysis.items(), key=lambda x: x[1]['count'], reverse=True)

        for i, (star_type, analysis) in enumerate(sorted_types[:15], 1):  # Top 15
            count = analysis['count']
            percentage = count / species_data.get('single_star_entries', 1) * 100

            report.append(f"{i:2}. {star_type:<8} {count:>6,} entries ({percentage:5.1f}%)")

            stellar_temp = analysis.get('stellar_temperature', {})
            body_temp = analysis.get('body_temperature', {})
            distance = analysis.get('distance_to_arrival', {})

            if stellar_temp.get('count', 0) > 0:
                mean = stellar_temp['mean']
                std = stellar_temp['std_dev']
                # Calculate approximate 75% bounds (mean ± 1.15*std covers ~75% for normal distribution)
                bound_range = 1.15 * std if std > 0 else 0
                lower_bound = mean - bound_range
                upper_bound = mean + bound_range
                report.append(f"    Stellar temp: {mean:.0f}K ± {std:.0f}K (75%: {lower_bound:.0f}K - {upper_bound:.0f}K)")
            if body_temp.get('count', 0) > 0:
                mean = body_temp['mean']
                std = body_temp['std_dev']
                bound_range = 1.15 * std if std > 0 else 0
                lower_bound = mean - bound_range
                upper_bound = mean + bound_range
                report.append(f"    Body temp:    {mean:.0f}K ± {std:.0f}K (75%: {lower_bound:.0f}K - {upper_bound:.0f}K)")
            if distance.get('count', 0) > 0:
                mean = distance['mean']
                std = distance['std_dev']
                bound_range = 1.15 * std if std > 0 else 0
                lower_bound = max(0, mean - bound_range)
                upper_bound = mean + bound_range
                report.append(f"    Distance:     {mean:.0f} ± {std:.0f} ls (75%: {lower_bound:.0f} - {upper_bound:.0f} ls)")

            report.append("")

    return "\n".join(report)

def generate_comparative_analysis(all_species_data: Dict[str, Dict[str, Any]]) -> str:
    """
    Generate a comparative analysis across all species.
    """
    report = []

    report.append("=" * 80)
    report.append("COMPARATIVE STELLAR ADAPTATION ANALYSIS")
    report.append("=" * 80)
    report.append("")

    # Overall statistics
    total_species = len(all_species_data)
    total_entries = sum(data['total_entries'] for data in all_species_data.values())
    total_single_star = sum(data['single_star_entries'] for data in all_species_data.values())

    report.append("DATASET OVERVIEW:")
    report.append(f"  Total species analyzed: {total_species}")
    report.append(f"  Total entries: {total_entries:,}")
    report.append(f"  Single-star entries: {total_single_star:,}")
    report.append(f"  Single-star percentage: {total_single_star/total_entries*100:.1f}%")
    report.append("")

    # Stellar class preferences across all species
    all_class_counts = defaultdict(int)
    species_by_class = defaultdict(list)

    for species_key, species_data in all_species_data.items():
        star_analysis = species_data.get('star_type_analysis', {})
        class_stats = calculate_stellar_class_stats(star_analysis)

        if class_stats:
            # Find preferred class (most entries)
            preferred_class = max(class_stats.keys(), key=lambda x: class_stats[x]['total_entries'])
            species_by_class[preferred_class].append((species_key, class_stats[preferred_class]['total_entries']))

            # Count all class occurrences
            for stellar_class, data in class_stats.items():
                all_class_counts[stellar_class] += data['total_entries']

    report.append("STELLAR CLASS DISTRIBUTION (all species):")
    for stellar_class, count in sorted(all_class_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = count / total_single_star * 100
        report.append(f"  {stellar_class}-class: {count:>8,} entries ({percentage:5.1f}%)")
    report.append("")

    report.append("SPECIES PREFERENCES BY STELLAR CLASS:")
    for stellar_class in sorted(species_by_class.keys()):
        species_list = species_by_class[stellar_class]
        report.append(f"  {stellar_class}-class specialists ({len(species_list)} species):")

        # Show top 5 species for this class
        for species_key, entries in sorted(species_list, key=lambda x: x[1], reverse=True)[:5]:
            report.append(f"    {species_key:<25} {entries:>6,} entries")
        if len(species_list) > 5:
            report.append(f"    ... and {len(species_list)-5} more species")
        report.append("")

    # Thermal regulation champions
    report.append("THERMAL REGULATION ANALYSIS:")

    regulation_rankings = []
    for species_key, species_data in all_species_data.items():
        star_analysis = species_data.get('star_type_analysis', {})
        class_stats = calculate_stellar_class_stats(star_analysis)

        if class_stats:
            # Calculate average thermal regulation across all classes
            temp_ranges = []
            for stellar_class, data in class_stats.items():
                if 'body_temp_range' in data['class_averages']:
                    temp_ranges.append(data['class_averages']['body_temp_range'])

            if temp_ranges:
                avg_temp_range = sum(temp_ranges) / len(temp_ranges)
                regulation_rankings.append((species_key, avg_temp_range, species_data['single_star_entries']))

    # Best thermal regulators
    if regulation_rankings:
        report.append("  Best thermal regulators (smallest temperature spans):")
        best_regulators = sorted(regulation_rankings, key=lambda x: x[1])[:10]
        for i, (species_key, temp_range, entries) in enumerate(best_regulators, 1):
            if entries >= 1000:  # Only species with substantial data
                report.append(f"    {i:2}. {species_key:<25} {temp_range:5.1f}K span ({entries:,} entries)")

        report.append("")
        report.append("  Poorest thermal regulators (largest temperature spans):")
        worst_regulators = sorted(regulation_rankings, key=lambda x: x[1], reverse=True)[:10]
        for i, (species_key, temp_range, entries) in enumerate(worst_regulators, 1):
            if entries >= 1000:  # Only species with substantial data
                report.append(f"    {i:2}. {species_key:<25} {temp_range:5.1f}K span ({entries:,} entries)")

    return "\n".join(report)

def main():
    parser = argparse.ArgumentParser(description='Generate comprehensive stellar adaptation summaries for all species')
    parser.add_argument('--input-file', required=True, help='Path to detailed stellar preferences JSON file')
    parser.add_argument('--output-dir', required=True, help='Path to save species summary files')

    args = parser.parse_args()

    input_file = Path(args.input_file)
    output_dir = Path(args.output_dir)

    if not input_file.exists():
        print(f"Error: Input file {input_file} does not exist")
        sys.exit(1)

    # Create output directories
    output_dir.mkdir(parents=True, exist_ok=True)
    species_dir = output_dir / "species_reports"
    species_dir.mkdir(exist_ok=True)

    print("=== GENERATING COMPREHENSIVE SPECIES SUMMARIES ===")
    print(f"Input file: {input_file}")
    print(f"Output directory: {output_dir}")
    print()

    start_time = time.time()

    # Load the detailed analysis data
    with open(input_file, 'r', encoding='utf-8') as f:
        all_species_data = json.load(f)

    print(f"Loaded data for {len(all_species_data)} species")

    # Generate individual species reports
    print("Generating individual species reports...")
    processed = 0

    for species_key, species_data in all_species_data.items():
        if species_data.get('single_star_entries', 0) > 0:  # Only species with single-star data
            report = generate_species_report(species_key, species_data)

            report_file = species_dir / f"{species_key}_stellar_adaptation.txt"
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)

            processed += 1
            if processed % 50 == 0:
                print(f"  Generated {processed} species reports...")

    print(f"Generated {processed} individual species reports")

    # Generate comparative analysis
    print("Generating comparative analysis...")
    comparative_report = generate_comparative_analysis(all_species_data)

    comparative_file = output_dir / "comparative_stellar_adaptation.txt"
    with open(comparative_file, 'w', encoding='utf-8') as f:
        f.write(comparative_report)

    elapsed_time = time.time() - start_time

    print(f"\nSummary generation complete in {elapsed_time:.1f} seconds")
    print(f"\nFiles generated:")
    print(f"  Individual reports: {species_dir}/ ({processed} files)")
    print(f"  Comparative analysis: {comparative_file}")
    print(f"\nAll files saved to: {output_dir}")

if __name__ == "__main__":
    main()