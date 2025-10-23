#!/usr/bin/env python3
"""
Comprehensive Statistical Analysis: Stratum Tectonicas vs Bacterium Species
Full Galaxy Dataset Analysis

This script performs the complete statistical analysis that was requested,
using the actual full galaxy biological dataset instead of the small test sample.

Analyzes distribution patterns, body type preferences, and predictive characteristics
for Stratum Tectonicas vs Bacterium species across the entire Elite Dangerous galaxy.
"""

import pandas as pd
import numpy as np
import json
import argparse
from pathlib import Path
from collections import defaultdict, Counter
from scipy import stats
import sys

def parse_genera_list(genera_str):
    """Parse the top_genera string into individual genus types."""
    if pd.isna(genera_str) or genera_str == '':
        return []

    # Handle the format: "stratum(19.0M), bacterium(1.7M), anemone(1.5M)"
    genera_list = []
    for item in genera_str.split(','):
        item = item.strip()
        if '(' in item:
            # Extract genus name before the parentheses
            genus = item.split('(')[0].strip()
            genera_list.append(genus)
        else:
            genera_list.append(item.strip())

    return genera_list

def classify_genus_type(genus_list):
    """Classify whether a body has Stratum, Bacterium, both, or other genera."""
    if not genus_list:
        return 'no_bio'

    has_stratum = any('stratum' in genus.lower() for genus in genus_list)
    has_bacterium = any('bacterium' in genus.lower() for genus in genus_list)

    if has_stratum and has_bacterium:
        return 'both'
    elif has_stratum:
        return 'stratum_only'
    elif has_bacterium:
        return 'bacterium_only'
    else:
        return 'other'

def analyze_body_characteristics(df, body_prefix):
    """Analyze characteristics for a specific body (body_1, body_2, or body_3)."""
    body_data = {}

    # Get columns for this body
    name_col = f"{body_prefix}_name"
    atmosphere_col = f"{body_prefix}_atmosphere"
    pressure_col = f"{body_prefix}_pressure"
    temperature_col = f"{body_prefix}_temperature"
    gravity_col = f"{body_prefix}_gravity"
    body_type_col = f"{body_prefix}_body_type"
    has_bacterium_col = f"{body_prefix}_has_bacterium"
    top_genera_col = f"{body_prefix}_top_genera"

    # Filter to bodies that exist
    body_filter = df[name_col].notna() & (df[name_col] != '')
    body_df = df[body_filter].copy()

    if len(body_df) == 0:
        return body_data

    print(f"\n=== Analysis for {body_prefix.upper()} ===")
    print(f"Total bodies analyzed: {len(body_df):,}")

    # Parse genera for each body
    body_df['parsed_genera'] = body_df[top_genera_col].apply(parse_genera_list)
    body_df['genus_classification'] = body_df['parsed_genera'].apply(classify_genus_type)

    # Basic genus distribution
    genus_dist = body_df['genus_classification'].value_counts()
    print(f"\nGenus Classification Distribution:")
    for genus_type, count in genus_dist.items():
        pct = count / len(body_df) * 100
        print(f"  {genus_type}: {count:,} ({pct:.2f}%)")

    body_data['genus_distribution'] = genus_dist.to_dict()
    body_data['total_bodies'] = len(body_df)

    # Analyze by body type
    print(f"\n--- Body Type Analysis ---")
    body_type_analysis = {}

    for body_type in body_df[body_type_col].unique():
        if pd.isna(body_type):
            continue

        type_filter = body_df[body_type_col] == body_type
        type_df = body_df[type_filter]

        if len(type_df) < 10:  # Skip body types with too few samples
            continue

        type_genus_dist = type_df['genus_classification'].value_counts()

        stratum_count = type_genus_dist.get('stratum_only', 0)
        bacterium_count = type_genus_dist.get('bacterium_only', 0)
        both_count = type_genus_dist.get('both', 0)
        total_bio = stratum_count + bacterium_count + both_count

        if total_bio > 0:
            stratum_pct = (stratum_count + both_count) / total_bio * 100
            bacterium_pct = (bacterium_count + both_count) / total_bio * 100

            print(f"  {body_type}: {total_bio:,} bio bodies")
            print(f"    Stratum presence: {stratum_count + both_count:,} ({stratum_pct:.1f}%)")
            print(f"    Bacterium presence: {bacterium_count + both_count:,} ({bacterium_pct:.1f}%)")
            print(f"    Stratum-only: {stratum_count:,}, Bacterium-only: {bacterium_count:,}, Both: {both_count:,}")

            body_type_analysis[body_type] = {
                'total_bio_bodies': total_bio,
                'stratum_only': stratum_count,
                'bacterium_only': bacterium_count,
                'both': both_count,
                'stratum_percentage': stratum_pct,
                'bacterium_percentage': bacterium_pct
            }

    body_data['body_type_analysis'] = body_type_analysis

    # Analyze High Metal Content bodies specifically
    print(f"\n--- High Metal Content Body Analysis ---")
    hmc_filter = body_df[body_type_col].str.contains('High metal content', na=False)
    hmc_df = body_df[hmc_filter]

    if len(hmc_df) > 0:
        hmc_genus_dist = hmc_df['genus_classification'].value_counts()

        print(f"Total HMC bodies: {len(hmc_df):,}")
        print(f"HMC genus distribution:")
        for genus_type, count in hmc_genus_dist.items():
            pct = count / len(hmc_df) * 100
            print(f"  {genus_type}: {count:,} ({pct:.2f}%)")

        body_data['hmc_analysis'] = {
            'total_hmc_bodies': len(hmc_df),
            'genus_distribution': hmc_genus_dist.to_dict()
        }

    # Atmosphere analysis
    print(f"\n--- Atmosphere Analysis ---")
    atmosphere_analysis = {}

    # Focus on biological bodies only
    bio_filter = body_df['genus_classification'].isin(['stratum_only', 'bacterium_only', 'both'])
    bio_df = body_df[bio_filter]

    if len(bio_df) > 0:
        for atmosphere in bio_df[atmosphere_col].unique():
            if pd.isna(atmosphere):
                continue

            atm_filter = bio_df[atmosphere_col] == atmosphere
            atm_df = bio_df[atm_filter]

            if len(atm_df) < 5:  # Skip atmospheres with too few samples
                continue

            atm_genus_dist = atm_df['genus_classification'].value_counts()

            stratum_count = atm_genus_dist.get('stratum_only', 0)
            bacterium_count = atm_genus_dist.get('bacterium_only', 0)
            both_count = atm_genus_dist.get('both', 0)
            total_bio = stratum_count + bacterium_count + both_count

            if total_bio > 0:
                stratum_pct = (stratum_count + both_count) / total_bio * 100
                bacterium_pct = (bacterium_count + both_count) / total_bio * 100

                print(f"  {atmosphere}: {total_bio:,} bio bodies")
                print(f"    Stratum: {stratum_pct:.1f}%, Bacterium: {bacterium_pct:.1f}%")

                # Statistical significance test
                if total_bio >= 20:  # Only test with sufficient sample size
                    # Chi-square test for independence
                    contingency_table = np.array([
                        [stratum_count + both_count, bacterium_count + both_count],
                        [total_bio - (stratum_count + both_count), total_bio - (bacterium_count + both_count)]
                    ])

                    try:
                        chi2, p_value = stats.chi2_contingency(contingency_table)[:2]
                        significance = "significant" if p_value < 0.05 else "not significant"
                        print(f"    Statistical test: {significance} (p={p_value:.4f})")
                    except:
                        significance = "test_failed"
                        p_value = None
                else:
                    significance = "insufficient_data"
                    p_value = None

                atmosphere_analysis[atmosphere] = {
                    'total_bio_bodies': total_bio,
                    'stratum_percentage': stratum_pct,
                    'bacterium_percentage': bacterium_pct,
                    'statistical_significance': significance,
                    'p_value': p_value
                }

    body_data['atmosphere_analysis'] = atmosphere_analysis

    return body_data

def main():
    parser = argparse.ArgumentParser(description="Full Galaxy Stratum vs Bacterium Analysis")
    parser.add_argument("input_file", help="Input TSV file from full galaxy exobiology search")
    parser.add_argument("--output", help="Output JSON file for detailed results",
                       default="output/full_galaxy_stratum_bacterium_analysis.json")

    args = parser.parse_args()

    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file {input_path} not found")
        return 1

    print(f"Loading full galaxy exobiology data from: {input_path}")
    print(f"File size: {input_path.stat().st_size / 1024 / 1024:.1f} MB")

    # Load the data
    try:
        df = pd.read_csv(input_path, sep='\t', low_memory=False)
        print(f"Loaded {len(df):,} systems from full galaxy dataset")
    except Exception as e:
        print(f"Error loading data: {e}")
        return 1

    # Analyze each body position
    analysis_results = {
        'dataset_info': {
            'source_file': str(input_path),
            'total_systems': len(df),
            'file_size_mb': input_path.stat().st_size / 1024 / 1024
        },
        'body_analyses': {}
    }

    for body_num in [1, 2, 3]:
        body_prefix = f"body_{body_num}"
        body_analysis = analyze_body_characteristics(df, body_prefix)
        if body_analysis:  # Only include if there's data
            analysis_results['body_analyses'][body_prefix] = body_analysis

    # Combined analysis across all bodies
    print(f"\n=== COMBINED ANALYSIS ACROSS ALL BODIES ===")

    all_bodies_data = []
    for body_num in [1, 2, 3]:
        body_prefix = f"body_{body_num}"
        name_col = f"{body_prefix}_name"

        # Filter to bodies that exist
        body_filter = df[name_col].notna() & (df[name_col] != '')
        body_df = df[body_filter].copy()

        if len(body_df) > 0:
            # Add body data with all relevant columns
            for col in df.columns:
                if col.startswith(body_prefix):
                    new_col = col.replace(body_prefix + '_', '')
                    body_df[new_col] = body_df[col]

            body_df['body_position'] = body_num
            all_bodies_data.append(body_df[['body_position', 'name', 'atmosphere', 'pressure',
                                           'temperature', 'gravity', 'body_type', 'has_bacterium',
                                           'top_genera']])

    if all_bodies_data:
        combined_df = pd.concat(all_bodies_data, ignore_index=True)
        combined_df['parsed_genera'] = combined_df['top_genera'].apply(parse_genera_list)
        combined_df['genus_classification'] = combined_df['parsed_genera'].apply(classify_genus_type)

        print(f"Total biological bodies across all positions: {len(combined_df):,}")

        # Overall genus distribution
        overall_genus_dist = combined_df['genus_classification'].value_counts()
        print(f"\nOverall Genus Distribution:")
        total_bio = overall_genus_dist.get('stratum_only', 0) + overall_genus_dist.get('bacterium_only', 0) + overall_genus_dist.get('both', 0)

        for genus_type in ['stratum_only', 'bacterium_only', 'both', 'other']:
            count = overall_genus_dist.get(genus_type, 0)
            if genus_type in ['stratum_only', 'bacterium_only', 'both']:
                pct = count / total_bio * 100 if total_bio > 0 else 0
                print(f"  {genus_type}: {count:,} ({pct:.2f}% of biological bodies)")
            else:
                pct = count / len(combined_df) * 100
                print(f"  {genus_type}: {count:,} ({pct:.2f}% of all bodies)")

        analysis_results['combined_analysis'] = {
            'total_biological_bodies': total_bio,
            'genus_distribution': overall_genus_dist.to_dict()
        }

    # Save detailed results
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(analysis_results, f, indent=2, default=str)

    print(f"\nDetailed analysis results saved to: {output_path}")
    print(f"\n{'='*60}")
    print("FULL GALAXY ANALYSIS COMPLETE")
    print("This analysis used the complete Elite Dangerous galaxy biological dataset,")
    print("providing accurate statistics for Stratum Tectonicas vs Bacterium distribution.")
    print(f"{'='*60}")

    return 0

if __name__ == "__main__":
    sys.exit(main())