#!/usr/bin/env python3
"""
Detailed Statistical Analysis with Full Statistical Details

This script provides complete statistical transparency, showing:
1. Exact sample sizes and calculations for all percentages
2. Detailed statistical test results with methodology explanations
3. Specific analysis of High Metal Content (HMC) bodies
4. Quantitative predictors for bacteria-only vs stratum-only vs mixed HMC bodies
"""

import json
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from collections import defaultdict, Counter
from scipy import stats
from scipy.stats import chi2_contingency, pearsonr, mannwhitneyu, fisher_exact
import warnings
warnings.filterwarnings('ignore')

def load_detailed_biological_data(jsonl_file):
    """Load biological data with detailed tracking of all genus combinations."""

    print(f"🔬 Loading detailed biological data from {jsonl_file}")

    biological_records = []
    genus_combinations = Counter()
    systems_processed = 0

    with open(jsonl_file, 'r') as f:
        for line_num, line in enumerate(f):
            if line.strip():
                try:
                    system = json.loads(line)
                    systems_processed += 1

                    system_name = system.get('name', '')
                    coords = system.get('coords', {})

                    for body in system.get('bodies', []):
                        signals = body.get('signals', {})

                        if 'signals' in signals or 'genuses' in signals:
                            # Extract detailed genus information
                            genus_details = extract_detailed_genus_info(signals)

                            # Track genus combinations
                            if genus_details and genus_details['all_genuses']:
                                combo_key = tuple(sorted(genus_details['all_genuses']))
                                genus_combinations[combo_key] += 1

                            record = {
                                # System info
                                'system_name': system_name,
                                'coord_x': coords.get('x', 0),
                                'coord_y': coords.get('y', 0),
                                'coord_z': coords.get('z', 0),

                                # Body characteristics
                                'body_name': body.get('name', ''),
                                'body_type': body.get('subType', ''),
                                'atmosphere': body.get('atmosphereType', ''),
                                'temperature': body.get('surfaceTemperature'),
                                'gravity': body.get('gravity'),
                                'pressure': body.get('surfacePressure'),
                                'volcanism': body.get('volcanismType', ''),
                                'is_landable': body.get('isLandable', False),
                                'distance_to_arrival': body.get('distanceToArrival'),

                                # Material composition
                                'materials': body.get('materials', {}),
                                'solid_composition': body.get('solidComposition', {}),
                                'atmosphere_composition': body.get('atmosphereComposition', {}),

                                # Detailed genus classification
                                'all_genuses': genus_details['all_genuses'] if genus_details else [],
                                'has_stratum_genus': genus_details['has_stratum'] if genus_details else False,
                                'has_bacterium_genus': genus_details['has_bacterium'] if genus_details else False,
                                'genus_classification': genus_details['classification'] if genus_details else 'no_genus_data',
                                'signals_raw': signals
                            }

                            biological_records.append(record)

                except json.JSONDecodeError:
                    continue

                if line_num % 5000 == 0 and line_num > 0:
                    print(f"   Processed {line_num:,} systems, found {len(biological_records):,} bio bodies")

    print(f"✅ Data loading complete: {systems_processed:,} systems, {len(biological_records):,} biological bodies")

    return biological_records, genus_combinations

def extract_detailed_genus_info(signals):
    """Extract detailed genus information with exact classifications."""

    if 'genuses' not in signals:
        if 'signals' in signals:
            return {
                'all_genuses': [],
                'has_stratum': False,
                'has_bacterium': False,
                'classification': 'signals_only_no_genus'
            }
        return None

    genuses = signals.get('genuses', [])

    if not isinstance(genuses, list):
        return None

    has_stratum = any('stratum' in genus.lower() for genus in genuses)
    has_bacterium = any('bacterial' in genus.lower() for genus in genuses)

    # Detailed classification
    if has_stratum and has_bacterium:
        classification = 'mixed_stratum_bacterium'
    elif has_stratum:
        classification = 'stratum_only'
    elif has_bacterium:
        classification = 'bacterium_only'
    elif genuses:
        classification = 'other_genus_only'
    else:
        classification = 'empty_genus_list'

    return {
        'all_genuses': genuses,
        'has_stratum': has_stratum,
        'has_bacterium': has_bacterium,
        'classification': classification
    }

def analyze_atmospheric_statistics_detailed(df):
    """Analyze atmospheric preferences with full statistical transparency."""

    print(f"\n🌫️  DETAILED ATMOSPHERIC ANALYSIS")
    print("="*80)

    # Filter for bodies with specific genus classifications
    stratum_only = df[df['genus_classification'] == 'stratum_only']
    bacterium_only = df[df['genus_classification'] == 'bacterium_only']
    mixed = df[df['genus_classification'] == 'mixed_stratum_bacterium']

    print(f"Sample sizes:")
    print(f"  Stratum only: {len(stratum_only):,}")
    print(f"  Bacterium only: {len(bacterium_only):,}")
    print(f"  Mixed (both): {len(mixed):,}")
    print(f"  Total for comparison: {len(stratum_only) + len(bacterium_only):,}")

    if len(stratum_only) == 0 or len(bacterium_only) == 0:
        print("❌ Insufficient data for atmospheric analysis")
        return

    # Create combined dataset for analysis
    comparison_df = pd.concat([stratum_only, bacterium_only])
    comparison_df['is_stratum'] = comparison_df['genus_classification'] == 'stratum_only'

    # Analyze each atmosphere type
    atmosphere_stats = []

    for atmosphere in comparison_df['atmosphere'].unique():
        if pd.isna(atmosphere) or atmosphere == '':
            atmosphere = 'No_atmosphere'

        subset = comparison_df[comparison_df['atmosphere'] == atmosphere]
        if len(subset) < 5:  # Skip very small samples
            continue

        stratum_count = len(subset[subset['is_stratum'] == True])
        bacterium_count = len(subset[subset['is_stratum'] == False])
        total_count = len(subset)

        stratum_pct = (stratum_count / total_count) * 100 if total_count > 0 else 0
        bacterium_pct = (bacterium_count / total_count) * 100 if total_count > 0 else 0

        # Statistical significance test
        p_value = None
        test_method = None

        if total_count >= 10:
            # Use chi-square test with Yates correction for smaller samples
            # Create 2x2 contingency table
            other_stratum = len(stratum_only) - stratum_count
            other_bacterium = len(bacterium_only) - bacterium_count

            if other_stratum >= 0 and other_bacterium >= 0:
                # 2x2 contingency table: [[this_atm_stratum, this_atm_bacterium], [other_atm_stratum, other_atm_bacterium]]
                contingency_table = [[stratum_count, bacterium_count],
                                   [other_stratum, other_bacterium]]

                try:
                    if total_count >= 30:
                        # Use chi-square test for larger samples
                        chi2_stat, p_value, _, _ = chi2_contingency(contingency_table)
                        test_method = "Chi-square test"
                    else:
                        # Use Fisher's exact test for smaller samples
                        _, p_value = fisher_exact(contingency_table, alternative='two-sided')
                        test_method = "Fisher's exact test"
                except Exception:
                    # Fallback to proportion test
                    overall_stratum_prop = len(stratum_only) / (len(stratum_only) + len(bacterium_only))
                    expected_stratum = total_count * overall_stratum_prop
                    # Simple z-test approximation
                    if expected_stratum > 5 and (total_count - expected_stratum) > 5:
                        z_score = (stratum_count - expected_stratum) / np.sqrt(expected_stratum * (1 - overall_stratum_prop))
                        p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))  # Two-tailed
                        test_method = "Z-test approximation"
                    else:
                        p_value = None
                        test_method = "Sample too small"

        atmosphere_stats.append({
            'atmosphere': atmosphere,
            'stratum_count': stratum_count,
            'bacterium_count': bacterium_count,
            'total_count': total_count,
            'stratum_pct': stratum_pct,
            'bacterium_pct': bacterium_pct,
            'p_value': p_value,
            'test_method': test_method
        })

    # Sort by total count (most common atmospheres first)
    atmosphere_stats.sort(key=lambda x: x['total_count'], reverse=True)

    print(f"\nATMOSPHERIC PREFERENCES (detailed statistics):")
    print("Atmosphere | Stratum | Bacterium | Total | Stratum% | P-value | Test Method")
    print("-" * 90)

    for stat in atmosphere_stats:
        significance = ""
        if stat['p_value'] is not None:
            if stat['p_value'] < 0.001:
                significance = "***"
            elif stat['p_value'] < 0.01:
                significance = "**"
            elif stat['p_value'] < 0.05:
                significance = "*"

        p_val_str = f"{stat['p_value']:.6f}" if stat['p_value'] is not None else "N/A"
        test_str = stat['test_method'] if stat['test_method'] is not None else "N/A"

        print(f"{stat['atmosphere']:<20} | {stat['stratum_count']:>7} | {stat['bacterium_count']:>9} | {stat['total_count']:>5} | {stat['stratum_pct']:>7.1f}% | {p_val_str:>8} | {test_str} {significance}")

    return atmosphere_stats

def analyze_hmc_bodies_detailed(df):
    """Detailed analysis of High Metal Content bodies to predict genus types."""

    print(f"\n🪐 DETAILED HIGH METAL CONTENT (HMC) BODY ANALYSIS")
    print("="*80)

    # Filter for HMC bodies
    hmc_bodies = df[df['body_type'] == 'High metal content world'].copy()

    if len(hmc_bodies) == 0:
        print("❌ No High Metal Content bodies found")
        return

    print(f"Total HMC bodies: {len(hmc_bodies):,}")

    # Classify HMC bodies by genus type
    hmc_stratum_only = hmc_bodies[hmc_bodies['genus_classification'] == 'stratum_only']
    hmc_bacterium_only = hmc_bodies[hmc_bodies['genus_classification'] == 'bacterium_only']
    hmc_mixed = hmc_bodies[hmc_bodies['genus_classification'] == 'mixed_stratum_bacterium']
    hmc_other = hmc_bodies[~hmc_bodies['genus_classification'].isin(['stratum_only', 'bacterium_only', 'mixed_stratum_bacterium'])]

    print(f"\nHMC Body Classification:")
    print(f"  Stratum only: {len(hmc_stratum_only):,} ({len(hmc_stratum_only)/len(hmc_bodies)*100:.1f}%)")
    print(f"  Bacterium only: {len(hmc_bacterium_only):,} ({len(hmc_bacterium_only)/len(hmc_bodies)*100:.1f}%)")
    print(f"  Mixed (both): {len(hmc_mixed):,} ({len(hmc_mixed)/len(hmc_bodies)*100:.1f}%)")
    print(f"  Other/No genus: {len(hmc_other):,} ({len(hmc_other)/len(hmc_bodies)*100:.1f}%)")

    if len(hmc_stratum_only) < 5 or len(hmc_bacterium_only) < 5:
        print("❌ Insufficient data for HMC predictive analysis")
        return

    # Analyze quantitative predictors
    print(f"\n📊 QUANTITATIVE PREDICTORS FOR HMC GENUS TYPES:")

    continuous_vars = ['temperature', 'gravity', 'pressure', 'distance_to_arrival']

    for var in continuous_vars:
        analyze_hmc_predictor(hmc_stratum_only, hmc_bacterium_only, hmc_mixed, var)

    # Material composition analysis
    analyze_hmc_material_predictors(hmc_stratum_only, hmc_bacterium_only, hmc_mixed)

    # Atmospheric analysis for HMC
    analyze_hmc_atmospheric_predictors(hmc_stratum_only, hmc_bacterium_only, hmc_mixed)

    return hmc_stratum_only, hmc_bacterium_only, hmc_mixed

def analyze_hmc_predictor(stratum_df, bacterium_df, mixed_df, variable):
    """Analyze a single quantitative predictor for HMC bodies."""

    stratum_values = stratum_df[variable].dropna()
    bacterium_values = bacterium_df[variable].dropna()
    mixed_values = mixed_df[variable].dropna()

    if len(stratum_values) < 3 or len(bacterium_values) < 3:
        print(f"\n{variable.upper()}: Insufficient data (S:{len(stratum_values)}, B:{len(bacterium_values)})")
        return

    print(f"\n{variable.upper()} ANALYSIS:")

    # Descriptive statistics
    print(f"  Stratum-only HMC (n={len(stratum_values)}):")
    print(f"    Mean: {stratum_values.mean():.4f} ± {stratum_values.std():.4f}")
    print(f"    Median: {stratum_values.median():.4f}")
    print(f"    Range: {stratum_values.min():.4f} to {stratum_values.max():.4f}")
    print(f"    IQR: {stratum_values.quantile(0.25):.4f} to {stratum_values.quantile(0.75):.4f}")

    print(f"  Bacterium-only HMC (n={len(bacterium_values)}):")
    print(f"    Mean: {bacterium_values.mean():.4f} ± {bacterium_values.std():.4f}")
    print(f"    Median: {bacterium_values.median():.4f}")
    print(f"    Range: {bacterium_values.min():.4f} to {bacterium_values.max():.4f}")
    print(f"    IQR: {bacterium_values.quantile(0.25):.4f} to {bacterium_values.quantile(0.75):.4f}")

    if len(mixed_values) > 0:
        print(f"  Mixed HMC (n={len(mixed_values)}):")
        print(f"    Mean: {mixed_values.mean():.4f} ± {mixed_values.std():.4f}")
        print(f"    Median: {mixed_values.median():.4f}")
        print(f"    Range: {mixed_values.min():.4f} to {mixed_values.max():.4f}")

    # Statistical test
    if len(stratum_values) >= 5 and len(bacterium_values) >= 5:
        try:
            # Mann-Whitney U test
            statistic, p_value = mannwhitneyu(stratum_values, bacterium_values, alternative='two-sided')

            # Effect size (Cohen's d)
            pooled_std = np.sqrt(((len(stratum_values) - 1) * stratum_values.var() +
                                 (len(bacterium_values) - 1) * bacterium_values.var()) /
                                (len(stratum_values) + len(bacterium_values) - 2))

            if pooled_std > 0:
                cohens_d = (stratum_values.mean() - bacterium_values.mean()) / pooled_std
            else:
                cohens_d = 0

            # Interpretation
            effect_size_interpretation = ""
            if abs(cohens_d) < 0.2:
                effect_size_interpretation = "negligible"
            elif abs(cohens_d) < 0.5:
                effect_size_interpretation = "small"
            elif abs(cohens_d) < 0.8:
                effect_size_interpretation = "medium"
            else:
                effect_size_interpretation = "large"

            significance = ""
            if p_value < 0.001:
                significance = "*** (highly significant)"
            elif p_value < 0.01:
                significance = "** (very significant)"
            elif p_value < 0.05:
                significance = "* (significant)"
            else:
                significance = "(not significant)"

            direction = "higher" if cohens_d > 0 else "lower"

            print(f"  Statistical Test Results:")
            print(f"    Mann-Whitney U = {statistic:.2f}, p-value = {p_value:.6f} {significance}")
            print(f"    Effect size (Cohen's d) = {cohens_d:.3f} ({effect_size_interpretation})")
            print(f"    Interpretation: Stratum-only HMC bodies have {direction} {variable} than Bacterium-only")

            # Practical cutoff analysis
            if abs(cohens_d) > 0.3:  # Meaningful effect size
                optimal_cutoff = find_optimal_cutoff(stratum_values, bacterium_values, variable)
                if optimal_cutoff is not None:
                    print(f"    Suggested cutoff: {optimal_cutoff:.4f}")

        except Exception as e:
            print(f"  Statistical test failed: {e}")

def find_optimal_cutoff(stratum_values, bacterium_values, variable):
    """Find optimal cutoff value for classification."""

    # Try different percentiles as potential cutoffs
    all_values = np.concatenate([stratum_values, bacterium_values])
    percentiles = np.arange(10, 91, 5)  # 10th to 90th percentile in 5% steps

    best_accuracy = 0
    best_cutoff = None

    for p in percentiles:
        cutoff = np.percentile(all_values, p)

        # Test cutoff: assume values above cutoff predict stratum
        stratum_correct = np.sum(stratum_values >= cutoff)
        stratum_total = len(stratum_values)
        bacterium_correct = np.sum(bacterium_values < cutoff)
        bacterium_total = len(bacterium_values)

        accuracy = (stratum_correct + bacterium_correct) / (stratum_total + bacterium_total)

        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_cutoff = cutoff

    if best_accuracy > 0.6:  # Only suggest if reasonably accurate
        return best_cutoff
    return None

def analyze_hmc_material_predictors(stratum_df, bacterium_df, mixed_df):
    """Analyze material composition predictors for HMC bodies."""

    print(f"\n⚗️  MATERIAL COMPOSITION PREDICTORS FOR HMC BODIES:")

    # Collect material data
    stratum_materials = defaultdict(list)
    bacterium_materials = defaultdict(list)

    for _, record in stratum_df.iterrows():
        materials = record.get('materials', {})
        if isinstance(materials, dict):
            for material, percentage in materials.items():
                if isinstance(percentage, (int, float)):
                    stratum_materials[material].append(percentage)

    for _, record in bacterium_df.iterrows():
        materials = record.get('materials', {})
        if isinstance(materials, dict):
            for material, percentage in materials.items():
                if isinstance(percentage, (int, float)):
                    bacterium_materials[material].append(percentage)

    # Find materials with meaningful differences
    material_differences = []

    all_materials = set(stratum_materials.keys()) | set(bacterium_materials.keys())

    for material in all_materials:
        stratum_values = stratum_materials.get(material, [])
        bacterium_values = bacterium_materials.get(material, [])

        if len(stratum_values) >= 5 and len(bacterium_values) >= 5:
            try:
                statistic, p_value = mannwhitneyu(stratum_values, bacterium_values, alternative='two-sided')

                stratum_mean = np.mean(stratum_values)
                bacterium_mean = np.mean(bacterium_values)
                difference = stratum_mean - bacterium_mean

                # Effect size
                pooled_std = np.sqrt((np.var(stratum_values) + np.var(bacterium_values)) / 2)
                effect_size = difference / pooled_std if pooled_std > 0 else 0

                material_differences.append({
                    'material': material,
                    'stratum_mean': stratum_mean,
                    'bacterium_mean': bacterium_mean,
                    'difference': difference,
                    'effect_size': effect_size,
                    'p_value': p_value,
                    'stratum_n': len(stratum_values),
                    'bacterium_n': len(bacterium_values)
                })

            except Exception:
                continue

    # Sort by effect size magnitude
    material_differences.sort(key=lambda x: abs(x['effect_size']), reverse=True)

    print(f"\nTop Material Predictors (HMC bodies only):")
    print("Material | Stratum Mean | Bacterium Mean | Difference | Effect Size | P-value | Significance")
    print("-" * 100)

    for diff in material_differences[:15]:
        significance = ""
        if diff['p_value'] < 0.001:
            significance = "***"
        elif diff['p_value'] < 0.01:
            significance = "**"
        elif diff['p_value'] < 0.05:
            significance = "*"

        direction = "↑" if diff['difference'] > 0 else "↓"

        print(f"{diff['material']:<12} | {diff['stratum_mean']:>11.2f} | {diff['bacterium_mean']:>13.2f} | {diff['difference']:>9.2f} {direction} | {diff['effect_size']:>10.3f} | {diff['p_value']:>7.5f} | {significance}")

def analyze_hmc_atmospheric_predictors(stratum_df, bacterium_df, mixed_df):
    """Analyze atmospheric predictors specifically for HMC bodies."""

    print(f"\n🌫️  ATMOSPHERIC PREDICTORS FOR HMC BODIES:")

    # Combine stratum and bacterium for analysis
    combined_df = pd.concat([stratum_df, bacterium_df])
    combined_df['is_stratum'] = combined_df['genus_classification'] == 'stratum_only'

    # Analyze each atmosphere
    atm_analysis = []

    for atmosphere in combined_df['atmosphere'].unique():
        if pd.isna(atmosphere) or atmosphere == '':
            atmosphere = 'No_atmosphere'

        subset = combined_df[combined_df['atmosphere'] == atmosphere]

        if len(subset) < 3:
            continue

        stratum_count = len(subset[subset['is_stratum'] == True])
        total_count = len(subset)
        stratum_pct = (stratum_count / total_count) * 100

        atm_analysis.append({
            'atmosphere': atmosphere,
            'stratum_count': stratum_count,
            'bacterium_count': total_count - stratum_count,
            'total_count': total_count,
            'stratum_pct': stratum_pct
        })

    # Sort by stratum percentage
    atm_analysis.sort(key=lambda x: x['stratum_pct'], reverse=True)

    print(f"\nAtmospheric Preferences for HMC Bodies:")
    print("Atmosphere | Stratum | Bacterium | Total | Stratum%")
    print("-" * 60)

    for atm in atm_analysis:
        print(f"{atm['atmosphere']:<20} | {atm['stratum_count']:>7} | {atm['bacterium_count']:>9} | {atm['total_count']:>5} | {atm['stratum_pct']:>7.1f}%")

def generate_hmc_prediction_model(stratum_df, bacterium_df, mixed_df):
    """Generate a predictive model for HMC body genus types."""

    print(f"\n🎯 HMC GENUS PREDICTION MODEL")
    print("="*60)

    # Combine data for model building
    combined_data = []

    # Add stratum data
    for _, record in stratum_df.iterrows():
        combined_data.append({
            'temperature': record.get('temperature'),
            'gravity': record.get('gravity'),
            'pressure': record.get('pressure'),
            'atmosphere': record.get('atmosphere', ''),
            'is_stratum': True
        })

    # Add bacterium data
    for _, record in bacterium_df.iterrows():
        combined_data.append({
            'temperature': record.get('temperature'),
            'gravity': record.get('gravity'),
            'pressure': record.get('pressure'),
            'atmosphere': record.get('atmosphere', ''),
            'is_stratum': False
        })

    model_df = pd.DataFrame(combined_data)

    # Remove rows with missing critical data
    model_df = model_df.dropna(subset=['temperature', 'gravity'])

    if len(model_df) < 10:
        print("❌ Insufficient data for prediction model")
        return

    print(f"Model training data: {len(model_df)} HMC bodies")
    print(f"  Stratum: {len(model_df[model_df['is_stratum'] == True])}")
    print(f"  Bacterium: {len(model_df[model_df['is_stratum'] == False])}")

    # Simple rule-based model
    stratum_bodies = model_df[model_df['is_stratum'] == True]
    bacterium_bodies = model_df[model_df['is_stratum'] == False]

    print(f"\nPREDICTION RULES FOR HMC BODIES:")

    # Temperature rule
    stratum_temp_median = stratum_bodies['temperature'].median()
    bacterium_temp_median = bacterium_bodies['temperature'].median()
    temp_cutoff = (stratum_temp_median + bacterium_temp_median) / 2

    print(f"1. TEMPERATURE RULE:")
    print(f"   Stratum median: {stratum_temp_median:.1f}K")
    print(f"   Bacterium median: {bacterium_temp_median:.1f}K")
    print(f"   Suggested cutoff: {temp_cutoff:.1f}K")
    print(f"   Rule: HMC with temperature > {temp_cutoff:.1f}K → Higher probability of Stratum")

    # Gravity rule
    stratum_grav_median = stratum_bodies['gravity'].median()
    bacterium_grav_median = bacterium_bodies['gravity'].median()
    grav_cutoff = (stratum_grav_median + bacterium_grav_median) / 2

    print(f"\n2. GRAVITY RULE:")
    print(f"   Stratum median: {stratum_grav_median:.3f}g")
    print(f"   Bacterium median: {bacterium_grav_median:.3f}g")
    print(f"   Suggested cutoff: {grav_cutoff:.3f}g")
    print(f"   Rule: HMC with gravity > {grav_cutoff:.3f}g → Higher probability of Stratum")

    # Combined rule accuracy
    temp_correct = 0
    grav_correct = 0
    combined_correct = 0
    total = len(model_df)

    for _, row in model_df.iterrows():
        actual_stratum = row['is_stratum']

        # Temperature prediction
        temp_pred_stratum = row['temperature'] > temp_cutoff
        if temp_pred_stratum == actual_stratum:
            temp_correct += 1

        # Gravity prediction
        grav_pred_stratum = row['gravity'] > grav_cutoff
        if grav_pred_stratum == actual_stratum:
            grav_correct += 1

        # Combined prediction (both conditions)
        combined_pred_stratum = (row['temperature'] > temp_cutoff) and (row['gravity'] > grav_cutoff)
        if combined_pred_stratum == actual_stratum:
            combined_correct += 1

    print(f"\n3. PREDICTION ACCURACY:")
    print(f"   Temperature alone: {temp_correct/total*100:.1f}%")
    print(f"   Gravity alone: {grav_correct/total*100:.1f}%")
    print(f"   Combined (temp AND gravity): {combined_correct/total*100:.1f}%")

    # Atmospheric enhancement
    print(f"\n4. ATMOSPHERIC ENHANCEMENT:")
    stratum_atmospheres = set(stratum_bodies['atmosphere'].value_counts().head(3).index)
    bacterium_atmospheres = set(bacterium_bodies['atmosphere'].value_counts().head(3).index)

    stratum_specific = stratum_atmospheres - bacterium_atmospheres
    bacterium_specific = bacterium_atmospheres - stratum_atmospheres

    if stratum_specific:
        print(f"   Stratum-favoring atmospheres: {list(stratum_specific)}")
    if bacterium_specific:
        print(f"   Bacterium-favoring atmospheres: {list(bacterium_specific)}")

def main():
    jsonl_file = sys.argv[1] if len(sys.argv) > 1 else 'output/bio_landmarks_full_20250919_1814/results.jsonl'

    print("🔬 DETAILED STATISTICAL ANALYSIS WITH FULL TRANSPARENCY")
    print("="*80)
    print(f"Dataset: {jsonl_file}")

    # Load data with detailed tracking
    biological_records, genus_combinations = load_detailed_biological_data(jsonl_file)

    if len(biological_records) == 0:
        print("❌ No biological data found!")
        return

    df = pd.DataFrame(biological_records)

    print(f"\n📊 GENUS COMBINATION STATISTICS:")
    print("(showing exact counts for transparency)")
    for combo, count in genus_combinations.most_common(10):
        print(f"   {combo}: {count:,}")

    # Detailed atmospheric analysis
    atmosphere_stats = analyze_atmospheric_statistics_detailed(df)

    # Detailed HMC analysis
    hmc_stratum, hmc_bacterium, hmc_mixed = analyze_hmc_bodies_detailed(df)

    if hmc_stratum is not None and hmc_bacterium is not None:
        # Generate prediction model
        generate_hmc_prediction_model(hmc_stratum, hmc_bacterium, hmc_mixed)

    # Save detailed results
    output_file = f"output/detailed_statistical_analysis_{int(time.time())}.json"

    analysis_results = {
        'total_biological_bodies': len(biological_records),
        'genus_combinations': dict(genus_combinations),
        'atmospheric_statistics': atmosphere_stats if 'atmosphere_stats' in locals() else [],
        'detailed_records': biological_records[:1000]  # Save first 1000 for inspection
    }

    with open(output_file, 'w') as f:
        json.dump(analysis_results, f, indent=2, default=str)

    print(f"\n💾 Detailed analysis results saved to: {output_file}")
    print(f"✅ Complete statistical analysis finished!")

if __name__ == "__main__":
    import time
    main()