#!/usr/bin/env python3
"""
Comprehensive Genus and Species Analysis

This script performs an exhaustive analysis of ALL genus types and species in the
biological dataset, with special focus on Stratum Tectonicas vs Bacterium differentiation.

Key analyses:
1. Complete enumeration of all genus types and their body/atmosphere preferences
2. Detailed Stratum vs Bacterium statistical comparison
3. Material composition analysis by genus
4. Temperature, gravity, and pressure ranges by genus
5. Predictive model for Stratum vs Bacterium classification
"""

import json
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from collections import defaultdict, Counter
from scipy import stats
from scipy.stats import chi2_contingency, pearsonr, mannwhitneyu
import warnings
warnings.filterwarnings('ignore')

def load_comprehensive_biological_data(jsonl_file):
    """Load and parse ALL biological data with complete genus information."""

    print(f"🔬 Loading comprehensive biological data from {jsonl_file}")

    biological_records = []
    genus_counter = Counter()
    systems_processed = 0

    with open(jsonl_file, 'r') as f:
        for line_num, line in enumerate(f):
            if line.strip():
                try:
                    system = json.loads(line)
                    systems_processed += 1

                    system_name = system.get('name', '')
                    coords = system.get('coords', {})

                    # Process all bodies in system
                    for body in system.get('bodies', []):
                        signals = body.get('signals', {})

                        # Include ALL biological bodies (with signals OR genuses)
                        if 'signals' in signals or 'genuses' in signals:

                            # Extract ALL genus information
                            genus_info = extract_all_genus_info(signals)

                            # Count all genuses
                            if genus_info and genus_info['genuses']:
                                for genus in genus_info['genuses']:
                                    genus_counter[genus] += 1

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

                                # Orbital characteristics
                                'orbital_period': body.get('orbitalPeriod'),
                                'semi_major_axis': body.get('semiMajorAxis'),
                                'orbital_eccentricity': body.get('orbitalEccentricity'),
                                'orbital_inclination': body.get('orbitalInclination'),
                                'tidal_lock': body.get('rotationalPeriodTidallyLocked', False),

                                # Comprehensive genus information
                                'all_genuses': genus_info['genuses'] if genus_info else [],
                                'primary_genus': genus_info['primary_genus'] if genus_info else 'unknown',
                                'genus_category': genus_info['category'] if genus_info else 'unknown',
                                'has_stratum': genus_info['has_stratum'] if genus_info else False,
                                'has_bacterium': genus_info['has_bacterium'] if genus_info else False,
                                'signals_raw': signals
                            }

                            biological_records.append(record)

                except json.JSONDecodeError:
                    continue

                if line_num % 5000 == 0 and line_num > 0:
                    print(f"   Processed {line_num:,} systems, found {len(biological_records):,} bio bodies")

    print(f"✅ Data loading complete: {systems_processed:,} systems, {len(biological_records):,} biological bodies")

    # Print complete genus distribution
    print(f"\n📊 COMPLETE GENUS DISTRIBUTION:")
    print(f"Found {len(genus_counter)} unique genus types:")
    for genus, count in genus_counter.most_common():
        print(f"   {genus}: {count:,}")

    return biological_records, genus_counter

def extract_all_genus_info(signals):
    """Extract comprehensive genus information from signal data."""

    if 'genuses' not in signals:
        # Even bodies without specific genus still have biological signals
        if 'signals' in signals:
            return {
                'genuses': [],
                'primary_genus': 'unspecified_biological',
                'category': 'unspecified_biological',
                'has_stratum': False,
                'has_bacterium': False
            }
        return None

    genuses = signals.get('genuses', [])

    if not isinstance(genuses, list):
        return None

    has_stratum = any('stratum' in genus.lower() for genus in genuses)
    has_bacterium = any('bacterial' in genus.lower() for genus in genuses)

    # Determine primary category
    if has_stratum and has_bacterium:
        category = 'mixed_stratum_bacterium'
        primary_genus = genuses[0]  # Take first as primary
    elif has_stratum:
        category = 'stratum_tectonicas'
        primary_genus = next((g for g in genuses if 'stratum' in g.lower()), genuses[0])
    elif has_bacterium:
        category = 'bacterium'
        primary_genus = next((g for g in genuses if 'bacterial' in g.lower()), genuses[0])
    else:
        category = 'other_biological'
        primary_genus = genuses[0] if genuses else 'unknown'

    return {
        'genuses': genuses,
        'primary_genus': primary_genus,
        'category': category,
        'has_stratum': has_stratum,
        'has_bacterium': has_bacterium
    }

def analyze_all_genus_types(biological_records, genus_counter):
    """Analyze body characteristics for every single genus type."""

    print(f"\n🔍 COMPREHENSIVE GENUS ANALYSIS")
    print("="*80)

    df = pd.DataFrame(biological_records)

    print(f"\n1. CATEGORY DISTRIBUTION:")
    category_counts = df['genus_category'].value_counts()
    total = len(df)
    for category, count in category_counts.items():
        percentage = (count / total) * 100
        print(f"   {category}: {count:,} ({percentage:.2f}%)")

    print(f"\n2. DETAILED GENUS ANALYSIS:")
    print("   Note: Analyzing body type and atmosphere preferences for each genus")

    # Analyze each genus individually
    genus_analysis = {}

    for genus_name in genus_counter.keys():
        # Find all bodies with this specific genus
        genus_bodies = df[df['all_genuses'].apply(lambda x: genus_name in x if isinstance(x, list) else False)]

        if len(genus_bodies) == 0:
            continue

        analysis = analyze_single_genus(genus_bodies, genus_name)
        genus_analysis[genus_name] = analysis

        print(f"\n   {genus_name} (n={len(genus_bodies)}):")
        print(f"     Body types: {analysis['top_body_types']}")
        print(f"     Atmospheres: {analysis['top_atmospheres']}")
        print(f"     Temperature: {analysis['temperature_stats']}")
        print(f"     Gravity: {analysis['gravity_stats']}")

    return genus_analysis

def analyze_single_genus(genus_df, genus_name):
    """Analyze characteristics of a single genus."""

    # Body type distribution
    body_types = genus_df['body_type'].value_counts()
    top_body_types = dict(body_types.head(3))

    # Atmosphere distribution
    atmospheres = genus_df['atmosphere'].value_counts()
    top_atmospheres = dict(atmospheres.head(3))

    # Temperature statistics
    temp_data = genus_df['temperature'].dropna()
    temp_stats = {
        'min': temp_data.min() if len(temp_data) > 0 else None,
        'max': temp_data.max() if len(temp_data) > 0 else None,
        'mean': temp_data.mean() if len(temp_data) > 0 else None,
        'median': temp_data.median() if len(temp_data) > 0 else None,
        'count': len(temp_data)
    }

    # Gravity statistics
    grav_data = genus_df['gravity'].dropna()
    grav_stats = {
        'min': grav_data.min() if len(grav_data) > 0 else None,
        'max': grav_data.max() if len(grav_data) > 0 else None,
        'mean': grav_data.mean() if len(grav_data) > 0 else None,
        'median': grav_data.median() if len(grav_data) > 0 else None,
        'count': len(grav_data)
    }

    # Material analysis
    all_materials = {}
    for _, record in genus_df.iterrows():
        materials = record.get('materials', {})
        if isinstance(materials, dict):
            for material, percentage in materials.items():
                if isinstance(percentage, (int, float)):
                    if material not in all_materials:
                        all_materials[material] = []
                    all_materials[material].append(percentage)

    # Calculate average material percentages
    avg_materials = {}
    for material, percentages in all_materials.items():
        if len(percentages) >= 5:  # Only include materials with sufficient samples
            avg_materials[material] = {
                'mean': np.mean(percentages),
                'median': np.median(percentages),
                'count': len(percentages)
            }

    return {
        'count': len(genus_df),
        'top_body_types': top_body_types,
        'top_atmospheres': top_atmospheres,
        'temperature_stats': temp_stats,
        'gravity_stats': grav_stats,
        'material_composition': avg_materials
    }

def deep_stratum_vs_bacterium_analysis(biological_records):
    """Perform detailed statistical analysis comparing Stratum vs Bacterium."""

    print(f"\n🎯 DEEP STRATUM TECTONICAS vs BACTERIUM ANALYSIS")
    print("="*80)

    df = pd.DataFrame(biological_records)

    # Filter for pure Stratum vs pure Bacterium (exclude mixed)
    stratum_df = df[df['genus_category'] == 'stratum_tectonicas']
    bacterium_df = df[df['genus_category'] == 'bacterium']

    print(f"Stratum Tectonicas bodies: {len(stratum_df):,}")
    print(f"Bacterium bodies: {len(bacterium_df):,}")

    if len(stratum_df) == 0 or len(bacterium_df) == 0:
        print("❌ Insufficient data for Stratum vs Bacterium comparison")
        return

    # Combine for statistical analysis
    comparison_df = pd.concat([stratum_df, bacterium_df])
    comparison_df['is_stratum'] = comparison_df['genus_category'] == 'stratum_tectonicas'

    print(f"\n1. BODY TYPE PREFERENCES:")
    analyze_feature_preference(comparison_df, 'body_type', 'is_stratum', 'Stratum Tectonicas')

    print(f"\n2. ATMOSPHERE PREFERENCES:")
    analyze_feature_preference(comparison_df, 'atmosphere', 'is_stratum', 'Stratum Tectonicas')

    print(f"\n3. VOLCANISM PREFERENCES:")
    analyze_feature_preference(comparison_df, 'volcanism', 'is_stratum', 'Stratum Tectonicas')

    print(f"\n4. CONTINUOUS VARIABLE COMPARISON:")
    continuous_vars = ['temperature', 'gravity', 'pressure', 'distance_to_arrival']

    for var in continuous_vars:
        analyze_continuous_difference(stratum_df, bacterium_df, var)

    print(f"\n5. MATERIAL COMPOSITION COMPARISON:")
    analyze_material_differences(stratum_df, bacterium_df)

    print(f"\n6. PREDICTIVE FEATURES RANKING:")
    rank_predictive_features(comparison_df)

    return comparison_df

def analyze_feature_preference(df, feature_col, target_col, target_name):
    """Analyze categorical feature preferences."""

    # Create contingency table
    contingency = pd.crosstab(df[feature_col], df[target_col])

    print(f"\n{feature_col.upper()} preferences:")

    for category in contingency.index:
        if contingency.loc[category].sum() >= 10:  # Minimum sample size

            stratum_count = contingency.loc[category, True] if True in contingency.columns else 0
            bacterium_count = contingency.loc[category, False] if False in contingency.columns else 0
            total = stratum_count + bacterium_count

            if total > 0:
                stratum_pct = (stratum_count / total) * 100

                # Statistical significance test
                if total >= 20:  # Enough for statistical test
                    # Expected based on overall ratio
                    overall_stratum_pct = (df[target_col].sum() / len(df)) * 100

                    # Chi-square test for this category
                    expected_stratum = total * (overall_stratum_pct / 100)
                    expected_bacterium = total * ((100 - overall_stratum_pct) / 100)

                    if expected_stratum > 5 and expected_bacterium > 5:
                        chi2_stat = ((stratum_count - expected_stratum)**2 / expected_stratum +
                                   (bacterium_count - expected_bacterium)**2 / expected_bacterium)
                        p_value = 1 - stats.chi2.cdf(chi2_stat, 1)

                        significance = ""
                        if p_value < 0.001:
                            significance = " ***"
                        elif p_value < 0.01:
                            significance = " **"
                        elif p_value < 0.05:
                            significance = " *"

                # Determine preference
                if stratum_pct >= 80:
                    preference = f"STRONG {target_name} preference"
                elif stratum_pct >= 60:
                    preference = f"Moderate {target_name} preference"
                elif stratum_pct <= 20:
                    preference = f"STRONG Bacterium preference"
                elif stratum_pct <= 40:
                    preference = f"Moderate Bacterium preference"
                else:
                    preference = f"Mixed"

                print(f"   {category}: {preference} ({stratum_pct:.1f}% S, {100-stratum_pct:.1f}% B, n={total}){significance if 'significance' in locals() else ''}")

def analyze_continuous_difference(stratum_df, bacterium_df, variable):
    """Analyze differences in continuous variables."""

    stratum_values = stratum_df[variable].dropna()
    bacterium_values = bacterium_df[variable].dropna()

    if len(stratum_values) < 5 or len(bacterium_values) < 5:
        print(f"\n{variable.upper()}: Insufficient data")
        return

    print(f"\n{variable.upper()}:")
    print(f"   Stratum (n={len(stratum_values)}): {stratum_values.mean():.3f} ± {stratum_values.std():.3f}")
    print(f"     Range: {stratum_values.min():.3f} - {stratum_values.max():.3f}")
    print(f"     Median: {stratum_values.median():.3f} (Q1: {stratum_values.quantile(0.25):.3f}, Q3: {stratum_values.quantile(0.75):.3f})")

    print(f"   Bacterium (n={len(bacterium_values)}): {bacterium_values.mean():.3f} ± {bacterium_values.std():.3f}")
    print(f"     Range: {bacterium_values.min():.3f} - {bacterium_values.max():.3f}")
    print(f"     Median: {bacterium_values.median():.3f} (Q1: {bacterium_values.quantile(0.25):.3f}, Q3: {bacterium_values.quantile(0.75):.3f})")

    # Statistical test
    try:
        statistic, p_value = mannwhitneyu(stratum_values, bacterium_values, alternative='two-sided')

        # Effect size (Cohen's d)
        pooled_std = np.sqrt(((len(stratum_values) - 1) * stratum_values.var() +
                             (len(bacterium_values) - 1) * bacterium_values.var()) /
                            (len(stratum_values) + len(bacterium_values) - 2))
        cohens_d = (stratum_values.mean() - bacterium_values.mean()) / pooled_std

        significance = ""
        if p_value < 0.001:
            significance = "*** (p < 0.001)"
        elif p_value < 0.01:
            significance = "** (p < 0.01)"
        elif p_value < 0.05:
            significance = "* (p < 0.05)"
        else:
            significance = "(not significant)"

        effect_interpretation = ""
        if abs(cohens_d) < 0.2:
            effect_interpretation = "negligible"
        elif abs(cohens_d) < 0.5:
            effect_interpretation = "small"
        elif abs(cohens_d) < 0.8:
            effect_interpretation = "medium"
        else:
            effect_interpretation = "large"

        direction = "higher" if cohens_d > 0 else "lower"

        print(f"   Result: Stratum {direction} than Bacterium {significance}")
        print(f"   Effect size: {cohens_d:.3f} ({effect_interpretation})")

    except Exception as e:
        print(f"   Statistical test failed: {e}")

def analyze_material_differences(stratum_df, bacterium_df):
    """Analyze material composition differences."""

    # Collect all materials
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

    # Find materials with significant differences
    significant_differences = []

    all_materials = set(stratum_materials.keys()) | set(bacterium_materials.keys())

    for material in all_materials:
        stratum_values = stratum_materials.get(material, [])
        bacterium_values = bacterium_materials.get(material, [])

        if len(stratum_values) >= 10 and len(bacterium_values) >= 10:
            try:
                statistic, p_value = mannwhitneyu(stratum_values, bacterium_values, alternative='two-sided')

                stratum_median = np.median(stratum_values)
                bacterium_median = np.median(bacterium_values)

                # Effect size
                pooled_std = np.sqrt((np.var(stratum_values) + np.var(bacterium_values)) / 2)
                effect_size = (stratum_median - bacterium_median) / pooled_std if pooled_std > 0 else 0

                significant_differences.append({
                    'material': material,
                    'p_value': p_value,
                    'stratum_median': stratum_median,
                    'bacterium_median': bacterium_median,
                    'effect_size': effect_size,
                    'stratum_n': len(stratum_values),
                    'bacterium_n': len(bacterium_values)
                })

            except Exception:
                continue

    # Sort by effect size magnitude
    significant_differences.sort(key=lambda x: abs(x['effect_size']), reverse=True)

    print(f"\nMATERIAL COMPOSITION DIFFERENCES (sorted by effect size):")
    for diff in significant_differences[:15]:
        direction = "higher" if diff['stratum_median'] > diff['bacterium_median'] else "lower"
        significance = ""
        if diff['p_value'] < 0.001:
            significance = " ***"
        elif diff['p_value'] < 0.01:
            significance = " **"
        elif diff['p_value'] < 0.05:
            significance = " *"

        print(f"   {diff['material']}: Stratum {direction} ({diff['stratum_median']:.2f}% vs {diff['bacterium_median']:.2f}%)")
        print(f"     Effect size: {diff['effect_size']:.3f}, p-value: {diff['p_value']:.6f}{significance}")

def rank_predictive_features(df):
    """Rank features by their ability to predict Stratum vs Bacterium."""

    # Prepare features
    features = []
    feature_names = []

    # Categorical features (one-hot encoded)
    categorical_cols = ['body_type', 'atmosphere', 'volcanism']

    for col in categorical_cols:
        unique_values = df[col].value_counts()
        common_values = unique_values[unique_values >= 20].index

        for value in common_values:
            feature_name = f"{col}_{value}"
            feature_vector = (df[col] == value).astype(int)
            features.append(feature_vector)
            feature_names.append(feature_name)

    # Continuous features (normalized)
    continuous_cols = ['temperature', 'gravity', 'pressure']

    for col in continuous_cols:
        clean_data = df[col].dropna()
        if len(clean_data) > 100:
            # Normalize to 0-1 range
            min_val, max_val = clean_data.min(), clean_data.max()
            if max_val > min_val:
                normalized = (df[col].fillna(clean_data.median()) - min_val) / (max_val - min_val)
                features.append(normalized)
                feature_names.append(f"{col}_normalized")

    if len(features) == 0:
        print("No suitable features for ranking")
        return

    # Create feature matrix
    X = np.column_stack(features)
    y = df['is_stratum'].astype(int)

    # Remove rows with NaN values
    valid_rows = ~np.isnan(X).any(axis=1)
    X = X[valid_rows]
    y = y[valid_rows]

    # Calculate correlations
    feature_importance = []

    for i, feature_name in enumerate(feature_names):
        try:
            correlation, p_value = pearsonr(X[:, i], y)
            feature_importance.append({
                'feature': feature_name,
                'correlation': correlation,
                'abs_correlation': abs(correlation),
                'p_value': p_value
            })
        except Exception:
            continue

    # Sort by absolute correlation
    feature_importance.sort(key=lambda x: x['abs_correlation'], reverse=True)

    print(f"\nTOP PREDICTIVE FEATURES FOR STRATUM TECTONICAS:")
    for feat in feature_importance[:20]:
        direction = "positive" if feat['correlation'] > 0 else "negative"
        significance = ""
        if feat['p_value'] < 0.001:
            significance = " ***"
        elif feat['p_value'] < 0.01:
            significance = " **"
        elif feat['p_value'] < 0.05:
            significance = " *"

        print(f"   {feat['feature']}: {feat['correlation']:+.4f} ({direction}){significance}")

def generate_classification_rules(comparison_df):
    """Generate evidence-based classification rules."""

    print(f"\n🎯 EVIDENCE-BASED CLASSIFICATION RULES")
    print("="*80)

    stratum_df = comparison_df[comparison_df['is_stratum'] == True]
    bacterium_df = comparison_df[comparison_df['is_stratum'] == False]

    print(f"Based on {len(stratum_df):,} Stratum Tectonicas vs {len(bacterium_df):,} Bacterium observations:")

    # Rule 1: Body type rules
    print(f"\n🪐 BODY TYPE CLASSIFICATION RULES:")
    body_type_rules = comparison_df.groupby('body_type')['is_stratum'].agg(['count', 'mean']).round(3)
    body_type_rules = body_type_rules[body_type_rules['count'] >= 20]  # Minimum sample size

    for body_type, stats in body_type_rules.iterrows():
        stratum_pct = stats['mean'] * 100
        sample_size = int(stats['count'])

        if stratum_pct >= 70:
            confidence = "HIGH"
            prediction = "STRATUM"
        elif stratum_pct >= 55:
            confidence = "MEDIUM"
            prediction = "STRATUM"
        elif stratum_pct <= 30:
            confidence = "HIGH"
            prediction = "BACTERIUM"
        elif stratum_pct <= 45:
            confidence = "MEDIUM"
            prediction = "BACTERIUM"
        else:
            confidence = "LOW"
            prediction = "MIXED"

        print(f"   Rule: {body_type} → {prediction} ({confidence} confidence: {stratum_pct:.1f}% Stratum, n={sample_size})")

    # Rule 2: Atmosphere rules
    print(f"\n🌫️  ATMOSPHERE CLASSIFICATION RULES:")
    atm_rules = comparison_df.groupby('atmosphere')['is_stratum'].agg(['count', 'mean']).round(3)
    atm_rules = atm_rules[atm_rules['count'] >= 20]

    for atmosphere, stats in atm_rules.iterrows():
        stratum_pct = stats['mean'] * 100
        sample_size = int(stats['count'])

        if stratum_pct >= 70:
            confidence = "HIGH"
            prediction = "STRATUM"
        elif stratum_pct >= 55:
            confidence = "MEDIUM"
            prediction = "STRATUM"
        elif stratum_pct <= 30:
            confidence = "HIGH"
            prediction = "BACTERIUM"
        elif stratum_pct <= 45:
            confidence = "MEDIUM"
            prediction = "BACTERIUM"
        else:
            confidence = "LOW"
            prediction = "MIXED"

        print(f"   Rule: {atmosphere} → {prediction} ({confidence} confidence: {stratum_pct:.1f}% Stratum, n={sample_size})")

    # Rule 3: Combined rules
    print(f"\n🔄 COMBINED CLASSIFICATION STRATEGY:")
    print(f"   1. Primary: Use body type classification")
    print(f"   2. Secondary: Validate with atmosphere type")
    print(f"   3. Tertiary: Apply temperature/gravity constraints")
    print(f"   4. Final: Material composition confirmation")

def main():
    jsonl_file = sys.argv[1] if len(sys.argv) > 1 else 'output/bio_landmarks_full_20250919_1814/results.jsonl'

    print("🔬 COMPREHENSIVE GENUS AND SPECIES ANALYSIS")
    print("="*80)
    print(f"Dataset: {jsonl_file}")

    # Load comprehensive data
    biological_records, genus_counter = load_comprehensive_biological_data(jsonl_file)

    if len(biological_records) == 0:
        print("❌ No biological data found!")
        return

    # Analyze all genus types
    genus_analysis = analyze_all_genus_types(biological_records, genus_counter)

    # Deep Stratum vs Bacterium analysis
    comparison_df = deep_stratum_vs_bacterium_analysis(biological_records)

    if comparison_df is not None:
        # Generate classification rules
        generate_classification_rules(comparison_df)

    # Save comprehensive results
    output_file = f"output/comprehensive_genus_analysis_{int(time.time())}.json"

    # Prepare data for JSON serialization
    serializable_data = {
        'biological_records': [],
        'genus_counter': dict(genus_counter),
        'genus_analysis': genus_analysis
    }

    # Convert numpy types to native Python types
    for record in biological_records:
        serializable_record = {}
        for key, value in record.items():
            if isinstance(value, (np.integer, np.floating)):
                serializable_record[key] = value.item()
            elif isinstance(value, np.ndarray):
                serializable_record[key] = value.tolist()
            else:
                serializable_record[key] = value
        serializable_data['biological_records'].append(serializable_record)

    with open(output_file, 'w') as f:
        json.dump(serializable_data, f, indent=2, default=str)

    print(f"\n💾 Comprehensive analysis data saved to: {output_file}")
    print(f"✅ Complete genus analysis finished!")

if __name__ == "__main__":
    import time
    main()