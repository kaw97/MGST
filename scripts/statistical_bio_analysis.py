#!/usr/bin/env python3
"""
Statistical Analysis of Biological Species Distribution

This script performs comprehensive statistical analysis on the actual biological
dataset to determine what body characteristics correlate with Stratum Tectonicas
vs bacteria based on real observed data, not existing rules.

Key analyses:
1. Species distribution by body type, atmosphere, temperature, gravity, etc.
2. Statistical significance testing
3. Correlation analysis
4. Confidence intervals and effect sizes
5. Feature importance ranking
"""

import json
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from collections import defaultdict, Counter
from scipy import stats
from scipy.stats import chi2_contingency, pearsonr, spearmanr
import warnings
warnings.filterwarnings('ignore')

def load_and_parse_biological_data(jsonl_file):
    """Load and parse all biological data into structured format."""

    print(f"🔬 Loading biological data from {jsonl_file}")

    biological_records = []
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
                        if 'signals' in signals or 'genuses' in signals:

                            # Extract genus/species information from signals
                            species_info = extract_species_from_signals(signals)

                            if species_info:
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

                                    # Species information
                                    'observed_species': species_info['species'],
                                    'species_category': species_info['category'],
                                    'genus_data': species_info['genus_data'],
                                    'signals_raw': signals
                                }

                                biological_records.append(record)

                except json.JSONDecodeError:
                    continue

                if line_num % 5000 == 0 and line_num > 0:
                    print(f"   Processed {line_num:,} systems, found {len(biological_records):,} bio bodies")

    print(f"✅ Data loading complete: {systems_processed:,} systems, {len(biological_records):,} biological bodies")
    return biological_records

def extract_species_from_signals(signals):
    """Extract species information from signal data."""

    if 'genuses' not in signals:
        return None

    genuses = signals.get('genuses', [])

    # Handle case where genuses is a list (actual format)
    if isinstance(genuses, list):
        species_list = []
        species_category = 'unknown'

        for genus_key in genuses:
            # Categorize by genus key patterns
            if 'stratum' in genus_key.lower():
                species_category = 'stratum_tectonicas'
            elif 'bacterial' in genus_key.lower():
                species_category = 'bacterium'
            elif any(term in genus_key.lower() for term in ['tussocks', 'shrubs', 'tubus', 'fonticulus', 'sphere', 'electricae', 'osseus', 'conchas', 'fungoids', 'cactoid', 'aleoids', 'cone', 'recepta']):
                species_category = 'other_biological'

            species_list.append({
                'genus_key': genus_key,
                'genus_name': genus_key,  # Use key as name since we don't have detailed species data
                'species_data': {}
            })

        return {
            'species': species_list,
            'category': species_category,
            'genus_data': genuses
        }

    # Handle case where genuses is a dict (fallback)
    elif isinstance(genuses, dict):
        species_list = []
        species_category = 'unknown'

        for genus_key, genus_data in genuses.items():
            genus_name = genus_data.get('name', genus_key)

            # Categorize by genus name patterns
            if any(term in genus_name.lower() for term in ['stratum', 'tectonicas']):
                species_category = 'stratum_tectonicas'
            elif any(term in genus_name.lower() for term in ['bacterium', 'bacteria']):
                species_category = 'bacterium'
            elif any(term in genus_name.lower() for term in ['anemone', 'aleoida', 'cactoida', 'clypeus', 'concha', 'electricae', 'fonticulua', 'frutexa', 'fumerola', 'fungoida', 'osseus', 'recepta', 'tubers', 'tubus', 'tussock']):
                species_category = 'other_biological'

            species_list.append({
                'genus_key': genus_key,
                'genus_name': genus_name,
                'species_data': genus_data.get('species', {})
            })

        return {
            'species': species_list,
            'category': species_category,
            'genus_data': genuses
        }

    return None

def perform_statistical_analysis(biological_records):
    """Perform comprehensive statistical analysis on biological data."""

    print(f"\n📊 STATISTICAL ANALYSIS OF {len(biological_records):,} BIOLOGICAL BODIES")
    print("="*80)

    # Convert to DataFrame for analysis
    df = pd.DataFrame(biological_records)

    # Basic species distribution
    print(f"\n1. SPECIES CATEGORY DISTRIBUTION:")
    species_counts = df['species_category'].value_counts()
    total = len(df)
    for category, count in species_counts.items():
        percentage = (count / total) * 100
        print(f"   {category}: {count:,} ({percentage:.2f}%)")

    # Filter for Stratum vs Bacterium comparison (exclude other biologicals)
    stratum_bacterium_df = df[df['species_category'].isin(['stratum_tectonicas', 'bacterium'])]
    print(f"\nFocusing on Stratum vs Bacterium: {len(stratum_bacterium_df):,} bodies")

    if len(stratum_bacterium_df) == 0:
        print("❌ No Stratum or Bacterium data found!")
        return

    # Analyze body type distribution
    analyze_categorical_association(stratum_bacterium_df, 'body_type', 'species_category', 'Body Type')

    # Analyze atmosphere distribution
    analyze_categorical_association(stratum_bacterium_df, 'atmosphere', 'species_category', 'Atmosphere Type')

    # Analyze volcanism distribution
    analyze_categorical_association(stratum_bacterium_df, 'volcanism', 'species_category', 'Volcanism Type')

    # Analyze continuous variables
    analyze_continuous_variables(stratum_bacterium_df)

    # Material composition analysis
    analyze_material_composition(stratum_bacterium_df)

    # Multivariate analysis
    perform_multivariate_analysis(stratum_bacterium_df)

    return stratum_bacterium_df

def analyze_categorical_association(df, feature, target, feature_name):
    """Analyze association between categorical feature and species category."""

    print(f"\n2. {feature_name.upper()} ANALYSIS:")
    print("-" * 60)

    # Create contingency table
    contingency = pd.crosstab(df[feature], df[target], margins=True)

    # Filter out categories with very few samples for cleaner output
    main_categories = contingency.index[contingency['All'] >= 10].tolist()
    if 'All' in main_categories:
        main_categories.remove('All')

    print(f"Contingency Table (categories with ≥10 samples):")
    display_contingency = contingency.loc[main_categories]
    print(display_contingency)

    # Chi-square test for independence
    try:
        # Remove margins for chi-square test
        test_contingency = contingency.iloc[:-1, :-1]  # Remove 'All' row and column

        if test_contingency.shape[0] > 1 and test_contingency.shape[1] > 1:
            chi2, p_value, dof, expected = chi2_contingency(test_contingency)

            print(f"\nChi-square test results:")
            print(f"   Chi-square statistic: {chi2:.4f}")
            print(f"   p-value: {p_value:.6f}")
            print(f"   Degrees of freedom: {dof}")

            if p_value < 0.001:
                significance = "*** (p < 0.001) - Highly significant"
            elif p_value < 0.01:
                significance = "** (p < 0.01) - Very significant"
            elif p_value < 0.05:
                significance = "* (p < 0.05) - Significant"
            else:
                significance = "(p ≥ 0.05) - Not significant"

            print(f"   Significance: {significance}")

            # Calculate effect size (Cramér's V)
            n = test_contingency.sum().sum()
            cramers_v = np.sqrt(chi2 / (n * (min(test_contingency.shape) - 1)))
            print(f"   Effect size (Cramér's V): {cramers_v:.4f}")

            if cramers_v < 0.1:
                effect_interpretation = "negligible"
            elif cramers_v < 0.3:
                effect_interpretation = "small"
            elif cramers_v < 0.5:
                effect_interpretation = "medium"
            else:
                effect_interpretation = "large"
            print(f"   Effect interpretation: {effect_interpretation}")

    except Exception as e:
        print(f"   Could not perform chi-square test: {e}")

    # Calculate preferences for each category
    print(f"\nSpecies preferences by {feature_name.lower()}:")
    for category in main_categories[:10]:  # Top 10 categories
        category_data = df[df[feature] == category]
        if len(category_data) >= 5:  # Minimum sample size

            stratum_count = len(category_data[category_data['species_category'] == 'stratum_tectonicas'])
            bacterium_count = len(category_data[category_data['species_category'] == 'bacterium'])
            total_count = len(category_data)

            if total_count > 0:
                stratum_pct = (stratum_count / total_count) * 100
                bacterium_pct = (bacterium_count / total_count) * 100

                # Determine preference
                if stratum_pct > bacterium_pct * 1.5:
                    preference = f"STRATUM preference ({stratum_pct:.1f}% vs {bacterium_pct:.1f}%)"
                elif bacterium_pct > stratum_pct * 1.5:
                    preference = f"BACTERIUM preference ({bacterium_pct:.1f}% vs {stratum_pct:.1f}%)"
                else:
                    preference = f"Mixed ({stratum_pct:.1f}% S, {bacterium_pct:.1f}% B)"

                print(f"   {category}: {preference} (n={total_count})")

def analyze_continuous_variables(df):
    """Analyze continuous variables (temperature, gravity, pressure) by species category."""

    print(f"\n3. CONTINUOUS VARIABLES ANALYSIS:")
    print("-" * 60)

    continuous_vars = ['temperature', 'gravity', 'pressure', 'distance_to_arrival']

    for var in continuous_vars:
        print(f"\n{var.upper()} Analysis:")

        # Remove null values
        var_data = df.dropna(subset=[var])

        if len(var_data) == 0:
            print(f"   No data available for {var}")
            continue

        stratum_values = var_data[var_data['species_category'] == 'stratum_tectonicas'][var]
        bacterium_values = var_data[var_data['species_category'] == 'bacterium'][var]

        if len(stratum_values) == 0 or len(bacterium_values) == 0:
            print(f"   Insufficient data for comparison")
            continue

        # Descriptive statistics
        print(f"   Stratum Tectonicas: n={len(stratum_values)}")
        print(f"     Mean: {stratum_values.mean():.3f}, Median: {stratum_values.median():.3f}")
        print(f"     Range: {stratum_values.min():.3f} - {stratum_values.max():.3f}")
        print(f"     Q1-Q3: {stratum_values.quantile(0.25):.3f} - {stratum_values.quantile(0.75):.3f}")

        print(f"   Bacterium: n={len(bacterium_values)}")
        print(f"     Mean: {bacterium_values.mean():.3f}, Median: {bacterium_values.median():.3f}")
        print(f"     Range: {bacterium_values.min():.3f} - {bacterium_values.max():.3f}")
        print(f"     Q1-Q3: {bacterium_values.quantile(0.25):.3f} - {bacterium_values.quantile(0.75):.3f}")

        # Statistical tests
        try:
            # Mann-Whitney U test (non-parametric)
            statistic, p_value = stats.mannwhitneyu(stratum_values, bacterium_values, alternative='two-sided')

            # Effect size (Cohen's d equivalent for Mann-Whitney)
            effect_size = (stratum_values.median() - bacterium_values.median()) / np.sqrt((stratum_values.var() + bacterium_values.var()) / 2)

            print(f"   Mann-Whitney U test: U={statistic:.2f}, p={p_value:.6f}")
            print(f"   Effect size (standardized): {effect_size:.3f}")

            if p_value < 0.001:
                significance = "*** Highly significant difference"
            elif p_value < 0.01:
                significance = "** Very significant difference"
            elif p_value < 0.05:
                significance = "* Significant difference"
            else:
                significance = "No significant difference"

            print(f"   Result: {significance}")

        except Exception as e:
            print(f"   Could not perform statistical test: {e}")

def analyze_material_composition(df):
    """Analyze material composition differences between species."""

    print(f"\n4. MATERIAL COMPOSITION ANALYSIS:")
    print("-" * 60)

    # Collect all materials mentioned
    all_materials = set()
    for _, record in df.iterrows():
        materials = record.get('materials', {})
        if isinstance(materials, dict):
            all_materials.update(materials.keys())

    print(f"Found {len(all_materials)} different materials")

    # Analyze top materials
    material_data = defaultdict(lambda: {'stratum': [], 'bacterium': []})

    for _, record in df.iterrows():
        materials = record.get('materials', {})
        species_cat = record['species_category']

        if isinstance(materials, dict) and species_cat in ['stratum_tectonicas', 'bacterium']:
            category = 'stratum' if species_cat == 'stratum_tectonicas' else 'bacterium'

            for material, percentage in materials.items():
                if isinstance(percentage, (int, float)) and percentage > 0:
                    material_data[material][category].append(percentage)

    # Find materials with significant differences
    significant_materials = []

    for material in sorted(all_materials):
        stratum_values = material_data[material]['stratum']
        bacterium_values = material_data[material]['bacterium']

        if len(stratum_values) >= 5 and len(bacterium_values) >= 5:
            try:
                statistic, p_value = stats.mannwhitneyu(stratum_values, bacterium_values, alternative='two-sided')

                stratum_median = np.median(stratum_values)
                bacterium_median = np.median(bacterium_values)

                if p_value < 0.05:
                    significant_materials.append({
                        'material': material,
                        'p_value': p_value,
                        'stratum_median': stratum_median,
                        'bacterium_median': bacterium_median,
                        'stratum_n': len(stratum_values),
                        'bacterium_n': len(bacterium_values)
                    })
            except:
                continue

    # Sort by p-value and display top differences
    significant_materials.sort(key=lambda x: x['p_value'])

    print(f"\nMaterials with significant composition differences (p < 0.05):")
    for mat in significant_materials[:10]:
        direction = "higher" if mat['stratum_median'] > mat['bacterium_median'] else "lower"
        print(f"   {mat['material']}: Stratum {direction} ({mat['stratum_median']:.2f}% vs {mat['bacterium_median']:.2f}%)")
        print(f"     p-value: {mat['p_value']:.6f}, n_stratum: {mat['stratum_n']}, n_bacterium: {mat['bacterium_n']}")

def perform_multivariate_analysis(df):
    """Perform multivariate analysis to identify the strongest predictors."""

    print(f"\n5. MULTIVARIATE ANALYSIS:")
    print("-" * 60)

    # Create binary target variable (1 = stratum_tectonicas, 0 = bacterium)
    df_analysis = df.copy()
    df_analysis['target'] = (df_analysis['species_category'] == 'stratum_tectonicas').astype(int)

    # Prepare features for analysis
    features = []
    feature_names = []

    # One-hot encode categorical variables
    categorical_features = ['body_type', 'atmosphere', 'volcanism']

    for cat_feature in categorical_features:
        unique_values = df_analysis[cat_feature].value_counts()
        # Only include categories with at least 10 samples
        common_values = unique_values[unique_values >= 10].index

        for value in common_values:
            feature_name = f"{cat_feature}_{value}"
            feature_names.append(feature_name)
            features.append((df_analysis[cat_feature] == value).astype(int))

    # Add continuous features (normalized)
    continuous_features = ['temperature', 'gravity', 'pressure']

    for cont_feature in continuous_features:
        clean_data = df_analysis[cont_feature].dropna()
        if len(clean_data) > 10:
            # Normalize to 0-1 range
            min_val, max_val = clean_data.min(), clean_data.max()
            if max_val > min_val:
                normalized = (df_analysis[cont_feature].fillna(clean_data.median()) - min_val) / (max_val - min_val)
                features.append(normalized)
                feature_names.append(f"{cont_feature}_normalized")

    if len(features) == 0:
        print("   No suitable features for multivariate analysis")
        return

    # Create feature matrix
    X = np.column_stack(features)
    y = df_analysis['target'].values

    # Remove rows with NaN values
    valid_rows = ~np.isnan(X).any(axis=1)
    X = X[valid_rows]
    y = y[valid_rows]

    if len(X) < 20:
        print("   Insufficient data for multivariate analysis")
        return

    print(f"   Feature matrix: {X.shape[0]} samples, {X.shape[1]} features")

    # Calculate correlation with target for feature importance
    feature_correlations = []

    for i, feature_name in enumerate(feature_names):
        try:
            correlation, p_value = pearsonr(X[:, i], y)
            feature_correlations.append({
                'feature': feature_name,
                'correlation': correlation,
                'p_value': p_value,
                'abs_correlation': abs(correlation)
            })
        except:
            continue

    # Sort by absolute correlation
    feature_correlations.sort(key=lambda x: x['abs_correlation'], reverse=True)

    print(f"\nTop predictive features (by correlation with Stratum Tectonicas):")
    for feat in feature_correlations[:15]:
        direction = "positive" if feat['correlation'] > 0 else "negative"
        significance = "***" if feat['p_value'] < 0.001 else "**" if feat['p_value'] < 0.01 else "*" if feat['p_value'] < 0.05 else ""
        print(f"   {feat['feature']}: {feat['correlation']:+.4f} ({direction}) {significance}")

def generate_recommendations(df):
    """Generate evidence-based recommendations for species differentiation."""

    print(f"\n6. EVIDENCE-BASED RECOMMENDATIONS:")
    print("="*60)

    stratum_count = len(df[df['species_category'] == 'stratum_tectonicas'])
    bacterium_count = len(df[df['species_category'] == 'bacterium'])

    print(f"Based on analysis of {stratum_count:,} Stratum Tectonicas and {bacterium_count:,} Bacterium observations:")

    # Body type recommendations
    print(f"\n🪐 BODY TYPE DIFFERENTIATORS:")
    body_type_analysis = df.groupby(['body_type', 'species_category']).size().unstack(fill_value=0)

    for body_type in body_type_analysis.index:
        stratum = body_type_analysis.loc[body_type].get('stratum_tectonicas', 0)
        bacterium = body_type_analysis.loc[body_type].get('bacterium', 0)
        total = stratum + bacterium

        if total >= 10:  # Minimum sample size
            stratum_pct = (stratum / total) * 100

            if stratum_pct >= 80:
                confidence = "High"
                print(f"   {body_type}: {confidence} confidence STRATUM indicator ({stratum_pct:.1f}%, n={total})")
            elif stratum_pct <= 20:
                confidence = "High"
                print(f"   {body_type}: {confidence} confidence BACTERIUM indicator ({100-stratum_pct:.1f}%, n={total})")
            elif stratum_pct >= 70 or stratum_pct <= 30:
                confidence = "Medium"
                indicator = "STRATUM" if stratum_pct >= 70 else "BACTERIUM"
                pct = stratum_pct if stratum_pct >= 70 else 100-stratum_pct
                print(f"   {body_type}: {confidence} confidence {indicator} indicator ({pct:.1f}%, n={total})")

    # Atmosphere recommendations
    print(f"\n🌫️  ATMOSPHERE DIFFERENTIATORS:")
    atmosphere_analysis = df.groupby(['atmosphere', 'species_category']).size().unstack(fill_value=0)

    for atmosphere in atmosphere_analysis.index:
        stratum = atmosphere_analysis.loc[atmosphere].get('stratum_tectonicas', 0)
        bacterium = atmosphere_analysis.loc[atmosphere].get('bacterium', 0)
        total = stratum + bacterium

        if total >= 10:  # Minimum sample size
            stratum_pct = (stratum / total) * 100

            if stratum_pct >= 80:
                print(f"   {atmosphere}: High confidence STRATUM indicator ({stratum_pct:.1f}%, n={total})")
            elif stratum_pct <= 20:
                print(f"   {atmosphere}: High confidence BACTERIUM indicator ({100-stratum_pct:.1f}%, n={total})")
            elif stratum_pct >= 70 or stratum_pct <= 30:
                indicator = "STRATUM" if stratum_pct >= 70 else "BACTERIUM"
                pct = stratum_pct if stratum_pct >= 70 else 100-stratum_pct
                print(f"   {atmosphere}: Medium confidence {indicator} indicator ({pct:.1f}%, n={total})")

    print(f"\n💡 RECOMMENDED CLASSIFICATION STRATEGY:")
    print(f"   1. Use body type as primary differentiator")
    print(f"   2. Apply atmosphere as secondary validation")
    print(f"   3. Consider temperature/gravity ranges for edge cases")
    print(f"   4. Implement confidence scoring based on multiple factors")

def main():
    jsonl_file = sys.argv[1] if len(sys.argv) > 1 else 'output/bio_landmarks_full_20250919_1814/results.jsonl'

    print("🔬 STATISTICAL BIOLOGICAL SPECIES ANALYSIS")
    print("="*80)
    print(f"Dataset: {jsonl_file}")

    # Load and parse data
    biological_records = load_and_parse_biological_data(jsonl_file)

    if len(biological_records) == 0:
        print("❌ No biological data found!")
        return

    # Perform statistical analysis
    analysis_df = perform_statistical_analysis(biological_records)

    if analysis_df is not None:
        # Generate recommendations
        generate_recommendations(analysis_df)

    # Save detailed results
    output_file = f"output/statistical_bio_analysis_{int(time.time())}.json"

    # Convert numpy types to native Python types for JSON serialization
    serializable_records = []
    for record in biological_records:
        serializable_record = {}
        for key, value in record.items():
            if isinstance(value, (np.integer, np.floating)):
                serializable_record[key] = value.item()
            elif isinstance(value, np.ndarray):
                serializable_record[key] = value.tolist()
            else:
                serializable_record[key] = value
        serializable_records.append(serializable_record)

    with open(output_file, 'w') as f:
        json.dump(serializable_records, f, indent=2, default=str)

    print(f"\n💾 Detailed analysis data saved to: {output_file}")
    print(f"✅ Statistical analysis complete!")

if __name__ == "__main__":
    import time
    main()