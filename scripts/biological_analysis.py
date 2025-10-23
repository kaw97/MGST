#!/usr/bin/env python3
"""
Biological Species Analysis Script

This script analyzes biological landmarks data to:
1. Identify patterns in planetary/stellar characteristics vs species occurrence
2. Improve differentiation rules between Stratum Tectonicas and bacteria
3. Quantify false positive rates in current detection rules
4. Generate enhanced biological detection configurations
"""

import json
import gzip
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Any, Tuple
import sys
import argparse
import multiprocessing as mp
from functools import partial
import time

# Try to import pandas/numpy, but make them optional
try:
    import pandas as pd
    import numpy as np
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("⚠️  pandas/numpy not available - using basic analysis mode")


class BiologicalAnalyzer:
    """Analyzer for biological signals and species detection patterns."""

    def __init__(self):
        self.species_rules = self.load_species_rules()
        self.bio_data = None
        self.raw_systems = []

    def load_species_rules(self) -> Dict[str, Any]:
        """Load existing species detection rules from rulesets."""
        rules = {}

        # Load Stratum Tectonicas rules
        try:
            import sys
            sys.path.append('/mnt/z/HITEC')
            from rulesets.stratum import catalog as stratum_catalog
            from rulesets.bacterium import catalog as bacterium_catalog

            rules['stratum'] = stratum_catalog
            rules['bacterium'] = bacterium_catalog

            print(f"✅ Loaded {len(stratum_catalog)} Stratum species")
            print(f"✅ Loaded {len(bacterium_catalog)} Bacterium species")

        except ImportError as e:
            print(f"⚠️  Could not load species rules: {e}")
            rules = {}

        return rules

    def load_biological_landmarks(self, filepath: str):
        """Load biological landmarks data (JSONL or TSV)."""
        try:
            if filepath.endswith('.jsonl'):
                systems = self.load_jsonl_multiprocessed(filepath)
                print(f"📊 Loaded {len(systems)} biological landmark systems from JSONL")
                self.raw_systems = systems
                return systems
            elif HAS_PANDAS:
                df = pd.read_csv(filepath, sep='\t')
                print(f"📊 Loaded {len(df)} biological landmark systems from TSV")
                self.bio_data = df
                return df
            else:
                print("❌ TSV loading requires pandas - use JSONL format instead")
                return []
        except Exception as e:
            print(f"❌ Error loading biological data: {e}")
            return []

    def load_jsonl_multiprocessed(self, filepath: str, chunk_size: int = 10000) -> List[Dict]:
        """Load large JSONL files using multiprocessing."""
        file_size = Path(filepath).stat().st_size
        print(f"📂 File size: {file_size / 1024 / 1024:.1f} MB")

        if file_size < 50 * 1024 * 1024:  # < 50MB, load normally
            systems = []
            with open(filepath, 'r') as f:
                for line in f:
                    if line.strip():
                        systems.append(json.loads(line))
            return systems

        print(f"🚀 Large file detected, using multiprocessed loading...")

        # Read file in chunks
        chunks = []
        with open(filepath, 'r') as f:
            chunk = []
            for i, line in enumerate(f):
                if line.strip():
                    chunk.append(line.strip())
                    if len(chunk) >= chunk_size:
                        chunks.append(chunk)
                        chunk = []
            if chunk:
                chunks.append(chunk)

        print(f"📦 Split into {len(chunks)} chunks of ~{chunk_size} lines each")

        # Process chunks in parallel
        num_processes = min(mp.cpu_count(), len(chunks))
        with mp.Pool(processes=num_processes) as pool:
            start_time = time.time()
            chunk_results = pool.map(self.process_jsonl_chunk, chunks)
            processing_time = time.time() - start_time

        # Combine results
        systems = []
        for chunk_result in chunk_results:
            systems.extend(chunk_result)

        print(f"⚡ Processed {len(systems)} systems in {processing_time:.1f}s using {num_processes} processes")
        return systems

    @staticmethod
    def process_jsonl_chunk(lines: List[str]) -> List[Dict]:
        """Process a chunk of JSONL lines."""
        systems = []
        for line in lines:
            try:
                systems.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return systems

    def load_raw_systems(self, sector_dirs: List[str]) -> List[Dict]:
        """Load raw system data from compressed sectors to get detailed biological info."""
        systems = []

        for sector_dir in sector_dirs:
            sector_path = Path(sector_dir)
            if sector_path.is_file() and sector_path.suffix == '.gz':
                # Single compressed file
                files = [sector_path]
            else:
                # Directory with compressed files
                files = list(Path(sector_dir).glob("*.jsonl.gz"))

            for file_path in files:
                print(f"📂 Processing {file_path.name}...")
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        for line_num, line in enumerate(f):
                            if line.strip():
                                try:
                                    system = json.loads(line)
                                    # Only keep systems with biological signals
                                    if self.has_biological_signals(system):
                                        systems.append(system)
                                except json.JSONDecodeError:
                                    continue

                                if line_num % 1000 == 0 and line_num > 0:
                                    print(f"   Processed {line_num} systems...")

                except Exception as e:
                    print(f"⚠️  Error processing {file_path}: {e}")

        print(f"📊 Loaded {len(systems)} systems with biological signals")
        self.raw_systems = systems
        return systems

    def has_biological_signals(self, system: Dict) -> bool:
        """Check if system has biological signals."""
        bodies = system.get('bodies', [])
        for body in bodies:
            signals = body.get('signals', {})
            if 'signals' in signals or 'genuses' in signals:
                return True
        return False

    def analyze_stellar_correlations(self) -> Dict[str, Any]:
        """Analyze correlations between stellar characteristics and biological occurrence."""
        if self.bio_data is None or self.bio_data.empty:
            return {}

        analysis = {}

        # Stellar class distribution
        stellar_classes = self.bio_data['spectral_class'].value_counts()
        analysis['stellar_class_distribution'] = stellar_classes.to_dict()

        # Temperature vs biological body count
        temp_bio_corr = self.bio_data[['surface_temperature', 'biological_bodies_count']].corr()
        analysis['temperature_bio_correlation'] = temp_bio_corr.iloc[0, 1]

        # Stellar mass vs bio occurrence
        mass_bio_corr = self.bio_data[['stellar_mass', 'biological_bodies_count']].corr()
        analysis['mass_bio_correlation'] = mass_bio_corr.iloc[0, 1]

        # Age vs bio occurrence
        age_bio_corr = self.bio_data[['stellar_age', 'biological_bodies_count']].corr()
        analysis['age_bio_correlation'] = age_bio_corr.iloc[0, 1]

        return analysis

    def analyze_body_characteristics_multiprocessed(self) -> Dict[str, Any]:
        """Analyze detailed body characteristics using multiprocessing for large datasets."""
        if not self.raw_systems:
            return {}

        print(f"🔬 Analyzing {len(self.raw_systems)} systems for body characteristics...")

        if len(self.raw_systems) < 5000:
            # Use single-threaded for smaller datasets
            return self.analyze_body_characteristics()

        # Use multiprocessing for large datasets
        num_processes = mp.cpu_count()
        chunk_size = max(1, len(self.raw_systems) // num_processes)

        print(f"🚀 Using {num_processes} processes with chunks of {chunk_size} systems each")

        # Split systems into chunks
        system_chunks = []
        for i in range(0, len(self.raw_systems), chunk_size):
            system_chunks.append(self.raw_systems[i:i + chunk_size])

        # Process chunks in parallel
        with mp.Pool(processes=num_processes) as pool:
            start_time = time.time()
            chunk_results = pool.map(self.analyze_body_characteristics_chunk, system_chunks)
            processing_time = time.time() - start_time

        print(f"⚡ Analyzed body characteristics in {processing_time:.1f}s")

        # Combine results
        return self.combine_body_analysis_results(chunk_results)

    @staticmethod
    def analyze_body_characteristics_chunk(systems: List[Dict]) -> Dict[str, Any]:
        """Analyze body characteristics for a chunk of systems."""
        body_analysis = {
            'body_types': defaultdict(int),
            'atmosphere_types': defaultdict(int),
            'temperature_ranges': [],
            'gravity_ranges': [],
            'pressure_ranges': [],
            'earth_masses_ranges': [],
            'radius_ranges': [],
            'volcanism_types': defaultdict(int),
            'terraforming_states': defaultdict(int),
            'body_type_atmosphere_combinations': defaultdict(int),
            'materials_analysis': defaultdict(lambda: defaultdict(list)),
            'solid_composition_analysis': defaultdict(lambda: defaultdict(list)),
            'atmosphere_composition_analysis': defaultdict(lambda: defaultdict(list)),
            'orbital_characteristics': {
                'orbital_periods': [],
                'semi_major_axes': [],
                'eccentricities': [],
                'inclinations': [],
                'rotational_periods': [],
                'axial_tilts': []
            },
            'parent_relationships': defaultdict(int),
            'landable_vs_non_landable': {'landable': 0, 'non_landable': 0}
        }

        for system in systems:
            # Use biological_bodies if available (from our enhanced config)
            bio_bodies = system.get('biological_bodies', [])

            if not bio_bodies:
                # Fallback to scanning all bodies
                bodies = system.get('bodies', [])
                bio_bodies = []
                for body in bodies:
                    signals = body.get('signals', {})
                    if 'signals' in signals or 'genuses' in signals:
                        bio_bodies.append(body)

            for body in bio_bodies:
                BiologicalAnalyzer._process_single_body(body, body_analysis)

        return body_analysis

    @staticmethod
    def _process_single_body(body: Dict[str, Any], body_analysis: Dict[str, Any]):
        """Process a single body for characteristics analysis."""
        # Basic characteristics
        sub_type = body.get('subType', 'Unknown')
        atmosphere = body.get('atmosphereType', 'None')
        temperature = body.get('surfaceTemperature')
        gravity = body.get('gravity')
        pressure = body.get('surfacePressure')
        earth_masses = body.get('earthMasses')
        radius = body.get('radius')
        volcanism = body.get('volcanismType', 'None')
        terraforming = body.get('terraformingState', 'Unknown')
        is_landable = body.get('isLandable', False)

        # Count basic characteristics
        body_analysis['body_types'][sub_type] += 1
        body_analysis['atmosphere_types'][atmosphere] += 1
        body_analysis['volcanism_types'][volcanism] += 1
        body_analysis['terraforming_states'][terraforming] += 1

        if is_landable:
            body_analysis['landable_vs_non_landable']['landable'] += 1
        else:
            body_analysis['landable_vs_non_landable']['non_landable'] += 1

        combination = f"{sub_type}_{atmosphere}"
        body_analysis['body_type_atmosphere_combinations'][combination] += 1

        # Collect ranges
        if temperature is not None:
            body_analysis['temperature_ranges'].append(temperature)
        if gravity is not None:
            body_analysis['gravity_ranges'].append(gravity)
        if pressure is not None:
            body_analysis['pressure_ranges'].append(pressure)
        if earth_masses is not None:
            body_analysis['earth_masses_ranges'].append(earth_masses)
        if radius is not None:
            body_analysis['radius_ranges'].append(radius)

        # Materials analysis
        materials = body.get('materials', {})
        for material, percentage in materials.items():
            body_analysis['materials_analysis'][sub_type][material].append(percentage)

        # Solid composition analysis
        solid_comp = body.get('solidComposition', {})
        for component, percentage in solid_comp.items():
            body_analysis['solid_composition_analysis'][sub_type][component].append(percentage)

        # Atmosphere composition analysis
        atm_comp = body.get('atmosphereComposition', {})
        for component, percentage in atm_comp.items():
            body_analysis['atmosphere_composition_analysis'][atmosphere][component].append(percentage)

        # Orbital characteristics
        orbital = body_analysis['orbital_characteristics']
        if body.get('orbitalPeriod') is not None:
            orbital['orbital_periods'].append(body.get('orbitalPeriod'))
        if body.get('semiMajorAxis') is not None:
            orbital['semi_major_axes'].append(body.get('semiMajorAxis'))
        if body.get('orbitalEccentricity') is not None:
            orbital['eccentricities'].append(body.get('orbitalEccentricity'))
        if body.get('orbitalInclination') is not None:
            orbital['inclinations'].append(body.get('orbitalInclination'))
        if body.get('rotationalPeriod') is not None:
            orbital['rotational_periods'].append(body.get('rotationalPeriod'))
        if body.get('axialTilt') is not None:
            orbital['axial_tilts'].append(body.get('axialTilt'))

        # Parent relationships
        parents = body.get('parents', [])
        for parent in parents:
            for parent_type, _ in parent.items():
                body_analysis['parent_relationships'][parent_type] += 1

    def combine_body_analysis_results(self, chunk_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Combine body analysis results from multiple chunks."""
        combined = {
            'body_types': defaultdict(int),
            'atmosphere_types': defaultdict(int),
            'temperature_ranges': [],
            'gravity_ranges': [],
            'pressure_ranges': [],
            'earth_masses_ranges': [],
            'radius_ranges': [],
            'volcanism_types': defaultdict(int),
            'terraforming_states': defaultdict(int),
            'body_type_atmosphere_combinations': defaultdict(int),
            'materials_analysis': defaultdict(lambda: defaultdict(list)),
            'solid_composition_analysis': defaultdict(lambda: defaultdict(list)),
            'atmosphere_composition_analysis': defaultdict(lambda: defaultdict(list)),
            'orbital_characteristics': {
                'orbital_periods': [],
                'semi_major_axes': [],
                'eccentricities': [],
                'inclinations': [],
                'rotational_periods': [],
                'axial_tilts': []
            },
            'parent_relationships': defaultdict(int),
            'landable_vs_non_landable': {'landable': 0, 'non_landable': 0}
        }

        for chunk_result in chunk_results:
            # Combine counts
            for key in ['body_types', 'atmosphere_types', 'volcanism_types', 'terraforming_states',
                       'body_type_atmosphere_combinations', 'parent_relationships']:
                for item, count in chunk_result[key].items():
                    combined[key][item] += count

            # Combine ranges
            for key in ['temperature_ranges', 'gravity_ranges', 'pressure_ranges',
                       'earth_masses_ranges', 'radius_ranges']:
                combined[key].extend(chunk_result[key])

            # Combine orbital characteristics
            for key in combined['orbital_characteristics']:
                combined['orbital_characteristics'][key].extend(
                    chunk_result['orbital_characteristics'][key]
                )

            # Combine landable counts
            combined['landable_vs_non_landable']['landable'] += chunk_result['landable_vs_non_landable']['landable']
            combined['landable_vs_non_landable']['non_landable'] += chunk_result['landable_vs_non_landable']['non_landable']

            # Combine materials and composition analysis
            for body_type, materials in chunk_result['materials_analysis'].items():
                for material, values in materials.items():
                    combined['materials_analysis'][body_type][material].extend(values)

            for body_type, compositions in chunk_result['solid_composition_analysis'].items():
                for component, values in compositions.items():
                    combined['solid_composition_analysis'][body_type][component].extend(values)

            for atmosphere, compositions in chunk_result['atmosphere_composition_analysis'].items():
                for component, values in compositions.items():
                    combined['atmosphere_composition_analysis'][atmosphere][component].extend(values)

        return combined

    def analyze_body_characteristics(self) -> Dict[str, Any]:
        """Analyze detailed body characteristics from JSONL system data (single-threaded)."""
        if not self.raw_systems:
            return {}

        body_analysis = {
            'body_types': defaultdict(int),
            'atmosphere_types': defaultdict(int),
            'temperature_ranges': [],
            'gravity_ranges': [],
            'pressure_ranges': [],
            'earth_masses_ranges': [],
            'radius_ranges': [],
            'volcanism_types': defaultdict(int),
            'terraforming_states': defaultdict(int),
            'body_type_atmosphere_combinations': defaultdict(int),
            'materials_analysis': defaultdict(lambda: defaultdict(list)),
            'solid_composition_analysis': defaultdict(lambda: defaultdict(list)),
            'atmosphere_composition_analysis': defaultdict(lambda: defaultdict(list)),
            'orbital_characteristics': {
                'orbital_periods': [],
                'semi_major_axes': [],
                'eccentricities': [],
                'inclinations': [],
                'rotational_periods': [],
                'axial_tilts': []
            },
            'parent_relationships': defaultdict(int),
            'landable_vs_non_landable': {'landable': 0, 'non_landable': 0}
        }

        for system in self.raw_systems:
            # Use biological_bodies if available (from our enhanced config)
            bio_bodies = system.get('biological_bodies', [])

            if not bio_bodies:
                # Fallback to scanning all bodies
                bodies = system.get('bodies', [])
                bio_bodies = []
                for body in bodies:
                    signals = body.get('signals', {})
                    if 'signals' in signals or 'genuses' in signals:
                        bio_bodies.append(body)

            for body in bio_bodies:
                # Basic characteristics
                sub_type = body.get('subType', 'Unknown')
                atmosphere = body.get('atmosphereType', 'None')
                temperature = body.get('surfaceTemperature')
                gravity = body.get('gravity')
                pressure = body.get('surfacePressure')
                earth_masses = body.get('earthMasses')
                radius = body.get('radius')
                volcanism = body.get('volcanismType', 'None')
                terraforming = body.get('terraformingState', 'Unknown')
                is_landable = body.get('isLandable', False)

                # Count basic characteristics
                body_analysis['body_types'][sub_type] += 1
                body_analysis['atmosphere_types'][atmosphere] += 1
                body_analysis['volcanism_types'][volcanism] += 1
                body_analysis['terraforming_states'][terraforming] += 1

                if is_landable:
                    body_analysis['landable_vs_non_landable']['landable'] += 1
                else:
                    body_analysis['landable_vs_non_landable']['non_landable'] += 1

                combination = f"{sub_type}_{atmosphere}"
                body_analysis['body_type_atmosphere_combinations'][combination] += 1

                # Collect ranges
                if temperature is not None:
                    body_analysis['temperature_ranges'].append(temperature)
                if gravity is not None:
                    body_analysis['gravity_ranges'].append(gravity)
                if pressure is not None:
                    body_analysis['pressure_ranges'].append(pressure)
                if earth_masses is not None:
                    body_analysis['earth_masses_ranges'].append(earth_masses)
                if radius is not None:
                    body_analysis['radius_ranges'].append(radius)

                # Materials analysis
                materials = body.get('materials', {})
                for material, percentage in materials.items():
                    body_analysis['materials_analysis'][sub_type][material].append(percentage)

                # Solid composition analysis
                solid_comp = body.get('solidComposition', {})
                for component, percentage in solid_comp.items():
                    body_analysis['solid_composition_analysis'][sub_type][component].append(percentage)

                # Atmosphere composition analysis
                atm_comp = body.get('atmosphereComposition', {})
                for component, percentage in atm_comp.items():
                    body_analysis['atmosphere_composition_analysis'][atmosphere][component].append(percentage)

                # Orbital characteristics
                orbital = body_analysis['orbital_characteristics']
                if body.get('orbitalPeriod') is not None:
                    orbital['orbital_periods'].append(body.get('orbitalPeriod'))
                if body.get('semiMajorAxis') is not None:
                    orbital['semi_major_axes'].append(body.get('semiMajorAxis'))
                if body.get('orbitalEccentricity') is not None:
                    orbital['eccentricities'].append(body.get('orbitalEccentricity'))
                if body.get('orbitalInclination') is not None:
                    orbital['inclinations'].append(body.get('orbitalInclination'))
                if body.get('rotationalPeriod') is not None:
                    orbital['rotational_periods'].append(body.get('rotationalPeriod'))
                if body.get('axialTilt') is not None:
                    orbital['axial_tilts'].append(body.get('axialTilt'))

                # Parent relationships
                parents = body.get('parents', [])
                for parent in parents:
                    for parent_type, _ in parent.items():
                        body_analysis['parent_relationships'][parent_type] += 1

        return body_analysis

    def analyze_stratum_vs_bacteria_overlap(self) -> Dict[str, Any]:
        """Analyze overlapping conditions between Stratum Tectonicas and bacteria."""
        if not self.species_rules:
            return {}

        overlap_analysis = {}

        # Extract Stratum Tectonicas conditions
        stratum_conditions = defaultdict(set)
        if 'stratum' in self.species_rules:
            for genus_data in self.species_rules['stratum'].values():
                for species_data in genus_data.values():
                    if 'rulesets' in species_data:
                        for ruleset in species_data['rulesets']:
                            for condition, value in ruleset.items():
                                if condition == 'atmosphere':
                                    stratum_conditions['atmospheres'].update(value)
                                elif condition == 'body_type':
                                    stratum_conditions['body_types'].update(value)
                                elif condition == 'volcanism':
                                    if isinstance(value, str):
                                        stratum_conditions['volcanism'].add(value)
                                    elif isinstance(value, list):
                                        stratum_conditions['volcanism'].update(value)

        # Extract bacteria conditions
        bacteria_conditions = defaultdict(set)
        if 'bacterium' in self.species_rules:
            for genus_data in self.species_rules['bacterium'].values():
                for species_data in genus_data.values():
                    if 'rulesets' in species_data:
                        for ruleset in species_data['rulesets']:
                            for condition, value in ruleset.items():
                                if condition == 'atmosphere':
                                    bacteria_conditions['atmospheres'].update(value)
                                elif condition == 'body_type':
                                    bacteria_conditions['body_types'].update(value)
                                elif condition == 'volcanism':
                                    if isinstance(value, str):
                                        bacteria_conditions['volcanism'].add(value)
                                    elif isinstance(value, list):
                                        bacteria_conditions['volcanism'].update(value)

        # Find overlaps
        overlap_analysis['atmosphere_overlap'] = list(
            stratum_conditions['atmospheres'] & bacteria_conditions['atmospheres']
        )
        overlap_analysis['body_type_overlap'] = list(
            stratum_conditions['body_types'] & bacteria_conditions['body_types']
        )
        overlap_analysis['volcanism_overlap'] = list(
            stratum_conditions['volcanism'] & bacteria_conditions['volcanism']
        )

        overlap_analysis['stratum_unique_atmospheres'] = list(
            stratum_conditions['atmospheres'] - bacteria_conditions['atmospheres']
        )
        overlap_analysis['stratum_unique_body_types'] = list(
            stratum_conditions['body_types'] - bacteria_conditions['body_types']
        )

        return overlap_analysis

    def analyze_stratum_tectonicas_conditions(self) -> Dict[str, Any]:
        """Detailed analysis of conditions that correlate with Stratum Tectonicas."""
        if not self.raw_systems:
            return {}

        # Extract bodies that match known Stratum conditions
        stratum_candidate_bodies = []
        bacteria_candidate_bodies = []

        for system in self.raw_systems:
            bio_bodies = system.get('biological_bodies', [])
            if not bio_bodies:
                bodies = system.get('bodies', [])
                bio_bodies = [b for b in bodies if b.get('signals', {}).get('signals') or b.get('signals', {}).get('genuses')]

            for body in bio_bodies:
                body_type = body.get('subType', '')
                atmosphere = body.get('atmosphereType', '')
                temperature = body.get('surfaceTemperature', 0)
                gravity = body.get('gravity', 0)
                pressure = body.get('surfacePressure', 0)

                # Check if body matches Stratum Tectonicas conditions
                stratum_match = self.matches_stratum_conditions(body_type, atmosphere, temperature, gravity, pressure)
                bacteria_match = self.matches_bacteria_conditions(body_type, atmosphere, temperature, gravity, pressure)

                if stratum_match:
                    stratum_candidate_bodies.append({
                        'system': system.get('name'),
                        'body': body.get('name'),
                        'body_type': body_type,
                        'atmosphere': atmosphere,
                        'temperature': temperature,
                        'gravity': gravity,
                        'pressure': pressure,
                        'materials': body.get('materials', {}),
                        'solid_composition': body.get('solidComposition', {}),
                        'volcanism': body.get('volcanismType', ''),
                        'biological_signals': body.get('biological_signals', {})
                    })

                if bacteria_match:
                    bacteria_candidate_bodies.append({
                        'system': system.get('name'),
                        'body': body.get('name'),
                        'body_type': body_type,
                        'atmosphere': atmosphere,
                        'temperature': temperature,
                        'gravity': gravity,
                        'pressure': pressure,
                        'materials': body.get('materials', {}),
                        'solid_composition': body.get('solidComposition', {}),
                        'volcanism': body.get('volcanismType', ''),
                        'biological_signals': body.get('biological_signals', {})
                    })

        return {
            'stratum_candidates': stratum_candidate_bodies,
            'bacteria_candidates': bacteria_candidate_bodies,
            'overlap_bodies': self.find_overlap_bodies(stratum_candidate_bodies, bacteria_candidate_bodies)
        }

    def matches_stratum_conditions(self, body_type: str, atmosphere: str, temperature: float, gravity: float, pressure: float) -> bool:
        """Check if body conditions match any Stratum Tectonicas ruleset."""
        if 'stratum' not in self.species_rules:
            return False

        for genus_data in self.species_rules['stratum'].values():
            for species_data in genus_data.values():
                rulesets = species_data.get('rulesets', [])
                for ruleset in rulesets:
                    if self.matches_ruleset(body_type, atmosphere, temperature, gravity, pressure, ruleset):
                        return True
        return False

    def matches_bacteria_conditions(self, body_type: str, atmosphere: str, temperature: float, gravity: float, pressure: float) -> bool:
        """Check if body conditions match any bacteria ruleset."""
        if 'bacterium' not in self.species_rules:
            return False

        for genus_data in self.species_rules['bacterium'].values():
            for species_data in genus_data.values():
                rulesets = species_data.get('rulesets', [])
                for ruleset in rulesets:
                    if self.matches_ruleset(body_type, atmosphere, temperature, gravity, pressure, ruleset):
                        return True
        return False

    def matches_ruleset(self, body_type: str, atmosphere: str, temperature: float, gravity: float, pressure: float, ruleset: Dict) -> bool:
        """Check if body conditions match a specific ruleset."""
        # Check body type
        if 'body_type' in ruleset:
            if body_type not in ruleset['body_type']:
                return False

        # Check atmosphere
        if 'atmosphere' in ruleset:
            if atmosphere not in ruleset['atmosphere']:
                return False

        # Check temperature range
        if 'min_temperature' in ruleset and temperature < ruleset['min_temperature']:
            return False
        if 'max_temperature' in ruleset and temperature > ruleset['max_temperature']:
            return False

        # Check gravity range
        if 'min_gravity' in ruleset and gravity < ruleset['min_gravity']:
            return False
        if 'max_gravity' in ruleset and gravity > ruleset['max_gravity']:
            return False

        # Check pressure range
        if 'min_pressure' in ruleset and pressure < ruleset['min_pressure']:
            return False
        if 'max_pressure' in ruleset and pressure > ruleset['max_pressure']:
            return False

        return True

    def find_overlap_bodies(self, stratum_candidates: List[Dict], bacteria_candidates: List[Dict]) -> List[Dict]:
        """Find bodies that match both Stratum and bacteria conditions."""
        overlap_bodies = []

        for stratum_body in stratum_candidates:
            for bacteria_body in bacteria_candidates:
                if (stratum_body['system'] == bacteria_body['system'] and
                    stratum_body['body'] == bacteria_body['body']):
                    overlap_bodies.append({
                        **stratum_body,
                        'overlap_type': 'stratum_bacteria',
                        'conflict_resolution_needed': True
                    })
                    break

        return overlap_bodies

    def generate_differentiation_rules(self) -> Dict[str, Any]:
        """Generate improved differentiation rules based on detailed analysis."""
        overlap = self.analyze_stratum_vs_bacteria_overlap()
        body_analysis = self.analyze_body_characteristics()
        stratum_analysis = self.analyze_stratum_tectonicas_conditions()

        # Enhanced differentiation rules based on detailed analysis
        differentiation_rules = {
            'high_confidence_stratum': {
                'description': 'High confidence Stratum Tectonicas identification',
                'body_type': ['High metal content body'],  # Stratum-specific
                'atmosphere': ['CarbonDioxide', 'SulphurDioxide', 'Ammonia', 'Oxygen', 'CarbonDioxideRich', 'Argon', 'ArgonRich', 'Water'],
                'temperature_range': (165, 450),
                'gravity_range': (0.035, 0.62),
                'additional_checks': {
                    'exclude_if_bacteria_volcanism': True,
                    'prefer_higher_gravity': True
                },
                'confidence': 0.9
            },
            'medium_confidence_stratum': {
                'description': 'Medium confidence, requires additional validation',
                'body_type': ['Rocky body'],
                'atmosphere': overlap.get('stratum_unique_atmospheres', []),
                'temperature_range': (165, 300),
                'gravity_range': (0.04, 0.6),
                'additional_checks': {
                    'require_no_volcanism_for_some_atmospheres': True
                },
                'confidence': 0.6
            },
            'bacteria_indicators': {
                'description': 'Strong bacteria indicators that rule out Stratum',
                'body_type': ['Icy body', 'Rocky ice body'],
                'atmosphere': ['Helium', 'Methane', 'Neon', 'NeonRich', 'Nitrogen'],
                'temperature_range': (20, 150),
                'volcanism_requirements': ['methane', 'carbon dioxide', 'nitrogen', 'ammonia'],
                'confidence': 0.8
            },
            'overlap_resolution': {
                'description': 'Rules for resolving overlapping conditions',
                'body_type_priority': 'High metal content body favors Stratum',
                'gravity_thresholds': {
                    'high_gravity_stratum': 0.4,  # Above this favors Stratum
                    'low_gravity_bacteria': 0.1   # Below this favors bacteria
                },
                'material_indicators': {
                    'stratum_materials': ['Iron', 'Nickel'],
                    'bacteria_materials': ['Carbon', 'Phosphorus']
                }
            }
        }

        return differentiation_rules

    def generate_report(self, output_dir: str):
        """Generate comprehensive analysis report."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Use multiprocessed analysis for better performance
        stellar_analysis = self.analyze_stellar_correlations() if HAS_PANDAS and self.bio_data is not None else {}
        body_analysis = self.analyze_body_characteristics_multiprocessed()
        overlap_analysis = self.analyze_stratum_vs_bacteria_overlap()
        stratum_analysis = self.analyze_stratum_tectonicas_conditions()

        report = {
            'dataset_info': {
                'total_systems': len(self.raw_systems) if self.raw_systems else 0,
                'total_bio_bodies': sum(len(s.get('biological_bodies', [])) for s in (self.raw_systems or [])),
                'has_pandas': HAS_PANDAS,
                'analysis_timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            },
            'stellar_analysis': stellar_analysis,
            'body_analysis': body_analysis,
            'overlap_analysis': overlap_analysis,
            'stratum_analysis': stratum_analysis,
            'differentiation_rules': self.generate_differentiation_rules()
        }

        # Save detailed report
        with open(output_path / 'biological_analysis_report.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)

        print(f"📊 Analysis report saved to {output_path / 'biological_analysis_report.json'}")

        # Print summary
        print("\n" + "="*60)
        print("BIOLOGICAL SPECIES ANALYSIS SUMMARY")
        print("="*60)

        if report['stellar_analysis']:
            print(f"\n🌟 STELLAR CORRELATIONS:")
            stellar = report['stellar_analysis']
            print(f"   Temperature-Bio correlation: {stellar.get('temperature_bio_correlation', 'N/A'):.3f}")
            print(f"   Mass-Bio correlation: {stellar.get('mass_bio_correlation', 'N/A'):.3f}")
            print(f"   Age-Bio correlation: {stellar.get('age_bio_correlation', 'N/A'):.3f}")

        if report['overlap_analysis']:
            print(f"\n🔬 STRATUM vs BACTERIA OVERLAP:")
            overlap = report['overlap_analysis']
            print(f"   Overlapping atmospheres: {overlap.get('atmosphere_overlap', [])}")
            print(f"   Overlapping body types: {overlap.get('body_type_overlap', [])}")
            print(f"   Stratum-unique body types: {overlap.get('stratum_unique_body_types', [])}")

        if report['body_analysis']:
            print(f"\n🪐 BODY CHARACTERISTICS:")
            body = report['body_analysis']
            print(f"   Most common body types: {dict(list(Counter(body['body_types']).most_common(5)))}")
            print(f"   Most common atmospheres: {dict(list(Counter(body['atmosphere_types']).most_common(5)))}")

        return report


def main():
    parser = argparse.ArgumentParser(description='Biological Species Analysis')
    parser.add_argument('--landmarks', help='Path to biological landmarks TSV file')
    parser.add_argument('--sectors', nargs='+', help='Paths to sector data (files or directories)')
    parser.add_argument('--output', default='output/biological_analysis', help='Output directory')

    args = parser.parse_args()

    analyzer = BiologicalAnalyzer()

    # Load biological landmarks data if provided
    if args.landmarks:
        analyzer.load_biological_landmarks(args.landmarks)

    # Load raw sector data if provided
    if args.sectors:
        analyzer.load_raw_systems(args.sectors)

    # Generate analysis report
    report = analyzer.generate_report(args.output)

    # Additional detailed analysis
    if analyzer.raw_systems:
        print(f"\n🔬 DETAILED STRATUM TECTONICAS ANALYSIS:")
        stratum_analysis = analyzer.analyze_stratum_tectonicas_conditions()

        print(f"   Stratum candidates found: {len(stratum_analysis.get('stratum_candidates', []))}")
        print(f"   Bacteria candidates found: {len(stratum_analysis.get('bacteria_candidates', []))}")
        print(f"   Overlap/conflict bodies: {len(stratum_analysis.get('overlap_bodies', []))}")

        # Show some examples of overlap conflicts
        overlap_bodies = stratum_analysis.get('overlap_bodies', [])
        if overlap_bodies:
            print(f"\n   🔍 Example overlap conflicts:")
            for i, body in enumerate(overlap_bodies[:3]):
                print(f"      {i+1}. {body['system']} - {body['body']}")
                print(f"         Body: {body['body_type']}, Atm: {body['atmosphere']}")
                print(f"         T: {body['temperature']:.1f}K, G: {body['gravity']:.3f}g, P: {body['pressure']:.4f}atm")

    print(f"\n✅ Analysis complete. Results saved to {args.output}/")


if __name__ == "__main__":
    main()