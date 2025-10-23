#!/usr/bin/env python3
"""
Test script for the stellar-adapted exobiology configuration.
Demonstrates enhanced species prediction with stellar adaptation data.
"""

import sys
import json
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from hitec_galaxy.configs.stellar_adapted_exobiology import StellarAdaptedExobiologyConfig

def create_test_system():
    """Create a test system with various stellar types."""
    return {
        'name': 'Test System',
        'id64': 123456789,
        'coords': {'x': 1000, 'y': 2000, 'z': 3000},
        'stars': [{
            'name': 'Test Star A',
            'mainStar': True,
            'spectralClass': 'K2',
            'surfaceTemperature': 4800.0,
            'solarMasses': 0.8
        }],
        'bodies': [
            {
                'name': 'Test Body 1',
                'bodyName': 'Test Body 1',
                'subType': 'High metal content body',
                'atmosphereType': 'Thin Ammonia',
                'surfacePressure': 0.05,
                'surfaceTemperature': 195.0,
                'gravity': 0.15,
                'distanceToArrival': 1200.0,
                'updateTime': '2021-01-01T00:00:00Z',
                'stellar_spectral_class': 'K2',
                'stellar_surface_temperature': 4800.0
            },
            {
                'name': 'Test Body 2',
                'bodyName': 'Test Body 2',
                'subType': 'Rocky body',
                'atmosphereType': 'Carbon dioxide',
                'surfacePressure': 0.08,
                'surfaceTemperature': 220.0,
                'gravity': 0.25,
                'distanceToArrival': 800.0,
                'updateTime': '2021-01-01T00:00:00Z',
                'stellar_spectral_class': 'K2',
                'stellar_surface_temperature': 4800.0
            }
        ]
    }

def test_stellar_adapted_config():
    """Test the stellar-adapted configuration."""
    print("=== TESTING STELLAR-ADAPTED EXOBIOLOGY CONFIGURATION ===")
    print()

    # Initialize configuration
    config = StellarAdaptedExobiologyConfig()
    print(f"Configuration: {config.name}")
    print(f"Description: {config.description[:100]}...")
    print()

    # Test system
    test_system = create_test_system()

    # Test species detection on first body
    body1 = test_system['bodies'][0]
    print("Testing species detection on Body 1:")
    print(f"  Body type: {body1['subType']}")
    print(f"  Atmosphere: {body1['atmosphereType']}")
    print(f"  Temperature: {body1['surfaceTemperature']}K")
    print(f"  Gravity: {body1['gravity']}g")
    print(f"  Distance: {body1['distanceToArrival']} ls")
    print()

    # Detect species
    detected_species = config.detect_species_on_body(body1, test_system)

    print(f"Detected {len(detected_species)} potential species:")
    for i, species in enumerate(detected_species[:10], 1):  # Show top 10
        confidence = species.get('confidence', 1.0)
        stellar_class = species.get('stellar_class', 'Unknown')
        thermal_reg = species.get('thermal_regulation', 'unknown')

        print(f"  {i:2}. {species['name']:<25} "
              f"Value: {species['value']:>8,} "
              f"Confidence: {confidence:>4.2f} "
              f"Stellar: {stellar_class} "
              f"Thermal: {thermal_reg}")

    print()

    # Test system filtering
    print("Testing system filtering:")
    result = config.filter_system(test_system)

    if result:
        print("✅ System QUALIFIES for enhanced exobiology search!")
        print(f"  Qualifying bodies: {result['qualifying_bodies']}")
        print(f"  Total species: {result['total_species']}")
        print(f"  Weighted value: {result['total_weighted_value']:,}")
        print(f"  Average confidence: {result['average_confidence']}")
        print(f"  Primary stellar class: {result['primary_stellar_class']}")
        print(f"  Stellar temperature: {result['stellar_temperature']}K")
    else:
        print("❌ System does not qualify")

    print()
    print("=== TEST COMPLETE ===")

def test_specific_species():
    """Test specific species predictions."""
    config = StellarAdaptedExobiologyConfig()

    print("=== TESTING SPECIFIC SPECIES PREDICTIONS ===")
    print()

    # Test Stratum Tectonicas in K-class system (should be high confidence)
    k_system = {
        'name': 'K-class Test',
        'stars': [{'mainStar': True, 'spectralClass': 'K2', 'surfaceTemperature': 4800.0}],
        'bodies': [{
            'name': 'Tectonicas Test Body',
            'subType': 'High metal content body',
            'atmosphereType': 'Thin Ammonia',
            'surfacePressure': 0.01,
            'surfaceTemperature': 200.0,
            'gravity': 0.15,
            'distanceToArrival': 1000.0,
            'updateTime': '2021-01-01T00:00:00Z',
            'stellar_spectral_class': 'K2',
            'stellar_surface_temperature': 4800.0
        }]
    }

    detected = config.detect_species_on_body(k_system['bodies'][0], k_system)
    stratum_species = [s for s in detected if 'Stratum' in s['name']]

    if stratum_species:
        print("Stratum species in K-class system:")
        for species in stratum_species:
            print(f"  {species['name']}: Confidence {species.get('confidence', 1.0):.2f}, "
                  f"Thermal regulation: {species.get('thermal_regulation', 'unknown')}")
    else:
        print("No Stratum species detected in K-class system")

    print()

    # Test same body in M-class system (should be lower confidence)
    m_system = {
        'name': 'M-class Test',
        'stars': [{'mainStar': True, 'spectralClass': 'M3', 'surfaceTemperature': 3100.0}],
        'bodies': [{
            'name': 'Tectonicas Test Body M',
            'subType': 'High metal content body',
            'atmosphereType': 'Thin Ammonia',
            'surfacePressure': 0.01,
            'surfaceTemperature': 200.0,
            'gravity': 0.15,
            'distanceToArrival': 200.0,  # Much closer for M-dwarf
            'updateTime': '2021-01-01T00:00:00Z',
            'stellar_spectral_class': 'M3',
            'stellar_surface_temperature': 3100.0
        }]
    }

    detected_m = config.detect_species_on_body(m_system['bodies'][0], m_system)
    stratum_species_m = [s for s in detected_m if 'Stratum' in s['name']]

    if stratum_species_m:
        print("Stratum species in M-class system:")
        for species in stratum_species_m:
            print(f"  {species['name']}: Confidence {species.get('confidence', 1.0):.2f}, "
                  f"Thermal regulation: {species.get('thermal_regulation', 'unknown')}")
    else:
        print("No Stratum species detected in M-class system")

if __name__ == "__main__":
    test_stellar_adapted_config()
    print()
    test_specific_species()