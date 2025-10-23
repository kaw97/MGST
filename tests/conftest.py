"""Pytest configuration and shared fixtures."""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path


@pytest.fixture
def sample_systems_data():
    """Sample system data for testing."""
    return [
        {
            'name': 'Sol',
            'coords': {'x': 0.0, 'y': 0.0, 'z': 0.0},
            'bodies': []
        },
        {
            'name': 'Alpha Centauri',
            'coords': {'x': -6.25, 'y': -1.25, 'z': -1.25},
            'bodies': []
        },
        {
            'name': 'Wolf 359',
            'coords': {'x': 7.86, 'y': 0.82, 'z': -2.14},
            'bodies': []
        }
    ]


@pytest.fixture
def sample_systems_df():
    """Sample systems DataFrame for testing."""
    data = {
        'system_name': ['Sol', 'Alpha Centauri', 'Wolf 359', 'Sirius'],
        'coords_x': [0.0, -6.25, 7.86, -9.5],
        'coords_y': [0.0, -1.25, 0.82, -5.25],
        'coords_z': [0.0, -1.25, -2.14, -1.25]
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_exobiology_body():
    """Sample body data for exobiology testing."""
    return {
        'bodyName': 'Test Body A',
        'atmosphereType': 'Thin Carbon dioxide',
        'surfacePressure': 0.05,
        'surfaceTemperature': 250.0,
        'gravity': 0.3,
        'subType': 'Rocky body',
        'updateTime': '2021-05-15 10:30:00+00:00',
        'bioscan_predictions': {
            'predicted_species': [
                {
                    'genus': 'Bacterium',
                    'species': 'Aurasus', 
                    'name': 'Bacterium Aurasus',
                    'value': 1000000
                },
                {
                    'genus': 'Stratum',
                    'species': 'Excutitus',
                    'name': 'Stratum Excutitus', 
                    'value': 16000000
                },
                {
                    'genus': 'Stratum',
                    'species': 'Paleas',
                    'name': 'Stratum Paleas',
                    'value': 15500000  
                }
            ]
        }
    }


@pytest.fixture
def temp_dir(tmp_path):
    """Temporary directory for test files."""
    return tmp_path


@pytest.fixture
def sample_tsv_file(temp_dir, sample_systems_df):
    """Create a temporary TSV file with sample data."""
    file_path = temp_dir / "sample_systems.tsv"
    sample_systems_df.to_csv(file_path, sep='\t', index=False)
    return file_path