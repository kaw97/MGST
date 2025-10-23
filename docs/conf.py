"""Sphinx configuration file for MGST documentation."""

import os
import sys
sys.path.insert(0, os.path.abspath('../src'))

# Project information
project = 'MGST'
copyright = '2024, MGST Team'
author = 'MGST Team'
release = '0.1.0'

# General configuration
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.automodule',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx_click',
    'sphinx_rtd_theme'
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# HTML output
html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

# Napoleon settings for Google/NumPy docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False