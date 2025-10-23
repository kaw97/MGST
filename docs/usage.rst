Usage Guide
===========

This guide covers the main use cases and workflows for HITEC Galaxy.

Command Line Interface
----------------------

HITEC Galaxy provides several CLI commands for different tasks:

System Filtering
~~~~~~~~~~~~~~~~

Filter galaxy data to find systems matching specific research criteria::

    hitec-galaxy filter --config exobiology --input-dir data/ --output results.tsv

Available configurations:

* ``exobiology`` - High-value exobiology research targets
* ``supply-hub-1`` - Basic supply hub location analysis  
* ``supply-hub-2`` - Advanced supply hub optimization
* ``supply-hub-4`` - Multi-criteria supply hub analysis

System Clustering
~~~~~~~~~~~~~~~~~

Group systems into clusters for efficient exploration::

    hitec-galaxy cluster systems.tsv --output-dir clusters/ --k auto

The clustering algorithm uses Mini-Batch K-Means with automatic optimal cluster determination.

Route Optimization
~~~~~~~~~~~~~~~~~~

Each cluster is automatically optimized using nearest-neighbor routing, starting from 
the system closest to the galactic center.

Data Conversion
~~~~~~~~~~~~~~~

Convert between different data formats::

    # Fix Excel formatting issues
    hitec-galaxy convert excel-fix data.xlsx

    # Convert to JSON
    hitec-galaxy convert to-json systems.tsv --format jsonl

Configuration Management
~~~~~~~~~~~~~~~~~~~~~~~~

Explore available configurations::

    # List all configurations
    hitec-galaxy config list

    # Get detailed description
    hitec-galaxy config describe exobiology

    # Show output columns
    hitec-galaxy config columns exobiology

Python API
----------

Use HITEC Galaxy programmatically:

.. code-block:: python

    from hitec_galaxy.core.clustering import cluster_and_route_systems
    from hitec_galaxy.configs.exobiology import ExobiologyConfig

    # Cluster systems programmatically
    results = cluster_and_route_systems(
        input_file="systems.tsv",
        output_dir="clusters/",
        k=None,  # Auto-determine optimal k
        workers=8
    )

    # Use configuration system
    config = ExobiologyConfig()
    if config.filter_system(system_data):
        print("System qualifies for exobiology research!")

Workflows
---------

Exobiology Research Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Obtain galaxy data** containing BioScan predictions
2. **Filter systems** using exobiology configuration
3. **Cluster systems** into exploration groups
4. **Plan routes** within each cluster
5. **Export routes** for navigation tools

Supply Hub Analysis Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Load trade and system data**
2. **Apply supply hub filters** based on requirements
3. **Analyze strategic positioning** 
4. **Generate optimization reports**

File Formats
------------

Input Formats
~~~~~~~~~~~~~

* **TSV** - Tab-separated values with system data
* **CSV** - Comma-separated values  
* **JSON/JSONL** - Structured data format
* **Excel** - Spreadsheet format (with automatic fixing)

Output Formats
~~~~~~~~~~~~~~

* **Cluster TSV files** - Individual routed clusters
* **Summary files** - Cluster statistics and metrics
* **JSON exports** - For integration with other tools

Expected Data Structure
~~~~~~~~~~~~~~~~~~~~~~~

System data should include:

* ``system_name`` - System identifier
* ``coords_x``, ``coords_y``, ``coords_z`` - Galactic coordinates
* ``bodies`` - List of planetary bodies (for filtering)
* Additional fields depending on analysis type