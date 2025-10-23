HITEC Galaxy Documentation
==========================

Welcome to HITEC Galaxy, a comprehensive toolkit for Elite Dangerous galaxy analysis, 
exobiology research, and exploration route planning.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   usage
   api
   configurations
   development

Features
--------

* **Advanced System Filtering** - Find high-value exobiology targets and supply hub locations
* **Intelligent Clustering** - Group systems into optimal exploration clusters  
* **Route Optimization** - Generate efficient travel routes using nearest-neighbor algorithms
* **Data Processing** - Convert and clean galaxy data from multiple formats
* **Flexible Configuration** - Extensible research configuration system

Quick Start
-----------

Install the package::

    pip install hitec-galaxy

Find high-value exobiology systems::

    hitec-galaxy filter --config exobiology --input-dir galaxy_data/ --output bio_targets.tsv

Cluster systems for exploration::

    hitec-galaxy cluster bio_targets.tsv --output-dir exploration_routes/

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`