#!/usr/bin/env python3
"""
Example workflows for spatial prefiltering in HITEC Galaxy.

This script demonstrates various spatial prefiltering scenarios
for Elite Dangerous galaxy analysis.
"""

import sys
from pathlib import Path

# Add src to Python path for examples
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def example_basic_spatial_search():
    """
    Example 1: Basic spatial prefiltering for exobiology research near Sol.
    
    This example shows how to filter for exobiology-rich systems within
    500 light years of key systems like Sol and Colonia.
    """
    print("🌟 Example 1: Basic Spatial Prefiltering")
    print("=" * 50)
    
    print("""
    # Step 1: Create a targets file with systems of interest
    # File: my_targets.tsv
    system_name	x	y	z
    Sol	0	0	0
    Colonia	-9530.5	-910.28	19808.125
    Sagittarius A*	25.21875	-20.90625	25899.96875
    
    # Step 2: Run spatial prefiltering with exobiology config
    hitec-galaxy filter \\
        --config exobiology \\
        --spatial-sector-db Databases/galaxy_sectors \\
        --spatial-targets my_targets.tsv \\
        --spatial-range 500 \\
        --input-dir Databases/galaxy_sectors \\
        --output output/exobio_spatial_500ly/results.tsv \\
        --workers 12
    
    # This will:
    # - Load target systems from my_targets.tsv (auto-detects columns)
    # - Find all sectors within 500ly of any target system
    # - Filter individual systems within 500ly (system-level filtering)
    # - Apply exobiology criteria to qualifying systems
    # - Achieve 70-95% processing time reduction vs full galaxy scan
    """)


def example_performance_optimization():
    """
    Example 2: Performance-optimized spatial search with advanced options.
    
    Shows how to tune spatial prefiltering for maximum performance.
    """
    print("🚀 Example 2: Performance-Optimized Spatial Search")
    print("=" * 50)
    
    print("""
    # Get range suggestions based on your sector database
    hitec-galaxy filter --spatial-suggest --spatial-sector-db Databases/galaxy_sectors
    
    # Output will show:
    # 💡 Suggested spatial ranges:
    #   --spatial-range 1250     # Tight search (local area)  
    #   --spatial-range 2500     # Normal search (recommended)
    #   --spatial-range 5000     # Wide search (broad area)
    #   --spatial-range 7500     # Very wide search (regional)
    
    # High-performance search with optimizations
    hitec-galaxy filter \\
        --config my_custom_config.py \\
        --spatial-sector-db Databases/galaxy_sectors \\
        --spatial-targets exploration_route.csv \\
        --spatial-range 2500 \\
        --spatial-min-sector-systems 50 \\
        --spatial-system-filter \\
        --spatial-stats \\
        --input-dir Databases/galaxy_sectors \\
        --output output/optimized_search/results.tsv \\
        --workers 16 \\
        --verbose
    
    # Performance optimizations explained:
    # --spatial-min-sector-systems 50: Skip sectors with <50 systems (reduces I/O)
    # --spatial-system-filter: Enable individual system distance checking
    # --spatial-stats: Show detailed performance statistics
    # --workers 16: Use more parallel workers for faster processing
    """)


def example_csv_format_flexibility():
    """
    Example 3: Flexible input formats - CSV/TSV with various column names.
    
    Shows how the enhanced parser handles different Elite Dangerous export formats.
    """
    print("📊 Example 3: Flexible Input Format Support")
    print("=" * 50)
    
    print("""
    # The spatial prefilter automatically detects various column formats:
    
    # Format 1: Elite Dangerous Navigator export
    # File: ed_navigator_export.csv
    Star System,Position X,Position Y,Position Z,Distance from Sol
    Wolf 359,7.856,-5.041,-1.234,7.86
    Barnard's Star,5.963,-0.289,-5.223,5.96
    
    # Format 2: EDSM API export  
    # File: edsm_systems.tsv
    name	coord_x	coord_y	coord_z	population
    Achenar	67.5	-119.46875	24.84375	15000000000
    Alioth	-33.65625	61.15625	-17.59375	4250000000
    
    # Format 3: Custom exploration log
    # File: my_exploration.tsv  
    systemname	galactic_x	galactic_y	galactic_z	visited_date
    HIP 12345	-1234.5	567.8	-890.1	2024-01-15
    
    # All of these work automatically - no format specification needed:
    hitec-galaxy filter \\
        --config exobiology \\
        --spatial-sector-db Databases/galaxy_sectors \\
        --spatial-targets ed_navigator_export.csv \\  # CSV format
        --spatial-range 1000 \\
        --output output/flexible_format/results.tsv
        
    # The parser will auto-detect:
    # - CSV vs TSV format (comma vs tab delimited)
    # - Column names for coordinates (x/y/z variants)
    # - System name columns (name/system_name/star_system/etc)
    # - Text encoding (UTF-8, latin1, cp1252)
    """)


def example_large_scale_analysis():
    """
    Example 4: Large-scale galactic analysis with multiple target regions.
    
    Demonstrates spatial prefiltering for complex multi-region analysis.
    """
    print("🌌 Example 4: Large-Scale Multi-Region Analysis")
    print("=" * 50)
    
    print("""
    # Create comprehensive target list covering major galactic regions
    # File: galactic_regions.tsv
    region_name	x	y	z	notes
    Core Worlds	0	0	0	Sol/Federation core
    Empire Core	67.5	-119.46875	24.84375	Achenar region
    Alliance Core	-33.65625	61.15625	-17.59375	Alioth region  
    Colonia	-9530.5	-910.28	19808.125	Independent colony
    Sagittarius A*	25.21875	-20.90625	25899.96875	Galactic center
    California Nebula	-393.25	-791.3125	-1412.6875	Nebula region
    Rosette Nebula	-757.59375	285.6875	-892.53125	Nebula region
    
    # Multi-stage analysis workflow:
    
    # Stage 1: Get baseline statistics  
    hitec-galaxy filter \\
        --spatial-suggest \\
        --spatial-sector-db Databases/galaxy_sectors
    
    # Stage 2: Wide area search (10,000 ly radius)
    hitec-galaxy filter \\
        --config exobiology \\
        --spatial-sector-db Databases/galaxy_sectors \\
        --spatial-targets galactic_regions.tsv \\
        --spatial-range 10000 \\
        --spatial-min-sector-systems 100 \\
        --spatial-stats \\
        --output output/wide_galactic_survey/results.tsv \\
        --workers 16
        
    # Expected performance:
    # - Processes ~30-60% of galaxy systems (massive reduction)
    # - Runs 2-4x faster than full galaxy scan
    # - Covers all major inhabited regions
    # - Finds high-value exobiology systems near civilization
    
    # Stage 3: Focused analysis on promising regions
    # Use results from Stage 2 to create refined target list
    hitec-galaxy filter \\
        --config my_advanced_exobio_config.py \\
        --spatial-sector-db Databases/galaxy_sectors \\
        --spatial-targets refined_targets.tsv \\
        --spatial-range 2500 \\
        --spatial-system-filter \\
        --output output/focused_analysis/results.tsv \\
        --workers 12
    """)


def example_integration_with_clustering():
    """
    Example 5: Integration with clustering for exploration route planning.
    
    Shows complete workflow from spatial filtering to route optimization.
    """
    print("🗺️  Example 5: Spatial Filtering + Route Planning")
    print("=" * 50)
    
    print("""
    # Complete workflow: Filter → Cluster → Route Planning
    
    # Step 1: Spatial filtering for exobiology systems near your current location
    hitec-galaxy filter \\
        --config exobiology \\
        --spatial-sector-db Databases/galaxy_sectors \\
        --spatial-targets current_location.tsv \\
        --spatial-range 3000 \\
        --output output/nearby_exobio/filtered_systems.tsv \\
        --workers 12
    
    # Step 2: Cluster systems into exploration routes
    hitec-galaxy cluster \\
        --input output/nearby_exobio/filtered_systems.tsv \\
        --output output/nearby_exobio/clusters/ \\
        --k-range 5-20 \\
        --max-jump-range 75
    
    # Step 3: Route optimization within each cluster  
    # The clustering tool automatically optimizes routes within each cluster
    # Results will be in output/nearby_exobio/clusters/optimized_routes.tsv
    
    # This workflow:
    # - Reduces initial dataset by 70-95% (spatial filtering)  
    # - Groups nearby systems into logical exploration routes
    # - Optimizes travel within each route for minimal jumps
    # - Perfect for planning multi-day exploration expeditions
    """)


def main():
    """Run all examples."""
    print("🚀 HITEC Galaxy Spatial Prefiltering Examples")
    print("=" * 60)
    print()
    
    examples = [
        example_basic_spatial_search,
        example_performance_optimization,
        example_csv_format_flexibility,
        example_large_scale_analysis,
        example_integration_with_clustering
    ]
    
    for i, example_func in enumerate(examples, 1):
        example_func()
        if i < len(examples):
            print("\n" + "─" * 60 + "\n")
    
    print()
    print("💡 Key Benefits of Spatial Prefiltering:")
    print("  • 50-95% reduction in processing time")
    print("  • Focus analysis on areas of interest") 
    print("  • Flexible input format support (CSV/TSV)")
    print("  • Intelligent sector and system-level filtering")
    print("  • Seamless integration with existing workflows")
    print()
    print("📚 For more information, see the HITEC Galaxy documentation.")


if __name__ == "__main__":
    main()