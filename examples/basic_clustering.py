#!/usr/bin/env python3
"""
Basic clustering example for HITEC Galaxy.

This example demonstrates how to:
1. Load system data
2. Perform clustering  
3. Generate optimized routes
4. Export results
"""

from pathlib import Path
from mgst.core.clustering import cluster_and_route_systems

def main():
    # Example system data file (TSV format)
    input_file = Path("sample_systems.tsv")
    
    if not input_file.exists():
        print(f"❌ Input file {input_file} not found")
        print("Please create a TSV file with columns: system_name, coords_x, coords_y, coords_z")
        return
    
    # Output directory for clusters
    output_dir = Path("exploration_clusters")
    
    print(f"🚀 Clustering systems from {input_file}")
    print(f"📁 Output directory: {output_dir}")
    
    try:
        results = cluster_and_route_systems(
            input_file=input_file,
            output_dir=output_dir,
            k=None,  # Auto-determine optimal number of clusters
            batch_size=5000,  # Smaller batch for example
            workers=2  # Use fewer workers for example
        )
        
        print(f"\n✅ Success!")
        print(f"📊 Created {results['clustering_info']['n_clusters']} clusters")
        
        if 'total_systems' in results:
            print(f"🎯 Processed {results['total_systems']} systems")
            
        print(f"📂 Results saved to: {output_dir}/")
        print(f"📋 Summary file: {output_dir}/auto_cluster_summary.tsv")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()