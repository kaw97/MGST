"""Galaxy system clustering algorithms."""

import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from sklearn.cluster import MiniBatchKMeans

from .routing import nearest_neighbor_route, calculate_route_metrics
from ..utils.file_utils import sanitize_filename, ensure_output_dir

def find_optimal_k(
    coords: np.ndarray, 
    k_range: Optional[List[int]] = None, 
    sample_size: int = 50000, 
    batch_size: int = 10000
) -> int:
    """Find optimal number of clusters using elbow method.
    
    Args:
        coords: System coordinates as numpy array (N, 3)
        k_range: Range of k values to test. If None, auto-determined
        sample_size: Maximum systems to sample for k optimization
        batch_size: Mini-batch size for K-means
        
    Returns:
        Optimal number of clusters
    """
    n_systems = len(coords)
    
    # Use reasonable k range based on dataset size
    if k_range is None:
        # Target clusters of roughly 50-200 systems each
        min_k = max(10, n_systems // 200)
        max_k = min(1000, n_systems // 50)
        k_range = np.linspace(min_k, max_k, 10, dtype=int)
        k_range = np.unique(k_range)  # Remove duplicates
    
    print(f"Testing k values: {list(k_range)}")
    
    # Sample data for k optimization if dataset is large
    if n_systems > sample_size:
        indices = np.random.choice(n_systems, sample_size, replace=False)
        sample_coords = coords[indices]
        print(f"Using {sample_size} sample points for k optimization")
    else:
        sample_coords = coords
    
    inertias = []
    k_values = []
    
    for k in k_range:
        print(f"  Testing k={k}...", end=' ')
        try:
            kmeans = MiniBatchKMeans(
                n_clusters=k, 
                batch_size=batch_size,
                random_state=42,
                n_init=3  # Reduced for speed
            )
            kmeans.fit(sample_coords)
            inertia = kmeans.inertia_
            inertias.append(inertia)
            k_values.append(k)
            print(f"inertia: {inertia:.1f}")
        except Exception as e:
            print(f"failed: {e}")
    
    if len(inertias) < 3:
        print("Insufficient k values tested, using default")
        return k_range[0] if len(k_range) > 0 else 100
    
    # Find elbow using second derivative
    inertias = np.array(inertias)
    k_values = np.array(k_values)
    
    # Calculate second derivative (curvature)
    if len(inertias) >= 3:
        second_deriv = np.diff(inertias, 2)
        # Find point of maximum curvature (most negative second derivative)
        elbow_idx = np.argmin(second_deriv) + 1  # +1 because diff reduces length
        optimal_k = k_values[elbow_idx]
    else:
        # Fallback to middle value
        optimal_k = k_values[len(k_values) // 2]
    
    print(f"Optimal k selected: {optimal_k}")
    return optimal_k

def cluster_systems(
    df: pd.DataFrame, 
    k: Optional[int] = None, 
    batch_size: int = 10000
) -> Tuple[np.ndarray, Dict]:
    """Cluster systems using Mini-Batch K-Means.
    
    Args:
        df: DataFrame with system data including coordinates
        k: Number of clusters. If None, auto-determined
        batch_size: Mini-batch size for K-means
        
    Returns:
        Tuple of (cluster_labels, clustering_info)
    """
    # Prepare coordinates
    coords = df[['coords_x', 'coords_y', 'coords_z']].values
    print(f"Coordinate ranges:")
    print(f"  X: {coords[:, 0].min():.1f} to {coords[:, 0].max():.1f}")
    print(f"  Y: {coords[:, 1].min():.1f} to {coords[:, 1].max():.1f}")  
    print(f"  Z: {coords[:, 2].min():.1f} to {coords[:, 2].max():.1f}")
    
    # Determine optimal k if not provided
    if k is None:
        print(f"\nFinding optimal number of clusters...")
        optimal_k = find_optimal_k(coords, batch_size=batch_size)
    else:
        optimal_k = k
        print(f"\nUsing specified k: {optimal_k}")
    
    # Perform Mini-Batch K-Means clustering
    print(f"\nRunning Mini-Batch K-Means clustering...")
    print(f"  k (clusters): {optimal_k}")
    print(f"  batch_size: {batch_size}")
    
    kmeans = MiniBatchKMeans(
        n_clusters=optimal_k,
        batch_size=batch_size,
        random_state=42,
        n_init=10
    )
    
    cluster_labels = kmeans.fit_predict(coords)
    
    # Analyze results
    unique_labels = np.unique(cluster_labels)
    n_clusters = len(unique_labels)
    
    # Analyze cluster sizes
    cluster_sizes = []
    for label in unique_labels:
        size = list(cluster_labels).count(label)
        cluster_sizes.append(size)
    
    cluster_sizes = np.array(cluster_sizes)
    
    clustering_info = {
        'n_clusters': n_clusters,
        'optimal_k': optimal_k,
        'cluster_sizes': {
            'min': cluster_sizes.min(),
            'max': cluster_sizes.max(),
            'mean': cluster_sizes.mean(),
            'median': np.median(cluster_sizes)
        },
        'kmeans_model': kmeans
    }
    
    print(f"\nClustering results:")
    print(f"  Total clusters: {n_clusters}")
    print(f"  Cluster size stats: min={cluster_sizes.min()}, max={cluster_sizes.max()}, "
          f"mean={cluster_sizes.mean():.1f}, median={np.median(cluster_sizes):.1f}")
    
    return cluster_labels, clustering_info

def process_cluster(cluster_data: pd.DataFrame, cluster_id: int, output_dir: Path) -> Dict:
    """Process a single cluster: route and save.
    
    Args:
        cluster_data: DataFrame containing systems in this cluster
        cluster_id: Unique cluster identifier
        output_dir: Directory to save cluster file
        
    Returns:
        Dictionary with cluster processing results
    """
    try:
        if len(cluster_data) == 0:
            return {'error': f"Cluster {cluster_id}: Empty cluster"}
        
        # Route the systems in the cluster
        routed_systems = nearest_neighbor_route(cluster_data)
        
        # Calculate routing metrics
        metrics = calculate_route_metrics(routed_systems)
        
        # Get representative system name for filename
        first_system = routed_systems.iloc[0]['system_name']
        filename = f"auto_cluster_{cluster_id:03d}_{sanitize_filename(first_system)}.tsv"
        
        # Save cluster
        output_file = output_dir / filename
        routed_systems.to_csv(output_file, sep='\t', index=False)
        
        # Calculate cluster center coordinates
        coords = routed_systems[['coords_x', 'coords_y', 'coords_z']].values
        center_x = round(coords[:, 0].mean(), 1)
        center_y = round(coords[:, 1].mean(), 1)
        center_z = round(coords[:, 2].mean(), 1)
        
        # Calculate distance from cluster center to galactic origin
        from ..utils.math_utils import distance_to_origin
        distance_to_origin_val = round(distance_to_origin((center_x, center_y, center_z)), 1)
        
        result = {
            'cluster_id': cluster_id,
            'representative_system': first_system,
            'filename': filename,
            'center_x': center_x,
            'center_y': center_y,
            'center_z': center_z,
            'distance_to_origin': distance_to_origin_val
        }
        
        # Add routing metrics
        result.update(metrics)
        
        return result
        
    except Exception as e:
        return {'error': f"Error processing cluster {cluster_id}: {e}"}

def cluster_and_route_systems(
    input_file: Path,
    output_dir: Path = Path("auto_clusters"),
    k: Optional[int] = None,
    batch_size: int = 10000,
    workers: int = 4
) -> Dict:
    """Complete clustering and routing pipeline.
    
    Args:
        input_file: Path to input TSV file with systems
        output_dir: Directory for output cluster files
        k: Number of clusters (auto-determined if None)
        batch_size: Mini-batch size for K-means
        workers: Number of worker threads for processing
        
    Returns:
        Dictionary with clustering results and summary
    """
    print(f"Loading systems from {input_file}...")
    df = pd.read_csv(input_file, sep='\t')
    print(f"Loaded {len(df)} systems")
    
    # Perform clustering
    cluster_labels, clustering_info = cluster_systems(df, k=k, batch_size=batch_size)
    
    # Create output directory
    ensure_output_dir(output_dir)
    
    # Prepare clusters for processing
    clusters_data = []
    unique_labels = np.unique(cluster_labels)
    for label in unique_labels:
        cluster_mask = cluster_labels == label
        cluster_df = df[cluster_mask].copy()
        if len(cluster_df) > 0:
            clusters_data.append(cluster_df)
    
    print(f"\nProcessing {len(clusters_data)} clusters with {workers} workers...")
    
    # Process clusters in parallel
    cluster_summaries = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for i, cluster_data in enumerate(clusters_data):
            future = executor.submit(process_cluster, cluster_data, i, output_dir)
            futures.append(future)
        
        for i, future in enumerate(futures):
            result = future.result()
            if 'error' not in result:
                cluster_summaries.append(result)
                print(f"  Cluster {i:3d}: {result['system_count']:3d} systems, "
                      f"{result['total_distance']:7.1f} LY total, "
                      f"{result['avg_distance_per_jump']:5.1f} LY/jump")
            else:
                print(f"  {result['error']}")
    
    # Create summary
    summary_results = {
        'clustering_info': clustering_info,
        'cluster_summaries': cluster_summaries,
        'output_dir': output_dir
    }
    
    if cluster_summaries:
        summary_df = pd.DataFrame(cluster_summaries)
        summary_file = output_dir / "auto_cluster_summary.tsv"
        summary_df.to_csv(summary_file, sep='\t', index=False)
        
        print(f"\nSummary:")
        print(f"  Created {len(cluster_summaries)} cluster files")
        print(f"  Total systems clustered: {summary_df['system_count'].sum()}")
        print(f"  Average systems per cluster: {summary_df['system_count'].mean():.1f}")
        print(f"  Average distance per jump: {summary_df['avg_distance_per_jump'].mean():.1f} LY")
        print(f"  Summary saved to: {summary_file}")
        
        summary_results['summary_file'] = summary_file
        summary_results['total_systems'] = summary_df['system_count'].sum()
    
    return summary_results