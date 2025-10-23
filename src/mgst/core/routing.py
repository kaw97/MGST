"""Route optimization algorithms for system clusters."""

import numpy as np
import pandas as pd
from typing import List, Tuple
from ..utils.math_utils import distance_3d

def nearest_neighbor_route(systems_df: pd.DataFrame) -> pd.DataFrame:
    """Optimize route through systems using nearest neighbor algorithm.
    
    Fast nearest neighbor routing that starts from the system closest to 
    galactic center and greedily selects the nearest unvisited system.
    
    Args:
        systems_df: DataFrame containing system data with coordinates
        
    Returns:
        DataFrame with systems reordered for optimal routing
    """
    if len(systems_df) <= 1:
        return systems_df
    
    systems = systems_df.copy().reset_index(drop=True)
    
    # Extract coordinates
    if all(col in systems.columns for col in ['coords_x', 'coords_y', 'coords_z']):
        coords = systems[['coords_x', 'coords_y', 'coords_z']].values
    else:
        raise ValueError("DataFrame must contain 'coords_x', 'coords_y', 'coords_z' columns")
    
    # Start with system closest to galactic center
    center_distances = np.sqrt(np.sum(coords ** 2, axis=1))
    start_idx = np.argmin(center_distances)
    
    route_order = [start_idx]
    remaining = set(range(len(systems))) - {start_idx}
    current_pos = coords[start_idx]
    
    # Greedy nearest neighbor selection
    while remaining:
        remaining_coords = coords[list(remaining)]
        distances = np.sqrt(np.sum((remaining_coords - current_pos) ** 2, axis=1))
        nearest_idx_in_remaining = np.argmin(distances)
        nearest_original_idx = list(remaining)[nearest_idx_in_remaining]
        
        route_order.append(nearest_original_idx)
        remaining.remove(nearest_original_idx)
        current_pos = coords[nearest_original_idx]
    
    return systems.iloc[route_order]

def calculate_route_metrics(systems_df: pd.DataFrame) -> dict:
    """Calculate routing metrics for a system cluster.
    
    Args:
        systems_df: DataFrame with routed systems
        
    Returns:
        Dictionary containing route metrics
    """
    if len(systems_df) == 0:
        return {
            'system_count': 0,
            'total_distance': 0,
            'avg_distance_per_jump': 0,
            'route_efficiency': 0
        }
    
    coords = systems_df[['coords_x', 'coords_y', 'coords_z']].values
    
    # Calculate total route distance
    total_distance = 0
    for i in range(len(coords) - 1):
        total_distance += distance_3d(coords[i], coords[i + 1])
    
    # Calculate metrics
    system_count = len(systems_df)
    avg_distance_per_jump = total_distance / max(1, system_count - 1)
    
    # Simple efficiency metric (lower is better)
    # Compare to direct point-to-point distances
    if system_count > 2:
        direct_distance = distance_3d(coords[0], coords[-1])
        route_efficiency = direct_distance / total_distance if total_distance > 0 else 0
    else:
        route_efficiency = 1.0
    
    return {
        'system_count': system_count,
        'total_distance': total_distance,
        'avg_distance_per_jump': avg_distance_per_jump,
        'route_efficiency': route_efficiency
    }