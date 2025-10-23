"""Mathematical utilities for galaxy analysis."""

import math
from typing import List, Tuple

def distance_3d(p1: Tuple[float, float, float], p2: Tuple[float, float, float]) -> float:
    """Calculate 3D Euclidean distance between two points.
    
    Args:
        p1: First point as (x, y, z) tuple
        p2: Second point as (x, y, z) tuple
        
    Returns:
        Euclidean distance between the points
    """
    return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2 + (p1[2] - p2[2])**2)

def distance_to_origin(point: Tuple[float, float, float]) -> float:
    """Calculate distance from point to galactic origin (0,0,0).
    
    Args:
        point: Point coordinates as (x, y, z) tuple
        
    Returns:
        Distance to origin
    """
    return math.sqrt(point[0]**2 + point[1]**2 + point[2]**2)

def calculate_route_distance(coordinates: List[Tuple[float, float, float]]) -> float:
    """Calculate total distance for a route through multiple points.
    
    Args:
        coordinates: List of (x, y, z) coordinate tuples
        
    Returns:
        Total route distance
    """
    if len(coordinates) < 2:
        return 0.0
    
    total_distance = 0.0
    for i in range(len(coordinates) - 1):
        total_distance += distance_3d(coordinates[i], coordinates[i + 1])
    
    return total_distance