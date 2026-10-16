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

def distance_to_line_segment(point: Tuple[float, float, float],
                             start: Tuple[float, float, float],
                             end: Tuple[float, float, float]) -> float:
    """Calculate distance from a point to a line segment in 3D space.

    Args:
        point: Point coordinates as (x, y, z) tuple
        start: Start of line segment as (x, y, z) tuple
        end: End of line segment as (x, y, z) tuple

    Returns:
        Shortest distance from the point to the line segment
    """
    line_x = end[0] - start[0]
    line_y = end[1] - start[1]
    line_z = end[2] - start[2]
    line_length = math.sqrt(line_x**2 + line_y**2 + line_z**2)

    if line_length == 0:
        return distance_3d(point, start)

    point_x = point[0] - start[0]
    point_y = point[1] - start[1]
    point_z = point[2] - start[2]

    dot_product = point_x * line_x + point_y * line_y + point_z * line_z
    projection_length = max(0, min(line_length, dot_product / line_length))

    closest_x = start[0] + (line_x / line_length) * projection_length
    closest_y = start[1] + (line_y / line_length) * projection_length
    closest_z = start[2] + (line_z / line_length) * projection_length

    return distance_3d(point, (closest_x, closest_y, closest_z))


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