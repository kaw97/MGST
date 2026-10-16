import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List, Tuple, Set, Optional
import heapq
from collections import defaultdict

@dataclass
class System:
    """Represents a star system"""
    name: str
    id64: int
    x: float
    y: float
    z: float
    elw_count: int = 0
    ww_count: int = 0
    aw_count: int = 0
    bh_count: int = 0
    ns_count: int = 0
    
    @property
    def coords(self) -> np.ndarray:
        return np.array([self.x, self.y, self.z])
    
    @property
    def value_score(self) -> int:
        """Total valuable bodies in system"""
        return self.elw_count + self.ww_count + self.aw_count + self.bh_count + self.ns_count

def distance_3d(sys1: System, sys2: System) -> float:
    """Calculate 3D distance between two systems"""
    return np.linalg.norm(sys1.coords - sys2.coords)

def load_systems(filepath: str) -> List[System]:
    """Load systems from TSV file"""
    df = pd.read_csv(filepath, sep='\t')
    
    systems = []
    for _, row in df.iterrows():
        systems.append(System(
            name=row['name'],
            id64=row['id64'],
            x=row['x'],
            y=row['y'],
            z=row['z'],
            elw_count=int(row.get('Earth-like world_count', 0)) if pd.notna(row.get('Earth-like world_count')) else 0,
            ww_count=int(row.get('Water world_count', 0)) if pd.notna(row.get('Water world_count')) else 0,
            aw_count=int(row.get('Ammonia world_count', 0)) if pd.notna(row.get('Ammonia world_count')) else 0,
            bh_count=int(row.get('Black Hole_count', 0)) if pd.notna(row.get('Black Hole_count')) else 0,
            ns_count=int(row.get('Neutron Star_count', 0)) if pd.notna(row.get('Neutron Star_count')) else 0,
        ))
    
    return systems

def build_spatial_index(systems: List[System]) -> dict:
    """Build a simple spatial index for fast neighbor queries"""
    # For production, use scipy.spatial.KDTree for better performance
    index = {}
    for i, sys in enumerate(systems):
        index[sys.name] = i
    return index

def get_neighbors(system: System, all_systems: List[System], max_range: float = 15.0) -> List[Tuple[System, float]]:
    """Get all systems within jump range, sorted by distance"""
    neighbors = []
    for other in all_systems:
        if other.name == system.name:
            continue
        dist = distance_3d(system, other)
        if dist <= max_range:
            neighbors.append((other, dist))
    
    neighbors.sort(key=lambda x: x[1])
    return neighbors

@dataclass
class PathNode:
    """Node in the search tree"""
    system: System
    g_cost: float  # Actual distance from start
    h_cost: float  # Heuristic distance to goal
    parent: Optional['PathNode'] = None
    
    @property
    def f_cost(self) -> float:
        return self.g_cost + self.h_cost
    
    def __lt__(self, other):
        return self.f_cost < other.f_cost

def reconstruct_path(node: PathNode) -> List[System]:
    """Reconstruct path from goal node to start"""
    path = []
    current = node
    while current:
        path.append(current.system)
        current = current.parent
    return list(reversed(path))

def bidirectional_beam_search(
    start_system: System,
    end_system: System,
    all_systems: List[System],
    beam_width: int = 150,
    max_iterations: int = 10000
) -> List[List[System]]:
    """
    Bidirectional beam search to find multiple good paths.
    Returns list of paths sorted by quality.
    """
    
    # Forward search from start
    forward_open = []
    forward_closed = set()
    forward_nodes = {}  # system.name -> PathNode
    
    start_node = PathNode(
        system=start_system,
        g_cost=0,
        h_cost=distance_3d(start_system, end_system)
    )
    heapq.heappush(forward_open, start_node)
    forward_nodes[start_system.name] = start_node
    
    # Backward search from end
    backward_open = []
    backward_closed = set()
    backward_nodes = {}
    
    end_node = PathNode(
        system=end_system,
        g_cost=0,
        h_cost=distance_3d(end_system, start_system)
    )
    heapq.heappush(backward_open, end_node)
    backward_nodes[end_system.name] = end_node
    
    paths_found = []
    iterations = 0
    
    while forward_open and backward_open and iterations < max_iterations:
        iterations += 1
        
        # Expand forward frontier
        forward_frontier = []
        beam_size = min(beam_width, len(forward_open))
        
        for _ in range(beam_size):
            if not forward_open:
                break
            current = heapq.heappop(forward_open)
            
            if current.system.name in forward_closed:
                continue
                
            forward_closed.add(current.system.name)
            
            # Check if we've met the backward search
            if current.system.name in backward_nodes:
                path = reconstruct_path(current) + reconstruct_path(backward_nodes[current.system.name])[1:]
                paths_found.append(path)
                if len(paths_found) >= 5:  # Found enough paths
                    return paths_found
            
            # Expand neighbors
            neighbors = get_neighbors(current.system, all_systems, max_range=15.0)
            for neighbor_sys, dist in neighbors:
                if neighbor_sys.name in forward_closed:
                    continue
                
                g_cost = current.g_cost + dist
                h_cost = distance_3d(neighbor_sys, end_system)
                
                neighbor_node = PathNode(
                    system=neighbor_sys,
                    g_cost=g_cost,
                    h_cost=h_cost,
                    parent=current
                )
                
                # Only add if we haven't seen this system or found a better path
                if neighbor_sys.name not in forward_nodes or g_cost < forward_nodes[neighbor_sys.name].g_cost:
                    forward_nodes[neighbor_sys.name] = neighbor_node
                    heapq.heappush(forward_open, neighbor_node)
        
        # Expand backward frontier
        backward_frontier = []
        beam_size = min(beam_width, len(backward_open))
        
        for _ in range(beam_size):
            if not backward_open:
                break
            current = heapq.heappop(backward_open)
            
            if current.system.name in backward_closed:
                continue
                
            backward_closed.add(current.system.name)
            
            # Check if we've met the forward search
            if current.system.name in forward_nodes:
                path = reconstruct_path(forward_nodes[current.system.name]) + reconstruct_path(current)[1:]
                paths_found.append(path)
                if len(paths_found) >= 5:
                    return paths_found
            
            # Expand neighbors
            neighbors = get_neighbors(current.system, all_systems, max_range=15.0)
            for neighbor_sys, dist in neighbors:
                if neighbor_sys.name in backward_closed:
                    continue
                
                g_cost = current.g_cost + dist
                h_cost = distance_3d(neighbor_sys, start_system)
                
                neighbor_node = PathNode(
                    system=neighbor_sys,
                    g_cost=g_cost,
                    h_cost=h_cost,
                    parent=current
                )
                
                if neighbor_sys.name not in backward_nodes or g_cost < backward_nodes[neighbor_sys.name].g_cost:
                    backward_nodes[neighbor_sys.name] = neighbor_node
                    heapq.heappush(backward_open, neighbor_node)
    
    return paths_found

def evaluate_corridor(path: List[System], all_systems: List[System], corridor_radius: float = 30.0) -> dict:
    """
    Evaluate the 30 LY corridor around a path for valuable systems.
    Returns dictionary with counts and details.
    """
    valuable_systems = []
    
    for system in all_systems:
        # Check if system is within corridor_radius of any path segment
        for path_sys in path:
            if distance_3d(system, path_sys) <= corridor_radius:
                if system.value_score > 0:
                    valuable_systems.append(system)
                break
    
    # Count valuable bodies
    total_elw = sum(s.elw_count for s in valuable_systems)
    total_ww = sum(s.ww_count for s in valuable_systems)
    total_aw = sum(s.aw_count for s in valuable_systems)
    total_bh = sum(s.bh_count for s in valuable_systems)
    total_ns = sum(s.ns_count for s in valuable_systems)
    
    return {
        'valuable_systems_count': len(valuable_systems),
        'elw_count': total_elw,
        'ww_count': total_ww,
        'aw_count': total_aw,
        'bh_count': total_bh,
        'ns_count': total_ns,
        'total_value': total_elw + total_ww + total_aw + total_bh + total_ns,
        'valuable_systems': valuable_systems
    }

def calculate_path_length(path: List[System]) -> float:
    """Calculate total path length in LY"""
    total = 0
    for i in range(len(path) - 1):
        total += distance_3d(path[i], path[i+1])
    return total

def find_colonization_routes(
    start_name: str,
    end_name: str,
    systems: List[System],
    beam_width: int = 150
) -> List[dict]:
    """
    Main function to find colonization routes.
    Returns list of route options with analysis.
    """
    
    # Find start and end systems
    start_system = next((s for s in systems if s.name == start_name), None)
    end_system = next((s for s in systems if s.name == end_name), None)
    
    if not start_system or not end_system:
        raise ValueError("Start or end system not found in dataset")
    
    print(f"Finding routes from {start_name} to {end_name}")
    print(f"Direct distance: {distance_3d(start_system, end_system):.2f} LY\n")
    
    # Find paths
    paths = bidirectional_beam_search(start_system, end_system, systems, beam_width)
    
    if not paths:
        print("No paths found!")
        return []
    
    # Evaluate each path
    results = []
    for i, path in enumerate(paths):
        path_length = calculate_path_length(path)
        corridor_data = evaluate_corridor(path, systems, corridor_radius=30.0)
        
        # Calculate efficiency score (value per LY)
        efficiency = corridor_data['total_value'] / path_length if path_length > 0 else 0
        
        results.append({
            'path_number': i + 1,
            'path': path,
            'num_systems': len(path),
            'path_length_ly': path_length,
            'corridor_valuable_systems': corridor_data['valuable_systems_count'],
            'elw_count': corridor_data['elw_count'],
            'ww_count': corridor_data['ww_count'],
            'aw_count': corridor_data['aw_count'],
            'bh_count': corridor_data['bh_count'],
            'ns_count': corridor_data['ns_count'],
            'total_value': corridor_data['total_value'],
            'efficiency': efficiency,
            'valuable_systems': corridor_data['valuable_systems']
        })
    
    # Sort by total value, then by efficiency
    results.sort(key=lambda x: (-x['total_value'], -x['efficiency']))
    
    return results

def find_waypoint_routes(
    waypoint_names: List[str],
    systems: List[System],
    beam_width: int = 150,
    paths_per_segment: int = 3
) -> dict:
    """
    Find routes through a series of waypoints using parallel bidirectional search.
    
    Args:
        waypoint_names: List of system names to route through in order
        systems: List of all available systems
        beam_width: Beam width for search (higher = better quality, slower)
        paths_per_segment: Number of alternative paths to find per segment
    
    Returns:
        Dictionary containing segment results and combined path options
    """
    
    if len(waypoint_names) < 2:
        raise ValueError("Need at least 2 waypoints")
    
    # Find all waypoint systems
    waypoints = []
    for name in waypoint_names:
        system = next((s for s in systems if s.name == name), None)
        if not system:
            raise ValueError(f"Waypoint '{name}' not found in dataset")
        waypoints.append(system)
    
    print(f"{'='*80}")
    print(f"MULTI-WAYPOINT ROUTING: {len(waypoints)} waypoints")
    print(f"{'='*80}\n")
    
    # Find paths for each segment
    segment_results = []
    
    for i in range(len(waypoints) - 1):
        start_wp = waypoints[i]
        end_wp = waypoints[i + 1]
        
        print(f"Segment {i+1}/{len(waypoints)-1}: {start_wp.name} -> {end_wp.name}")
        print(f"  Direct distance: {distance_3d(start_wp, end_wp):.2f} LY")
        
        # Find paths for this segment
        paths = bidirectional_beam_search(start_wp, end_wp, systems, beam_width)
        
        if not paths:
            print(f"  WARNING: No path found for segment {i+1}!")
            segment_results.append({
                'segment': i + 1,
                'start': start_wp.name,
                'end': end_wp.name,
                'paths': []
            })
            continue
        
        # Evaluate and sort paths
        evaluated_paths = []
        for path in paths[:paths_per_segment]:
            path_length = calculate_path_length(path)
            corridor_data = evaluate_corridor(path, systems, corridor_radius=30.0)
            efficiency = corridor_data['total_value'] / path_length if path_length > 0 else 0
            
            evaluated_paths.append({
                'path': path,
                'length': path_length,
                'num_systems': len(path),
                'total_value': corridor_data['total_value'],
                'efficiency': efficiency,
                'corridor_data': corridor_data
            })
        
        evaluated_paths.sort(key=lambda x: (-x['total_value'], -x['efficiency']))
        
        print(f"  Found {len(evaluated_paths)} paths")
        print(f"  Best path: {evaluated_paths[0]['length']:.2f} LY, "
              f"{evaluated_paths[0]['total_value']} value, "
              f"{evaluated_paths[0]['num_systems']} systems\n")
        
        segment_results.append({
            'segment': i + 1,
            'start': start_wp.name,
            'end': end_wp.name,
            'paths': evaluated_paths
        })
    
    # Generate combined route options
    combined_routes = generate_combined_routes(segment_results, systems)
    
    return {
        'waypoints': waypoint_names,
        'segments': segment_results,
        'combined_routes': combined_routes
    }

def generate_combined_routes(segment_results: List[dict], systems: List[System]) -> List[dict]:
    """
    Generate combined routes by selecting different path options for each segment.
    Creates multiple permutations to show different tradeoffs.
    """
    
    # Check if all segments have paths
    if any(len(seg['paths']) == 0 for seg in segment_results):
        return []
    
    # Strategy 1: Take best path from each segment (max value)
    best_value_route = combine_segments([seg['paths'][0] for seg in segment_results], systems)
    best_value_route['strategy'] = 'Maximum Value'
    
    # Strategy 2: Take shortest path from each segment (if we have alternatives)
    shortest_route = None
    if all(len(seg['paths']) > 1 for seg in segment_results):
        shortest_paths = [min(seg['paths'], key=lambda p: p['length']) for seg in segment_results]
        shortest_route = combine_segments(shortest_paths, systems)
        shortest_route['strategy'] = 'Shortest Path'
    
    # Strategy 3: Take most efficient path from each segment
    efficient_route = None
    if all(len(seg['paths']) > 1 for seg in segment_results):
        efficient_paths = [max(seg['paths'], key=lambda p: p['efficiency']) for seg in segment_results]
        efficient_route = combine_segments(efficient_paths, systems)
        efficient_route['strategy'] = 'Most Efficient'
    
    routes = [best_value_route]
    if shortest_route:
        routes.append(shortest_route)
    if efficient_route:
        routes.append(efficient_route)
    
    # Sort by total value
    routes.sort(key=lambda x: -x['total_value'])
    
    return routes

def combine_segments(segment_paths: List[dict], systems: List[System]) -> dict:
    """
    Combine segment paths into a single route and evaluate the full corridor.
    """
    
    # Combine all systems in path (avoiding duplicates at segment boundaries)
    full_path = []
    for i, seg_path in enumerate(segment_paths):
        if i == 0:
            full_path.extend(seg_path['path'])
        else:
            # Skip first system of this segment (duplicate of last system of previous)
            full_path.extend(seg_path['path'][1:])
    
    # Calculate combined metrics
    total_length = sum(seg['length'] for seg in segment_paths)
    
    # Evaluate full corridor
    corridor_data = evaluate_corridor(full_path, systems, corridor_radius=30.0)
    
    return {
        'full_path': full_path,
        'num_systems': len(full_path),
        'total_length': total_length,
        'num_segments': len(segment_paths),
        'corridor_valuable_systems': corridor_data['valuable_systems_count'],
        'elw_count': corridor_data['elw_count'],
        'ww_count': corridor_data['ww_count'],
        'aw_count': corridor_data['aw_count'],
        'bh_count': corridor_data['bh_count'],
        'ns_count': corridor_data['ns_count'],
        'total_value': corridor_data['total_value'],
        'efficiency': corridor_data['total_value'] / total_length if total_length > 0 else 0,
        'segment_paths': segment_paths
    }

def export_branch_opportunities(
    route_path: List[System],
    all_systems: List[System],
    output_file: str,
    branch_radius: float = 30.0
):
    """
    Export a TSV file of high-value systems near the main path that could be branches.
    
    Args:
        route_path: The main colonization path
        all_systems: All available systems
        output_file: Path to output TSV file
        branch_radius: Distance from path to search for branches
    """
    
    branch_opportunities = []
    
    # For each system in path, find nearby valuable systems
    for path_index, path_system in enumerate(route_path):
        for system in all_systems:
            # Skip if system is already in the path
            if system.name == path_system.name:
                continue
            
            # Check if within branch radius
            dist = distance_3d(system, path_system)
            if dist <= branch_radius and system.value_score > 0:
                branch_opportunities.append({
                    'branch_system': system.name,
                    'nearest_path_system': path_system.name,
                    'path_index': path_index,
                    'distance_from_path': dist,
                    'elw_count': system.elw_count,
                    'ww_count': system.ww_count,
                    'aw_count': system.aw_count,
                    'bh_count': system.bh_count,
                    'ns_count': system.ns_count,
                    'total_value': system.value_score,
                    'x': system.x,
                    'y': system.y,
                    'z': system.z
                })
    
    # Sort by total value (descending) then distance (ascending)
    branch_opportunities.sort(key=lambda x: (-x['total_value'], x['distance_from_path']))
    
    # Write to TSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        if branch_opportunities:
            fieldnames = [
                'branch_system', 'nearest_path_system', 'path_index', 
                'distance_from_path', 'elw_count', 'ww_count', 'aw_count',
                'bh_count', 'ns_count', 'total_value', 'x', 'y', 'z'
            ]
            writer = pd.DataFrame(branch_opportunities)
            writer.to_csv(f, sep='\t', index=False)
    
    print(f"\nExported {len(branch_opportunities)} branch opportunities to {output_file}")
    return branch_opportunities

def export_route_to_file(route: dict, output_file: str):
    """Export a route to a TSV file"""
    
    path_data = []
    for i, system in enumerate(route['full_path']):
        path_data.append({
            'sequence': i + 1,
            'system_name': system.name,
            'x': system.x,
            'y': system.y,
            'z': system.z,
            'elw_count': system.elw_count,
            'ww_count': system.ww_count,
            'aw_count': system.aw_count,
            'bh_count': system.bh_count,
            'ns_count': system.ns_count,
            'value_in_system': system.value_score
        })
    
    df = pd.DataFrame(path_data)
    df.to_csv(output_file, sep='\t', index=False)
    print(f"Exported route to {output_file}")

def print_waypoint_summary(waypoint_results: dict):
    """Print summary of waypoint routing results"""
    
    print(f"\n{'='*80}")
    print(f"WAYPOINT ROUTING SUMMARY")
    print(f"{'='*80}\n")
    
    print(f"Waypoints: {' -> '.join(waypoint_results['waypoints'])}\n")
    
    # Print segment summaries
    print(f"SEGMENT ANALYSIS:")
    print(f"{'-'*80}")
    for seg in waypoint_results['segments']:
        if not seg['paths']:
            print(f"Segment {seg['segment']}: {seg['start']} -> {seg['end']}")
            print(f"  ERROR: No path found!\n")
            continue
            
        best = seg['paths'][0]
        print(f"Segment {seg['segment']}: {seg['start']} -> {seg['end']}")
        print(f"  Best path: {best['length']:.2f} LY, {best['num_systems']} systems")
        print(f"  Corridor value: {best['total_value']} ({best['corridor_data']['valuable_systems_count']} valuable systems)")
        print(f"  Alternatives found: {len(seg['paths'])}\n")
    
    # Print combined route options
    if waypoint_results['combined_routes']:
        print(f"\nCOMBINED ROUTE OPTIONS:")
        print(f"{'-'*80}")
        
        for i, route in enumerate(waypoint_results['combined_routes'], 1):
            print(f"\nRoute Option {i}: {route['strategy']}")
            print(f"  Total systems: {route['num_systems']}")
            print(f"  Total distance: {route['total_length']:.2f} LY")
            print(f"  Valuable systems in corridor: {route['corridor_valuable_systems']}")
            print(f"  Earth-like Worlds: {route['elw_count']}")
            print(f"  Water Worlds: {route['ww_count']}")
            print(f"  Ammonia Worlds: {route['aw_count']}")
            print(f"  Black Holes: {route['bh_count']}")
            print(f"  Neutron Stars: {route['ns_count']}")
            print(f"  Total Value: {route['total_value']}")
            print(f"  Efficiency: {route['efficiency']:.4f} value/LY")
    else:
        print("\nNo complete routes found (one or more segments failed)")
    
    print(f"\n{'='*80}\n")

def print_route_summary(routes: List[dict], top_n: int = 3):
    """Print a summary of the top routes"""
    print(f"\n{'='*80}")
    print(f"TOP {min(top_n, len(routes))} COLONIZATION ROUTES")
    print(f"{'='*80}\n")
    
    for i, route in enumerate(routes[:top_n]):
        print(f"ROUTE #{route['path_number']}")
        print(f"  Systems in path: {route['num_systems']}")
        print(f"  Total length: {route['path_length_ly']:.2f} LY")
        print(f"  Valuable systems in 30 LY corridor: {route['corridor_valuable_systems']}")
        print(f"  Earth-like Worlds: {route['elw_count']}")
        print(f"  Water Worlds: {route['ww_count']}")
        print(f"  Ammonia Worlds: {route['aw_count']}")
        print(f"  Black Holes: {route['bh_count']}")
        print(f"  Neutron Stars: {route['ns_count']}")
        print(f"  Total Value Score: {route['total_value']}")
        print(f"  Efficiency (value/LY): {route['efficiency']:.4f}")
        print(f"\n  Path: {route['path'][0].name} ->")
        for sys in route['path'][1:-1]:
            marker = " *" if sys.value_score > 0 else ""
            print(f"        {sys.name}{marker}")
        print(f"        -> {route['path'][-1].name}")
        print()

# Example usage
if __name__ == "__main__":
    # Load your data
    systems = load_systems('results_test.tsv')
    
    print(f"Loaded {len(systems)} systems\n")
    
    # Option 1: Simple point-to-point routing
    # routes = find_colonization_routes(
    #     "Traikaae EV-M b34-15",
    #     "Bleia Dryiae TF-L d9-105",
    #     systems,
    #     beam_width=150
    # )
    # print_route_summary(routes, top_n=3)
    
    # Option 2: Multi-waypoint routing
    # waypoint_results = find_waypoint_routes(
    #     waypoint_names=[
    #         "Traikaae EV-M b34-15",
    #         "Intermediate System 1",
    #         "Intermediate System 2", 
    #         "Bleia Dryiae TF-L d9-105"
    #     ],
    #     systems=systems,
    #     beam_width=150,
    #     paths_per_segment=3  # Find 3 alternative paths per segment
    # )
    # print_waypoint_summary(waypoint_results)
