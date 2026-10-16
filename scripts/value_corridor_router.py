#!/usr/bin/env python3
"""
Two-phase colonization routing:
1. Use valuable systems to define target corridors
2. Build main path through common systems that brings valuable systems into range

Uses KDTree spatial indexing for efficient neighbor search with large datasets.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List, Tuple, Set, Optional
import heapq
from scipy.spatial import KDTree
import sys

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
        """Total valuable bodies in system (excluding Water Worlds)"""
        return self.elw_count + self.aw_count + self.bh_count + self.ns_count


def distance_3d(pos1: np.ndarray, pos2: np.ndarray) -> float:
    """Calculate 3D distance between two positions"""
    return np.linalg.norm(pos1 - pos2)


def load_systems(filepath: str) -> List[System]:
    """Load systems from TSV file"""
    print(f"Loading systems from: {filepath}")
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

    print(f"  Loaded {len(systems)} systems")
    return systems


class SpatialIndex:
    """Spatial index using KDTree for fast neighbor queries"""

    def __init__(self, systems: List[System]):
        self.systems = systems
        self.coords = np.array([s.coords for s in systems])
        self.kdtree = KDTree(self.coords)
        self.name_to_idx = {s.name: i for i, s in enumerate(systems)}

    def get_neighbors(self, position: np.ndarray, max_range: float) -> List[Tuple[System, float]]:
        """Get all systems within max_range of position, sorted by distance"""
        indices = self.kdtree.query_ball_point(position, max_range)

        neighbors = []
        for idx in indices:
            system = self.systems[idx]
            dist = distance_3d(position, system.coords)
            neighbors.append((system, dist))

        neighbors.sort(key=lambda x: x[1])
        return neighbors

    def get_nearest(self, position: np.ndarray, k: int = 1) -> List[Tuple[System, float]]:
        """Get k nearest systems to position"""
        distances, indices = self.kdtree.query(position, k=k)

        if k == 1:
            return [(self.systems[indices], distances)]

        return [(self.systems[idx], dist) for idx, dist in zip(indices, distances)]


class ValueCorridorAnalyzer:
    """Analyzes valuable system distribution to guide pathfinding"""

    def __init__(self, valuable_systems: List[System], corridor_width: float = 50.0):
        self.valuable_systems = valuable_systems
        self.corridor_width = corridor_width

        # Build spatial index for valuable systems
        self.valuable_coords = np.array([s.coords for s in valuable_systems])
        self.valuable_kdtree = KDTree(self.valuable_coords)

        print(f"\nValue Corridor Analyzer:")
        print(f"  Valuable systems: {len(valuable_systems)}")
        print(f"  Corridor width: {corridor_width} LY")

    def distance_to_nearest_valuable(self, position: np.ndarray) -> float:
        """Get distance to nearest valuable system"""
        dist, _ = self.valuable_kdtree.query(position)
        return dist

    def valuable_systems_in_range(self, position: np.ndarray, radius: float) -> List[System]:
        """Get all valuable systems within radius"""
        indices = self.valuable_kdtree.query_ball_point(position, radius)
        return [self.valuable_systems[idx] for idx in indices]

    def is_in_value_corridor(self, position: np.ndarray) -> bool:
        """Check if position is within corridor_width of any valuable system"""
        dist = self.distance_to_nearest_valuable(position)
        return dist <= self.corridor_width


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


def value_guided_pathfinding(
    start_system: System,
    end_system: System,
    spatial_index: SpatialIndex,
    value_analyzer: ValueCorridorAnalyzer,
    max_jump_range: float = 15.0,
    beam_width: int = 200,
    max_iterations: int = 20000
) -> List[System]:
    """
    Find path from start to end that stays near valuable systems.

    Heuristic balances:
    - Distance to goal (want short path)
    - Proximity to valuable systems (want to stay in value corridors)
    """

    open_set = []
    closed_set = set()
    g_scores = {}

    # Start node
    start_h = distance_3d(start_system.coords, end_system.coords)
    start_node = PathNode(
        system=start_system,
        g_cost=0,
        h_cost=start_h
    )
    heapq.heappush(open_set, start_node)
    g_scores[start_system.name] = 0

    iterations = 0
    best_node = start_node
    best_distance_to_goal = start_h

    print(f"\nPathfinding from {start_system.name} to {end_system.name}")
    print(f"  Direct distance: {distance_3d(start_system.coords, end_system.coords):.2f} LY")
    print(f"  Max jump range: {max_jump_range} LY")
    print(f"  Beam width: {beam_width}")

    while open_set and iterations < max_iterations:
        iterations += 1

        if iterations % 500 == 0:
            progress = 100 * (1 - best_distance_to_goal / start_h)
            print(f"  Iteration {iterations}: {len(open_set)} nodes in queue, "
                  f"progress: {progress:.1f}%, best distance to goal: {best_distance_to_goal:.2f} LY")

        # Beam search: only expand top beam_width nodes
        nodes_to_expand = []
        for _ in range(min(beam_width, len(open_set))):
            if not open_set:
                break
            nodes_to_expand.append(heapq.heappop(open_set))

        for current in nodes_to_expand:
            if current.system.name in closed_set:
                continue

            closed_set.add(current.system.name)

            # Check if we reached the goal
            dist_to_goal = distance_3d(current.system.coords, end_system.coords)
            if dist_to_goal < best_distance_to_goal:
                best_distance_to_goal = dist_to_goal
                best_node = current

            if dist_to_goal <= max_jump_range:
                # Close enough to goal - add final jump
                final_node = PathNode(
                    system=end_system,
                    g_cost=current.g_cost + dist_to_goal,
                    h_cost=0,
                    parent=current
                )
                path = reconstruct_path(final_node)
                print(f"\n  Path found! {len(path)} systems, {final_node.g_cost:.2f} LY total")
                return path

            # Get neighbors within jump range
            neighbors = spatial_index.get_neighbors(current.system.coords, max_jump_range)

            for neighbor_sys, dist in neighbors:
                if neighbor_sys.name in closed_set:
                    continue

                # Skip if this neighbor has valuable bodies (we want common systems for the path)
                if neighbor_sys.value_score > 0:
                    continue

                g_cost = current.g_cost + dist

                # Only consider if we found a better path
                if neighbor_sys.name in g_scores and g_cost >= g_scores[neighbor_sys.name]:
                    continue

                # Calculate heuristic
                h_cost = distance_3d(neighbor_sys.coords, end_system.coords)

                # Add penalty if far from valuable systems (encourage staying in value corridors)
                dist_to_valuable = value_analyzer.distance_to_nearest_valuable(neighbor_sys.coords)
                corridor_penalty = 0
                if dist_to_valuable > value_analyzer.corridor_width:
                    # Penalize being outside the corridor
                    corridor_penalty = (dist_to_valuable - value_analyzer.corridor_width) * 0.5

                h_cost += corridor_penalty

                neighbor_node = PathNode(
                    system=neighbor_sys,
                    g_cost=g_cost,
                    h_cost=h_cost,
                    parent=current
                )

                g_scores[neighbor_sys.name] = g_cost
                heapq.heappush(open_set, neighbor_node)

    # No path found to exact goal, return best partial path
    print(f"\n  Could not reach goal. Best distance: {best_distance_to_goal:.2f} LY")
    print(f"  Returning partial path to closest point")
    return reconstruct_path(best_node)


def evaluate_path(
    path: List[System],
    valuable_systems: List[System],
    corridor_radius: float = 30.0
) -> dict:
    """
    Evaluate path by counting valuable systems within corridor_radius.
    """
    # Build KDTree for valuable systems
    valuable_coords = np.array([s.coords for s in valuable_systems])
    valuable_kdtree = KDTree(valuable_coords)

    # For each valuable system, check if it's within corridor_radius of path
    systems_in_range = set()

    for valuable_sys in valuable_systems:
        for path_sys in path:
            dist = distance_3d(valuable_sys.coords, path_sys.coords)
            if dist <= corridor_radius:
                systems_in_range.add(valuable_sys.name)
                break

    # Get the actual systems
    valuable_in_range = [s for s in valuable_systems if s.name in systems_in_range]

    # Count bodies
    total_elw = sum(s.elw_count for s in valuable_in_range)
    total_ww = sum(s.ww_count for s in valuable_in_range)
    total_aw = sum(s.aw_count for s in valuable_in_range)
    total_bh = sum(s.bh_count for s in valuable_in_range)
    total_ns = sum(s.ns_count for s in valuable_in_range)

    # Calculate path length
    path_length = 0
    for i in range(len(path) - 1):
        path_length += distance_3d(path[i].coords, path[i+1].coords)

    return {
        'path_length': path_length,
        'num_systems': len(path),
        'valuable_systems_count': len(valuable_in_range),
        'valuable_systems': valuable_in_range,
        'elw_count': total_elw,
        'ww_count': total_ww,
        'aw_count': total_aw,
        'bh_count': total_bh,
        'ns_count': total_ns,
        'total_value': total_elw + total_aw + total_bh + total_ns  # Excludes WWs from value score
    }


def export_results(path: List[System], evaluation: dict,
                  output_route: str, output_branches: str):
    """Export path and branch opportunities"""

    # Export main route
    route_data = []
    for i, system in enumerate(path):
        # Calculate distance to next system
        if i < len(path) - 1:
            distance_to_next = distance_3d(system.coords, path[i + 1].coords)
        else:
            distance_to_next = 0.0  # Last system has no next

        route_data.append({
            'sequence': i + 1,
            'system_name': system.name,
            'id64': system.id64,
            'x': system.x,
            'y': system.y,
            'z': system.z,
            'distance_to_next': round(distance_to_next, 2)
        })

    df_route = pd.DataFrame(route_data)
    df_route.to_csv(output_route, sep='\t', index=False)
    print(f"\nExported route to: {output_route}")

    # Export branch opportunities (valuable systems in range)
    branch_data = []
    for valuable_sys in evaluation['valuable_systems']:
        # Find nearest path system
        min_dist = float('inf')
        nearest_path_sys = None
        nearest_idx = None

        for i, path_sys in enumerate(path):
            dist = distance_3d(valuable_sys.coords, path_sys.coords)
            if dist < min_dist:
                min_dist = dist
                nearest_path_sys = path_sys
                nearest_idx = i

        branch_data.append({
            'valuable_system': valuable_sys.name,
            'nearest_path_system': nearest_path_sys.name,
            'path_index': nearest_idx,
            'distance_ly': min_dist,
            'elw_count': valuable_sys.elw_count,
            'ww_count': valuable_sys.ww_count,
            'aw_count': valuable_sys.aw_count,
            'bh_count': valuable_sys.bh_count,
            'ns_count': valuable_sys.ns_count,
            'total_value': valuable_sys.value_score,
            'x': valuable_sys.x,
            'y': valuable_sys.y,
            'z': valuable_sys.z
        })

    # Sort by value then distance
    branch_data.sort(key=lambda x: (-x['total_value'], x['distance_ly']))

    df_branches = pd.DataFrame(branch_data)
    df_branches.to_csv(output_branches, sep='\t', index=False)
    print(f"Exported {len(branch_data)} branch opportunities to: {output_branches}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Two-phase value corridor routing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  %(prog)s \\
    --full-systems full_results.tsv \\
    --valuable-systems elw_results.tsv \\
    --start "System A" \\
    --end "System B" \\
    --output-route route.tsv \\
    --output-branches branches.tsv
        """
    )

    parser.add_argument('--full-systems', type=str, required=True,
                       help='TSV file with all systems (for building path)')
    parser.add_argument('--valuable-systems', type=str, required=True,
                       help='TSV file with valuable systems only (for defining corridors)')
    parser.add_argument('--start', type=str,
                       help='Start system name (use --start/--end OR --waypoints-file)')
    parser.add_argument('--end', type=str,
                       help='End system name (use --start/--end OR --waypoints-file)')
    parser.add_argument('--waypoints-file', type=str,
                       help='File with waypoint system names (one per line)')
    parser.add_argument('--output-route', type=str, required=True,
                       help='Output file for main route')
    parser.add_argument('--output-branches', type=str, required=True,
                       help='Output file for branch opportunities')
    parser.add_argument('--jump-range', type=float, default=15.0,
                       help='Maximum jump range (default: 15 LY)')
    parser.add_argument('--corridor-width', type=float, default=50.0,
                       help='Width of value corridor for pathfinding guidance (default: 50 LY)')
    parser.add_argument('--branch-radius', type=float, default=30.0,
                       help='Radius for branch opportunities (default: 30 LY)')
    parser.add_argument('--beam-width', type=int, default=200,
                       help='Beam width for search (default: 200)')

    args = parser.parse_args()

    # Validate arguments
    if args.waypoints_file:
        if args.start or args.end:
            parser.error("Cannot use both --waypoints-file and --start/--end")
    else:
        if not (args.start and args.end):
            parser.error("Must provide either --waypoints-file OR both --start and --end")

    print("="*80)
    print("VALUE CORRIDOR ROUTING")
    print("="*80)

    # Phase 1: Load systems
    print("\n--- PHASE 1: LOADING DATA ---")
    full_systems = load_systems(args.full_systems)
    valuable_systems = load_systems(args.valuable_systems)

    # Determine waypoints
    waypoint_names = []
    if args.waypoints_file:
        print(f"\nLoading waypoints from: {args.waypoints_file}")
        with open(args.waypoints_file, 'r') as f:
            waypoint_names = [line.strip() for line in f if line.strip()]
        print(f"  Loaded {len(waypoint_names)} waypoints")
    else:
        waypoint_names = [args.start, args.end]

    # Find waypoint systems in full dataset
    waypoint_systems = []
    for name in waypoint_names:
        sys = next((s for s in full_systems if s.name == name), None)
        if not sys:
            print(f"ERROR: Waypoint system '{name}' not found in full dataset")
            return
        waypoint_systems.append(sys)

    # Phase 2: Build spatial indices
    print("\n--- PHASE 2: BUILDING SPATIAL INDEX ---")
    print("  Building KDTree for fast neighbor queries...")
    spatial_index = SpatialIndex(full_systems)
    print(f"  Index built for {len(full_systems)} systems")

    # Phase 3: Analyze value corridors
    print("\n--- PHASE 3: ANALYZING VALUE CORRIDORS ---")
    value_analyzer = ValueCorridorAnalyzer(valuable_systems, args.corridor_width)

    # Phase 4: Find path (with waypoints if multiple)
    print("\n--- PHASE 4: PATHFINDING ---")
    if len(waypoint_systems) == 2:
        # Simple point-to-point
        path = value_guided_pathfinding(
            waypoint_systems[0],
            waypoint_systems[1],
            spatial_index,
            value_analyzer,
            max_jump_range=args.jump_range,
            beam_width=args.beam_width
        )
    else:
        # Multi-waypoint routing
        print(f"  Routing through {len(waypoint_systems)} waypoints")
        path = []
        total_distance = 0

        for i in range(len(waypoint_systems) - 1):
            print(f"\n  Segment {i+1}/{len(waypoint_systems)-1}: {waypoint_systems[i].name} -> {waypoint_systems[i+1].name}")
            segment_path = value_guided_pathfinding(
                waypoint_systems[i],
                waypoint_systems[i+1],
                spatial_index,
                value_analyzer,
                max_jump_range=args.jump_range,
                beam_width=args.beam_width
            )

            if not segment_path:
                print(f"ERROR: No path found for segment {i+1}")
                return

            # Calculate segment distance
            segment_distance = 0
            for j in range(len(segment_path) - 1):
                segment_distance += distance_3d(segment_path[j].coords, segment_path[j+1].coords)
            total_distance += segment_distance
            print(f"    Segment: {len(segment_path)} systems, {segment_distance:.2f} LY")

            # Add segment to path (skip first system if not first segment to avoid duplicates)
            if i == 0:
                path.extend(segment_path)
            else:
                path.extend(segment_path[1:])

        print(f"\n  Combined path: {len(path)} systems, {total_distance:.2f} LY total")

    if not path:
        print("ERROR: No path found")
        return

    # Phase 5: Evaluate path
    print("\n--- PHASE 5: EVALUATING PATH ---")
    evaluation = evaluate_path(path, valuable_systems, args.branch_radius)

    print(f"\nPATH EVALUATION:")
    print(f"  Systems in path: {evaluation['num_systems']}")
    print(f"  Path length: {evaluation['path_length']:.2f} LY")
    print(f"  Valuable systems in {args.branch_radius} LY: {evaluation['valuable_systems_count']}")
    print(f"  Earth-like Worlds: {evaluation['elw_count']}")
    print(f"  Water Worlds: {evaluation['ww_count']}")
    print(f"  Ammonia Worlds: {evaluation['aw_count']}")
    print(f"  Black Holes: {evaluation['bh_count']}")
    print(f"  Neutron Stars: {evaluation['ns_count']}")
    print(f"  Total Value: {evaluation['total_value']}")
    print(f"  Efficiency: {evaluation['total_value'] / evaluation['path_length']:.4f} value/LY")

    # Phase 6: Export results
    print("\n--- PHASE 6: EXPORTING RESULTS ---")
    export_results(path, evaluation, args.output_route, args.output_branches)

    print("\n" + "="*80)
    print("ROUTING COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
