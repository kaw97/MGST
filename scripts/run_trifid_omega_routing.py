#!/usr/bin/env python3
"""
Run colonization routing for Trifid to Omega sector.
"""

import sys
sys.path.insert(0, '/mnt/z/MGST/scripts')

from efficient_population_router import (
    load_systems,
    find_colonization_routes,
    print_route_summary,
    export_route_to_file,
    export_branch_opportunities
)

def main():
    # Paths
    input_file = 'output/mint_search_202511115_trifid_omega_all/trifid_omega_fullresults.tsv'
    output_dir = 'output/mint_search_202511115_trifid_omega_all'

    # Waypoints
    start_system = "Trifid Sector MN-T c3-50"
    end_system = "Omega Sector EL-Y d165"

    print("="*80)
    print("TRIFID TO OMEGA COLONIZATION ROUTE FINDER")
    print("="*80)
    print()

    # Load systems
    print(f"Loading systems from: {input_file}")
    systems = load_systems(input_file)
    print(f"Loaded {len(systems)} systems\n")

    # Find routes
    print(f"Finding routes from {start_system} to {end_system}")
    print(f"Using beam width: 150")
    print(f"Max jump range: 15 LY")
    print(f"Corridor radius: 30 LY")
    print()

    routes = find_colonization_routes(
        start_system,
        end_system,
        systems,
        beam_width=150
    )

    if not routes:
        print("ERROR: No routes found!")
        return

    # Print summary
    print_route_summary(routes, top_n=3)

    # Export best route
    best_route = routes[0]
    route_file = f"{output_dir}/best_route.tsv"
    export_route_to_file(best_route, route_file)

    # Export branch opportunities
    branch_file = f"{output_dir}/branch_opportunities.tsv"
    export_branch_opportunities(
        best_route['path'],
        systems,
        branch_file,
        branch_radius=30.0
    )

    print("\n" + "="*80)
    print("ROUTING COMPLETE")
    print("="*80)
    print(f"\nFiles created:")
    print(f"  - {route_file}")
    print(f"  - {branch_file}")
    print()

if __name__ == "__main__":
    main()
