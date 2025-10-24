"""Enhanced Galaxy filtering CLI with mandatory search mode selection."""

import click
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from ..core.filtering import filter_galaxy_data, validate_config
from ..core.spatial import SpatialPrefilter
from ..core.search_modes import (
    SearchMode, SearchParameters, SectorResolver,
    parse_coordinates, validate_search_parameters
)
from ..configs.config_loader import config_loader


@click.command()
@click.option('--config', '-c',
              help='Configuration name (e.g., exobiology) or path to config file')
@click.option('--mode', '-m',
              type=click.Choice(['galaxy', 'sectors', 'corridor', 'pattern']),
              required=True,
              help='Search mode: galaxy (all sectors), sectors (specific sectors), corridor (between points), pattern (system pattern matching)')

# Database path
@click.option('--database', '-d',
              type=click.Path(exists=True, path_type=Path),
              required=True,
              help='Database directory containing sector JSONL files')

# Search mode specific options
@click.option('--sectors',
              help='Comma-separated list of sectors to search (for sectors mode)')
@click.option('--start',
              help='Start coordinates for corridor search (format: x,y,z)')
@click.option('--end',
              help='End coordinates for corridor search (format: x,y,z)')
@click.option('--radius',
              type=float,
              help='Corridor radius in light years')
@click.option('--sector-index',
              type=click.Path(exists=True, path_type=Path),
              help='Optional path to sector index file (default: database/sector_index.json)')
@click.option('--pattern-file',
              type=click.Path(exists=True, path_type=Path),
              help='JSON/JSONL file with system pattern for pattern mode')

# Output options
@click.option('--output', '-o',
              type=click.Path(path_type=Path),
              default=Path('filtered_systems.tsv'),
              help='Output file for filtered results')
@click.option('--format', 'output_format',
              type=click.Choice(['tsv', 'jsonl']),
              default='tsv',
              help='Output format (default: tsv)')

# Processing options
@click.option('--workers', '-w',
              type=int,
              default=8,
              help='Number of worker processes (default: 8)')
@click.option('--chunk-size',
              type=int,
              default=10485760,
              help='Chunk size for processing large files in bytes (default: 10MB)')

# Utility options
@click.option('--test',
              is_flag=True,
              help='Test mode: process only first 1000 systems per file')
@click.option('--validate-only',
              is_flag=True,
              help='Only validate configuration and search parameters')
@click.option('--verbose', '-v',
              is_flag=True,
              help='Enable verbose logging')
@click.option('--list-configs',
              is_flag=True,
              help='List all available built-in configurations')
@click.option('--dry-run',
              is_flag=True,
              help='Show which sector files would be searched without executing')

def filter_cmd(
    config: Optional[str],
    mode: str,
    database: Path,
    sectors: Optional[str],
    start: Optional[str],
    end: Optional[str],
    radius: Optional[float],
    sector_index: Optional[Path],
    pattern_file: Optional[Path],
    output: Path,
    output_format: str,
    workers: int,
    chunk_size: int,
    test: bool,
    validate_only: bool,
    verbose: bool,
    list_configs: bool,
    dry_run: bool
):
    """Filter galaxy data using enhanced search modes.

    MGST Filter with mandatory search mode selection.

    Search Modes:
    - galaxy: Search entire galaxy (all sectors)
    - sectors: Search specific sectors (--sectors "Col_285,Lagoon_Sector")
    - corridor: Search corridor between points (--start X,Y,Z --end X,Y,Z --radius R)
    - pattern: Pattern-based system search (--pattern-file file.json)

    Examples:

    # Search entire galaxy
    mgst filter --mode galaxy --config exobiology --database /path/to/sectors

    # Search specific sectors
    mgst filter --mode sectors --sectors "Col_285,Lagoon_Sector" --config exobiology --database /path/to/sectors

    # Search corridor from Sol to Colonia (with default sector index)
    mgst filter --mode corridor --start "0,0,0" --end "22000,-1000,49000" --radius 500 --config exobiology --database /path/to/sectors

    # Search corridor with custom sector index path
    mgst filter --mode corridor --start "0,0,0" --end "22000,-1000,49000" --radius 500 --sector-index /path/to/sector_index.json --config exobiology --database /path/to/sectors
    """

    # List configurations if requested
    if list_configs:
        click.echo("Built-in configurations:")
        for name, desc in config_loader.list_configs():
            click.echo(f"  {name}: {desc}")
        return

    # Validate required parameters
    # For pattern and corridor modes, allow using pattern-file instead of config
    if not config:
        if mode == 'pattern' and pattern_file:
            # Use pattern file as config
            pass  # Will load pattern file below
        elif mode == 'corridor' and pattern_file:
            # Use pattern file as config for corridor search
            pass  # Will load pattern file below
        else:
            raise click.ClickException("Configuration is required (use --config or --pattern-file)")

    # Setup logging
    import logging
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        # Parse search mode
        search_mode = SearchMode(mode)

        # Parse mode-specific parameters
        params = SearchParameters(
            mode=search_mode,
            database_path=database,
            sector_index_path=sector_index  # Optional: custom sector index path
        )

        if mode == 'sectors':
            if not sectors:
                raise click.ClickException("Sectors mode requires --sectors parameter")
            params.sectors = [s.strip() for s in sectors.split(',')]

        elif mode == 'corridor':
            if not all([start, end, radius]):
                raise click.ClickException("Corridor mode requires --start, --end, and --radius parameters")
            params.start_coords = parse_coordinates(start)
            params.end_coords = parse_coordinates(end)
            params.radius = radius

        elif mode == 'pattern':
            if not pattern_file:
                raise click.ClickException("Pattern mode requires --pattern-file parameter")
            params.pattern_file = pattern_file

        # Validate search parameters
        validate_search_parameters(params)

        # Resolve which sector files to search
        resolver = SectorResolver(database)
        sector_files = resolver.resolve_search_files(params)

        if not sector_files:
            click.echo("No sector files found for search criteria", err=True)
            return

        logger.info(f"Search mode: {mode}")
        logger.info(f"Sector files to search: {len(sector_files)}")

        # Show what would be searched in dry-run mode
        if dry_run:
            click.echo(f"DRY RUN: Would search {len(sector_files)} sector files:")
            for file_path in sector_files[:10]:  # Show first 10
                click.echo(f"  {file_path.name}")
            if len(sector_files) > 10:
                click.echo(f"  ... and {len(sector_files) - 10} more files")
            return

        # Load and validate configuration
        if config:
            # Load config from name or file path
            config_obj = config_loader.load_config(config)
        elif pattern_file:
            # Load JSON pattern as config
            from ..configs.json_pattern import JSONPatternConfig

            # Pass corridor parameters if in corridor mode
            corridor_params = None
            if mode == 'corridor' and all([params.start_coords, params.end_coords, params.radius]):
                corridor_params = {
                    'start_coords': (params.start_coords.x, params.start_coords.y, params.start_coords.z),
                    'end_coords': (params.end_coords.x, params.end_coords.y, params.end_coords.z),
                    'radius': params.radius
                }

            config_obj = JSONPatternConfig(pattern_file, corridor_params=corridor_params)

        if validate_only:
            config_name = config if config else f"pattern:{pattern_file.name}"
            click.echo(f"Configuration '{config_name}' is valid")
            click.echo(f"Search parameters for mode '{mode}' are valid")
            click.echo(f"Would search {len(sector_files)} sector files")
            return

        # Create temporary input directory with symlinks to target sector files
        # This allows us to use the existing filtering infrastructure
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create symlinks to target sector files
            for sector_file in sector_files:
                symlink_path = temp_path / sector_file.name
                try:
                    # Use absolute path for symlink target
                    os.symlink(sector_file.absolute(), symlink_path)
                except OSError:
                    # If symlinks not supported, copy files (slower but compatible)
                    import shutil
                    shutil.copy2(sector_file, symlink_path)

            logger.info(f"Created temporary search directory with {len(sector_files)} files")

            # Record search metadata
            search_metadata = {
                'search_mode': mode,
                'search_parameters': {
                    'sectors': params.sectors,
                    'start_coords': [params.start_coords.x, params.start_coords.y, params.start_coords.z] if params.start_coords else None,
                    'end_coords': [params.end_coords.x, params.end_coords.y, params.end_coords.z] if params.end_coords else None,
                    'radius': params.radius,
                    'pattern_file': str(params.pattern_file) if params.pattern_file else None
                },
                'sector_files_searched': len(sector_files),
                'sector_files': [f.name for f in sector_files],
                'timestamp': datetime.now().isoformat()
            }

            # Save search metadata
            output_dir = output.parent
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create log files for comprehensive run tracking
            stdin_path = output_dir / "stdin.txt"
            stdout_path = output_dir / "stdout.txt"
            stderr_path = output_dir / "stderr.txt"

            # Write command info to stdin.txt
            with open(stdin_path, 'w', encoding='utf-8') as f:
                f.write(f"# HITEC Galaxy JSON Pattern Search Run\n")
                f.write(f"# Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"# Command: {' '.join(sys.argv)}\n")
                f.write(f"# Working Directory: {os.getcwd()}\n")
                f.write(f"# Output Path: {output}\n")
                f.write(f"# Output Directory: {output_dir}\n\n")

                f.write(f"# Arguments:\n")
                f.write(f"# --mode: {mode}\n")
                f.write(f"# --database: {database}\n")
                if config:
                    f.write(f"# --config: {config}\n")
                if pattern_file:
                    f.write(f"# --pattern-file: {pattern_file}\n")
                f.write(f"# --output: {output}\n")
                f.write(f"# --workers: {workers}\n")
                f.write(f"# --chunk-size: {chunk_size}\n")
                f.write(f"# --format: {output_format}\n")
                f.write(f"# --test: {test}\n")
                f.write(f"# --verbose: {verbose}\n")

                # Mode-specific parameters
                if mode == 'sectors' and sectors:
                    f.write(f"# --sectors: {sectors}\n")
                elif mode == 'corridor' and all([start, end, radius]):
                    f.write(f"# --start: {start}\n")
                    f.write(f"# --end: {end}\n")
                    f.write(f"# --radius: {radius}\n")

                f.write("\n")

                # Write environment info
                f.write("# Relevant Environment Variables:\n")
                for key, value in os.environ.items():
                    if any(term in key.upper() for term in ['PYTHON', 'PATH', 'HOME', 'USER', 'VIRTUAL']):
                        f.write(f"# {key}={value}\n")
                f.write("\n")

            metadata_file = output_dir / f"{output.stem}_search_metadata.json"
            with open(metadata_file, 'w') as f:
                import json
                json.dump(search_metadata, f, indent=2)

            logger.info(f"Search metadata saved to: {metadata_file}")

            # TeeWriter class for capturing output to both console and log file
            class TeeWriter:
                def __init__(self, original_stream, log_file):
                    self.original_stream = original_stream
                    self.log_file = log_file

                def write(self, text):
                    self.original_stream.write(text)
                    self.log_file.write(text)
                    self.log_file.flush()

                def flush(self):
                    self.original_stream.flush()
                    self.log_file.flush()

            # Capture stdout and stderr to log files
            original_stdout = sys.stdout
            original_stderr = sys.stderr

            try:
                with open(stdout_path, 'w', encoding='utf-8') as stdout_file, \
                     open(stderr_path, 'w', encoding='utf-8') as stderr_file:

                    # Redirect streams through TeeWriter
                    sys.stdout = TeeWriter(original_stdout, stdout_file)
                    sys.stderr = TeeWriter(original_stderr, stderr_file)

                    # Execute the actual filtering using existing infrastructure
                    filter_galaxy_data(
                        config=config_obj,
                        input_dir=temp_path,
                        output_path=output,
                        workers=workers,
                        chunk_size=chunk_size,
                        output_format=output_format,
                        test_mode=test,
                        verbose=verbose
                    )
            finally:
                # Restore original stdout and stderr
                sys.stdout = original_stdout
                sys.stderr = original_stderr

        click.echo(f"\n📝 Logging to: {output_dir}")
        click.echo(f"   Command info: {stdin_path}")
        click.echo(f"   Output log: {stdout_path}")
        click.echo(f"   Error log: {stderr_path}")
        click.echo(f"\nFiltering completed. Results saved to: {output}")
        click.echo(f"Search metadata saved to: {metadata_file}")

    except ValueError as e:
        raise click.ClickException(str(e))
    except Exception as e:
        logger.error(f"Filtering failed: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        raise click.ClickException(f"Filtering failed: {e}")


def main():
    """Entry point for mgst filter command."""
    filter_cmd()


if __name__ == '__main__':
    main()