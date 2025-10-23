"""CLI commands for galaxy database management."""

import click
import logging
from pathlib import Path
from typing import Optional

from ..database import GalaxyDatabaseBuilder, GalaxyDatabaseUpdater

logger = logging.getLogger(__name__)


@click.group()
def db():
    """Galaxy database management commands."""
    pass


@db.command()
@click.option('--output-dir', '-o', type=click.Path(path_type=Path),
              default='Databases', help='Base output directory for database')
@click.option('--dataset-type', '-t', type=click.Choice(['full', '1month', '7days', '1day']),
              default='full', help='Type of dataset to download and build from')
@click.option('--workers', '-w', type=int, default=None,
              help='Number of worker processes (default: CPU count)')
@click.option('--batch-size', '-b', type=int, default=50000,
              help='Systems per batch for processing')
@click.option('--force-download', '-f', is_flag=True,
              help='Force redownload even if file exists')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
def build(output_dir: Path, dataset_type: str, workers: Optional[int], 
          batch_size: int, force_download: bool, verbose: bool):
    """Build galaxy database from scratch using Spansh data.
    
    Downloads the specified dataset and creates a compressed, organized
    sector database optimized for filtering and analysis.
    
    Examples:
    
    \b
    # Build full galaxy database with default settings
    hitec-galaxy db build
    
    \b
    # Build from 1-month dataset with 8 workers
    hitec-galaxy db build --dataset-type 1month --workers 8
    
    \b
    # Build with custom output directory and verbose logging
    hitec-galaxy db build --output-dir /data/galaxy --verbose
    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
        
    click.echo(f"Building galaxy database from {dataset_type} dataset...")
    click.echo(f"Output directory: {output_dir}")
    click.echo(f"Workers: {workers or 'auto'}")
    click.echo(f"Batch size: {batch_size:,}")
    
    try:
        builder = GalaxyDatabaseBuilder(output_dir, workers, batch_size)
        
        with click.progressbar(length=100, label='Building database') as bar:
            # Note: Real progress tracking would require callback mechanism
            stats = builder.build_from_scratch(dataset_type, force_download)
            bar.update(100)
        
        click.echo("\n" + "="*60)
        click.echo("DATABASE BUILD COMPLETED")
        click.echo("="*60)
        click.echo(f"Systems processed: {stats.total_systems:,}")
        click.echo(f"Stations processed: {stats.total_stations:,}")
        click.echo(f"Sectors created: {stats.sectors_created:,}")
        click.echo(f"Build time: {stats.build_time_seconds:.1f} seconds")
        click.echo(f"Original size: {stats.original_size_mb:.1f} MB")
        click.echo(f"Compressed size: {stats.compressed_size_mb:.1f} MB")
        click.echo(f"Compression ratio: {stats.compression_ratio:.1f}% savings")
        click.echo(f"Database location: {output_dir / 'galaxy_sectors_compressed'}")
        
    except Exception as e:
        click.echo(f"Error building database: {e}", err=True)
        raise click.ClickException(str(e))


@db.command()
@click.option('--database-dir', '-d', type=click.Path(path_type=Path),
              default='Databases', help='Database directory')
@click.option('--dataset-type', '-t', type=click.Choice(['1day', '7days', '1month']),
              default='1day', help='Type of incremental dataset to download')
@click.option('--workers', '-w', type=int, default=None,
              help='Number of worker processes (default: CPU count)')
@click.option('--batch-size', '-b', type=int, default=10000,
              help='Systems per batch for processing')
@click.option('--force-download', '-f', is_flag=True,
              help='Force redownload even if file exists')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
def update(database_dir: Path, dataset_type: str, workers: Optional[int],
           batch_size: int, force_download: bool, verbose: bool):
    """Update galaxy database with latest changes from Spansh.
    
    Downloads incremental changes and updates the database while tracking
    all faction, powerplay, and economic changes over time.
    
    Examples:
    
    \b
    # Daily update with default settings
    hitec-galaxy db update
    
    \b
    # Weekly update with 4 workers
    hitec-galaxy db update --dataset-type 7days --workers 4
    
    \b
    # Monthly update with verbose logging
    hitec-galaxy db update --dataset-type 1month --verbose
    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
        
    click.echo(f"Updating galaxy database with {dataset_type} changes...")
    click.echo(f"Database directory: {database_dir}")
    click.echo(f"Workers: {workers or 'auto'}")
    click.echo(f"Batch size: {batch_size:,}")
    
    try:
        updater = GalaxyDatabaseUpdater(database_dir, workers, batch_size)
        
        with click.progressbar(length=100, label='Updating database') as bar:
            # Note: Real progress tracking would require callback mechanism
            stats = updater.update_from_spansh(dataset_type, force_download)
            bar.update(100)
        
        click.echo("\n" + "="*60)
        click.echo("DATABASE UPDATE COMPLETED")
        click.echo("="*60)
        click.echo(f"Systems processed: {stats.systems_processed:,}")
        click.echo(f"Systems changed: {stats.systems_changed:,}")
        click.echo(f"Systems discovered: {stats.systems_discovered:,}")
        click.echo(f"Stations changed: {stats.stations_changed:,}")
        click.echo(f"Stations discovered: {stats.stations_discovered:,}")
        click.echo(f"Change records written: {stats.change_records_written:,}")
        click.echo(f"Update time: {stats.update_time_seconds:.1f} seconds")
        click.echo(f"Time-series data: {database_dir / 'galaxy_timeseries'}")
        
    except Exception as e:
        click.echo(f"Error updating database: {e}", err=True)
        raise click.ClickException(str(e))


@db.command()
@click.option('--database-dir', '-d', type=click.Path(path_type=Path),
              default='Databases', help='Database directory to verify')
@click.option('--check-integrity', '-i', is_flag=True,
              help='Perform detailed integrity checks')
@click.option('--repair-indices', '-r', is_flag=True,
              help='Attempt to repair corrupted indices')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose output')
def verify(database_dir: Path, check_integrity: bool, repair_indices: bool, verbose: bool):
    """Verify galaxy database integrity and provide statistics.
    
    Checks database files for corruption, validates structure, and provides
    detailed statistics about the database contents.
    
    Examples:
    
    \b
    # Basic verification
    hitec-galaxy db verify
    
    \b
    # Full integrity check with repair
    hitec-galaxy db verify --check-integrity --repair-indices --verbose
    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
        
    click.echo(f"Verifying database: {database_dir}")
    
    try:
        builder = GalaxyDatabaseBuilder(database_dir)
        results = builder.verify_database()
        
        click.echo("\n" + "="*60)
        click.echo("DATABASE VERIFICATION RESULTS")
        click.echo("="*60)
        
        if results['valid']:
            click.echo("✅ Database is VALID")
        else:
            click.echo("❌ Database has ERRORS")
            
        # Statistics
        stats = results.get('statistics', {})
        if stats:
            click.echo(f"\nStatistics:")
            click.echo(f"  Total sector files: {stats.get('total_sector_files', 0):,}")
            click.echo(f"  Sampled files: {stats.get('sampled_files', 0):,}")
            click.echo(f"  Corrupt files: {stats.get('corrupt_files', 0):,}")
            click.echo(f"  Sample systems: {stats.get('sample_systems', 0):,}")
            click.echo(f"  Sample stations: {stats.get('sample_stations', 0):,}")
            
        # Errors
        errors = results.get('errors', [])
        if errors:
            click.echo(f"\nErrors found ({len(errors)}):")
            for error in errors[:10]:  # Show first 10 errors
                click.echo(f"  • {error}")
            if len(errors) > 10:
                click.echo(f"  ... and {len(errors) - 10} more errors")
                
        if not results['valid']:
            raise click.ClickException("Database verification failed")
            
    except Exception as e:
        click.echo(f"Error verifying database: {e}", err=True)
        raise click.ClickException(str(e))


@db.command()
@click.option('--database-dir', '-d', type=click.Path(path_type=Path),
              default='Databases', help='Database directory')
@click.option('--older-than', type=str, default='6months',
              help='Archive snapshots older than specified time (e.g., 6months, 1year)')
@click.option('--dry-run', is_flag=True,
              help='Show what would be archived without actually doing it')
def archive(database_dir: Path, older_than: str, dry_run: bool):
    """Archive old database snapshots to save space.
    
    Moves old time-series data and snapshots to compressed archives
    to reduce storage usage while preserving data.
    
    Examples:
    
    \b
    # Archive data older than 6 months (dry run)
    hitec-galaxy db archive --dry-run
    
    \b
    # Archive data older than 1 year
    hitec-galaxy db archive --older-than 1year
    """
    click.echo(f"Database archival not yet implemented")
    click.echo(f"Would archive data older than: {older_than}")
    if dry_run:
        click.echo("This is a dry run - no files would be modified")


@db.command()
@click.option('--database-dir', '-d', type=click.Path(path_type=Path),
              default='Databases', help='Database directory')
def info(database_dir: Path):
    """Show information about the galaxy database.
    
    Displays database statistics, recent updates, and storage usage.
    
    Examples:
    
    \b
    # Show database information
    hitec-galaxy db info
    """
    import json
    from datetime import datetime
    
    click.echo(f"Galaxy Database Information")
    click.echo("="*60)
    
    # Check if database exists
    galaxy_sectors_dir = database_dir / "galaxy_sectors_compressed"
    timeseries_dir = database_dir / "galaxy_timeseries"
    
    if not galaxy_sectors_dir.exists():
        click.echo("❌ No database found. Run 'hitec-galaxy db build' first.")
        return
        
    # Build metadata
    metadata_file = database_dir / "build_metadata.json"
    if metadata_file.exists():
        try:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
                
            build_time = datetime.fromtimestamp(metadata['build_time'])
            click.echo(f"Built: {build_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            click.echo(f"Dataset: {metadata.get('dataset_type', 'unknown')}")
            click.echo(f"Systems: {metadata.get('total_systems', 0):,}")
            click.echo(f"Stations: {metadata.get('total_stations', 0):,}")
            click.echo(f"Sectors: {metadata.get('sectors_created', 0):,}")
            click.echo(f"Size: {metadata.get('compressed_size_mb', 0):.1f} MB")
            click.echo(f"Compression: {metadata.get('compression_ratio_percent', 0):.1f}% savings")
            
        except Exception as e:
            click.echo(f"Could not read build metadata: {e}")
    else:
        # Basic file count
        sector_files = list(galaxy_sectors_dir.glob("*.jsonl.gz"))
        total_size = sum(f.stat().st_size for f in sector_files) / (1024**2)
        click.echo(f"Sector files: {len(sector_files):,}")
        click.echo(f"Total size: {total_size:.1f} MB")
    
    # Update log
    update_log_file = database_dir / "update_log.jsonl" 
    if update_log_file.exists():
        try:
            updates = []
            with open(update_log_file, 'r') as f:
                for line in f:
                    if line.strip():
                        updates.append(json.loads(line))
                        
            if updates:
                click.echo(f"\nRecent Updates ({len(updates)}):")
                for update in updates[-5:]:  # Show last 5 updates
                    update_time = datetime.fromisoformat(update['update_time'].rstrip('Z'))
                    click.echo(f"  {update_time.strftime('%Y-%m-%d %H:%M')} - "
                             f"{update['dataset_type']}: "
                             f"{update['systems_changed']:,} systems changed")
                             
        except Exception as e:
            click.echo(f"Could not read update log: {e}")
    
    # Time-series data
    if timeseries_dir.exists():
        system_months = list((timeseries_dir / "systems").glob("*/")) if (timeseries_dir / "systems").exists() else []
        station_months = list((timeseries_dir / "stations").glob("*/")) if (timeseries_dir / "stations").exists() else []
        
        if system_months or station_months:
            click.echo(f"\nTime-Series Data:")
            click.echo(f"  System change months: {len(system_months)}")
            click.echo(f"  Station change months: {len(station_months)}")
        
    click.echo(f"\nLocations:")
    click.echo(f"  Current data: {galaxy_sectors_dir}")
    click.echo(f"  Time-series: {timeseries_dir}")
    click.echo(f"  Downloads: {database_dir / 'downloads'}")


# Register commands with main CLI
def register_database_commands(main_cli):
    """Register database commands with the main CLI."""
    main_cli.add_command(db)