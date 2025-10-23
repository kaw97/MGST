# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MGST (Mikunn Galactic Search Tool) is an Elite Dangerous galaxy analysis toolkit built as a modular Python package. It provides flexible JSON pattern-based filtering with multiple search modes (corridor, sectors, subsectors, galaxy) and database management tools. The project is designed around a simple, powerful pattern matching system that allows complex searches without modifying core code.

## Development Commands

### Environment Setup
```bash
# Install in development mode
pip install -e .[dev]

# Install with documentation dependencies
pip install -e .[docs]
```

### Testing
```bash
# Run all tests with coverage
pytest

# Run specific test file
pytest tests/test_core/test_filtering.py

# Run tests with verbose output
pytest -v

# Run tests matching pattern
pytest -k "test_pattern"
```

### Code Quality
```bash
# Format code with black
black src/ tests/

# Type checking with mypy
mypy src/

# Lint with flake8
flake8 src/

# Run pre-commit hooks
pre-commit run --all-files
```

### Documentation
```bash
# Build documentation
cd docs/
make html

# Serve documentation locally
cd docs/_build/html && python -m http.server
```

### Database Management
```bash
# Compress existing sector database (saves 83.6% space)
python scripts/compress_sector_database.py

# Build subsector index for efficient searches (required for subsector/corridor modes)
python scripts/build_lightweight_index.py \
  --source Databases/galaxy_sectors_compressed \
  --target Databases/galaxy_sectors_compressed \
  --workers 12 \
  --batch-size 500
```

**Database Compression Benefits:**
- **Space Savings**: 83.6% reduction (609.8GB → 99.9GB for full galaxy database)
- **Performance**: Maintains streaming speed with 64MB decompression buffers
- **Compatibility**: All existing commands work transparently with compressed databases
- **Dual Support**: System automatically detects and uses compressed files when available

**Subsector Index Benefits:**
- **Efficient Subsector Searches**: Maps subsectors to sector files for fast targeted searches
- **Low Memory Usage**: <1GB RAM regardless of database size (stores counts, not offsets)
- **Parallel Search**: Enables multi-threaded subsector and corridor searches
- **Incremental Updates**: Rebuild index quickly after database updates (~10-15 minutes)

## Package Architecture

### Core Components

The package follows a layered architecture with clear separation of concerns:

**`src/mgst/core/`** - Core processing engines
- `filtering.py` - High-performance parallel galaxy data filtering with JSONL streaming
- `spatial.py` - Spatial prefiltering and corridor search optimization
- `search_modes.py` - Search mode implementations (corridor, sectors, subsectors, galaxy)

**`src/mgst/configs/`** - Pattern matching system
- `json_pattern.py` - JSON pattern matching engine
- `pattern_validator.py` - Pattern validation and error handling
- Pattern files in `patterns/` directory define search criteria

**`src/mgst/data/`** - Data processing utilities
- `loaders.py` - Data loading utilities with validation and batch processing
- `validators.py` - Comprehensive data validation for system and body data
- `compressed_reader.py` - Transparent gzip compression support with streaming decompression
- `indexed_reader.py` - Indexed database reader for efficient subsector-based searches

**`src/mgst/cli/`** - Command-line interfaces
- `main.py` - Main entry point with subcommands (filter, db)
- `filter.py` - Filter command with all search modes
- `database.py` - Database construction and management commands

### Key Architectural Patterns

**JSON Pattern Matching**: The `JSONPatternConfig` class enables adding new search criteria by creating JSON files that define:
1. System name patterns (with wildcards)
2. Body criteria (type, atmosphere, temperature, gravity, etc.)
3. Parent-child relationships (moons, rings)
4. Logical combinations (AND/OR)

**Multiple Search Modes**: The filtering system supports:
- **Galaxy**: Search entire galaxy
- **Sectors**: Search specific named sectors
- **Subsectors**: Search specific subsectors
- **Corridor**: Search cylindrical corridor between two coordinates
- **Pattern**: Generic pattern-based search

**Parallel Processing**: The filtering system uses `ProcessPoolExecutor` with memory-efficient JSONL streaming to handle millions of systems across multiple worker processes.

**Memory Management**: Large datasets are processed using:
- JSONL streaming with configurable chunk sizes
- Garbage collection between processing chunks
- Sector-level file organization

**Compressed Database Support**: The system supports transparent gzip compression for massive space savings:
- Automatic detection of compressed (.jsonl.gz) and uncompressed (.jsonl) files
- Streaming decompression maintaining memory efficiency with 50GB+ files
- 83.6% space reduction (609.8GB → 99.9GB) with production galaxy databases
- Compatible with all existing processing: filtering, spatial prefiltering
- Use compressed databases with: `mgst filter --database Databases/galaxy_sectors_compressed ...`

**Indexed Database Architecture**: For efficient subsector-based searching:
- **Database Structure**: Sector-level JSONL.gz files (one per sector, ~12,000 files total)
- **Lightweight Index**: JSON file mapping subsectors to sector files with system counts
- **Memory Efficient**: Index uses <1GB RAM regardless of database size
- **Parallel Search**: `IndexedDatabaseReader` enables multi-threaded subsector searches
- **Search Strategy**: When searching a subsector, scan only its sector file and filter by subsector code
- **Performance**: With 12 workers processing different sectors in parallel, searches remain very fast
- **Easy Updates**: Update individual sector files, then rebuild index (10-15 minutes)

## JSON Pattern System

### Creating New Search Patterns

Patterns are JSON files that define search criteria:

```json
{
  "description": "Supply Hub Candidates - ELW or Water World with rocky moon",
  "name": "*",
  "bodies": [
    {
      "comment": "Match ELW or Water World (the parent planet)",
      "subType": ["Earth-like world", "Water world"]
    },
    {
      "comment": "Match rocky moon orbiting the ELW/Water World",
      "subType": "Rocky body",
      "parents": [{"Planet": "*"}]
    }
  ]
}
```

### Usage Patterns

**Pattern files** are used with the filter command:
```bash
mgst filter --mode corridor \
  --start "-468,-92,4474" \
  --end "-575,-37,5142" \
  --radius 500 \
  --pattern-file patterns/supply_hub_1.json \
  --database Databases/galaxy_sectors_compressed \
  --output output/supply_hubs_001/results.jsonl
```

### Output Organization

**IMPORTANT**: All analysis runs should be organized into unique subdirectories within the `output/` directory to prevent file conflicts and maintain clean organization. Each run should use a descriptive subdirectory name that includes:

- **Run identifier**: Sequential number, timestamp, or descriptive name
- **Search type**: Corridor, sector, or subsector search
- **Purpose**: Brief description of the search criteria

**Recommended naming patterns**:
```bash
# Sequential runs with descriptive names
output/corridor_lagoon_trifid_001/
output/sector_search_test_002/
output/supply_hub_candidates_20251023/

# Timestamp-based runs
output/corridor_search_$(date +%Y%m%d_%H%M%S)/

# Purpose-based
output/supply_hub_validation/
output/exploration_route_planning/
```

**Example commands with proper output organization**:
```bash
# Corridor search with organized output
mgst filter --mode corridor \
  --start "0,0,0" --end "1000,0,0" --radius 500 \
  --pattern-file patterns/interesting_systems.json \
  --database Databases/galaxy_sectors_compressed \
  --output output/corridor_test_$(date +%Y%m%d)/results.jsonl \
  --workers 12

# Sector search
mgst filter --mode sectors \
  --sectors "Lagoon_Sector,Trifid_Sector" \
  --pattern-file patterns/supply_hub_1.json \
  --database Databases/galaxy_sectors_compressed \
  --output output/lagoon_trifid_supply_$(date +%Y%m%d)/results.tsv
```

### Automatic Run Logging

**IMPORTANT**: The filtering system automatically creates comprehensive log files in each output subdirectory:

- **`stdin.txt`**: Complete command information including:
  - Timestamp and full command line
  - All arguments and parameters used
  - Working directory and output paths
  - Relevant environment variables
  - Python path and virtual environment info

- **`stdout.txt`**: Complete output log capturing:
  - All console output during processing
  - Configuration details and validation
  - Processing statistics and progress
  - Error messages and warnings
  - Final results summary

- **`stderr.txt`**: Progress tracking:
  - Progress bar updates
  - Worker status information

- **`*_search_metadata.json`**: Search parameters:
  - Search mode and coordinates
  - Pattern file used
  - Database files searched
  - Timestamp and configuration

These log files enable full reproducibility and debugging of any analysis run. Each output subdirectory becomes a complete record of the analysis performed.

## Data Flow Architecture

1. **Input Processing**: JSONL.gz files are read with streaming decompression across multiple worker processes
2. **Pattern Matching**: Each system passes through the JSON pattern matcher
3. **Output Generation**: Qualifying systems are written to TSV/JSONL with automatic logging
4. **Spatial Optimization**: Corridor/subsector searches use spatial indexing for efficiency

## Testing Strategy

The test suite uses pytest with fixtures for sample data. Key testing patterns:

- **Pattern testing** with sample system data
- **Integration testing** for the complete filtering pipeline
- **Search mode validation** ensuring correct sector/corridor filtering
- **Data validation testing** for all supported file formats

## CLI Command Structure

The CLI follows a simple two-command pattern:
- Main entry point (`mgst`) with subcommands: `filter` and `db`
- Consistent error handling with user-friendly messages
- Progress bars and verbose logging options
- Dry-run modes for testing searches

The modular design allows adding new search modes by extending `search_modes.py` and new pattern features by updating `json_pattern.py`.

## Common Workflows

### Corridor Search Between Two Nebulas

```bash
mgst filter --mode corridor \
  --start "-468,-92,4474" \
  --end "-575,-37,5142" \
  --radius 500 \
  --pattern-file patterns/interesting_systems.json \
  --database Databases/galaxy_sectors_compressed \
  --output output/corridor_lagoon_trifid_$(date +%Y%m%d)/results.jsonl \
  --workers 12
```

### Sector-Specific Search

```bash
mgst filter --mode sectors \
  --sectors "Lagoon_Sector,Trifid_Sector,Omega_Sector" \
  --pattern-file patterns/supply_hub_1.json \
  --database Databases/galaxy_sectors_compressed \
  --output output/multi_sector_$(date +%Y%m%d)/results.tsv \
  --workers 8
```

### Dry Run (Preview Search)

```bash
mgst filter --mode corridor \
  --start "0,0,0" --end "1000,0,0" --radius 100 \
  --pattern-file patterns/all_systems.json \
  --database Databases/galaxy_sectors_compressed \
  --dry-run
```

This will show which sector files would be searched without actually running the search.