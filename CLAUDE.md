# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MGST (Mikunn Galactic Search Tool) is an Elite Dangerous galaxy analysis toolkit built as a modular Python package. It provides flexible JSON pattern-based filtering with multiple search modes (corridor, sectors, galaxy) and database management tools. The project is designed around a simple, powerful pattern matching system that allows complex searches without modifying core code.

## Development Commands

### Environment Setup
```bash
# Create and activate base environment (automatically installs MGST)
micromamba create -f environment.yml
micromamba activate mgst

# Or create development environment (includes testing and linting tools)
micromamba create -f environment-dev.yml
micromamba activate mgst-dev
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

### Database Management
```bash
# Compress existing sector database (saves 83.6% space)
python scripts/compress_sector_database.py

# Build sector index for efficient corridor searches
python scripts/build_sector_index.py \
  --source Databases/galaxy_sectors_compressed \
  --target Databases/galaxy_sectors_compressed \
  --workers 12
```

**Database Compression Benefits:**
- **Space Savings**: 83.6% reduction (609.8GB → 99.9GB for full galaxy database)
- **Performance**: Maintains streaming speed with 64MB decompression buffers
- **Compatibility**: All existing commands work transparently with compressed databases
- **Dual Support**: System automatically detects and uses compressed files when available

**Sector Index Benefits:**
- **Efficient Corridor Searches**: Maps sector center coordinates for fast spatial prefiltering
- **Low Memory Usage**: <1GB RAM regardless of database size
- **Parallel Search**: Enables multi-threaded sector-based corridor searches
- **Incremental Updates**: Rebuild index quickly after database updates

## Package Architecture

### Core Components

The package follows a layered architecture with clear separation of concerns:

**`src/mgst/core/`** - Core processing engines
- `filtering.py` - High-performance parallel galaxy data filtering with JSONL streaming
- `spatial.py` - Spatial prefiltering and corridor search optimization
- `search_modes.py` - Search mode implementations (corridor, sectors, galaxy)

**`src/mgst/configs/`** - Pattern matching system
- `json_pattern.py` - JSON pattern matching engine
- `pattern_validator.py` - Pattern validation and error handling
- Pattern files in `patterns/` directory define search criteria

**`src/mgst/data/`** - Data processing utilities
- `loaders.py` - Data loading utilities with validation and batch processing
- `validators.py` - Comprehensive data validation for system and body data
- `compressed_reader.py` - Transparent gzip compression support with streaming decompression
- `indexed_reader.py` - Indexed database reader for efficient sector-based searches

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

**Indexed Database Architecture**: For efficient sector-based searching:
- **Database Structure**: Sector-level JSONL.gz files (one per sector, ~12,000 files total)
- **Lightweight Index**: JSON file with sector center coordinates for spatial prefiltering
- **Memory Efficient**: Index uses <1GB RAM regardless of database size
- **Parallel Search**: `SectorResolver` enables multi-threaded sector-based searches
- **Search Strategy**: For corridor searches, use sector index to identify nearby sectors, then scan only those sector files
- **Performance**: With 12 workers processing different sectors in parallel, searches remain very fast
- **Easy Updates**: Update individual sector files, then rebuild index quickly

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
- **Search type**: Corridor, sector, or galaxy search
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
4. **Spatial Optimization**: Corridor searches use sector-level spatial indexing for efficiency

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

## Exobiology Species Prediction Search

The primary bio search tool is `scripts/find_high_value_systems.py`. It uses species-level rules from `src/mgst/bio/species_rules.py` to predict which bodies host high-value (>=10M credit) exobiology species based on environmental parameters (atmosphere, gravity, temperature, pressure, volcanism, star class, galactic region).

**IMPORTANT**: This script operates on the **Parquet database** at `/data/Databases_v2/galaxy_sectors_parquet`, not the compressed JSONL database. It uses `--database`, not `--db`.

**IMPORTANT**: By default it applies a date cutoff (`--max-system-date 2021-04-01`) to exclude systems already visited after Odyssey launched, since those have real bio data. To search ALL systems regardless of visit date, pass `--max-system-date none`.

### Sphere Search (Near a Location)

Use corridor mode with the same start and end coordinates to search a sphere:

```bash
# Search within 500 ly of Hen 2-333
python -I scripts/find_high_value_systems.py \
  --mode search \
  --database /data/Databases_v2/galaxy_sectors_parquet \
  --output output/hen2_333_bio_$(date +%Y%m%d)/results.tsv \
  --corridor-start "-840.66,-561.16,13361.81" \
  --corridor-end "-840.66,-561.16,13361.81" \
  --corridor-radius 500 \
  --workers 8 \
  --min-bodies 1
```

### Corridor Search (Between Two Points)

```bash
# Search corridor from Sol to Colonia
python -I scripts/find_high_value_systems.py \
  --mode search \
  --database /data/Databases_v2/galaxy_sectors_parquet \
  --output output/sol_colonia_bio_$(date +%Y%m%d)/results.tsv \
  --corridor-start "0,0,0" \
  --corridor-end "-9530.5,-910.28125,19808.125" \
  --corridor-radius 500 \
  --workers 8 \
  --min-bodies 1
```

### Sector Search

```bash
# Search specific sectors by name prefix
python -I scripts/find_high_value_systems.py \
  --mode search \
  --database /data/Databases_v2/galaxy_sectors_parquet \
  --output output/lagoon_bio_$(date +%Y%m%d)/results.tsv \
  --sectors "Lagoon_Sector,Trifid_Sector" \
  --workers 8 \
  --min-bodies 1
```

### Galaxy-Wide Search

```bash
# Search entire galaxy (takes ~5 min with 8 workers)
python -I scripts/find_high_value_systems.py \
  --mode search \
  --database /data/Databases_v2/galaxy_sectors_parquet \
  --output output/galaxy_bio_$(date +%Y%m%d)/results.tsv \
  --workers 12 \
  --min-bodies 2
```

### Key Parameters

| Parameter | Default | Description |
|---|---|---|
| `--min-bodies` | 2 | Minimum qualifying bodies per system. Use 1 for all predictions, 2+ for multi-body systems |
| `--max-system-date` | `2021-04-01` | Exclude systems visited after this date. Use `none` to include all |
| `--workers` | 4 | Parallel workers. 8-12 recommended for large searches |
| `--mode` | `search` | `search` finds systems, `validate` checks prediction accuracy |

### Species Rules Architecture

Rules are defined in `src/mgst/bio/species_rules.py`:
- 18 high-value species (>=10M credits) with environmental rulesets
- Each species has one or more rulesets defining atmosphere, body type, gravity, temperature, pressure, volcanism, and star class requirements
- Some species have galactic region constraints (e.g., Tubus Cavas → Scutum-Centaurus, Stratum Cucumisis → Sagittarius-Carina)
- Star-class exclusions come in two kinds: `exclude_star_classes` checks the system's main star (`star_class`); `exclude_own_star_classes` checks the star the body orbits (`parent_star_class`, found by walking the body's `parent_N_*` columns with `parent_star_class()`). Own-star exclusions follow SrvSurvey's genus colour tables: a species only appears around star classes that have a colour for it
- Clypeus Speculumi requires >= 2,500 ls from the arrival star (`distanceToArrival` constraint), not a distance from Sol
- The `score_body()` function takes a flat dict of body properties and returns matching species. Callers must set `star_class`, `parent_star_class`, `star_age` (main star's age, millions of years) and `system_x/y/z` on the row
- Evaluate rules against landmark ground truth with `experiments/bio_rule_optimization/ruleset_breakdown.py` (per-species and per-ruleset TP/FN/FP/TN and FNR/FDR/FPR) on the extract built by `extract_landmark_bodies.py`. Count a prediction as wrong only when the species is definitely absent; predictions on unscanned bodies are not false positives

### Validation Mode

Checks prediction accuracy against actual genus signal data in the database:

```bash
python -I scripts/find_high_value_systems.py \
  --mode validate \
  --database /data/Databases_v2/galaxy_sectors_parquet \
  --output output/validation_$(date +%Y%m%d)/results.tsv \
  --workers 8
```

### Clustering and Routing

After a bio search, cluster nearby systems and generate nearest-neighbor routes:

```bash
# Cluster results using HDBSCAN (adaptive density — no distance param needed)
python -I scripts/cluster_systems.py \
  -i output/hen2_333_bio_search/results.tsv \
  -d 50 \
  -o output/hen2_333_bio_search/clusters.tsv \
  -m hdbscan \
  --min-cluster-size 3 \
  --min-samples 2
```

This produces:
- `clusters.tsv` — one row per cluster with centroid, size, diameter, system list
- `clusters_summary.txt` — human-readable summary with top-20 clusters and size distribution
- `clusters/` directory — per-cluster TSV files with systems in **nearest-neighbor order** and `jump_distance` column
- `clusters/clusters_index.tsv` — index of all multi-system cluster files

The per-cluster files are ready-to-use route plans: systems are ordered by greedy nearest-neighbor traversal starting from the system closest to galactic center, with jump distances between consecutive systems.

To route only through richer systems, filter by predicted signals (species predicted per body, counted from the `body_details` column) before clustering:

```bash
# Keep systems where at least one body has 2+ predicted species
python -I scripts/cluster_systems.py -i results.tsv -o clusters.tsv -m hdbscan \
  --min-cluster-size 3 --min-samples 2 --min-body-signals 2

# Keep systems where every predicted body has 2+ predicted species
python -I scripts/cluster_systems.py -i results.tsv -o clusters.tsv -m hdbscan \
  --min-cluster-size 3 --min-samples 2 --min-body-signals 2 --signal-filter all
```

The summary file records the filter and how many systems passed.

### Spansh Landmark Validation (Galaxy-Wide)

For deeper validation against species-level ground truth from Spansh:

1. **Download landmarks**: `python -I scripts/download_spansh_landmarks.py` — downloads confirmed species sightings (10K cap per species, ~1.5 hours)
2. **Merge into database**: `python -I scripts/merge_landmarks_to_parquet.py` — adds landmark columns to Parquet DB
3. **Validate**: `python -I scripts/validate_species_galaxy.py` — compares predictions vs confirmed landmarks galaxy-wide