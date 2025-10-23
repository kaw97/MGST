# MGST (Mikunn Galactic Search Tool) 🚀

**Elite Dangerous galaxy analysis toolkit with flexible JSON pattern matching**

A Python package for analyzing Elite Dangerous galaxy data using flexible JSON patterns and powerful search modes. Find systems matching complex criteria across the galaxy, specific sectors, or along corridor routes. Future plans include system list-based proximity searches, BGS tracking, and colonization routing tools.

## Key Features

- **🔍 Flexible Pattern Matching** - Define search criteria using JSON patterns (implemented)
- **🗺️ Multiple Search Modes** - Galaxy-wide, sectors, or corridor searches (implemented)
- **⚡ High Performance** - Parallel processing with compressed database support (implemented)
- **📊 Comprehensive Logging** - Full reproducibility with automatic run documentation (implemented)
- **🛠️ Database Tools** - Build and update galaxy databases from Spansh data (planned)

## Installation

### Via Micromamba


1. **Install MGST**:
   ```bash
   # Clone repository
   git clone https://github.com/kaw97/MGST
   cd mgst

   # Create and activate environment (automatically installs MGST)
   micromamba create -f environment.yml
   micromamba activate mgst

   # Verify installation
   mgst --help
   ```

2. **Download Database**:

Building the initial database is very time-consuming (>24 hours with 12 processors on a 7950X), but users can download a preformatted database at https://drive.google.com/drive/folders/1m4Fl14xxaEm5rY9uoo9w9BKsAj7gHq_a . I plan on fully implementing database updating, but I want to implement BGS tracking alongside it so I haven't gotten to it yet. 

## Quick Start

### 1. Search a Corridor Between Two Points

```bash
mgst filter \
  --mode corridor \
  --start "0,0,0" \
  --end "22000,-1000,49000" \
  --radius 500 \
  --pattern-file patterns/supply_hub_1.json \
  --database /path/to/galaxy_database \
  --output results/corridor_search.jsonl
```

### 2. Search Specific Sectors

```bash
mgst filter \
  --mode sectors \
  --sectors "Lagoon_Sector,Trifid_Sector,Omega_Sector" \
  --pattern-file patterns/interesting_systems.json \
  --database /path/to/galaxy_database \
  --output results/sector_search.tsv
```

### 3. Search Entire Galaxy

```bash
mgst filter \
  --mode galaxy \
  --pattern-file patterns/all_systems.json \
  --database /path/to/galaxy_database \
  --output results/galaxy_scan.jsonl \
  --workers 12
```

## CLI Commands

### `mgst filter`

Flexible galaxy data filtering with multiple search modes.

**Search Modes:**
- `galaxy` - Search entire galaxy
- `sectors` - Search specific sectors
- `corridor` - Search corridor between two coordinates


**Common Options:**
- `--pattern-file PATH` - JSON file defining search criteria
- `--database PATH` - Galaxy database directory (sector files)
- `--output PATH` - Output file (auto-creates subdirectory with logs)
- `--workers N` - Number of parallel workers (default: 8)
- `--format [tsv|jsonl]` - Output format (default: tsv)
- `--dry-run` - Show what would be searched without executing

**Corridor Search Options:**
- `--start X,Y,Z` - Start coordinates
- `--end X,Y,Z` - End coordinates
- `--radius R` - Corridor radius in light years

**Examples:**

```bash
# Corridor search with 500 LY radius
mgst filter --mode corridor \
  --start "-468,-92,4474" \
  --end "-575,-37,5142" \
  --radius 500 \
  --pattern-file patterns/supply_hub_1.json \
  --database galaxy_sectors/ \
  --output output/supply_hubs_$(date +%Y%m%d)/results.jsonl

# Sector search
mgst filter --mode sectors \
  --sectors "Col_285,Lagoon_Sector" \
  --pattern-file patterns/interesting_systems.json \
  --database galaxy_sectors/ \
  --output results.tsv

# Dry run to see what would be searched
mgst filter --mode corridor \
  --start "0,0,0" --end "1000,0,0" --radius 100 \
  --pattern-file patterns/all_systems.json \
  --database galaxy_sectors/ \
  --dry-run
```

## JSON Pattern Format

Patterns are JSON files that define search criteria for systems and bodies:

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

**Pattern Features:**
- Match system names with wildcards
- Filter bodies by type, atmosphere, temperature, gravity
- Match parent-child relationships (moons, rings, etc.)
- Combine multiple criteria with AND/OR logic
- Use wildcards (`*`) for flexible matching

**Example Patterns:**

See `patterns/` directory for examples:
- `supply_hub_1.json` - ELW/Water worlds with rocky moons
- `interesting_systems.json` - High-value exploration targets
- `all_systems.json` - Match any system (useful for testing)

## Output Organization

Each filter run automatically creates comprehensive logs:

```
output/my_search_20251023/
├── results.jsonl              # Search results
├── results_search_metadata.json  # Search parameters
├── stdin.txt                  # Command used
├── stdout.txt                 # Processing output
└── stderr.txt                 # Progress/errors
```

**Best Practice:** Always use unique subdirectories for each search:

```bash
output/corridor_lagoon_trifid_$(date +%Y%m%d)/
output/sector_search_test_001/
output/supply_hub_candidates/
```

## Database Format

The galaxy database consists of sector-level JSONL files:

```
galaxy_sectors_compressed/
├── Lagoon_Sector.jsonl.gz
├── Trifid_Sector.jsonl.gz
├── Col_285_Sector.jsonl.gz
└── ...
```

**Features:**
- Gzip compression (83.6% space savings)
- Streaming decompression (low memory usage)
- Sector-based organization for efficient searches
- Compatible with Spansh galaxy dumps

## Performance

- **Parallel Processing**: 12+ workers for large searches
- **Compressed Support**: Automatic gzip detection and decompression
- **Smart Filtering**: Sector-level prefiltering reduces search space
- **Fast Processing**: 20,000+ systems/sec on modern hardware

## Requirements

- Python 3.8+
- pandas ≥1.5.0
- numpy ≥1.21.0
- scikit-learn ≥1.1.0
- tqdm ≥4.64.0
- click ≥8.0.0
- pydantic ≥1.10.0
- requests ≥2.28.0

All dependencies are automatically installed when using the micromamba environment files.




## License

This project is licensed under the GNU General Public License Version 3 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Elite Dangerous community for galaxy data and exploration knowledge
- Spansh for comprehensive galaxy database dumps
- Mercs of Mikunn for feedback and search suggestions

---

**Colonize the galaxy. Rise with us.** 🚀✨
