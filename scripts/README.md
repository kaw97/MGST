# Database Building Scripts

This directory contains scripts for building and maintaining the HITEC Galaxy database.

## Core Database Scripts

### `build_lightweight_index.py`

Builds a lightweight subsector index for efficient searches. **This should be run after creating or updating the sector database.**

```bash
python scripts/build_lightweight_index.py \
  --source Databases/galaxy_sectors_compressed \
  --target Databases/galaxy_sectors_compressed \
  --workers 12 \
  --batch-size 500
```

**Features:**
- Maps subsectors to sector files without storing per-system offsets
- Uses <1GB RAM regardless of database size
- Writes index incrementally every N sectors to prevent memory issues
- Enables efficient parallel subsector and corridor searches

**Output:**
- `subsector_index.json` - Lightweight index file mapping subsectors to sectors

**When to run:**
- After initial database creation
- After updating sector files
- If index becomes corrupted or outdated

**Build time:** ~10-15 minutes for full galaxy database

---

### `compress_sector_database.py`

Compresses sector database files using gzip compression.

```bash
python scripts/compress_sector_database.py
```

**Benefits:**
- 83.6% space reduction (609.8GB → 99.9GB)
- Maintains streaming performance
- Transparent decompression in all tools

---

### `reorganize_to_subsectors.py`

**Legacy script** - Creates individual subsector files (not recommended for full galaxy).

This script splits sector files into individual subsector files. While this provides maximum search speed, it creates hundreds of thousands of files which can:
- Crash file explorers (VSCode, Windows Explorer)
- Consume excessive inodes on Linux filesystems
- Make database updates difficult

**Use the lightweight index approach instead** for the full galaxy database.

---

## Database Building Workflow

### Initial Setup

1. **Obtain sector database** (compressed or uncompressed):
   ```
   Databases/galaxy_sectors_compressed/
   ├── Aaefong.jsonl.gz
   ├── Aaekaae.jsonl.gz
   ├── Aaekeau.jsonl.gz
   └── ... (~12,000 sector files)
   ```

2. **Build subsector index**:
   ```bash
   python scripts/build_lightweight_index.py \
     --source Databases/galaxy_sectors_compressed \
     --target Databases/galaxy_sectors_compressed \
     --workers 12 \
     --batch-size 500
   ```

3. **Verify index creation**:
   ```bash
   ls -lh Databases/galaxy_sectors_compressed/subsector_index.json
   ```

   The index file should be a few hundred MB.

### Database Updates

When updating the database with new data:

1. **Update sector files** - Add/modify individual sector JSONL.gz files

2. **Rebuild index** - Run the lightweight index builder again:
   ```bash
   python scripts/build_lightweight_index.py \
     --source Databases/galaxy_sectors_compressed \
     --target Databases/galaxy_sectors_compressed \
     --workers 12 \
     --batch-size 500
   ```

The index rebuild is fast (~10-15 minutes) and can be done regularly.

---

## Advanced Options

### Memory-Constrained Environments

If running on a system with limited RAM, reduce the batch size:

```bash
python scripts/build_lightweight_index.py \
  --source Databases/galaxy_sectors_compressed \
  --target Databases/galaxy_sectors_compressed \
  --workers 8 \
  --batch-size 100
```

This writes to disk more frequently, keeping memory usage even lower.

### Faster Index Building

If you have lots of RAM (32GB+), you can increase batch size:

```bash
python scripts/build_lightweight_index.py \
  --source Databases/galaxy_sectors_compressed \
  --target Databases/galaxy_sectors_compressed \
  --workers 16 \
  --batch-size 1000
```

---

## Troubleshooting

### Index Build Runs Out of Memory

**Symptoms:** Process killed by OS, "MemoryError" exception

**Solution:** Reduce batch size to write more frequently:
```bash
--batch-size 100
```

### Index Build Too Slow

**Symptoms:** Taking hours instead of minutes

**Solution:** Increase workers if you have CPU cores available:
```bash
--workers 16
```

### Index File Corrupted

**Symptoms:** Search commands fail with JSON parse errors

**Solution:** Delete and rebuild the index:
```bash
rm Databases/galaxy_sectors_compressed/subsector_index.json
python scripts/build_lightweight_index.py ...
```

---

## Index File Format

The `subsector_index.json` file contains:

```json
{
  "subsectors": {
    "Aaekaae_OD-T": {
      "sector_file": "Aaekaae.jsonl.gz",
      "system_count": 42
    },
    ...
  },
  "sectors": {
    "Aaekaae": {
      "file": "Aaekaae.jsonl.gz",
      "subsectors": ["OD-T", "IR-W", ...],
      "system_count": 1234
    },
    ...
  }
}
```

**Key features:**
- Maps subsectors to their parent sector files
- Stores system counts for quick statistics
- Does NOT store per-system offsets (keeps file size small)
- Search tools scan sector files and filter by subsector name

---

## See Also

- `CLAUDE.md` - Complete project documentation
- `src/hitec_galaxy/data/indexed_reader.py` - Index reader implementation
- `src/hitec_galaxy/core/search_modes.py` - Search mode implementations
