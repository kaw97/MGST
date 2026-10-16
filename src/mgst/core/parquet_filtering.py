"""Parquet-aware galaxy data filtering.

The Parquet equivalent of the JSONL filtering in filtering.py.
Uses a two-phase approach:
1. Predicate pushdown: read only needed columns, filter at the row-group level
2. Full reconstruction: for candidate systems that pass pushdown, reconstruct
   system dicts and run the existing JSONPatternMatcher for correctness

This module handles patterns of all complexity levels:
- Simple patterns (exact match, range, list/OR) push down fully
- Complex patterns (variable binding, parent matching) push down what they
  can, then reconstruct and backtrack only on the narrowed candidates

Large sectors are automatically split into sub-batches so that multiple
workers can process a single sector in parallel.
"""

import gc
import json
import logging
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from ..configs.base import BaseConfig
from ..configs.json_pattern import JSONPatternConfig
from ..core.json_pattern_matcher import JSONPatternMatcher
from ..core.pattern_compiler import CompiledPattern, PatternCompiler
from ..data.parquet_reader import ParquetSectorReader, detect_parquet_database
from .filtering import (
    FilteringResult,
    ProgressHandler,
    StreamingTSVWriter,
    write_result_to_file,
)

try:
    from tqdm import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

try:
    import fcntl

    HAS_FCNTL = True
except ImportError:
    HAS_FCNTL = False

logger = logging.getLogger(__name__)

# Sectors with more candidates than this threshold get split across workers
SECTOR_SPLIT_THRESHOLD = 5000


def process_parquet_sector_batch(
    sector_dir: Path,
    pattern: dict,
    compiled: CompiledPattern,
    config: BaseConfig,
    output_path: str,
    output_format: str,
    write_directly: bool,
    system_ids: List[int],
) -> Dict[str, Any]:
    """Process a batch of system IDs from a single Parquet sector.

    This is the worker function for sub-sector parallelism. It reconstructs
    and matches only the given system_ids.

    Args:
        sector_dir: Path to sector directory containing Parquet files
        pattern: Original JSON pattern dict
        compiled: CompiledPattern from PatternCompiler
        config: Configuration object for output formatting
        output_path: Output file path (for direct writes)
        output_format: Output format ('tsv' or 'jsonl')
        write_directly: Whether to write results directly to file
        system_ids: List of system id64 values to process

    Returns:
        Dictionary with processing results
    """
    try:
        reader = ParquetSectorReader(sector_dir)
        sector_name = reader.name
        matched_systems = []
        matches_found = 0
        id_set = set(system_ids)

        system_dicts = reader.reconstruct_system_dicts(system_ids=id_set)

        if compiled.needs_full_reconstruction:
            matcher = JSONPatternMatcher(pattern)
            for system in system_dicts:
                matcher.reset()
                if matcher.matches(system):
                    matches_found += 1
                    result = config.filter_system(system)
                    if result:
                        if write_directly and output_path:
                            write_result_to_file(
                                result, system, output_path, output_format, config
                            )
                        else:
                            result["_complete_system_record"] = system
                            matched_systems.append(result)
        else:
            for system in system_dicts:
                matches_found += 1
                result = config.filter_system(system)
                if result:
                    if write_directly and output_path:
                        write_result_to_file(
                            result, system, output_path, output_format, config
                        )
                    else:
                        result["_complete_system_record"] = system
                        matched_systems.append(result)

        del system_dicts
        gc.collect()

        return {
            "file": sector_name,
            "matched_systems": matched_systems,
            "total_processed": len(system_ids),
            "matches_found": matches_found,
            "errors": [],
        }

    except Exception as e:
        return {
            "file": sector_dir.name,
            "matched_systems": [],
            "total_processed": 0,
            "matches_found": 0,
            "errors": [f"Parquet batch processing error: {e}"],
        }


def process_parquet_sector(
    sector_dir: Path,
    pattern: dict,
    compiled: CompiledPattern,
    config: BaseConfig,
    output_path: str,
    output_format: str,
    write_directly: bool,
) -> Dict[str, Any]:
    """Process a single Parquet sector directory with filtering.

    Two-phase approach:
    1. Use compiled predicates to narrow candidate systems via Parquet pushdown
    2. If needed, reconstruct full dicts and run JSONPatternMatcher

    For small sectors this does everything in one worker. Large sectors are
    handled by the dispatcher which splits candidate IDs into batches.

    Args:
        sector_dir: Path to sector directory containing Parquet files
        pattern: Original JSON pattern dict
        compiled: CompiledPattern from PatternCompiler
        config: Configuration object for output formatting
        output_path: Output file path (for direct writes)
        output_format: Output format ('tsv' or 'jsonl')
        write_directly: Whether to write results directly to file

    Returns:
        Dictionary with processing results
    """
    try:
        reader = ParquetSectorReader(sector_dir)
        sector_name = reader.name

        if not reader.has_systems:
            return _empty_result(sector_name)

        total_systems = reader.system_count()
        matched_systems = []
        matches_found = 0

        # Phase 1: Pushdown filtering to find candidate system IDs
        candidate_ids = _get_candidate_ids(reader, compiled)

        if not candidate_ids:
            return {
                "file": sector_name,
                "matched_systems": [],
                "total_processed": total_systems,
                "matches_found": 0,
                "errors": [],
            }

        # Phase 2: Full matching
        if compiled.needs_full_reconstruction:
            # Reconstruct and run full pattern matcher on candidates
            system_dicts = reader.reconstruct_system_dicts(
                system_ids=candidate_ids
            )

            matcher = JSONPatternMatcher(pattern)
            for system in system_dicts:
                matcher.reset()
                if matcher.matches(system):
                    matches_found += 1
                    result = config.filter_system(system)
                    if result:
                        if write_directly and output_path:
                            write_result_to_file(
                                result, system, output_path, output_format, config
                            )
                        else:
                            result["_complete_system_record"] = system
                            matched_systems.append(result)
        else:
            # No reconstruction needed — all candidates are matches.
            # But we still need to reconstruct for output formatting.
            system_dicts = reader.reconstruct_system_dicts(
                system_ids=candidate_ids
            )
            for system in system_dicts:
                matches_found += 1
                result = config.filter_system(system)
                if result:
                    if write_directly and output_path:
                        write_result_to_file(
                            result, system, output_path, output_format, config
                        )
                    else:
                        result["_complete_system_record"] = system
                        matched_systems.append(result)

        gc.collect()

        return {
            "file": sector_name,
            "matched_systems": matched_systems,
            "total_processed": total_systems,
            "matches_found": matches_found,
            "errors": [],
        }

    except Exception as e:
        return {
            "file": sector_dir.name,
            "matched_systems": [],
            "total_processed": 0,
            "matches_found": 0,
            "errors": [f"Parquet processing error: {e}"],
        }


def _get_candidate_ids(
    reader: ParquetSectorReader, compiled: CompiledPattern
) -> Set[int]:
    """Use compiled predicates to narrow candidate system IDs.

    Phase 1 of the two-phase filtering approach:
    1. Apply system-level filters to systems.parquet
    2. For each body entry, apply body-level filters to bodies.parquet
    3. Intersect body candidate sets — systems must have candidates
       for ALL body entries
    """
    # Step 1: System-level filtering
    sys_filter = compiled.combined_system_filter()
    if sys_filter is not None:
        try:
            candidate_ids = reader.get_system_ids(filters=sys_filter)
        except Exception:
            # If filter fails (e.g., column doesn't exist), fall back to all
            candidate_ids = reader.get_system_ids()
    else:
        candidate_ids = reader.get_system_ids()

    if not candidate_ids:
        return set()

    # Step 2: Body-level filtering (intersect across body entries)
    if compiled.body_entry_filters:
        for entry_idx, entry_filters in enumerate(compiled.body_entry_filters):
            if not entry_filters:
                continue

            body_filter = compiled.combined_body_filter(entry_idx)
            if body_filter is None:
                continue

            try:
                body_sys_ids = reader.get_body_system_ids(filters=body_filter)
            except Exception:
                # If filter fails, skip this body entry's pushdown
                continue

            # Intersect: system must have candidates for this body entry
            candidate_ids = candidate_ids & body_sys_ids

            if not candidate_ids:
                return set()

    return candidate_ids


def _empty_result(sector_name: str) -> Dict[str, Any]:
    return {
        "file": sector_name,
        "matched_systems": [],
        "total_processed": 0,
        "matches_found": 0,
        "errors": [],
    }


def _split_ids(ids: Set[int], chunk_size: int) -> List[List[int]]:
    """Split a set of IDs into chunks of at most chunk_size."""
    id_list = sorted(ids)
    return [id_list[i:i + chunk_size] for i in range(0, len(id_list), chunk_size)]


def filter_galaxy_data_parquet(
    database_dir: Path,
    config: BaseConfig,
    output_path: Path,
    output_format: str = "tsv",
    workers: int = 8,
    test_mode: bool = False,
    max_test_systems: int = 1000,
    verbose: bool = False,
    sector_dirs: Optional[List[Path]] = None,
) -> FilteringResult:
    """Filter galaxy data from a Parquet sector database.

    Parquet equivalent of filter_galaxy_data() in filtering.py.
    Uses predicate pushdown for fast filtering, with full reconstruction
    fallback for complex patterns.

    Large sectors (more than SECTOR_SPLIT_THRESHOLD candidates) are
    automatically split into sub-batches across workers for parallelism.

    Args:
        database_dir: Root directory of the Parquet database
        config: Configuration object with filtering logic
        output_path: Output file path
        output_format: Output format ('tsv' or 'jsonl')
        workers: Number of worker processes
        test_mode: Whether to run in test mode
        max_test_systems: Maximum systems per sector in test mode
        verbose: Enable verbose logging
        sector_dirs: Optional pre-filtered list of sector directories
                    (from spatial prefiltering). If None, processes all sectors.

    Returns:
        FilteringResult with processing statistics
    """
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Validate database
    if not database_dir.exists():
        raise FileNotFoundError(f"Database directory not found: {database_dir}")

    if not detect_parquet_database(database_dir):
        raise ValueError(f"Not a Parquet database: {database_dir}")

    # Find sector directories
    if sector_dirs is not None:
        all_sector_dirs = sector_dirs
    else:
        all_sector_dirs = sorted(
            d for d in database_dir.iterdir()
            if d.is_dir() and (d / "systems.parquet").exists()
        )

    if not all_sector_dirs:
        raise FileNotFoundError(f"No Parquet sectors found in {database_dir}")

    # Extract pattern from config
    pattern = {}
    if isinstance(config, JSONPatternConfig):
        pattern = config.pattern
    else:
        logger.warning(
            "Config is not JSONPatternConfig; Parquet pushdown disabled"
        )

    # Compile pattern for pushdown
    compiler = PatternCompiler()
    compiled = compiler.compile(pattern)

    print(f"Found {len(all_sector_dirs)} Parquet sectors to process")
    print(f"Configuration: {config.get_description()}")
    if compiled.needs_full_reconstruction:
        print(f"Pattern requires full reconstruction: {compiled.reconstruction_reasons}")
    else:
        print("Pattern fully pushdown-compilable (fast path)")

    if compiled.system_filters:
        print(f"System-level filters: {len(compiled.system_filters)}")
    if compiled.body_entry_filters:
        total_filters = sum(len(f) for f in compiled.body_entry_filters)
        print(
            f"Body-level filters: {total_filters} across "
            f"{len(compiled.body_entry_filters)} body entries"
        )

    if test_mode:
        print(f"TEST MODE: Processing first {max_test_systems} sectors")
        all_sector_dirs = all_sector_dirs[:max_test_systems]

    # Create output directory and file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_directly = not test_mode

    if not test_mode:
        with open(output_path, "w", encoding="utf-8"):
            pass  # Create empty file

    # ---- Phase 1: lightweight pushdown scan to find candidates ----
    # Run pushdown on ALL sectors first to discover candidate IDs.
    # This is fast (reads only a few columns, no reconstruction).
    # Then we know which sectors are large and need splitting.
    print("Phase 1: Predicate pushdown scan...")
    phase1_start = time.time()

    sector_candidates: Dict[Path, Set[int]] = {}
    sector_total_systems: Dict[Path, int] = {}

    mp_context = multiprocessing.get_context("fork")

    with ProcessPoolExecutor(
        max_workers=workers, mp_context=mp_context
    ) as executor:
        futures = {
            executor.submit(_pushdown_scan, sd, compiled): sd
            for sd in all_sector_dirs
        }

        progress = ProgressHandler(len(all_sector_dirs), "Scanning sectors")
        try:
            for future in as_completed(futures):
                progress.update(1)
                sd = futures[future]
                try:
                    total_sys, cand_ids = future.result()
                    sector_total_systems[sd] = total_sys
                    if cand_ids:
                        sector_candidates[sd] = cand_ids
                except Exception as e:
                    logger.error("Pushdown scan error for %s: %s", sd.name, e)
                    sector_total_systems[sd] = 0
        finally:
            progress.close()

    total_candidates = sum(len(ids) for ids in sector_candidates.values())
    phase1_time = time.time() - phase1_start
    print(f"Phase 1 complete: {total_candidates:,} candidates in "
          f"{len(sector_candidates)} sectors ({phase1_time:.1f}s)")

    if total_candidates == 0:
        processing_time = time.time() - phase1_start
        print(f"No candidates found — skipping reconstruction.")
        return FilteringResult(
            files_processed=len(all_sector_dirs),
            systems_processed=sum(sector_total_systems.values()),
            matches_found=0,
            processing_time=processing_time,
            errors=[],
        )

    # ---- Phase 2: reconstruct and match candidates in parallel ----
    print("Phase 2: Reconstruct and match candidates...")
    phase2_start = time.time()

    # Build work units: split large sectors into sub-batches
    work_units: List[Tuple[Path, List[int]]] = []
    for sd, cand_ids in sector_candidates.items():
        if len(cand_ids) > SECTOR_SPLIT_THRESHOLD:
            chunks = _split_ids(cand_ids, SECTOR_SPLIT_THRESHOLD)
            for chunk in chunks:
                work_units.append((sd, chunk))
            if verbose:
                logger.info("Split %s (%d candidates) into %d batches",
                            sd.name, len(cand_ids), len(chunks))
        else:
            work_units.append((sd, sorted(cand_ids)))

    print(f"  {len(work_units)} work units "
          f"({len(sector_candidates)} sectors, "
          f"{len(work_units) - len(sector_candidates)} sub-batches from large sectors)")

    total_processed = sum(sector_total_systems.values())
    total_matches = 0
    all_errors: List[str] = []
    all_matched_systems: List[Dict] = []

    with ProcessPoolExecutor(
        max_workers=workers, mp_context=mp_context
    ) as executor:
        futures = {
            executor.submit(
                process_parquet_sector_batch,
                sd,
                pattern,
                compiled,
                config,
                str(output_path) if write_directly else "",
                output_format,
                write_directly,
                id_batch,
            ): (sd, len(id_batch))
            for sd, id_batch in work_units
        }

        progress = ProgressHandler(len(work_units), "Matching candidates")

        try:
            for future in as_completed(futures):
                progress.update(1)

                try:
                    result = future.result()
                    total_matches += result["matches_found"]
                    all_errors.extend(result["errors"])
                    all_matched_systems.extend(result["matched_systems"])

                    if verbose and result["matches_found"] > 0:
                        logger.info(
                            "Sector %s: %d matches from %d candidates",
                            result["file"],
                            result["matches_found"],
                            result["total_processed"],
                        )
                except Exception as e:
                    sd, batch_size = futures[future]
                    logger.error("Error processing %s: %s", sd.name, e)
                    all_errors.append(
                        f"Processing error for {sd.name}: {e}"
                    )
        finally:
            progress.close()

    processing_time = time.time() - phase1_start

    # Write results (batch mode)
    if all_matched_systems and not test_mode and not write_directly:
        from .filtering import write_results

        write_results(all_matched_systems, config, output_path, output_format)
        print(f"Results written to: {output_path}")
    elif not test_mode and write_directly:
        if total_matches > 0:
            print(f"Results streamed to: {output_path}")
        else:
            print(f"No matches found - empty file: {output_path}")
    elif test_mode:
        print(f"TEST MODE: Found {total_matches} matches (results not saved)")

    result = FilteringResult(
        files_processed=len(all_sector_dirs),
        systems_processed=total_processed,
        matches_found=total_matches,
        processing_time=processing_time,
        errors=all_errors,
    )

    print(f"\n=== Processing Complete ===")
    print(f"Sectors processed: {result.files_processed}")
    print(f"Systems processed: {result.systems_processed:,}")
    print(f"Matches found: {result.matches_found:,}")
    print(f"Match rate: {result.match_rate:.3f}%")
    print(f"Processing time: {result.processing_time:.1f}s")
    print(f"Processing speed: {result.processing_speed:.0f} systems/sec")

    if result.errors:
        print(f"\nWarnings/Errors: {len(result.errors)}")
        if verbose:
            for error in result.errors[:10]:
                print(f"  - {error}")

    return result


def _pushdown_scan(
    sector_dir: Path, compiled: CompiledPattern
) -> Tuple[int, Set[int]]:
    """Run Phase 1 pushdown on a single sector, returning candidate IDs.

    This is lightweight — reads only the columns needed for filtering,
    no full reconstruction.

    Returns:
        (total_system_count, candidate_id_set)
    """
    reader = ParquetSectorReader(sector_dir)
    if not reader.has_systems:
        return 0, set()

    total = reader.system_count()
    candidates = _get_candidate_ids(reader, compiled)
    return total, candidates
