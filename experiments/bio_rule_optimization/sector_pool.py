"""Run a per-sector function over the Parquet database without memory build-up.

- Sectors are processed in batches, each with a fresh forked process pool, so
  worker processes exit (and return their memory) after every batch. Spawned
  workers with max_tasks_per_child would do this automatically, but spawn hangs
  in this environment and fork does not support max_tasks_per_child.
- After each sector a worker drops Python garbage and returns Arrow's unused
  buffers to the OS.
- Results are handed to the caller as each sector finishes, so the caller can
  write them to disk instead of keeping them.
"""

import gc
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Callable, Iterator, List

import pyarrow as pa

_FN: Callable[[str], Any] = None  # set in the parent before forking


def _run_one(sector: str) -> Any:
    try:
        return _FN(sector)
    finally:
        gc.collect()
        pa.default_memory_pool().release_unused()


def run_sectors(
    fn: Callable[[str], Any],
    sectors: List[str],
    workers: int = 8,
    batch_size: int = 500,
    progress_every: int = 1000,
) -> Iterator[Any]:
    """Yield fn(sector) for every sector, in order."""
    global _FN
    _FN = fn
    ctx = multiprocessing.get_context("fork")
    done = 0
    for start in range(0, len(sectors), batch_size):
        batch = sectors[start:start + batch_size]
        with ProcessPoolExecutor(workers, mp_context=ctx) as ex:
            for result in ex.map(_run_one, batch, chunksize=4):
                done += 1
                if progress_every and done % progress_every == 0:
                    print(f"  {done}/{len(sectors)} sectors", flush=True)
                yield result
