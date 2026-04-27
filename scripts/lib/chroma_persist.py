"""TAILOR — force chromadb 1.x HNSW segment to persist via idempotent re-upsert.

ChromaDB 1.x flushes its in-memory HNSW state to disk when one of two
things happens: the segment's internal write counter crosses
`hnsw:sync_threshold` (default 1000), or `System.stop()` / atexit fires
during a clean shutdown. The latter is the unreliable one — process kills,
signal interruptions, or fs hiccups can lose the pickle write, leaving
SQL metadata advanced past the HNSW state. That's the drift documented in
`scripts/maintenance/repair_hnsw_index.md` (upstream chroma-core/chroma#6975).

This helper forces the first path: it issues `target_writes` (default 1100
= sync_threshold 1000 + ~10% margin to clear boundary races) idempotent
re-upserts of a known sample. ChromaDB treats them as no-ops semantically
(same ids, same embeddings/metadatas/documents) but the writes still bump
the internal counter and trigger `_persist()`.

Originally executed manually as "Track 3a" on 2026-04-25 to flush the
9ad2790b vector segment after a 66-orphan repair. Formalised here so
`repair_hnsw_index.py --apply` can invoke it automatically post-repair,
eliminating the "second restart dance" that previously had to be done
by hand.

Reference: chroma-core/chroma#6975 (chromadb 1.5.5).
"""

from __future__ import annotations

import math
import os
import sys
from typing import Any, Sequence

DEFAULT_TARGET_WRITES = 1100  # hnsw:sync_threshold (1000) + ~10% margin


def _stat_mtime(path: str | None) -> float | None:
    if not path:
        return None
    try:
        return os.stat(path).st_mtime
    except FileNotFoundError:
        return None


def force_chroma_persist(
    collection: Any,
    sample_chunk_ids: Sequence[str],
    *,
    target_writes: int = DEFAULT_TARGET_WRITES,
    pickle_path: str | None = None,
) -> dict:
    """Force chromadb's HNSW segment to flush to disk.

    Re-upserts `sample_chunk_ids` ⌈target_writes / N⌉ times with the same
    data fetched from the collection. ChromaDB treats the upserts as no-ops
    semantically; the writes still bump the segment's internal counter past
    sync_threshold and trigger `_persist()`.

    `pickle_path` is optional. When provided, the function records mtime
    before/after and reports whether it advanced. If it didn't, a warning
    is emitted to stderr but the function still returns `success=True` —
    mtime advancement is best-effort observation, not enforcement. The next
    audit run catches any residual drift; failing here would just hide that.

    Returns a dict::

        {
          "success": bool,
          "iterations": int,         # number of upsert calls made
          "total_writes": int,       # iterations * ids_persisted
          "ids_persisted": int,      # actual N (may be < len(sample) if some missing)
          "mtime_advanced": bool | None,   # None if pickle_path not provided
          "mtime_before": float | None,
          "mtime_after": float | None,
        }

    Empty `sample_chunk_ids` → no-op success (`iterations=0, total_writes=0`).
    Sample ids that don't resolve via `collection.get()` → `success=False`
    with a stderr warning, no upserts attempted.
    """
    result: dict[str, Any] = {
        "success": True,
        "iterations": 0,
        "total_writes": 0,
        "ids_persisted": 0,
        "mtime_advanced": None,
        "mtime_before": None,
        "mtime_after": None,
    }

    if not sample_chunk_ids:
        return result

    fetched = collection.get(
        ids=list(sample_chunk_ids),
        include=["embeddings", "metadatas", "documents"],
    )
    ids = list(fetched.get("ids") or [])
    embeddings = list(fetched.get("embeddings") or [])
    metadatas = list(fetched.get("metadatas") or [])
    documents = list(fetched.get("documents") or [])

    if not ids:
        print(
            f"[chroma_persist] WARN: none of the {len(sample_chunk_ids)} sample ids "
            f"resolved via collection.get() — cannot force persist. Next audit will "
            f"catch any residual drift.",
            file=sys.stderr,
        )
        result["success"] = False
        return result

    n = len(ids)
    iterations = math.ceil(target_writes / n)

    result["mtime_before"] = _stat_mtime(pickle_path)

    for _ in range(iterations):
        collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas,
        )

    result["iterations"] = iterations
    result["total_writes"] = iterations * n
    result["ids_persisted"] = n

    if pickle_path is not None:
        mtime_after = _stat_mtime(pickle_path)
        result["mtime_after"] = mtime_after
        before = result["mtime_before"]
        if before is None or mtime_after is None or mtime_after <= before:
            result["mtime_advanced"] = False
            print(
                f"[chroma_persist] WARN: HNSW pickle mtime did not advance after "
                f"{result['total_writes']} idempotent writes ({iterations} iterations × {n} ids). "
                f"The persist may not have triggered; next audit will catch any drift.",
                file=sys.stderr,
            )
        else:
            result["mtime_advanced"] = True

    return result
