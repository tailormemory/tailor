"""
TAILOR — Fuzzy finder over KB document *metadata* (filename / folder / path).

`kb_find_document` complements `kb_search` for structured documents whose
contents don't embed well (tables, spec sheets, forms). When the user
knows what they want by name ("piano integrazione aprile") but semantic
similarity over the body text misses it, this tool ranks documents by
metadata match instead.

The heavy lifting is in `find_documents`, a pure function over a list of
`(chunk_id, metadata)` tuples so tests don't need ChromaDB.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from urllib.parse import quote


_NORM_SEP = re.compile(r"[\s_\-]+")


def _norm(s: str) -> str:
    """Lowercase and collapse whitespace/underscore/hyphen runs to a single
    space so `"integrazione aprile"` matches `"piano_integrazione_aprile2026"`.
    """
    return _NORM_SEP.sub(" ", (s or "").lower()).strip()


def _is_superseded(meta: dict) -> bool:
    """A chunk is considered superseded if either `superseded_by` or
    `superseded_at` carries a non-empty value. The KB historically uses
    `superseded_by`; accept both so this works if the schema evolves."""
    return bool(meta.get("superseded_by") or meta.get("superseded_at"))


def _score(query: str, meta: dict) -> tuple[int, float]:
    """Return (tier, ratio). Higher tier wins; ratio breaks ties.

    Tiers:
      3 — query matches filename (title)
      2 — query matches folder
      1 — query matches anywhere in file_path
      0 — no substring match on any field (fallback to best fuzzy ratio)
    """
    q = _norm(query)
    title = _norm(meta.get("title") or "")
    folder = _norm(meta.get("folder") or "")
    path = _norm(meta.get("file_path") or "")

    if q and q in title:
        return 3, SequenceMatcher(None, q, title).ratio()
    if q and q in folder:
        return 2, SequenceMatcher(None, q, folder).ratio()
    if q and q in path:
        return 1, SequenceMatcher(None, q, path).ratio()
    # Fallback: best fuzzy ratio across all three fields.
    best = 0.0
    for field in (title, folder, path):
        if not field:
            continue
        best = max(best, SequenceMatcher(None, q, field).ratio())
    return 0, best


def _download_url(file_path: str) -> str:
    return f"/api/kb/document?path={quote(file_path, safe='')}" if file_path else ""


def find_documents(
    query: str,
    chunks: list[tuple[str, dict]],
    n_results: int = 10,
) -> list[dict]:
    """Rank documents by metadata fuzzy-match.

    chunks: list of (chunk_id, metadata) — pass every source=document chunk
    you want considered. Dedup-by-file_path and supersession filtering
    happen here.
    """
    # Group chunks by file_path — one result per document.
    groups: dict[str, dict] = {}
    for _cid, meta in chunks:
        if (meta or {}).get("source") != "document":
            continue
        fp = meta.get("file_path") or ""
        if not fp:
            continue
        g = groups.setdefault(fp, {
            "title": meta.get("title") or "",
            "file_path": fp,
            "folder": meta.get("folder") or "",
            "file_type": meta.get("file_type") or "",
            "chunk_count": 0,
            "_all_superseded": True,
            "_dates": [],
            "_meta_for_scoring": meta,
        })
        g["chunk_count"] += 1
        if not _is_superseded(meta):
            g["_all_superseded"] = False
        # Keep non-empty title/folder/file_type values from any chunk.
        for k in ("title", "folder", "file_type"):
            if not g[k] and meta.get(k):
                g[k] = meta[k]
        if meta.get("date"):
            g["_dates"].append(meta["date"])

    results = []
    for fp, g in groups.items():
        if g["_all_superseded"]:
            continue
        tier, ratio = _score(query, g["_meta_for_scoring"])
        last_indexed = max(g["_dates"]) if g["_dates"] else ""
        results.append({
            "title": g["title"],
            "file_path": fp,
            "folder": g["folder"],
            "file_type": g["file_type"],
            "download_url": _download_url(fp),
            "last_indexed_date": last_indexed,
            "chunk_count": g["chunk_count"],
            "_tier": tier,
            "_ratio": ratio,
        })

    results.sort(key=lambda r: (r["_tier"], r["_ratio"]), reverse=True)
    out = []
    for r in results[: max(0, int(n_results))]:
        r.pop("_tier", None)
        r.pop("_ratio", None)
        out.append(r)
    return out
