#!/usr/bin/env python3
"""
TAILOR — HNSW index repair.

Detects and repairs drift between ChromaDB's SQL metadata segment and HNSW
vector segment. Drift can occur when ChromaDB 1.x's persist-on-shutdown of
the HNSW pickle does not fire cleanly; SQL `max_seq_id` advances and the
`embeddings_queue` purges, but the on-disk index never receives the writes.
Symptom: `kb_search` returns "Internal error: Error finding id" for chunks
that exist in metadata but are missing from HNSW.

Usage:
  python scripts/maintenance/repair_hnsw_index.py            # audit (default)
  python scripts/maintenance/repair_hnsw_index.py --audit    # explicit
  python scripts/maintenance/repair_hnsw_index.py --apply    # repair (mutates)
  python scripts/maintenance/repair_hnsw_index.py --json     # machine output

Audit is read-only and side-effect-free except for an audit-history JSON
file under logs/hnsw_audits/ and (optionally) a row in maintenance_log.
Apply requires the MCP to be in maintenance mode (SIGUSR1 already issued)
and a backup taken within the last 60 minutes.

Restart-aware: when a previous audit JSON exists from <2h ago, the report
includes a delta section so race-induced drift around an MCP restart is
visible.
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import re
import shutil
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(BASE_DIR, "db")
DB_PATH = os.path.join(DB_DIR, "chroma.sqlite3")
BACKUPS_DIR = os.path.join(BASE_DIR, "backups")
MAINTENANCE_LOCK = os.path.join(BASE_DIR, "maintenance.lock")
AUDIT_HISTORY_DIR = os.path.join(BASE_DIR, "logs", "hnsw_audits")
COLLECTION_NAME = "tailor_kb_v2"

UPSERT_BATCH_SIZE = 10
MAX_TEXT_CHARS = 4000  # mirrors ingest_docs.py
DELTA_WINDOW_SECONDS = 7200  # 2 hours
BACKUP_MAX_AGE_SECONDS = 3600  # 60 minutes


# ============================================================
# DATA CLASSES
# ============================================================


@dataclass
class OrphanChunk:
    embedding_id: str
    int_id: int
    metadata: dict[str, Any]  # all metadata keys including chroma:document
    classification: str  # "known_document" | "known_summary" | "known_other_source" | "unknown"


@dataclass
class GhostChunk:
    embedding_id: str
    classification: str  # "known_superseded_doc" | "known_kb_add_ghost" |
                        # "known_kb_session_ghost" | "known_live_capture_ghost" |
                        # "unknown"
    note: str = ""


@dataclass
class DriftReport:
    timestamp: str
    timestamp_unix: int
    db_path: str
    metadata_segment: str
    vector_segment: str
    sql_total: int
    hnsw_total: int
    queue_total: int
    queue_oldest_iso: str | None
    queue_newest_iso: str | None
    sql_only_orphans: list[OrphanChunk] = field(default_factory=list)
    hnsw_only_ghosts: list[GhostChunk] = field(default_factory=list)
    unknown_count: int = 0  # orphans + ghosts of class "unknown"

    @property
    def sql_only_count(self) -> int:
        return len(self.sql_only_orphans)

    @property
    def hnsw_only_count(self) -> int:
        return len(self.hnsw_only_ghosts)

    def is_clean(self) -> bool:
        return self.sql_only_count == 0 and self.hnsw_only_count == 0

    def has_unknowns(self) -> bool:
        return self.unknown_count > 0

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d


@dataclass
class Delta:
    prev_timestamp: str
    age_seconds: int
    sql_only_change: int
    hnsw_only_change: int
    queue_change: int
    new_orphan_ids: list[str]
    resolved_orphan_ids: list[str]


# ============================================================
# AUDIT
# ============================================================


def _open_sqlite_ro(db_path: str) -> sqlite3.Connection:
    uri = f"file:{db_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _open_sqlite_rw(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def _load_segment_ids(con: sqlite3.Connection) -> tuple[str, str]:
    cur = con.cursor()
    cur.execute("SELECT id FROM segments WHERE scope='METADATA'")
    meta_row = cur.fetchone()
    cur.execute("SELECT id FROM segments WHERE scope='VECTOR'")
    vec_row = cur.fetchone()
    if not meta_row or not vec_row:
        raise RuntimeError("Could not locate METADATA and VECTOR segments")
    return meta_row[0], vec_row[0]


def _resolve_hnsw_pickle_path(db_path: str, db_dir: str) -> str | None:
    """Best-effort resolution of the HNSW pickle path. Returns None if either
    the segments table or the pickle file is missing — callers use this for
    optional mtime observation, not enforcement."""
    try:
        con = _open_sqlite_ro(db_path)
        try:
            _, vec_seg = _load_segment_ids(con)
        finally:
            con.close()
        path = os.path.join(db_dir, vec_seg, "index_metadata.pickle")
        return path if os.path.exists(path) else None
    except Exception:
        return None


def _load_hnsw_pickle_ids(db_dir: str, vec_seg: str) -> set[str]:
    pickle_path = os.path.join(db_dir, vec_seg, "index_metadata.pickle")
    if not os.path.exists(pickle_path):
        raise RuntimeError(f"HNSW pickle not found at {pickle_path}")
    with open(pickle_path, "rb") as f:
        m = pickle.load(f)
    return set(m.get("id_to_label", {}).keys())


def _load_metadata_for(con: sqlite3.Connection, embedding_ids: set[str], meta_seg: str) -> dict[str, dict[str, Any]]:
    """Returns {embedding_id: {metadata_key: value, '__int_id__': id}}."""
    if not embedding_ids:
        return {}
    cur = con.cursor()
    placeholders = ",".join("?" * len(embedding_ids))
    cur.execute(
        f"SELECT id, embedding_id FROM embeddings "
        f"WHERE segment_id=? AND embedding_id IN ({placeholders})",
        [meta_seg, *embedding_ids],
    )
    int_id_to_eid = {row[0]: row[1] for row in cur.fetchall()}
    if not int_id_to_eid:
        return {}
    int_ids = list(int_id_to_eid.keys())
    int_placeholders = ",".join("?" * len(int_ids))
    cur.execute(
        f"SELECT id, key, string_value, int_value, float_value, bool_value "
        f"FROM embedding_metadata WHERE id IN ({int_placeholders})",
        int_ids,
    )
    out: dict[str, dict[str, Any]] = {}
    for int_id, eid in int_id_to_eid.items():
        out[eid] = {"__int_id__": int_id}
    for row in cur.fetchall():
        int_id, key, sv, iv, fv, bv = row
        eid = int_id_to_eid[int_id]
        # Pick the populated typed column; matches Chroma's metadata storage convention
        if sv is not None:
            out[eid][key] = sv
        elif iv is not None:
            out[eid][key] = iv
        elif fv is not None:
            out[eid][key] = fv
        elif bv is not None:
            out[eid][key] = bool(bv)
        else:
            out[eid][key] = None
    return out


def _classify_orphan(eid: str, meta: dict[str, Any]) -> str:
    source = meta.get("source")
    conv_id = meta.get("conv_id") or ""
    if source == "document":
        if conv_id.startswith("doc_summary_"):
            return "known_summary"
        if conv_id.startswith("doc_"):
            return "known_document"
        return "unknown"  # source=document but conv_id pattern unrecognised
    if source in ("conversation", "email"):
        return "known_other_source"
    return "unknown"


# Dispatch table for _classify_ghost. Each entry is
# (compiled_regex, classification_string, note_string).
# Matched in order; first match wins. The doc_<hash>_chunk_<N> namespace
# is intentionally NOT in this table — it lives in the fallthrough below
# because its classification depends on the live doc_registry, not on
# the id pattern alone.
_KNOWN_GHOST_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (
        re.compile(r"^(claude|chatgpt)_\d{8}_\d{6}_\d{4}$"),
        "known_kb_add_ghost",
        "kb_add namespace — stranded by an mcp_server kb_add persist failure",
    ),
    (
        re.compile(r"^(claude|chatgpt)_session_\d{8}_\d{6}$"),
        "known_kb_session_ghost",
        "kb_update_session namespace — stranded by an mcp_server kb_update_session persist failure",
    ),
    (
        re.compile(r"^live_[A-Za-z0-9_\-]+_[0-9a-f]{16}$"),
        "known_live_capture_ghost",
        "/api/ingest-live namespace — stranded by a browser-extension ingest persist failure",
    ),
]


def _classify_ghost(eid: str, registry_doc_hashes: set[str]) -> tuple[str, str]:
    """Returns (classification, note).

    Two-stage classification:
      (1) Match against `_KNOWN_GHOST_PATTERNS` in order. These cover
          ghosts produced by mcp_server write paths (kb_add /
          kb_update_session / /api/ingest-live) that lost their SQL row
          but kept the HNSW vector. First match wins.
      (2) Fallthrough: the original doc_<hash>_chunk_<N> logic, preserved
          bit-identical from the pre-A3 implementation. Routes superseded
          doc_ ghosts to "known_superseded_doc" and everything else to
          "unknown" with the original note text.
    """
    for pattern, cls, note in _KNOWN_GHOST_PATTERNS:
        if pattern.match(eid):
            return cls, note

    # === Original doc_<hash>_chunk_<N> logic — preserved bit-identical ===
    if "_chunk_" not in eid:
        return "unknown", "id does not match `<prefix>_chunk_NNNN` pattern"
    prefix = eid.rsplit("_chunk_", 1)[0]
    if prefix.startswith("doc_"):
        doc_hash = prefix[len("doc_"):]
        if doc_hash and doc_hash not in registry_doc_hashes:
            return "known_superseded_doc", f"prefix {prefix} not in current doc_registry — likely superseded prior version"
        return "unknown", f"prefix {prefix} matches a doc_hash still in registry — unexpected"
    return "unknown", f"prefix {prefix} does not match document namespace"


def _load_registry_doc_hashes(db_dir: str) -> set[str]:
    registry_path = os.path.join(db_dir, "doc_registry.json")
    if not os.path.exists(registry_path):
        return set()
    try:
        with open(registry_path) as f:
            registry = json.load(f)
    except Exception:
        return set()
    hashes: set[str] = set()
    for info in registry.values():
        if not isinstance(info, dict):
            continue
        h = info.get("hash") or info.get("doc_hash") or ""
        if h:
            hashes.add(h[:12])  # match 12-char prefix used in chunk_id
            hashes.add(h)
    return hashes


def _queue_extents(con: sqlite3.Connection) -> tuple[int, str | None, str | None]:
    cur = con.cursor()
    cur.execute("SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM embeddings_queue")
    row = cur.fetchone()
    return row[0], row[1], row[2]


def audit(db_path: str | None = None, db_dir: str | None = None) -> DriftReport:
    """Read-only diagnosis. Returns a DriftReport."""
    if db_path is None:
        db_path = DB_PATH
    if db_dir is None:
        db_dir = DB_DIR
    con = _open_sqlite_ro(db_path)
    try:
        meta_seg, vec_seg = _load_segment_ids(con)
        cur = con.cursor()
        cur.execute("SELECT embedding_id FROM embeddings WHERE segment_id=?", (meta_seg,))
        sql_ids = {r[0] for r in cur.fetchall()}
        hnsw_ids = _load_hnsw_pickle_ids(db_dir, vec_seg)
        cur.execute("SELECT id FROM embeddings_queue")
        queue_ids = {r[0] for r in cur.fetchall()}

        sql_only = sql_ids - hnsw_ids - queue_ids
        hnsw_only = hnsw_ids - sql_ids

        sql_only_meta = _load_metadata_for(con, sql_only, meta_seg)
        registry_hashes = _load_registry_doc_hashes(db_dir)

        orphans: list[OrphanChunk] = []
        for eid in sorted(sql_only):
            meta = sql_only_meta.get(eid, {})
            int_id = meta.get("__int_id__", -1)
            cls = _classify_orphan(eid, meta)
            orphans.append(OrphanChunk(embedding_id=eid, int_id=int_id, metadata=meta, classification=cls))

        ghosts: list[GhostChunk] = []
        for eid in sorted(hnsw_only):
            cls, note = _classify_ghost(eid, registry_hashes)
            ghosts.append(GhostChunk(embedding_id=eid, classification=cls, note=note))

        queue_total, queue_oldest, queue_newest = _queue_extents(con)
        unknown = sum(1 for o in orphans if o.classification == "unknown") + \
                  sum(1 for g in ghosts if g.classification == "unknown")

        now = datetime.now()
        return DriftReport(
            timestamp=now.strftime("%Y-%m-%d %H:%M:%S"),
            timestamp_unix=int(now.timestamp()),
            db_path=db_path,
            metadata_segment=meta_seg,
            vector_segment=vec_seg,
            sql_total=len(sql_ids),
            hnsw_total=len(hnsw_ids),
            queue_total=queue_total,
            queue_oldest_iso=queue_oldest,
            queue_newest_iso=queue_newest,
            sql_only_orphans=orphans,
            hnsw_only_ghosts=ghosts,
            unknown_count=unknown,
        )
    finally:
        con.close()


# ============================================================
# AUDIT HISTORY + DELTA
# ============================================================


def _ensure_audit_history_dir(audit_dir: str) -> None:
    Path(audit_dir).mkdir(parents=True, exist_ok=True)


def write_audit_history(report: DriftReport, audit_dir: str | None = None) -> str:
    if audit_dir is None:
        audit_dir = AUDIT_HISTORY_DIR
    _ensure_audit_history_dir(audit_dir)
    fname = f"audit_{datetime.fromtimestamp(report.timestamp_unix).strftime('%Y%m%d_%H%M%S')}.json"
    path = os.path.join(audit_dir, fname)
    with open(path, "w") as f:
        json.dump(report.to_dict(), f, indent=2, default=str)
    return path


def find_recent_audit(
    audit_dir: str | None = None,
    max_age_seconds: int = DELTA_WINDOW_SECONDS,
    exclude: str | None = None,
) -> str | None:
    if audit_dir is None:
        audit_dir = AUDIT_HISTORY_DIR
    if not os.path.isdir(audit_dir):
        return None
    candidates = []
    for fname in os.listdir(audit_dir):
        if not fname.startswith("audit_") or not fname.endswith(".json"):
            continue
        full = os.path.join(audit_dir, fname)
        if exclude and os.path.abspath(full) == os.path.abspath(exclude):
            continue
        try:
            mtime = os.path.getmtime(full)
        except OSError:
            continue
        candidates.append((mtime, full))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    mtime, path = candidates[0]
    if (time.time() - mtime) > max_age_seconds:
        return None
    return path


def compute_delta(prev_path: str, current: DriftReport) -> Delta | None:
    try:
        with open(prev_path) as f:
            prev = json.load(f)
    except Exception:
        return None
    prev_ts_unix = int(prev.get("timestamp_unix", 0))
    age = current.timestamp_unix - prev_ts_unix
    prev_orphan_ids = {o["embedding_id"] for o in prev.get("sql_only_orphans", [])}
    cur_orphan_ids = {o.embedding_id for o in current.sql_only_orphans}
    new_ids = sorted(cur_orphan_ids - prev_orphan_ids)
    resolved_ids = sorted(prev_orphan_ids - cur_orphan_ids)
    return Delta(
        prev_timestamp=prev.get("timestamp", "?"),
        age_seconds=age,
        sql_only_change=len(cur_orphan_ids) - len(prev_orphan_ids),
        hnsw_only_change=current.hnsw_only_count - len(prev.get("hnsw_only_ghosts", [])),
        queue_change=current.queue_total - int(prev.get("queue_total", 0)),
        new_orphan_ids=new_ids,
        resolved_orphan_ids=resolved_ids,
    )


# ============================================================
# REPORT FORMATTING
# ============================================================


def _format_age(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{m}m {s}s ago"
    h, rem = divmod(seconds, 3600)
    m = rem // 60
    return f"{h}h {m}m ago"


def format_report_human(report: DriftReport, delta: Delta | None = None) -> str:
    lines: list[str] = []
    lines.append(f"HNSW REPAIR AUDIT — {report.timestamp}")
    lines.append("=" * 40)
    lines.append("")
    lines.append(f"Collection: {COLLECTION_NAME}")
    lines.append(f"Active vector segment: {report.vector_segment}")
    lines.append(f"SQL total: {report.sql_total}    HNSW total: {report.hnsw_total}    Queue: {report.queue_total}")
    lines.append("")
    lines.append("DRIFT SUMMARY")
    lines.append(f"  SQL-only orphans:    {report.sql_only_count:>4}  (in SQL, missing from HNSW)")
    lines.append(f"  HNSW-only ghosts:    {report.hnsw_only_count:>4}  (in HNSW, missing from SQL)")
    lines.append(f"  Queue backlog:       {report.queue_total:>4}  pending ops in embeddings_queue")
    lines.append("")

    if delta is not None:
        lines.append(f"DELTA SINCE LAST AUDIT ({delta.prev_timestamp}, {_format_age(delta.age_seconds)}):")
        lines.append(f"  SQL-only orphans:   {delta.sql_only_change:+d}")
        lines.append(f"  HNSW-only ghosts:   {delta.hnsw_only_change:+d}")
        lines.append(f"  Queue backlog:      {delta.queue_change:+d}")
        if delta.new_orphan_ids:
            lines.append(f"  New orphans ({len(delta.new_orphan_ids)}):")
            for nid in delta.new_orphan_ids[:10]:
                lines.append(f"    + {nid}")
            if len(delta.new_orphan_ids) > 10:
                lines.append(f"    ... and {len(delta.new_orphan_ids) - 10} more")
        if delta.resolved_orphan_ids:
            lines.append(f"  Resolved orphans ({len(delta.resolved_orphan_ids)}):")
            for rid in delta.resolved_orphan_ids[:10]:
                lines.append(f"    - {rid}")
            if len(delta.resolved_orphan_ids) > 10:
                lines.append(f"    ... and {len(delta.resolved_orphan_ids) - 10} more")
        if delta.sql_only_change == 0 and delta.hnsw_only_change == 0 and not delta.new_orphan_ids:
            lines.append("  Status: stable, no new drift events detected")
        lines.append("")

    if report.sql_only_orphans:
        lines.append("SQL-ONLY ORPHANS (queryable via metadata, fail vector search):")
        groups: dict[str, list[OrphanChunk]] = defaultdict(list)
        for o in report.sql_only_orphans:
            conv_id = o.metadata.get("conv_id") or "(no conv_id)"
            groups[conv_id].append(o)
        lines.append("  Grouped by conv_id:")
        for conv_id in sorted(groups.keys()):
            items = groups[conv_id]
            titles = {i.metadata.get("title", "") for i in items if i.metadata.get("title")}
            cls = items[0].classification
            indices = sorted({int(i.metadata.get("chunk_index", -999)) for i in items})
            indices = [i for i in indices if i != -999 and i >= 0]
            range_str = ""
            if indices:
                if len(indices) == 1:
                    range_str = f"  (chunk_index {indices[0]})"
                elif indices == list(range(indices[0], indices[-1] + 1)):
                    range_str = f"  (chunks {indices[0]}–{indices[-1]})"
                else:
                    range_str = f"  ({len(indices)} chunks, non-contiguous)"
            if len(titles) == 1:
                title_part = next(iter(titles))
            elif len(titles) > 1:
                title_part = f"({len(titles)} distinct titles)"
            else:
                title_part = ""
            label = conv_id
            if title_part:
                label += f"  {title_part}"
            n = len(items)
            chunk_word = "chunk" if n == 1 else "chunks"
            lines.append(f"    {label:<60} {n:>3} {chunk_word}  [{cls}]{range_str}")
        lines.append("")

    if report.hnsw_only_ghosts:
        lines.append("HNSW-ONLY GHOSTS (occupy index, never queryable):")
        ghost_groups: dict[str, list[GhostChunk]] = defaultdict(list)
        for g in report.hnsw_only_ghosts:
            prefix = g.embedding_id.rsplit("_chunk_", 1)[0] if "_chunk_" in g.embedding_id else g.embedding_id
            ghost_groups[prefix].append(g)
        for prefix in sorted(ghost_groups.keys()):
            items = ghost_groups[prefix]
            n = len(items)
            cls = items[0].classification
            note = items[0].note
            chunk_word = "chunk" if n == 1 else "chunks"
            lines.append(f"  {prefix}  ({n} {chunk_word})  [{cls}]")
            if note:
                lines.append(f"    Note: {note}")
        lines.append("")

    if report.queue_total:
        lines.append("QUEUE BACKLOG:")
        oldest = report.queue_oldest_iso or "?"
        newest = report.queue_newest_iso or "?"
        lines.append(f"  {report.queue_total} pending ops, from {oldest} to {newest}")
        lines.append(f"  Vector segment max_seq_id is behind metadata segment by these ops.")
        lines.append(f"  Will be consumed on next MCP restart.")
        lines.append(f"  WARNING: queue consumption on restart may itself trigger drift if HNSW")
        lines.append(f"  persist races with shutdown. Re-run audit after restart to verify.")
        lines.append("")

    lines.append("UNEXPECTED DRIFT TYPES:")
    unknowns = [o for o in report.sql_only_orphans if o.classification == "unknown"]
    unknown_ghosts = [g for g in report.hnsw_only_ghosts if g.classification == "unknown"]
    if not unknowns and not unknown_ghosts:
        lines.append("  none")
        lines.append("  (script knows about:")
        lines.append("     SQL-only:  docs / summaries / conversations / emails")
        lines.append("     HNSW-only: doc_<hash> superseded by registry")
        lines.append("                claude|chatgpt_<YYYYMMDD>_<HHMMSS>_<NNNN>    (kb_add)")
        lines.append("                claude|chatgpt_session_<YYYYMMDD>_<HHMMSS>   (kb_update_session)")
        lines.append("                live_<source>_<16hex>                        (/api/ingest-live)")
        lines.append("     queue lag from pending upserts.)")
    else:
        for o in unknowns:
            lines.append(f"  ORPHAN  {o.embedding_id}  source={o.metadata.get('source')!r}  conv_id={o.metadata.get('conv_id')!r}")
        for g in unknown_ghosts:
            lines.append(f"  GHOST   {g.embedding_id}  ({g.note})")
        lines.append("  --apply will REFUSE while these exist. Investigate before proceeding.")
    lines.append("")

    if report.is_clean():
        lines.append("EXIT: no drift detected — index is in sync.")
    elif report.has_unknowns():
        lines.append(f"EXIT: drift with {report.unknown_count} unknown chunk(s) — --apply blocked.")
    else:
        lines.append(f"EXIT: drift detected — run --apply to repair ({report.sql_only_count} chunks will be re-embedded)")
        if report.hnsw_only_count:
            lines.append(f"      ({report.hnsw_only_count} HNSW-only ghosts NOT repaired in this version — see --prune-hnsw-ghosts)")
    return "\n".join(lines)


def format_report_json(report: DriftReport, delta: Delta | None = None) -> dict[str, Any]:
    out: dict[str, Any] = report.to_dict()
    if delta is not None:
        out["delta"] = asdict(delta)
    return out


# ============================================================
# PRECONDITIONS for --apply
# ============================================================


def is_in_maintenance_mode(lock_path: str | None = None) -> bool:
    if lock_path is None:
        lock_path = MAINTENANCE_LOCK
    return os.path.exists(lock_path)


def find_recent_backup(
    backups_dir: str | None = None,
    db_dir: str | None = None,
    max_age_seconds: int = BACKUP_MAX_AGE_SECONDS,
) -> str | None:
    """Look for a chroma backup with mtime within max_age_seconds. Returns path or None."""
    if backups_dir is None:
        backups_dir = BACKUPS_DIR
    if db_dir is None:
        db_dir = DB_DIR
    candidates: list[tuple[float, str]] = []
    if os.path.isdir(backups_dir):
        for fname in os.listdir(backups_dir):
            if fname.startswith("chroma_") and fname.endswith(".sqlite3.gz"):
                full = os.path.join(backups_dir, fname)
                try:
                    candidates.append((os.path.getmtime(full), full))
                except OSError:
                    pass
    if os.path.isdir(db_dir):
        for fname in os.listdir(db_dir):
            if fname.startswith("chroma.sqlite3.backup."):
                full = os.path.join(db_dir, fname)
                try:
                    candidates.append((os.path.getmtime(full), full))
                except OSError:
                    pass
    if not candidates:
        return None
    candidates.sort(reverse=True)
    mtime, path = candidates[0]
    if (time.time() - mtime) > max_age_seconds:
        return None
    return path


# ============================================================
# REPAIR (--apply)
# ============================================================


# Metadata keys that are NOT user metadata — Chroma reserves these / we
# manage them through other args. Strip before passing to collection.upsert(metadatas=...).
_RESERVED_META_KEYS = {"__int_id__", "chroma:document"}


def _build_embedding_text(meta: dict[str, Any], doc_text: str) -> str:
    """Reconstruct the text that ingest_docs.py would have used for embedding.

    For document chunks (conv_id=doc_<hash>), the original embedding was computed
    over `f"{prefix}\\n\\n{text}"` where prefix is built from folder/doc_type/title.
    For doc_summary chunks, the prefix was already baked into chroma:document
    by the summary writer, so we use chroma:document as-is.

    For other sources (conversations, emails), there's no document-style prefix;
    the original text in chroma:document is what was embedded.
    """
    conv_id = meta.get("conv_id") or ""
    source = meta.get("source")
    if source == "document" and conv_id.startswith("doc_") and not conv_id.startswith("doc_summary_"):
        folder = meta.get("folder") or ""
        doc_type = meta.get("doc_type") or ""
        title = meta.get("title") or ""
        prefix_parts = []
        if folder:
            prefix_parts.append(f"Cartella: {folder}")
        if doc_type:
            prefix_parts.append(f"Tipo: {doc_type}")
        prefix_parts.append(f"File: {title}")
        prefix = " | ".join(prefix_parts)
        return f"{prefix}\n\n{doc_text}"
    return doc_text


def _strip_metadata_for_upsert(meta: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in meta.items() if k not in _RESERVED_META_KEYS}


def _ensure_maintenance_log_row(con: sqlite3.Connection, mode: str, report: DriftReport, action: str, extra: dict[str, Any] | None = None) -> None:
    """Insert one audit row into maintenance_log. Schema is minimal
    (id INT PK, timestamp INT, operation TEXT) — encode the rest as JSON
    in operation."""
    payload = {
        "tool": "repair_hnsw_index.py",
        "mode": mode,
        "drift_summary": {
            "sql_only_count": report.sql_only_count,
            "hnsw_only_count": report.hnsw_only_count,
            "queue_total": report.queue_total,
            "unknown_count": report.unknown_count,
        },
        "action_taken": action,
    }
    if extra:
        payload.update(extra)
    cur = con.cursor()
    cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM maintenance_log")
    next_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO maintenance_log (id, timestamp, operation) VALUES (?, ?, ?)",
        (next_id, int(time.time()), json.dumps(payload, sort_keys=True)),
    )
    con.commit()


def repair(
    report: DriftReport,
    *,
    embed_fn,
    collection,
    batch_size: int = UPSERT_BATCH_SIZE,
    progress=None,
) -> dict[str, Any]:
    """Re-embed and upsert all SQL-only orphans. Caller is responsible for
    preconditions (maintenance mode, backup) and for the collection handle."""
    if report.has_unknowns():
        raise RuntimeError(
            f"Cannot --apply while {report.unknown_count} unknown drift entries exist. "
            f"Investigate the UNEXPECTED DRIFT TYPES section of the audit first."
        )
    if not report.sql_only_orphans:
        return {"repaired": 0, "batches": 0, "skipped_oversize": 0}

    repaired = 0
    batches = 0
    skipped_oversize = 0
    repaired_ids: list[str] = []

    orphans = report.sql_only_orphans
    for batch_start in range(0, len(orphans), batch_size):
        batch = orphans[batch_start : batch_start + batch_size]
        ids: list[str] = []
        texts: list[str] = []
        documents: list[str] = []
        metadatas: list[dict[str, Any]] = []
        for o in batch:
            doc_text = o.metadata.get("chroma:document") or ""
            if not doc_text:
                # No text to embed; cannot recover this chunk via re-embed
                skipped_oversize += 1
                continue
            if len(doc_text) > MAX_TEXT_CHARS:
                # ingest_docs truncates documents to MAX_TEXT_CHARS, but the original
                # embedding was over the full pre-truncation text; we cannot exactly
                # reproduce that vector here. Skip and surface for human review.
                skipped_oversize += 1
                continue
            ids.append(o.embedding_id)
            texts.append(_build_embedding_text(o.metadata, doc_text))
            documents.append(doc_text)
            metadatas.append(_strip_metadata_for_upsert(o.metadata))

        if not ids:
            continue

        embeddings = embed_fn(texts)
        collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas,
        )
        repaired += len(ids)
        repaired_ids.extend(ids)
        batches += 1
        if progress:
            progress(repaired, len(orphans))

    return {
        "repaired": repaired,
        "batches": batches,
        "skipped_oversize": skipped_oversize,
        "repaired_ids": repaired_ids,
    }


# ============================================================
# CLI ENTRY
# ============================================================


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="HNSW index drift audit + repair tool.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--audit", action="store_true", help="Read-only diagnosis (default)")
    mode.add_argument("--apply", action="store_true", help="Repair SQL-only orphans (mutates)")
    p.add_argument("--prune-hnsw-ghosts", action="store_true",
                   help="Remove HNSW-only ghosts (NOT IMPLEMENTED — see docs)")
    p.add_argument("--json", action="store_true", help="Emit JSON instead of human-readable")
    p.add_argument("--no-history", action="store_true", help="Do not write audit history JSON")
    return p.parse_args(argv)


def _print(text: str, json_mode: bool) -> None:
    if not json_mode:
        print(text)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.prune_hnsw_ghosts:
        raise NotImplementedError(
            "--prune-hnsw-ghosts is deferred to a future release. See "
            "scripts/maintenance/repair_hnsw_index.md (Open question #3 from "
            "the Phase A diagnosis) for design notes. The 50 known HNSW-only "
            "ghosts have no functional impact (they cannot be reached without "
            "the SQL row that resolves the result), so this can wait."
        )

    apply_mode = args.apply

    # Audit always runs first
    report = audit()
    delta = None
    if not args.no_history:
        history_path = write_audit_history(report)
        prev = find_recent_audit(exclude=history_path)
        if prev:
            delta = compute_delta(prev, report)

    if args.json:
        out = format_report_json(report, delta)
        out["preconditions"] = {
            "maintenance_mode": is_in_maintenance_mode(),
            "recent_backup": find_recent_backup(),
        }
        if apply_mode:
            out["apply_attempted"] = True
        print(json.dumps(out, indent=2, default=str))
    else:
        print(format_report_human(report, delta))

    # Log audit run regardless of mode (best effort — read-only DB still has wal/journal allowed via separate connection)
    try:
        con = _open_sqlite_rw(DB_PATH)
        try:
            _ensure_maintenance_log_row(con, mode="audit", report=report, action="audit-only")
        finally:
            con.close()
    except Exception as e:
        # Audit must not fail because logging failed
        if not args.json:
            print(f"\n[warn] could not write maintenance_log row: {e}", file=sys.stderr)

    if not apply_mode:
        return 0 if report.is_clean() else 1

    # ----- APPLY MODE -----
    if not is_in_maintenance_mode():
        msg = (
            f"\nREFUSING --apply: MCP is not in maintenance mode "
            f"(no lock at {MAINTENANCE_LOCK}).\n"
            f"  To enter maintenance mode: kill -USR1 <mcp_pid>\n"
            f"  To exit:                   kill -USR2 <mcp_pid>"
        )
        _print(msg, args.json)
        if args.json:
            print(json.dumps({"error": "not_in_maintenance_mode", "lock_path": MAINTENANCE_LOCK}))
        return 2

    backup = find_recent_backup()
    if not backup:
        msg = (
            f"\nREFUSING --apply: no recent backup (<{BACKUP_MAX_AGE_SECONDS // 60} min) found in\n"
            f"  {BACKUPS_DIR}/chroma_*.sqlite3.gz  or\n"
            f"  {DB_DIR}/chroma.sqlite3.backup.*\n"
            f"  Take a backup first: bash scripts/maintenance/backup_db.sh"
        )
        _print(msg, args.json)
        if args.json:
            print(json.dumps({"error": "no_recent_backup"}))
        return 3

    if report.has_unknowns():
        msg = (
            f"\nREFUSING --apply: {report.unknown_count} unknown drift entries found.\n"
            f"  See UNEXPECTED DRIFT TYPES section above. Investigate before proceeding."
        )
        _print(msg, args.json)
        if args.json:
            print(json.dumps({"error": "unknown_drift_types", "count": report.unknown_count}))
        return 4

    if report.is_clean():
        _print("\nNothing to repair. Exiting.", args.json)
        return 0

    # Lazy imports — only needed for apply
    sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
    import chromadb  # noqa: E402
    from embedding import get_embeddings  # noqa: E402
    import chroma_persist  # noqa: E402

    client = chromadb.PersistentClient(path=DB_DIR)
    collection = client.get_or_create_collection(name=COLLECTION_NAME, metadata={"hnsw:space": "cosine"})

    def _progress(done: int, total: int) -> None:
        if not args.json:
            print(f"  re-embedded {done}/{total}", flush=True)

    # Pre-apply pickle backup — internal safety net additive to the external
    # backup gate (L867-878). Taken only when repair() will do real work
    # (sql_only_orphans non-empty); on failure, abort BEFORE repair() touches
    # HNSW. Naming follows docs/conventions.md: <file>.backup.<reason>_<ts>.
    if report.sql_only_orphans:
        pre_apply_pickle = _resolve_hnsw_pickle_path(DB_PATH, DB_DIR)
        if pre_apply_pickle is None:
            msg = (
                f"\nFATAL: cannot resolve HNSW pickle path for pre-apply backup "
                f"(segments lookup or pickle file missing). Refusing to run repair()."
            )
            _print(msg, args.json)
            if args.json:
                print(json.dumps({"error": "pre_apply_backup_failed", "reason": "pickle_not_found"}))
            return 6
        backup_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        pre_apply_dest = f"{pre_apply_pickle}.backup.pre_apply_{backup_ts}"
        try:
            shutil.copy2(pre_apply_pickle, pre_apply_dest)
        except (IOError, OSError) as e:
            msg = (
                f"\nFATAL: pre-apply pickle backup failed "
                f"({type(e).__name__}: {e}). Refusing to run repair()."
            )
            _print(msg, args.json)
            if args.json:
                print(json.dumps({"error": "pre_apply_backup_failed", "reason": f"{type(e).__name__}: {e}"}))
            return 6
        _print(f"\nPre-apply backup: {pre_apply_dest}", args.json)

    _print(f"\nRe-embedding {report.sql_only_count} orphan chunks via collection.upsert()...", args.json)
    result = repair(report, embed_fn=get_embeddings, collection=collection, progress=_progress)

    # Auto-persist: force chromadb to flush HNSW state to disk so the repair
    # survives the next restart even if shutdown is unclean. Skipped when
    # nothing was repaired (no ids to replay).
    auto_persist: dict[str, Any] = {"triggered": False}
    repaired_ids = result.get("repaired_ids") or []
    if repaired_ids:
        pickle_path = _resolve_hnsw_pickle_path(DB_PATH, DB_DIR)
        _print(
            f"\nForcing HNSW persist via {chroma_persist.DEFAULT_TARGET_WRITES} idempotent re-upserts "
            f"of the {len(repaired_ids)} repaired chunks...",
            args.json,
        )
        persist_result = chroma_persist.force_chroma_persist(
            collection, repaired_ids, pickle_path=pickle_path,
        )
        auto_persist = {
            "triggered": True,
            "iterations": persist_result["iterations"],
            "total_writes": persist_result["total_writes"],
            "mtime_advanced": persist_result["mtime_advanced"],
            "success": persist_result["success"],
        }

    # Re-audit to confirm
    post = audit()
    success = post.sql_only_count == 0

    try:
        con = _open_sqlite_rw(DB_PATH)
        try:
            _ensure_maintenance_log_row(
                con,
                mode="apply",
                report=report,
                action=f"repaired {result['repaired']} chunks in {result['batches']} batches"
                       + (f" (skipped {result['skipped_oversize']} oversize)" if result['skipped_oversize'] else ""),
                extra={
                    "post_apply_sql_only_count": post.sql_only_count,
                    "backup_used": os.path.basename(backup),
                    "auto_persist": auto_persist,
                },
            )
        finally:
            con.close()
    except Exception as e:
        if not args.json:
            print(f"\n[warn] could not write maintenance_log row: {e}", file=sys.stderr)

    if args.json:
        print(json.dumps({
            "apply": result,
            "auto_persist": auto_persist,
            "post_apply_sql_only_count": post.sql_only_count,
            "success": success,
        }, indent=2))
    else:
        print(f"\nRepair result: {result}")
        if auto_persist.get("triggered"):
            print(
                f"Auto-persist: {auto_persist['iterations']} iterations × {len(repaired_ids)} ids "
                f"= {auto_persist['total_writes']} writes; "
                f"mtime_advanced={auto_persist['mtime_advanced']}"
            )
        print(f"Post-apply SQL-only orphans: {post.sql_only_count}")
        if success:
            print("OK — drift is now zero.")
        else:
            print("WARN — drift remains. Investigate. Re-run --audit.")

    return 0 if success else 5


if __name__ == "__main__":
    sys.exit(main())
