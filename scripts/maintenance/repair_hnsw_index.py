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
  python scripts/maintenance/repair_hnsw_index.py --flush-queue-backlog
                                                               # force HNSW flush (mutates)
  python scripts/maintenance/repair_hnsw_index.py --json     # machine output

Audit is read-only and side-effect-free except for an audit-history JSON
file under logs/hnsw_audits/ and (optionally) a row in maintenance_log.
Apply requires the MCP to be in maintenance mode (SIGUSR1 already issued)
and a backup taken within the last 60 minutes.

`--flush-queue-backlog` is a targeted workaround for chromadb 1.5.9
chroma-core/chroma#6975: PersistentLocalHnswSegment only persists after
`hnsw:sync_threshold` cumulative writes. Queries and nightly reads do not
force the flush. The 2026-05-30 live incident confirmed drift 821 -> 21
after an idempotent burst, while `--apply` was a no-op because all lagging
ids were still in embeddings_queue rather than SQL-only orphans.

A plain restart does NOT drain the backlog: at boot the segment loads its
persisted pickle at the old max_seq_id and only persists again past
`hnsw:sync_threshold` cumulative writes. ChromaDB 1.5.9 does NOT drain on
shutdown either: the PersistentClient lives in mcp_server module globals,
detached from the uvicorn lifespan, so neither a graceful restart
(`bootout`/`bootstrap`) nor a `launchctl kickstart -k` (SIGKILL) triggers a
`System.stop()`/atexit persist — the queue survives the restart untouched
(verified 2026-06-10; see RUNBOOK_drift_alert.md §0). Only a threshold-crossing
burst (this mode) drains it.

Actionability is therefore gated on EITHER seq-drift >= DRIFT_WARNING OR a
queue backlog >= FLUSH_QUEUE_BACKLOG_MIN (configurable via
--queue-backlog-min): a large #6975 backlog is worth flushing even when
seq-drift alone is still below WARNING.

Restart-aware: when a previous audit JSON exists from <2h ago, the report
includes a delta section so race-induced drift around an MCP restart is
visible.
"""

from __future__ import annotations

import argparse
import json
import math
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

# Contratto unico caricato per FILE via importlib: modulo puro (nessuna
# dipendenza), quindi niente sys.path.insert(scripts/lib) a load-time. Evita di
# esporre `config`, `embedding`, ecc. come nomi top-level (shadowing) quando
# repair_hnsw_index viene importato (test, auto_flush). E' la fonte UNICA del
# taglio del testo da embeddare.
import importlib.util as _ilu  # noqa: E402
_ec_spec = _ilu.spec_from_file_location(
    "_tailor_embedding_contract",
    os.path.join(BASE_DIR, "scripts", "lib", "embedding_contract.py"),
)
_ec_mod = _ilu.module_from_spec(_ec_spec)
_ec_spec.loader.exec_module(_ec_mod)
embedding_text = _ec_mod.embedding_text
MAX_EMBED_CHARS = _ec_mod.MAX_EMBED_CHARS
del _ec_spec, _ec_mod

UPSERT_BATCH_SIZE = 10
DELTA_WINDOW_SECONDS = 7200  # 2 hours
BACKUP_MAX_AGE_SECONDS = 3600  # 60 minutes
DRIFT_WARNING = 800
FLUSH_BURST_MARGIN = 100
DEFAULT_HNSW_SYNC_THRESHOLD = 1000
# Cap delle iterazioni di top-up allineamento nel flush (#6975 fix A). Ogni
# iterazione chiude al più un residuo < sync_threshold, quindi in maintenance
# esclusiva l'allineamento converge in 1 passata; il cap è una rete di sicurezza
# se processed < richiesto (skip/error) o se Chroma cambia il timing di persist.
MAX_TOPUP_ITERS = 5
# A clean #6975 backlog can be actionable even when seq-drift is below
# DRIFT_WARNING: a large embeddings_queue that no plain restart drains is itself
# a reason to flush. Conservative default; override with --queue-backlog-min.
FLUSH_QUEUE_BACKLOG_MIN = 300


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
    benign: bool = False                    # net pending WAL op (sopra watermark) = DELETE
    pending_delete_seq: int | None = None   # seq_id di quel DELETE (forense)


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
    collection_queue_pending_count: int = 0
    collection_name: str = COLLECTION_NAME  # collection auditata (per consumer della cache audit)
    collection_topic: str | None = None
    collection_topic_ambiguous: bool = False
    sql_only_orphans: list[OrphanChunk] = field(default_factory=list)
    hnsw_only_ghosts: list[GhostChunk] = field(default_factory=list)
    unknown_count: int = 0  # orphans + ghosts of class "unknown"
    # Seq-drift esposto per i consumer (auto_flush_queue): additivo, nessuna
    # logica gate del tool dipende da questi campi.
    drift: int = 0                       # metadata_seq - vector_seq
    vector_seq: int = 0                  # = vector_max_seq_id (watermark HNSW)
    metadata_seq: int = 0                # seq_id del segmento METADATA
    drift_warning: int = DRIFT_WARNING   # soglia di allerta (costante)

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

    # ── Fix 1: split benigni / anomali ──────────────────────────────────────
    @property
    def benign_ghosts(self) -> list[GhostChunk]:
        return [g for g in self.hnsw_only_ghosts if g.benign]

    @property
    def anomalous_ghosts(self) -> list[GhostChunk]:
        return [g for g in self.hnsw_only_ghosts if not g.benign]

    @property
    def benign_ghost_count(self) -> int:
        return len(self.benign_ghosts)

    @property
    def anomalous_ghost_count(self) -> int:
        return len(self.anomalous_ghosts)

    @property
    def blocking_unknown_count(self) -> int:
        """Unknown bloccanti: orphan SQL-only unknown + ghost ANOMALI unknown.
        I ghost benigni unknown NON bloccano (il WAL ne ha già il DELETE)."""
        return (sum(1 for o in self.sql_only_orphans if o.classification == "unknown")
                + sum(1 for g in self.anomalous_ghosts if g.classification == "unknown"))

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


def _load_segment_ids(con: sqlite3.Connection, collection_name: str = COLLECTION_NAME) -> tuple[str, str]:
    """Return (metadata_segment_id, vector_segment_id) for the named collection.

    The collection filter is critical when the database hosts more than one
    collection (e.g. during the tailor_kb → tailor_kb_v2 migration window).
    Without it, the first segment encountered for each scope was returned,
    which silently pointed audits at the wrong (stale) collection — see
    bug discovered 2026-05-24.
    """
    cur = con.cursor()
    cur.execute("""
        SELECT s.id FROM segments s
        JOIN collections c ON c.id = s.collection
        WHERE s.scope = 'METADATA' AND c.name = ?
    """, (collection_name,))
    meta_row = cur.fetchone()
    cur.execute("""
        SELECT s.id FROM segments s
        JOIN collections c ON c.id = s.collection
        WHERE s.scope = 'VECTOR' AND c.name = ?
    """, (collection_name,))
    vec_row = cur.fetchone()
    if not meta_row or not vec_row:
        raise RuntimeError(
            f"Could not locate METADATA and VECTOR segments for collection '{collection_name}'"
        )
    return meta_row[0], vec_row[0]


def _load_collection_id(con: sqlite3.Connection, collection_name: str = COLLECTION_NAME) -> str:
    cur = con.cursor()
    cur.execute("SELECT id FROM collections WHERE name = ?", (collection_name,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Could not locate collection '{collection_name}'")
    return row[0]


def _decode_seq_id(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return int(value)


def _load_segment_seq(con: sqlite3.Connection, segment_id: str) -> int:
    cur = con.cursor()
    cur.execute("SELECT seq_id FROM max_seq_id WHERE segment_id = ?", (segment_id,))
    row = cur.fetchone()
    return _decode_seq_id(row[0]) if row else 0


def _load_seq_state(
    con: sqlite3.Connection,
    *,
    collection_name: str = COLLECTION_NAME,
) -> dict[str, Any]:
    collection_id = _load_collection_id(con, collection_name)
    meta_seg, vec_seg = _load_segment_ids(con, collection_name)
    vector_seq = _load_segment_seq(con, vec_seg)
    metadata_seq = _load_segment_seq(con, meta_seg)
    return {
        "collection_id": collection_id,
        "metadata_segment": meta_seg,
        "vector_segment": vec_seg,
        "vector_seq": vector_seq,
        "metadata_seq": metadata_seq,
        "drift": metadata_seq - vector_seq,
    }


def _load_hnsw_sync_threshold(
    con: sqlite3.Connection,
    *,
    collection_id: str,
    vector_segment: str,
) -> int:
    """Return hnsw:sync_threshold, falling back to Chroma's 1.x default.

    Collection-level metadata is checked first because Tailor creates
    collections through Chroma's public API. Segment metadata is kept as a
    fallback for schema/version differences.
    """
    cur = con.cursor()
    for table, id_col, id_value in (
        ("collection_metadata", "collection_id", collection_id),
        ("segment_metadata", "segment_id", vector_segment),
    ):
        try:
            cur.execute(
                f"SELECT int_value FROM {table} WHERE {id_col} = ? AND key = ?",
                (id_value, "hnsw:sync_threshold"),
            )
            row = cur.fetchone()
        except sqlite3.OperationalError:
            row = None
        if row and row[0] is not None:
            return int(row[0])
    return DEFAULT_HNSW_SYNC_THRESHOLD


def _load_existing_embedding_ids(con: sqlite3.Connection, meta_seg: str, limit: int) -> list[str]:
    cur = con.cursor()
    cur.execute(
        """
        SELECT embedding_id
        FROM embeddings
        WHERE segment_id = ?
        ORDER BY id
        LIMIT ?
        """,
        (meta_seg, limit),
    )
    return [row[0] for row in cur.fetchall()]


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


def _resolve_collection_topic(
    con: sqlite3.Connection,
    collection_id: str,
) -> tuple[str | None, bool]:
    """Topic WAL esatto della collection (chromadb: persistent://<tenant>/<db>/<cid>).
    Lo ricava dal suffisso /<collection_id> (UUID → unico) invece di costruirlo,
    così è robusto a tenant/database diversi.

    Ritorna (topic, ambiguous):
      * 0 match  → (None, False): nessuna op pendente per la collection.
      * 1 match  → (topic, False): topic risolto.
      * >1 match → (None, True): stato ambiguo, non-actionable.
    """
    cur = con.cursor()
    cur.execute("SELECT DISTINCT topic FROM embeddings_queue WHERE topic LIKE ?",
                ("%/" + collection_id,))
    # endswith esatto: difesa contro il wildcard '_' di LIKE
    topics = [t for (t,) in cur.fetchall() if t and t.endswith("/" + collection_id)]
    if not topics:
        return None, False
    if len(topics) == 1:
        return topics[0], False
    return None, True


def _load_final_queue_ops(
    con: sqlite3.Connection,
    *,
    topic: str,
    vector_max_seq_id: int,
) -> dict[str, tuple[int, int]]:
    """NET op pendente per id, SOLO per `topic` (la collection auditata) e SOLO
    per righe con seq_id > vector_max_seq_id (la finestra che Chroma rileggerà
    davvero al prossimo consume). {embedding_id: (operation, seq_id)} dal seq_id
    PIÙ ALTO. Encoding WAL: 2 = upsert, 3 = delete.

    Le due scoping sono entrambe necessarie:
      * topic        → gli id sono unici solo DENTRO una collection.
      * seq_id window → un DELETE <= watermark NON verrà riapplicato (è il ghost
                        anomalo classico: delete perso dall'HNSW), quindi NON
                        rende benigno."""
    cur = con.cursor()
    cur.execute(
        "SELECT id, operation, seq_id FROM embeddings_queue "
        "WHERE topic = ? AND seq_id > ? ORDER BY seq_id",
        (topic, vector_max_seq_id),
    )
    final: dict[str, tuple[int, int]] = {}
    for eid, op, seq in cur.fetchall():
        final[eid] = (int(op), int(seq))  # seq crescente → l'ultimo sovrascrive
    return final


def audit(db_path: str | None = None, db_dir: str | None = None) -> DriftReport:
    """Read-only diagnosis. Returns a DriftReport."""
    if db_path is None:
        db_path = DB_PATH
    if db_dir is None:
        db_dir = DB_DIR
    con = _open_sqlite_ro(db_path)
    try:
        # Una singola read transaction mantiene tutte le SELECT sulla stessa
        # fotografia WAL per l'intera diagnosi.
        con.execute("BEGIN")
        meta_seg, vec_seg = _load_segment_ids(con)
        collection_id = _load_collection_id(con)
        vector_max_seq_id = _load_segment_seq(con, vec_seg)
        metadata_seq = _load_segment_seq(con, meta_seg)
        drift = metadata_seq - vector_max_seq_id
        our_topic, topic_ambiguous = _resolve_collection_topic(con, collection_id)
        final_ops = (
            _load_final_queue_ops(con, topic=our_topic, vector_max_seq_id=vector_max_seq_id)
            if our_topic else {}
        )

        cur = con.cursor()
        cur.execute("SELECT embedding_id FROM embeddings WHERE segment_id=?", (meta_seg,))
        sql_ids = {r[0] for r in cur.fetchall()}
        hnsw_ids = _load_hnsw_pickle_ids(db_dir, vec_seg)

        pending_ids = set(final_ops)
        sql_only = sql_ids - hnsw_ids - pending_ids
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
            op_seq = final_ops.get(eid)
            benign = op_seq is not None and op_seq[0] == 3
            ghosts.append(GhostChunk(
                embedding_id=eid, classification=cls, note=note,
                benign=benign,
                pending_delete_seq=op_seq[1] if benign else None,
            ))

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
            collection_queue_pending_count=len(final_ops),
            collection_topic=our_topic,
            collection_topic_ambiguous=topic_ambiguous,
            sql_only_orphans=orphans,
            hnsw_only_ghosts=ghosts,
            unknown_count=unknown,
            drift=drift,
            vector_seq=vector_max_seq_id,
            metadata_seq=metadata_seq,
            drift_warning=DRIFT_WARNING,
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
    lines.append(
        f"SQL total: {report.sql_total}    HNSW total: {report.hnsw_total}    "
        f"Queue: {report.queue_total} (collection pending: {report.collection_queue_pending_count})"
    )
    if report.collection_topic_ambiguous:
        lines.append("WARNING: collection WAL topic is ambiguous — mutating modes are blocked.")
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
        lines.append(f"  Drained ONLY by a threshold-crossing burst (--flush-queue-backlog).")
        lines.append(f"  No restart drains it — neither a graceful restart nor a `kickstart -k`/SIGKILL:")
        lines.append(f"  ChromaDB 1.5.9 does not persist on shutdown, so boot reloads the old pickle")
        lines.append(f"  and the backlog stays.")
        lines.append(f"  WARNING: queue consumption may itself trigger drift if HNSW persist races")
        lines.append(f"  with shutdown. Re-run audit after any restart/flush to verify.")
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

    if report.collection_topic_ambiguous:
        lines.append("EXIT: audit diagnostic only — collection WAL topic ambiguous; mutating modes blocked.")
    elif report.is_clean():
        lines.append("EXIT: no drift detected — index is in sync.")
    elif report.has_unknowns():
        lines.append(f"EXIT: drift with {report.unknown_count} unknown chunk(s) — --apply blocked.")
    else:
        lines.append(f"EXIT: drift detected — run --apply to repair ({report.sql_only_count} chunks will be re-embedded)")
        if report.hnsw_only_count:
            lines.append(f"      ({report.hnsw_only_count} HNSW-only ghosts non riparati in questa versione — pruning deferred; investigare / rebuild manuale)")
    return "\n".join(lines)


def format_report_json(report: DriftReport, delta: Delta | None = None) -> dict[str, Any]:
    out: dict[str, Any] = report.to_dict()
    # asdict() serializza i campi (inclusi g["benign"]/g["pending_delete_seq"]) ma
    # NON le @property → li aggiungo per i consumer JSON (auto_flush_queue).
    out["benign_ghost_count"] = report.benign_ghost_count
    out["anomalous_ghost_count"] = report.anomalous_ghost_count
    out["blocking_unknown_count"] = report.blocking_unknown_count
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
    preconditions (maintenance mode, backup) and for the collection handle.

    IMPORTANTE — --apply CANONICALIZZA, NON ripristina. Il testo viene
    embeddato via il contratto unico (nudo, troncato a MAX_EMBED_CHARS). Per i
    chunk piu' lunghi di MAX_EMBED_CHARS il vettore ricostruito NON coincide con
    quello storico: e' una MIGRAZIONE al contratto, non un restore bit-identico.
    Quei chunk sono contati a parte in `migrated_truncated` e dichiarati nel
    maintenance_log. La post-condizione sql_only_count==0 verifica la presenza
    in HNSW, NON l'uguaglianza al vettore originale — by design.
    """
    if report.collection_topic_ambiguous:
        raise RuntimeError("Cannot --apply while the collection WAL topic is ambiguous.")
    if report.has_unknowns():
        raise RuntimeError(
            f"Cannot --apply while {report.unknown_count} unknown drift entries exist. "
            f"Investigate the UNEXPECTED DRIFT TYPES section of the audit first."
        )
    if not report.sql_only_orphans:
        return {"repaired": 0, "batches": 0, "skipped_empty": 0, "migrated_truncated": 0}

    repaired = 0
    batches = 0
    skipped_empty = 0
    migrated_truncated = 0
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
                # Nessun testo da embeddare: chunk non ricostruibile via re-embed.
                # Questo e' l'UNICO caso di skip rimasto dopo la rimozione del
                # guard >4000 (il contratto tronca, quindi i chunk lunghi sono
                # ricostruibili). La chiave-risultato si chiama `skipped_empty`:
                # conta cio' che conta davvero (documento vuoto), non "oversize".
                skipped_empty += 1
                continue
            # Contratto unico: embedda e salva lo STESSO testo (nudo, troncato).
            # Niente piu' skip sui chunk lunghi: il contratto tronca, quindi il
            # chunk e' ricostruibile (prima > MAX_EMBED_CHARS restavano orfani in
            # silenzio). documents == testo embeddato: non salviamo cio' che non
            # abbiamo embeddato.
            embed_text = embedding_text(o.metadata.get("source"), o.metadata, doc_text)
            if len(doc_text) > MAX_EMBED_CHARS:
                # Chunk piu' lungo del contratto: il vettore ricostruito e'
                # troncato e DIVERSO dallo storico -> canonicalizzazione al
                # contratto, non ripristino. Contato a parte e dichiarato.
                migrated_truncated += 1
            ids.append(o.embedding_id)
            texts.append(embed_text)
            documents.append(embed_text)
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
        "skipped_empty": skipped_empty,
        "migrated_truncated": migrated_truncated,
        "repaired_ids": repaired_ids,
    }


# ============================================================
# FLUSH QUEUE BACKLOG (--flush-queue-backlog)
# ============================================================


class OllamaNomicEmbeddingFunction:
    """Explicit Chroma embedding function for the #6975 flush path.

    ChromaDB 1.5.9 validates embedding function names on `get_collection()`.
    Tailor's collection was persisted with the name "default", so this class
    deliberately reports that name while routing calls through
    scripts/lib/embedding.py (Ollama nomic-embed-text, 768d). This mirrors the
    2026-05-30 live burst prototype that resolved drift 821 -> 21.
    """

    def __init__(self, embed_fn) -> None:
        self._embed_fn = embed_fn

    def name(self) -> str:
        return "default"

    def __call__(self, input: list[str]) -> list[list[float]]:
        embeddings = self._embed_fn(list(input))
        for i, embedding in enumerate(embeddings):
            if len(embedding) != 768:
                raise ValueError(f"embedding[{i}] has {len(embedding)} dims, expected 768")
        return embeddings


def flush_queue_backlog(
    *,
    collection,
    chunk_ids: list[str],
    progress=None,
) -> dict[str, Any]:
    """Force ChromaDB #6975 HNSW persist via idempotent re-upsert burst.

    MECCANISMO REALE (NON un bump del processo MCP vivo):
      * PersistentClient SEPARATO mentre MCP è in maintenance (del client su
        SIGUSR1). Race a due client evitata dall'handoff —
        [[project_chromadb_1_5_8_segfault_race_condition]].
      * Il segmento HNSW fresco riprende dal pickle persistito a vector_max_seq_id
        e ri-consuma dal WAL SOLO i record della PROPRIA collection
        (topic persistent://<tenant>/<db>/<cid>) con seq_id > vector_max_seq_id.
        NON "l'intera queue": altri topic e i record sotto il watermark sono ignorati.
      * I record di backlog riletti INCREMENTANO il contatore persist (non solo le
        re-upsert idempotenti qui sotto). Al superamento di hnsw:sync_threshold
        scatta _persist(): scrive il pickle coi DELETE riconsumati applicati e
        avanza VECTOR max_seq_id.
      * La queue si drena come SIDE EFFECT (purge quando METADATA+VECTOR superano la
        riga). Per questo il burst pulisce anche i ghost benigni (DELETE sopra il
        watermark) oltre al seq-drift.
      * MCP lo vede solo dopo lazy-reopen (prima query post-SIGUSR2) o restart.

    Il chiamante (main, ramo --flush-queue-backlog) dimensiona burst_n ALLINEATO:
    (drift + burst_n) = più piccolo multiplo di sync_threshold >= drift + margin,
    così che l'ultimo _persist() atterri sulla testa del WAL → residuo 0 (es. reale
    2026-06-16: drift=500 → burst_n=500, non un fisso >= sync_threshold). Eventuali
    skip/error che riducono i record consumati sono chiusi dal loop di top-up.
    """
    processed = 0
    # Due guasti DIVERSI, contatori distinti: id non risolto vs vettore assente.
    skipped_missing_id = 0
    skipped_missing_embedding = 0
    errors = 0

    for chunk_id in chunk_ids:
        try:
            fetched = collection.get(
                ids=[chunk_id],
                include=["embeddings", "documents", "metadatas"],
            )
            ids = fetched.get("ids") or []
            if not ids:
                skipped_missing_id += 1
                print(f"[flush-queue-backlog] skip missing id: {chunk_id}", file=sys.stderr)
                continue

            # Vettori ESPLICITI riletti dalla collection (come force_chroma_persist).
            # Senza `embeddings=`, ChromaDB ri-embedda documents[0] via
            # embedding_function -> writer semantico travestito da manutenzione
            # (il bug: 3.058 chunk ri-embeddati dal 12/06). Se il record NON ha
            # un vettore, NON upsertarlo senza vettore (ricadresti nel bug):
            # salta, conta, logga.
            embeddings = list(fetched.get("embeddings") or [])
            if not embeddings or embeddings[0] is None or len(embeddings[0]) == 0:
                skipped_missing_embedding += 1
                print(f"[flush-queue-backlog] skip missing embedding: {chunk_id}", file=sys.stderr)
                continue

            documents = fetched.get("documents") or [""]
            metadatas = fetched.get("metadatas") or [{}]
            collection.upsert(
                ids=[ids[0]],
                embeddings=[embeddings[0]],
                documents=[documents[0]],
                metadatas=[metadatas[0]],
            )
            processed += 1
            if progress and processed % 100 == 0:
                progress(processed, len(chunk_ids),
                         skipped_missing_id + skipped_missing_embedding, errors)
        except Exception as e:
            errors += 1
            print(f"[flush-queue-backlog] error {chunk_id}: {type(e).__name__}: {e}", file=sys.stderr)

    return {
        "processed": processed,
        # `skipped` = somma, retro-compatibile (top-up, progress, success).
        "skipped": skipped_missing_id + skipped_missing_embedding,
        "skipped_missing_id": skipped_missing_id,
        "skipped_missing_embedding": skipped_missing_embedding,
        "errors": errors,
        "candidate_ids": len(chunk_ids),
    }


# ============================================================
# CLI ENTRY
# ============================================================


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="HNSW index drift audit + repair tool.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--audit", action="store_true", help="Read-only diagnosis (default)")
    mode.add_argument("--apply", action="store_true", help="Repair SQL-only orphans (mutates)")
    mode.add_argument(
        "--flush-queue-backlog",
        action="store_true",
        help="Force a ChromaDB #6975 HNSW flush when backlog is still in embeddings_queue (mutates)",
    )
    p.add_argument("--prune-hnsw-ghosts", action="store_true",
                   help="Remove HNSW-only ghosts (NOT IMPLEMENTED — see docs)")
    p.add_argument(
        "--queue-backlog-min",
        type=int,
        default=FLUSH_QUEUE_BACKLOG_MIN,
        metavar="N",
        help=(
            "Queue-backlog size that makes --flush-queue-backlog actionable even "
            f"when seq-drift < DRIFT_WARNING (default {FLUSH_QUEUE_BACKLOG_MIN})"
        ),
    )
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
    flush_mode = args.flush_queue_backlog

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
        if flush_mode:
            out["flush_queue_backlog_attempted"] = True
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

    if not apply_mode and not flush_mode:
        return 0 if report.is_clean() else 1

    # ----- MUTATING MODES -----
    if report.collection_topic_ambiguous:
        mode_name = "--flush-queue-backlog" if flush_mode else "--apply"
        msg = (
            f"\nREFUSING {mode_name}: collection WAL topic is ambiguous. "
            f"Audit diagnostics are available, but mutation is not safe."
        )
        _print(msg, args.json)
        if args.json:
            print(json.dumps({"error": "ambiguous_collection_topic"}))
        return 4

    if not is_in_maintenance_mode():
        mode_name = "--flush-queue-backlog" if flush_mode else "--apply"
        msg = (
            f"\nREFUSING {mode_name}: MCP is not in maintenance mode "
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
        mode_name = "--flush-queue-backlog" if flush_mode else "--apply"
        msg = (
            f"\nREFUSING {mode_name}: no recent backup (<{BACKUP_MAX_AGE_SECONDS // 60} min) found in\n"
            f"  {BACKUPS_DIR}/chroma_*.sqlite3.gz  or\n"
            f"  {DB_DIR}/chroma.sqlite3.backup.*\n"
            f"  Take a backup first: bash scripts/maintenance/backup_db.sh"
        )
        _print(msg, args.json)
        if args.json:
            print(json.dumps({"error": "no_recent_backup"}))
        return 3

    if flush_mode:
        # Fix 1: i ghost BENIGNI (DELETE pendente sopra il watermark, nel topic
        # della collection) sono compattati dal flush stesso → non bloccano.
        # Bloccano solo orphan SQL-only, ghost ANOMALI, e unknown bloccanti.
        if report.sql_only_count or report.anomalous_ghost_count or report.blocking_unknown_count:
            msg = (
                f"\nREFUSING --flush-queue-backlog: drift anomalo presente "
                f"(flushabili solo backlog #6975 pulito + ghost benigni).\n"
                f"  sql_only_orphans={report.sql_only_count}, "
                f"anomalous_ghosts={report.anomalous_ghost_count}, "
                f"blocking_unknown={report.blocking_unknown_count} "
                f"(benign_ghosts={report.benign_ghost_count} OK, compattati dal flush)\n"
                f"  Ghost benigno = id HNSW-only con DELETE pendente sopra il watermark.\n"
                f"  Usa --apply per orphan SQL-only; ghost anomali → investigare / rebuild manuale."
            )
            _print(msg, args.json)
            if args.json:
                print(json.dumps({
                    "error": "anomalous_drift",
                    "sql_only_count": report.sql_only_count,
                    "anomalous_ghost_count": report.anomalous_ghost_count,
                    "benign_ghost_count": report.benign_ghost_count,
                    "blocking_unknown_count": report.blocking_unknown_count,
                }))
            return 4

        con = _open_sqlite_ro(DB_PATH)
        try:
            pre_seq = _load_seq_state(con)
            sync_threshold = _load_hnsw_sync_threshold(
                con,
                collection_id=pre_seq["collection_id"],
                vector_segment=pre_seq["vector_segment"],
            )
            # Fix 2 — il flush gira in un PersistentClient SEPARATO (MCP ha rilasciato
            # il suo su SIGUSR1). Il segmento fresco riprende dal pickle a
            # vector_max_seq_id e ri-consuma dal WAL SOLO i record del proprio topic
            # con seq_id > watermark. Quei record di backlog riletti GIÀ incrementano
            # _num_log_records_since_last_persist (non solo le re-upsert nuove).
            #
            # Fix A — burst ALLINEATO (#6975): _persist() scatta ogni sync_threshold
            # record consumati e avanza il watermark di esattamente sync_threshold;
            # il residuo finale è (record_consumati) mod sync_threshold, dove
            # record_consumati = drift_iniziale + record appesi dal burst. Il vecchio
            # `max(threshold+margin, drift+margin)` attraversava la soglia ma NON
            # allineava il totale a un multiplo, lasciando (drift+burst) mod threshold
            # pendenti (caso 2026-06-16: (500+1100) mod 1000 = 600 → rc=5).
            #
            # Si dimensiona burst così che (drift + burst) sia il più piccolo multiplo
            # di sync_threshold >= drift + FLUSH_BURST_MARGIN → l'ultimo _persist()
            # atterra sulla testa del WAL, residuo atteso 0. Si usa pre_seq["drift"]
            # (record WAL sopra il vector watermark), NON collection_queue_pending_count
            # (netto per id: non conta tutti i record che incrementano il contatore).
            aligned_total = math.ceil(
                (pre_seq["drift"] + FLUSH_BURST_MARGIN) / sync_threshold
            ) * sync_threshold
            burst_n = aligned_total - pre_seq["drift"]
            if burst_n < FLUSH_BURST_MARGIN:
                # Difensivo: per costruzione burst_n >= FLUSH_BURST_MARGIN; clamp solo
                # contro configurazioni anomale (margin >= sync_threshold).
                burst_n += sync_threshold
            chunk_ids = _load_existing_embedding_ids(con, pre_seq["metadata_segment"], burst_n)
        finally:
            con.close()

        queue_backlog_min = args.queue_backlog_min
        drift_actionable = pre_seq["drift"] >= DRIFT_WARNING
        # Metrica gate = collection_queue_pending_count (backlog AZIONABILE per la
        # collection sopra il watermark), NON queue_total (globale, multi-topic:
        # backlog di altri topic non rende flushabile questa collection). queue_total
        # resta riportato per osservabilità. Ramo drift e min=300 invariati.
        queue_actionable = report.collection_queue_pending_count >= queue_backlog_min
        if not drift_actionable and not queue_actionable:
            msg = (
                f"\nWARNING --flush-queue-backlog not needed: drift={pre_seq['drift']} "
                f"is below WARNING={DRIFT_WARNING} and collection_queue_pending="
                f"{report.collection_queue_pending_count} is below "
                f"QUEUE_BACKLOG_MIN={queue_backlog_min} (queue_total={report.queue_total}, globale)."
            )
            _print(msg, args.json)
            if args.json:
                print(json.dumps({
                    "flush_queue_backlog": {
                        "skipped": True,
                        "reason": "below_drift_and_queue_thresholds",
                    },
                    "pre": pre_seq,
                    "drift_warning": DRIFT_WARNING,
                    "collection_queue_pending_count": report.collection_queue_pending_count,
                    "queue_total": report.queue_total,
                    "queue_backlog_min": queue_backlog_min,
                }, indent=2))
            return 0
        flush_trigger = "drift" if drift_actionable else "queue_backlog"

        if report.collection_queue_pending_count == 0:
            msg = (
                f"\nREFUSING --flush-queue-backlog: drift={pre_seq['drift']} but no net replayable "
                f"op pending for this collection above the watermark "
                f"(collection_queue_pending=0, queue_total={report.queue_total} globale). "
                f"This does not match the chromadb#6975 queue-backlog pattern."
            )
            _print(msg, args.json)
            if args.json:
                print(json.dumps({
                    "error": "collection_queue_empty",
                    "collection_queue_pending_count": report.collection_queue_pending_count,
                    "queue_total": report.queue_total,
                    "pre": pre_seq,
                }, indent=2))
            return 4

        # Lazy imports — only needed for the mutating Chroma flush path.
        # embedding.py fa `from config import` (serve scripts/lib) e, per
        # openai/google, `from lib.api_keys import` (serve scripts): entrambi su
        # path PRIMA dell'import, e solo qui (non a load-time -> no shadowing).
        sys.path.insert(0, os.path.join(BASE_DIR, "scripts"))
        sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
        import chromadb  # noqa: E402
        from embedding import get_embeddings, info as embedding_info  # noqa: E402

        client = chromadb.PersistentClient(path=DB_DIR)
        collection = client.get_collection(
            name=COLLECTION_NAME,
            embedding_function=OllamaNomicEmbeddingFunction(get_embeddings),
        )

        def _flush_progress(done: int, total: int, skipped: int, errors: int) -> None:
            if not args.json:
                print(f"  flush upsert {done}/{total} skipped={skipped} errors={errors}", flush=True)

        _print(
            f"\nFlushing queue backlog via {len(chunk_ids)} idempotent re-upserts "
            f"(target={burst_n}, sync_threshold={sync_threshold}, drift={pre_seq['drift']})...",
            args.json,
        )
        result = flush_queue_backlog(
            collection=collection,
            chunk_ids=chunk_ids,
            progress=_flush_progress,
        )

        con = _open_sqlite_ro(DB_PATH)
        try:
            post_seq = _load_seq_state(con)
        finally:
            con.close()

        # Fix A — top-up allineamento (#6975). I record realmente consumati =
        # result["processed"], che può differire da burst_n se collection.get()
        # salta id mancanti o un upsert va in errore; in quel caso il drift SQL
        # residuo (= record consumati mod sync_threshold) non è 0 e la queue resta
        # non drenata. Si itera (capped) finché il drift è allineato a un multiplo
        # di sync_threshold → il prossimo _persist() chiude a 0. Gira nello STESSO
        # client (contatore _num_log_records_since_last_persist vivo) e in
        # maintenance esclusiva (MCP fermo via SIGUSR1). Riusa la lettura post_seq
        # appena fatta (nessuna lettura extra quando il burst allineato chiude già
        # a 0); result e post_seq vengono aggiornati a ogni passata.
        result["topup_iterations"] = 0
        for _ in range(MAX_TOPUP_ITERS):
            residual = post_seq["drift"] % sync_threshold
            if residual == 0:
                break
            top_up = sync_threshold - residual
            con = _open_sqlite_ro(DB_PATH)
            try:
                topup_ids = _load_existing_embedding_ids(
                    con, pre_seq["metadata_segment"], top_up
                )
            finally:
                con.close()
            _print(
                f"  top-up allineamento: drift={post_seq['drift']} residual={residual} "
                f"→ {len(topup_ids)} re-upsert aggiuntive",
                args.json,
            )
            topup_result = flush_queue_backlog(
                collection=collection,
                chunk_ids=topup_ids,
                progress=_flush_progress,
            )
            result["processed"] += topup_result["processed"]
            result["skipped"] += topup_result["skipped"]
            result["errors"] += topup_result["errors"]
            result["candidate_ids"] += topup_result["candidate_ids"]
            # Granulari: .get default 0 -> robusto se un fake non li espone.
            result["skipped_missing_id"] = (
                result.get("skipped_missing_id", 0) + topup_result.get("skipped_missing_id", 0))
            result["skipped_missing_embedding"] = (
                result.get("skipped_missing_embedding", 0) + topup_result.get("skipped_missing_embedding", 0))
            result["topup_iterations"] += 1
            con = _open_sqlite_ro(DB_PATH)
            try:
                post_seq = _load_seq_state(con)
            finally:
                con.close()

        post_report = audit()
        vector_delta = post_seq["vector_seq"] - pre_seq["vector_seq"]
        success = (
            vector_delta >= sync_threshold
            and post_seq["drift"] < DRIFT_WARNING
            and post_report.collection_queue_pending_count < queue_backlog_min
            # Post-condizione allineamento (#6975 fix A): il burst deve aver chiuso il
            # residuo a zero (drift allineato a un multiplo di sync_threshold). Senza
            # questo, skip/error nel top-up potrebbero lasciare un residuo sotto le
            # soglie sopra e ritornare rc=0 con la queue NON drenata → success=False
            # ⇒ rc=5 ⇒ auto_flush manda FAIL.
            and post_seq["drift"] % sync_threshold == 0
            # Post-condizione: il flush deve aver compattato TUTTI i ghost.
            # Niente assert in prod → success=False ⇒ rc=5 ⇒ auto_flush manda FAIL.
            and post_report.benign_ghost_count == 0
            and post_report.anomalous_ghost_count == 0
        )

        try:
            con = _open_sqlite_rw(DB_PATH)
            try:
                _ensure_maintenance_log_row(
                    con,
                    mode="flush_queue_backlog",
                    report=report,
                    action=(
                        f"chromadb#6975 flush burst: processed {result['processed']} "
                        f"of {result['candidate_ids']} candidate ids"
                    ),
                    extra={
                        "backup_used": os.path.basename(backup),
                        "issue": "chroma-core/chroma#6975",
                        "session_reference": "2026-05-30 drift 821->21 via idempotent burst",
                        "embedding": embedding_info(),
                        "sync_threshold": sync_threshold,
                        "drift_warning": DRIFT_WARNING,
                        "queue_backlog_min": queue_backlog_min,
                        "flush_trigger": flush_trigger,
                        "burst_target": burst_n,
                        "pre_seq": pre_seq,
                        "post_seq": post_seq,
                        "vector_seq_delta": vector_delta,
                        "queue_total": report.queue_total,
                        "post_queue_total": post_report.queue_total,
                        "collection_queue_pending_count": report.collection_queue_pending_count,
                        "post_collection_queue_pending_count": post_report.collection_queue_pending_count,
                        "post_benign_ghost_count": post_report.benign_ghost_count,
                        "post_anomalous_ghost_count": post_report.anomalous_ghost_count,
                        "result": result,
                        "success": success,
                    },
                )
            finally:
                con.close()
        except Exception as e:
            if not args.json:
                print(f"\n[warn] could not write maintenance_log row: {e}", file=sys.stderr)

        if args.json:
            print(json.dumps({
                "flush_queue_backlog": result,
                "pre": pre_seq,
                "post": post_seq,
                "sync_threshold": sync_threshold,
                "drift_warning": DRIFT_WARNING,
                "queue_backlog_min": queue_backlog_min,
                "flush_trigger": flush_trigger,
                "burst_target": burst_n,
                "vector_seq_delta": vector_delta,
                "queue_total": report.queue_total,
                "post_queue_total": post_report.queue_total,
                "collection_queue_pending_count": report.collection_queue_pending_count,
                "post_collection_queue_pending_count": post_report.collection_queue_pending_count,
                "post_benign_ghost_count": post_report.benign_ghost_count,
                "post_anomalous_ghost_count": post_report.anomalous_ghost_count,
                "success": success,
            }, indent=2, default=str))
        else:
            print(f"\nFlush result: {result}")
            print(f"Trigger: {flush_trigger} (drift={pre_seq['drift']}/WARNING={DRIFT_WARNING}, "
                  f"collection_pending={report.collection_queue_pending_count}/MIN={queue_backlog_min}, "
                  f"queue_total={report.queue_total} globale)")
            print(f"Vector seq: {pre_seq['vector_seq']} -> {post_seq['vector_seq']} ({vector_delta:+d})")
            print(f"Metadata seq: {pre_seq['metadata_seq']} -> {post_seq['metadata_seq']}")
            print(f"Drift: {pre_seq['drift']} -> {post_seq['drift']} (WARNING={DRIFT_WARNING})")
            print(f"Collection pending: {report.collection_queue_pending_count} -> "
                  f"{post_report.collection_queue_pending_count} (MIN={queue_backlog_min})")
            print(f"Queue total (globale): {report.queue_total} -> {post_report.queue_total}")
            if success:
                print("OK — HNSW backlog flushed below warning.")
            else:
                print("WARN — flush did not meet success criteria. Investigate before retrying.")

        return 0 if success else 5

    # ----- APPLY MODE -----
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
    # embedding.py: `from config import` (scripts/lib) + openai/google
    # `from lib.api_keys import` (scripts). Entrambi su path prima dell'uso.
    sys.path.insert(0, os.path.join(BASE_DIR, "scripts"))
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
                       + (f" (skipped {result['skipped_empty']} empty)" if result['skipped_empty'] else "")
                       + (f"; {result['migrated_truncated']} chunk canonicalizzati al contratto "
                          f"(troncati a {MAX_EMBED_CHARS}, vettore != storico)"
                          if result.get('migrated_truncated') else ""),
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
