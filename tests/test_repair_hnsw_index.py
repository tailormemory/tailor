"""Tests for scripts.maintenance.repair_hnsw_index.

The script reads a ChromaDB SQLite + the HNSW segment's
`index_metadata.pickle` to detect drift, and uses
`collection.upsert()` to repair SQL-only orphans. Tests build a
synthetic minimal Chroma layout under a tmp_path and exercise the
audit/apply/refusal paths without requiring chromadb to be installed
or the live DB to be touched.

CONCURRENCY WARNING — the apply-path tests added in v1.2.3
(`test_apply_triggers_auto_persist_on_nonempty_repair`,
`test_apply_skips_auto_persist_when_repair_set_is_empty`,
`test_apply_records_mtime_did_not_advance`) inject fake `chromadb`,
`embedding`, and `chroma_persist` modules via
`monkeypatch.setitem(sys.modules, ...)`. pytest's `monkeypatch`
reverts cleanly between tests in serial execution, but `sys.modules`
is process-global. **Do NOT run this file under pytest-xdist or any
parallel test runner** without first refactoring the fakes to
fixture-scoped instances and serialising the apply tests with a
mutex. Today they are safe only because pytest runs them in-process
and sequentially.
"""

from __future__ import annotations

import json
import os
import pickle
import sqlite3
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts", "maintenance"))

import repair_hnsw_index as rhi  # noqa: E402


# ============================================================
# FIXTURES
# ============================================================


META_SEG_ID = "meta-seg-test"
VEC_SEG_ID = "vec-seg-test"
COLLECTION_ID = "coll-test"


def _make_chroma_sqlite(db_path: str, *, embeddings: list[dict], queue_ids: list[str] | None = None,
                        queue_ops: list[tuple] | None = None) -> None:
    """Create a minimal Chroma-shaped SQLite at db_path.

    Each entry in `embeddings` is a dict with keys:
        embedding_id (str), int_id (int), metadata (dict)
    All embeddings are attached to the metadata segment.
    """
    queue_ids = queue_ids or []
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.executescript(
        """
        CREATE TABLE segments (
            id TEXT PRIMARY KEY,
            type TEXT,
            scope TEXT,
            collection TEXT
        );
        CREATE TABLE embeddings (
            id INTEGER PRIMARY KEY,
            segment_id TEXT NOT NULL,
            embedding_id TEXT NOT NULL,
            seq_id BLOB NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (segment_id, embedding_id)
        );
        CREATE TABLE embedding_metadata (
            id INTEGER REFERENCES embeddings(id),
            key TEXT NOT NULL,
            string_value TEXT,
            int_value INTEGER,
            float_value REAL,
            bool_value INTEGER,
            PRIMARY KEY (id, key)
        );
        CREATE TABLE embeddings_queue (
            seq_id INTEGER PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            operation INTEGER NOT NULL,
            topic TEXT NOT NULL,
            id TEXT NOT NULL,
            vector BLOB,
            encoding TEXT,
            metadata TEXT
        );
        CREATE TABLE max_seq_id (
            segment_id TEXT PRIMARY KEY,
            seq_id INTEGER
        );
        CREATE TABLE collections (
            id TEXT PRIMARY KEY,
            name TEXT
        );
        CREATE TABLE maintenance_log (
            id INT PRIMARY KEY,
            timestamp INT NOT NULL,
            operation TEXT NOT NULL
        );
        """
    )
    cur.execute("INSERT INTO segments VALUES (?, ?, ?, ?)", (META_SEG_ID, "urn:chroma:segment/metadata/sqlite", "METADATA", COLLECTION_ID))
    cur.execute("INSERT INTO segments VALUES (?, ?, ?, ?)", (VEC_SEG_ID, "urn:chroma:segment/vector/hnsw-local-persisted", "VECTOR", COLLECTION_ID))
    cur.execute("INSERT INTO collections VALUES (?, ?)", (COLLECTION_ID, rhi.COLLECTION_NAME))

    for e in embeddings:
        cur.execute(
            "INSERT INTO embeddings (id, segment_id, embedding_id, seq_id) VALUES (?, ?, ?, ?)",
            (e["int_id"], META_SEG_ID, e["embedding_id"], e["int_id"]),
        )
        for key, val in e["metadata"].items():
            sv = iv = fv = bv = None
            if isinstance(val, bool):
                bv = 1 if val else 0
            elif isinstance(val, int):
                iv = val
            elif isinstance(val, float):
                fv = val
            else:
                sv = str(val) if val is not None else None
            cur.execute(
                "INSERT INTO embedding_metadata (id, key, string_value, int_value, float_value, bool_value) VALUES (?, ?, ?, ?, ?, ?)",
                (e["int_id"], key, sv, iv, fv, bv),
            )
    OUR_TOPIC = f"persistent://default/default/{COLLECTION_ID}"
    seq = 1000
    used: list[int] = []
    for qid in queue_ids:
        cur.execute(
            "INSERT INTO embeddings_queue (seq_id, operation, topic, id) VALUES (?, ?, ?, ?)",
            (seq, 2, OUR_TOPIC, qid),
        )
        used.append(seq)
        seq += 1
    for item in (queue_ops or []):
        # (op, id) | (op, id, topic) | (op, id, topic, seq)
        op, qid = item[0], item[1]
        topic = item[2] if len(item) > 2 else OUR_TOPIC
        if len(item) > 3:
            qseq = item[3]
        else:
            qseq = seq
            seq += 1
        cur.execute(
            "INSERT INTO embeddings_queue (seq_id, operation, topic, id) VALUES (?, ?, ?, ?)",
            (qseq, op, topic, qid),
        )
        used.append(qseq)
    # META watermark deve coprire il MAX seq realmente inserito (anche con seq
    # espliciti fuori sequenza), altrimenti il watermark metadata risulta < queue.
    meta_max = (max(used) + 1) if used else 1000
    cur.execute("INSERT INTO max_seq_id VALUES (?, ?)", (META_SEG_ID, meta_max))
    cur.execute("INSERT INTO max_seq_id VALUES (?, ?)", (VEC_SEG_ID, 1000))
    con.commit()
    con.close()


def _make_hnsw_pickle(db_dir: str, vec_seg_id: str, hnsw_ids: list[str]) -> None:
    seg_dir = os.path.join(db_dir, vec_seg_id)
    os.makedirs(seg_dir, exist_ok=True)
    payload = {
        "dimensionality": 768,
        "total_elements_added": len(hnsw_ids),
        "max_seq_id": None,
        "id_to_label": {hid: i for i, hid in enumerate(hnsw_ids)},
        "label_to_id": {i: hid for i, hid in enumerate(hnsw_ids)},
        "id_to_seq_id": {},
    }
    with open(os.path.join(seg_dir, "index_metadata.pickle"), "wb") as f:
        pickle.dump(payload, f)


def _make_doc_chunk(int_id: int, embedding_id: str, conv_id: str, title: str, chunk_index: int, text: str = "doc body text") -> dict:
    return {
        "int_id": int_id,
        "embedding_id": embedding_id,
        "metadata": {
            "source": "document",
            "conv_id": conv_id,
            "title": title,
            "chunk_index": chunk_index,
            "folder": "Wealth",
            "doc_type": "spreadsheet",
            "chroma:document": text,
            "char_count": len(text),
        },
    }


def _make_summary_chunk(int_id: int, embedding_id: str, text: str = "[RIASSUNTO] summary text") -> dict:
    return {
        "int_id": int_id,
        "embedding_id": embedding_id,
        "metadata": {
            "source": "document",
            "conv_id": "doc_summary_20260416",
            "title": "summary.pdf",
            "chunk_index": -1,
            "chroma:document": text,
            "char_count": len(text),
        },
    }


@pytest.fixture
def synthetic_db(tmp_path, monkeypatch):
    """Build a Chroma-shaped DB with: 2 healthy doc chunks, 3 SQL-only doc orphans,
    1 SQL-only summary orphan, 2 HNSW-only ghosts (under a doc_<hash> not in registry)."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    db_path = db_dir / "chroma.sqlite3"

    healthy = [
        _make_doc_chunk(1, "doc_aaaaaaaaaaaa_chunk_0000", "doc_aaaaaaaaaaaa", "healthy.pdf", 0, "healthy chunk 0"),
        _make_doc_chunk(2, "doc_aaaaaaaaaaaa_chunk_0001", "doc_aaaaaaaaaaaa", "healthy.pdf", 1, "healthy chunk 1"),
    ]
    orphans = [
        _make_doc_chunk(10, "doc_bbbbbbbbbbbb_chunk_0000", "doc_bbbbbbbbbbbb", "orphan.pdf", 0, "orphan body 0"),
        _make_doc_chunk(11, "doc_bbbbbbbbbbbb_chunk_0001", "doc_bbbbbbbbbbbb", "orphan.pdf", 1, "orphan body 1"),
        _make_doc_chunk(12, "doc_cccccccccccc_chunk_0000", "doc_cccccccccccc", "lonely.pdf", 0, "lonely body"),
        _make_summary_chunk(20, "doc_summary_aaaa1111", "summary text alpha"),
    ]
    embeddings = healthy + orphans

    hnsw_ids = [
        "doc_aaaaaaaaaaaa_chunk_0000",
        "doc_aaaaaaaaaaaa_chunk_0001",
        # ghosts (not in SQL):
        "doc_dddddddddddd_chunk_0000",
        "doc_dddddddddddd_chunk_0001",
    ]

    _make_chroma_sqlite(str(db_path), embeddings=embeddings, queue_ids=[])
    _make_hnsw_pickle(str(db_dir), VEC_SEG_ID, hnsw_ids)

    # doc_registry only contains the live doc (so doc_dddd... is correctly classified
    # as superseded). doc_bbbb / cccc must be in registry for orphans to be repairable
    # (the audit doesn't require this, but it's realistic).
    registry = {
        "/cloud/healthy.pdf": {"hash": "aaaaaaaaaaaaaaaa", "chunks": 2},
        "/cloud/orphan.pdf": {"hash": "bbbbbbbbbbbbbbbb", "chunks": 2},
        "/cloud/lonely.pdf": {"hash": "ccccccccccccccccc", "chunks": 1},
    }
    (db_dir / "doc_registry.json").write_text(json.dumps(registry))

    # Point module-level constants at the synthetic dirs
    monkeypatch.setattr(rhi, "BASE_DIR", str(tmp_path))
    monkeypatch.setattr(rhi, "DB_DIR", str(db_dir))
    monkeypatch.setattr(rhi, "DB_PATH", str(db_path))
    monkeypatch.setattr(rhi, "BACKUPS_DIR", str(tmp_path / "backups"))
    monkeypatch.setattr(rhi, "MAINTENANCE_LOCK", str(tmp_path / "maintenance.lock"))
    monkeypatch.setattr(rhi, "AUDIT_HISTORY_DIR", str(tmp_path / "logs" / "hnsw_audits"))

    return {
        "tmp_path": tmp_path,
        "db_path": str(db_path),
        "db_dir": str(db_dir),
        "audit_dir": str(tmp_path / "logs" / "hnsw_audits"),
        "backups_dir": str(tmp_path / "backups"),
        "lock_path": str(tmp_path / "maintenance.lock"),
        "expected_orphan_ids": [
            "doc_bbbbbbbbbbbb_chunk_0000",
            "doc_bbbbbbbbbbbb_chunk_0001",
            "doc_cccccccccccc_chunk_0000",
            "doc_summary_aaaa1111",
        ],
        "expected_ghost_ids": [
            "doc_dddddddddddd_chunk_0000",
            "doc_dddddddddddd_chunk_0001",
        ],
    }


class FakeCollection:
    """Stand-in for chromadb.Collection. Records upsert() calls."""

    def __init__(self):
        self.upsert_calls: list[dict] = []

    def upsert(self, *, ids, embeddings, documents, metadatas):
        assert len(ids) == len(embeddings) == len(documents) == len(metadatas)
        self.upsert_calls.append({
            "ids": list(ids),
            "embeddings": [list(e) for e in embeddings],
            "documents": list(documents),
            "metadatas": [dict(m) for m in metadatas],
        })


def _deterministic_embed(texts):
    """Embedding mock: hash each text into a 4-dim vector. Deterministic, reproducible."""
    out = []
    for t in texts:
        h = hash(t)
        out.append([
            ((h >> 0) & 0xFFFF) / 65535.0,
            ((h >> 16) & 0xFFFF) / 65535.0,
            ((h >> 32) & 0xFFFF) / 65535.0,
            ((h >> 48) & 0xFFFF) / 65535.0,
        ])
    return out


# ============================================================
# TESTS
# ============================================================


def test_audit_dry_run_reports_known_drift(synthetic_db):
    """B.4.1 — Audit-mode dry-run on synthetic collection produces expected report."""
    report = rhi.audit(db_path=synthetic_db["db_path"], db_dir=synthetic_db["db_dir"])
    assert report.sql_only_count == 4  # 3 doc orphans + 1 summary
    assert report.hnsw_only_count == 2
    assert report.queue_total == 0
    assert report.unknown_count == 0
    # queue vuota → nessun DELETE pendente → entrambi i ghost anomali
    assert report.anomalous_ghost_count == 2
    assert report.benign_ghost_count == 0
    found_orphans = sorted(o.embedding_id for o in report.sql_only_orphans)
    assert found_orphans == sorted(synthetic_db["expected_orphan_ids"])
    found_ghosts = sorted(g.embedding_id for g in report.hnsw_only_ghosts)
    assert found_ghosts == sorted(synthetic_db["expected_ghost_ids"])

    text = rhi.format_report_human(report)
    assert "SQL-only orphans:" in text
    assert "doc_bbbbbbbbbbbb" in text
    # Grouping is by conv_id, not embedding_id
    assert "doc_summary_20260416" in text
    assert "doc_dddddddddddd" in text
    assert "[known_document]" in text
    assert "[known_summary]" in text
    assert "[known_superseded_doc]" in text
    assert "UNEXPECTED DRIFT TYPES" in text
    assert "  none" in text


def test_apply_repairs_orphans_via_upsert(synthetic_db):
    """B.4.5 — Apply correctly re-embeds and upserts SQL-only orphans."""
    report = rhi.audit(db_path=synthetic_db["db_path"], db_dir=synthetic_db["db_dir"])
    coll = FakeCollection()
    result = rhi.repair(report, embed_fn=_deterministic_embed, collection=coll, batch_size=2)
    assert result["repaired"] == 4
    assert result["skipped_oversize"] == 0
    upserted_ids = [i for call in coll.upsert_calls for i in call["ids"]]
    assert sorted(upserted_ids) == sorted(synthetic_db["expected_orphan_ids"])

    # Each upsert call must include parallel embeddings/documents/metadatas
    for call in coll.upsert_calls:
        assert all(isinstance(e, list) and len(e) == 4 for e in call["embeddings"])
        # chroma:document must NOT be passed in metadatas (it's the documents arg)
        for m in call["metadatas"]:
            assert "chroma:document" not in m
            assert "__int_id__" not in m

    # documents arg passed verbatim from chroma:document for all sources
    for call in coll.upsert_calls:
        for eid, doc, meta in zip(call["ids"], call["documents"], call["metadatas"]):
            assert meta.get("source") == "document"
            if eid.startswith("doc_summary_"):
                assert doc == "summary text alpha"  # raw chroma:document
            else:
                assert "orphan body" in doc or "lonely body" in doc


def test_apply_is_idempotent_after_simulated_success(synthetic_db):
    """B.4.2 — Re-running --apply after a successful run reports 0 drift to fix."""
    # Simulate successful repair by adding the orphan ids to the HNSW pickle
    report1 = rhi.audit(db_path=synthetic_db["db_path"], db_dir=synthetic_db["db_dir"])
    assert report1.sql_only_count == 4
    coll = FakeCollection()
    rhi.repair(report1, embed_fn=_deterministic_embed, collection=coll)

    # Update the pickle to reflect the writes
    pickle_path = os.path.join(synthetic_db["db_dir"], VEC_SEG_ID, "index_metadata.pickle")
    with open(pickle_path, "rb") as f:
        m = pickle.load(f)
    next_label = max(m["id_to_label"].values()) + 1
    for eid in synthetic_db["expected_orphan_ids"]:
        m["id_to_label"][eid] = next_label
        m["label_to_id"][next_label] = eid
        next_label += 1
    with open(pickle_path, "wb") as f:
        pickle.dump(m, f)

    report2 = rhi.audit(db_path=synthetic_db["db_path"], db_dir=synthetic_db["db_dir"])
    assert report2.sql_only_count == 0
    coll2 = FakeCollection()
    result2 = rhi.repair(report2, embed_fn=_deterministic_embed, collection=coll2)
    assert result2["repaired"] == 0
    assert coll2.upsert_calls == []


def test_apply_refuses_when_not_in_maintenance_mode(synthetic_db):
    """B.4.3 — --apply refuses when MCP is not in maintenance mode."""
    assert not os.path.exists(synthetic_db["lock_path"])
    rc = rhi.main(["--apply", "--no-history"])
    assert rc == 2  # not_in_maintenance_mode


def test_apply_refuses_when_no_recent_backup(synthetic_db):
    """B.4.4 — --apply refuses when no recent backup exists."""
    Path(synthetic_db["lock_path"]).write_text("12345")  # enter maintenance mode
    # No backup at all
    rc = rhi.main(["--apply", "--no-history"])
    assert rc == 3  # no_recent_backup

    # Stale backup (older than 60 min) — also refused
    Path(synthetic_db["backups_dir"]).mkdir(exist_ok=True)
    stale_backup = Path(synthetic_db["backups_dir"]) / "chroma_20260101_010101.sqlite3.gz"
    stale_backup.write_text("fake")
    old_mtime = time.time() - 7200  # 2h old
    os.utime(str(stale_backup), (old_mtime, old_mtime))
    rc2 = rhi.main(["--apply", "--no-history"])
    assert rc2 == 3


def test_apply_refuses_when_unknown_drift_present(synthetic_db):
    """B.4.6 — Unexpected drift type triggers refusal."""
    # Add an "unknown" SQL-only chunk: source not in known set, conv_id off-pattern
    con = sqlite3.connect(synthetic_db["db_path"])
    cur = con.cursor()
    cur.execute(
        "INSERT INTO embeddings (id, segment_id, embedding_id, seq_id) VALUES (?, ?, ?, ?)",
        (99, META_SEG_ID, "weirdo_chunk", 99),
    )
    cur.execute(
        "INSERT INTO embedding_metadata (id, key, string_value, int_value, float_value, bool_value) VALUES (?, ?, ?, ?, ?, ?)",
        (99, "source", "mystery_source", None, None, None),
    )
    con.commit()
    con.close()

    report = rhi.audit(db_path=synthetic_db["db_path"], db_dir=synthetic_db["db_dir"])
    assert report.unknown_count >= 1
    with pytest.raises(RuntimeError, match="unknown drift"):
        rhi.repair(report, embed_fn=_deterministic_embed, collection=FakeCollection())

    # CLI path: maintenance lock + fresh backup, but unknown drift -> rc=4
    Path(synthetic_db["lock_path"]).write_text("12345")
    Path(synthetic_db["backups_dir"]).mkdir(exist_ok=True)
    fresh = Path(synthetic_db["backups_dir"]) / "chroma_now.sqlite3.gz"
    fresh.write_text("fake")
    rc = rhi.main(["--apply", "--no-history"])
    assert rc == 4


def test_prune_hnsw_ghosts_raises_not_implemented(synthetic_db):
    """B.4.7 — --prune-hnsw-ghosts raises NotImplementedError cleanly."""
    with pytest.raises(NotImplementedError, match="deferred"):
        rhi.main(["--prune-hnsw-ghosts"])


def test_maintenance_log_row_written_per_run(synthetic_db):
    """B.4.8 — maintenance_log row written on each run."""
    rc = rhi.main(["--audit", "--no-history"])
    assert rc == 1  # drift exists
    con = sqlite3.connect(synthetic_db["db_path"])
    rows = con.execute("SELECT id, timestamp, operation FROM maintenance_log").fetchall()
    con.close()
    assert len(rows) == 1
    payload = json.loads(rows[0][2])
    assert payload["mode"] == "audit"
    assert payload["drift_summary"]["sql_only_count"] == 4
    assert payload["drift_summary"]["hnsw_only_count"] == 2
    assert payload["action_taken"] == "audit-only"

    # Second run -> second row, with monotonic id
    rhi.main(["--audit", "--no-history"])
    con = sqlite3.connect(synthetic_db["db_path"])
    ids = [r[0] for r in con.execute("SELECT id FROM maintenance_log ORDER BY id").fetchall()]
    con.close()
    assert ids == [1, 2]


def test_delta_section_when_recent_history_exists(synthetic_db):
    """B.4.9 — Delta section appears when audit history JSON exists from <2h ago."""
    Path(synthetic_db["audit_dir"]).mkdir(parents=True, exist_ok=True)
    # Seed a prior audit that thought there were 100 orphans (different state)
    fake_prev = {
        "timestamp": "2026-04-25 09:14:02",
        "timestamp_unix": int(time.time()) - 3600,  # 1h ago
        "sql_only_orphans": [
            {"embedding_id": "doc_zzz_chunk_0000"},
            {"embedding_id": "doc_bbbbbbbbbbbb_chunk_0000"},  # still orphan
        ],
        "hnsw_only_ghosts": [{"embedding_id": "doc_dddddddddddd_chunk_0000"}],
        "queue_total": 100,
    }
    prev_path = Path(synthetic_db["audit_dir"]) / "audit_20260425_091402.json"
    prev_path.write_text(json.dumps(fake_prev))
    one_hour_ago = time.time() - 3600
    os.utime(str(prev_path), (one_hour_ago, one_hour_ago))

    report = rhi.audit(db_path=synthetic_db["db_path"], db_dir=synthetic_db["db_dir"])
    history_path = rhi.write_audit_history(report, audit_dir=synthetic_db["audit_dir"])
    found_prev = rhi.find_recent_audit(audit_dir=synthetic_db["audit_dir"], exclude=history_path)
    assert found_prev == str(prev_path)
    delta = rhi.compute_delta(found_prev, report)
    assert delta is not None
    assert delta.queue_change == -100  # current is 0, prev was 100
    assert "doc_zzz_chunk_0000" in delta.resolved_orphan_ids
    assert "doc_cccccccccccc_chunk_0000" in delta.new_orphan_ids
    assert "doc_summary_aaaa1111" in delta.new_orphan_ids

    text = rhi.format_report_human(report, delta)
    assert "DELTA SINCE LAST AUDIT" in text
    assert "Queue backlog:" in text


def test_delta_section_omitted_when_no_recent_history(synthetic_db):
    """B.4.10 — Delta section is omitted when no recent audit history exists."""
    # Empty history dir
    found = rhi.find_recent_audit(audit_dir=synthetic_db["audit_dir"])
    assert found is None

    # Stale history (>2h old) is ignored
    Path(synthetic_db["audit_dir"]).mkdir(parents=True, exist_ok=True)
    stale = Path(synthetic_db["audit_dir"]) / "audit_old.json"
    stale.write_text(json.dumps({"timestamp": "old", "timestamp_unix": 0, "sql_only_orphans": [], "hnsw_only_ghosts": [], "queue_total": 0}))
    old_mtime = time.time() - 10800  # 3h ago
    os.utime(str(stale), (old_mtime, old_mtime))
    found2 = rhi.find_recent_audit(audit_dir=synthetic_db["audit_dir"])
    assert found2 is None

    report = rhi.audit(db_path=synthetic_db["db_path"], db_dir=synthetic_db["db_dir"])
    text = rhi.format_report_human(report, delta=None)
    assert "DELTA SINCE LAST AUDIT" not in text


def test_audit_history_json_written_to_known_dir(synthetic_db):
    """The audit-history JSON is written under logs/hnsw_audits/ on each run."""
    rc = rhi.main(["--audit"])  # no --no-history this time
    assert rc == 1
    files = list(Path(synthetic_db["audit_dir"]).glob("audit_*.json"))
    assert len(files) == 1
    payload = json.loads(files[0].read_text())
    assert payload["sql_total"] >= 4
    assert payload["sql_only_orphans"]


def test_repair_returns_repaired_ids(synthetic_db):
    """B.4.11 — repair() returns the ids it actually upserted, in order, for
    the auto-persist downstream consumer."""
    report = rhi.audit(db_path=synthetic_db["db_path"], db_dir=synthetic_db["db_dir"])
    coll = FakeCollection()
    result = rhi.repair(report, embed_fn=_deterministic_embed, collection=coll, batch_size=2)
    assert "repaired_ids" in result
    assert sorted(result["repaired_ids"]) == sorted(synthetic_db["expected_orphan_ids"])
    # Order in repaired_ids matches the order they were upserted across batches
    upserted_in_order = [i for call in coll.upsert_calls for i in call["ids"]]
    assert result["repaired_ids"] == upserted_in_order


def _install_apply_mocks(monkeypatch, synthetic_db):
    """Install fake chromadb / embedding / chroma_persist at the lazy-import
    sites inside main(). Returns (fake_collection, persist_calls list)."""
    fake_collection = FakeCollection()

    class _FakeClient:
        def __init__(self, *a, **kw):
            pass
        def get_or_create_collection(self, name, metadata=None):
            return fake_collection

    fake_chromadb = type(sys)("chromadb")
    fake_chromadb.PersistentClient = _FakeClient
    monkeypatch.setitem(sys.modules, "chromadb", fake_chromadb)

    fake_embedding = type(sys)("embedding")
    fake_embedding.get_embeddings = _deterministic_embed
    monkeypatch.setitem(sys.modules, "embedding", fake_embedding)

    persist_calls: list[dict] = []

    def _fake_force(collection, ids, *, pickle_path=None, target_writes=1100):
        persist_calls.append({
            "ids": list(ids),
            "pickle_path": pickle_path,
            "target_writes": target_writes,
        })
        return {
            "success": True,
            "iterations": 17,
            "total_writes": 17 * len(ids),
            "ids_persisted": len(ids),
            "mtime_advanced": True,
            "mtime_before": 1000.0,
            "mtime_after": 1001.0,
        }

    fake_chroma_persist = type(sys)("chroma_persist")
    fake_chroma_persist.DEFAULT_TARGET_WRITES = 1100
    fake_chroma_persist.force_chroma_persist = _fake_force
    monkeypatch.setitem(sys.modules, "chroma_persist", fake_chroma_persist)

    # Apply preconditions: maintenance lock + fresh backup
    Path(synthetic_db["lock_path"]).write_text("12345")
    Path(synthetic_db["backups_dir"]).mkdir(exist_ok=True)
    fresh = Path(synthetic_db["backups_dir"]) / "chroma_now.sqlite3.gz"
    fresh.write_text("fake")

    return fake_collection, persist_calls


def test_apply_triggers_auto_persist_on_nonempty_repair(synthetic_db, monkeypatch):
    """B.4.12 — --apply with a non-empty repair set invokes force_chroma_persist
    on the repaired ids, and records the result in maintenance_log."""
    fake_collection, persist_calls = _install_apply_mocks(monkeypatch, synthetic_db)

    rc = rhi.main(["--apply", "--no-history"])
    # post-audit still sees drift (fake collection didn't actually update SQL/HNSW),
    # so rc=5 is expected. The auto-persist still ran because repair() returned ids.
    assert rc == 5

    # Persist invoked exactly once with the repaired ids
    assert len(persist_calls) == 1
    assert sorted(persist_calls[0]["ids"]) == sorted(synthetic_db["expected_orphan_ids"])
    # Pickle path resolved from the synthetic DB
    assert persist_calls[0]["pickle_path"] is not None
    assert persist_calls[0]["pickle_path"].endswith("index_metadata.pickle")

    # maintenance_log apply row contains auto_persist details
    con = sqlite3.connect(synthetic_db["db_path"])
    rows = con.execute(
        "SELECT operation FROM maintenance_log "
        "WHERE json_extract(operation, '$.mode') = 'apply'"
    ).fetchall()
    con.close()
    assert len(rows) == 1
    payload = json.loads(rows[0][0])
    ap = payload["auto_persist"]
    assert ap["triggered"] is True
    assert ap["iterations"] == 17
    assert ap["total_writes"] == 17 * 4  # 4 orphans
    assert ap["mtime_advanced"] is True
    assert ap["success"] is True


def test_apply_skips_auto_persist_when_repair_set_is_empty(synthetic_db, monkeypatch):
    """B.4.13 — When repair() returns no repaired_ids (e.g. all orphans were
    oversize-skipped), force_chroma_persist is NOT invoked and the
    maintenance_log row records `auto_persist.triggered=false`."""
    fake_collection, persist_calls = _install_apply_mocks(monkeypatch, synthetic_db)

    # Make repair return an empty ids list
    def _fake_repair(report, *, embed_fn, collection, batch_size=10, progress=None):
        return {"repaired": 0, "batches": 0, "skipped_oversize": 4, "repaired_ids": []}
    monkeypatch.setattr(rhi, "repair", _fake_repair)

    rc = rhi.main(["--apply", "--no-history"])
    # Drift remains since repair did nothing → rc=5
    assert rc == 5
    assert persist_calls == []  # auto-persist NOT invoked

    con = sqlite3.connect(synthetic_db["db_path"])
    rows = con.execute(
        "SELECT operation FROM maintenance_log "
        "WHERE json_extract(operation, '$.mode') = 'apply'"
    ).fetchall()
    con.close()
    assert len(rows) == 1
    payload = json.loads(rows[0][0])
    assert payload["auto_persist"] == {"triggered": False}


def test_apply_records_mtime_did_not_advance(synthetic_db, monkeypatch):
    """B.4.14 — When force_chroma_persist reports mtime_advanced=False
    (defensive observation), the apply still succeeds and the
    maintenance_log row preserves the False flag for operator visibility."""
    fake_collection, _ = _install_apply_mocks(monkeypatch, synthetic_db)

    # Override the persist mock to report no mtime advance
    def _fake_force_no_advance(collection, ids, *, pickle_path=None, target_writes=1100):
        return {
            "success": True,
            "iterations": 17,
            "total_writes": 17 * len(ids),
            "ids_persisted": len(ids),
            "mtime_advanced": False,
            "mtime_before": 1000.0,
            "mtime_after": 1000.0,
        }
    sys.modules["chroma_persist"].force_chroma_persist = _fake_force_no_advance

    rhi.main(["--apply", "--no-history"])

    con = sqlite3.connect(synthetic_db["db_path"])
    rows = con.execute(
        "SELECT operation FROM maintenance_log "
        "WHERE json_extract(operation, '$.mode') = 'apply'"
    ).fetchall()
    con.close()
    payload = json.loads(rows[0][0])
    assert payload["auto_persist"]["mtime_advanced"] is False
    assert payload["auto_persist"]["triggered"] is True


def test_doc_chunk_embedding_text_includes_prefix(synthetic_db):
    """White-box: re-embed text for doc chunks must reconstruct the ingest_docs prefix."""
    report = rhi.audit(db_path=synthetic_db["db_path"], db_dir=synthetic_db["db_dir"])
    coll = FakeCollection()

    captured_texts: list[str] = []

    def capture_embed(texts):
        captured_texts.extend(texts)
        return _deterministic_embed(texts)

    rhi.repair(report, embed_fn=capture_embed, collection=coll, batch_size=10)
    # Find the text used for a doc chunk and a summary chunk
    doc_text = next(t for t in captured_texts if "orphan body 0" in t)
    summary_text = next(t for t in captured_texts if "summary text alpha" in t)
    # Doc chunk gets prefix
    assert "Cartella: Wealth" in doc_text
    assert "Tipo: spreadsheet" in doc_text
    assert "File: orphan.pdf" in doc_text
    # Summary chunk does NOT get an extra prefix (uses chroma:document as-is)
    assert summary_text == "summary text alpha"
    assert "Cartella:" not in summary_text


# ============================================================
# A3 — _classify_ghost dispatcher unit tests
# ============================================================
# After A3, _classify_ghost prefers 3 known mcp_server namespaces
# (kb_add / kb_update_session / /api/ingest-live) before falling
# through to the original doc_<hash>_chunk_<N> logic, which is
# preserved bit-identical. These tests cover both directions:
# positives + edge cases for the 3 new patterns, and explicit
# non-regression of the doc_ and "truly-unknown" paths.


def test_classify_ghost_kb_add_pattern_positive():
    cls, note = rhi._classify_ghost("claude_20260516_213045_4271", set())
    assert cls == "known_kb_add_ghost"
    assert "kb_add" in note

    cls2, _ = rhi._classify_ghost("chatgpt_20260516_213045_0000", set())
    assert cls2 == "known_kb_add_ghost"


def test_classify_ghost_kb_add_edge_cases():
    # 3 digits instead of 4 in the trailing hash slot → no match → unknown
    cls, _ = rhi._classify_ghost("claude_20260516_213045_427", set())
    assert cls == "unknown"
    # 7-digit date instead of 8 → no match → unknown
    cls2, _ = rhi._classify_ghost("claude_2026051_213045_4271", set())
    assert cls2 == "unknown"
    # Unknown source prefix (regex is rigid on (claude|chatgpt))
    cls3, _ = rhi._classify_ghost("gemini_20260516_213045_4271", set())
    assert cls3 == "unknown"


def test_classify_ghost_kb_session_pattern_positive():
    cls, note = rhi._classify_ghost("claude_session_20260516_213045", set())
    assert cls == "known_kb_session_ghost"
    assert "kb_update_session" in note

    cls2, _ = rhi._classify_ghost("chatgpt_session_20260516_213045", set())
    assert cls2 == "known_kb_session_ghost"


def test_classify_ghost_kb_session_edge_cases():
    # Invalid prefix — regex is rigid
    cls, _ = rhi._classify_ghost("manual_session_20260516_213045", set())
    assert cls == "unknown"
    # Truncated id (missing HHMMSS segment)
    cls2, _ = rhi._classify_ghost("claude_session_20260516", set())
    assert cls2 == "unknown"


def test_classify_ghost_live_pattern_positive():
    cls, note = rhi._classify_ghost("live_claude_abcdef0123456789", set())
    assert cls == "known_live_capture_ghost"
    assert "ingest-live" in note

    # <source> is permissive: any [A-Za-z0-9_\-]+ value the browser
    # extension might send
    cls2, _ = rhi._classify_ghost("live_web_0000111122223333", set())
    assert cls2 == "known_live_capture_ghost"

    cls3, _ = rhi._classify_ghost("live_chatgpt_deadbeef12345678", set())
    assert cls3 == "known_live_capture_ghost"


def test_classify_ghost_live_edge_cases():
    # Hex segment too short
    cls, _ = rhi._classify_ghost("live_claude_abc123", set())
    assert cls == "unknown"
    # Uppercase hex — regex requires lowercase [0-9a-f]
    cls2, _ = rhi._classify_ghost("live_claude_ABCDEF0123456789", set())
    assert cls2 == "unknown"


def test_classify_ghost_truly_unknown_remains_unknown():
    """Non-regression: ids that returned 'unknown' pre-A3 still return
    'unknown' with the original note text (bit-identical fallthrough)."""
    cls, note = rhi._classify_ghost("some_random_id", set())
    assert cls == "unknown"
    assert note == "id does not match `<prefix>_chunk_NNNN` pattern"

    # _chunk_ present, prefix not doc_ — original third 'unknown' branch
    cls2, note2 = rhi._classify_ghost("weirdo_chunk_0001", set())
    assert cls2 == "unknown"
    assert note2 == "prefix weirdo does not match document namespace"


def test_classify_ghost_doc_superseded_unchanged():
    """Non-regression: doc_<hash>_chunk_<N> behaves bit-identically.
    Both 'known_superseded_doc' branch and the two doc_ 'unknown'
    branches keep their exact pre-A3 note text."""
    # Hash NOT in registry → known_superseded_doc
    cls, note = rhi._classify_ghost("doc_abc123def456_chunk_0007", set())
    assert cls == "known_superseded_doc"
    assert note == (
        "prefix doc_abc123def456 not in current doc_registry "
        "— likely superseded prior version"
    )

    # Hash IN registry → unknown with the original note text
    cls2, note2 = rhi._classify_ghost(
        "doc_abc123def456_chunk_0007", {"abc123def456"}
    )
    assert cls2 == "unknown"
    assert note2 == (
        "prefix doc_abc123def456 matches a doc_hash still in registry "
        "— unexpected"
    )


def test_classify_ghost_precedence_session_vs_add():
    """Dispatcher ordering: kb_add and kb_session regexes are mutually
    exclusive (the first digit after `claude_` discriminates: digit →
    kb_add, 's' → kb_session). Confirm neither classification leaks
    across."""
    cls_session, _ = rhi._classify_ghost(
        "claude_session_20260516_213045", set()
    )
    assert cls_session == "known_kb_session_ghost"

    cls_add, _ = rhi._classify_ghost("claude_20260516_213045_4271", set())
    assert cls_add == "known_kb_add_ghost"


# ============================================================
# A3 — end-to-end audit with the 3 new ghost namespaces present
# ============================================================


@pytest.fixture
def synthetic_db_with_modern_ghosts(synthetic_db):
    """Extends synthetic_db's HNSW pickle with 3 extra ghosts under the
    new mcp_server namespaces (kb_add, kb_update_session, ingest-live).
    The new ghosts have no SQL row → they appear as HNSW-only entries.

    Built as a derivative fixture (NOT by parametrising synthetic_db)
    so the 16 baseline tests keep their fixed expectations
    (hnsw_only_count == 2) untouched.

    Resulting state for this fixture:
      hnsw_only_count == 5 (2 doc-superseded + 3 modern)
      unknown_count   == 0 (all classified after A3)
    """
    extra_ghosts = [
        "claude_20260516_213045_4271",      # kb_add namespace
        "claude_session_20260516_213045",   # kb_update_session namespace
        "live_claude_abcdef0123456789",     # /api/ingest-live namespace
    ]
    pickle_path = os.path.join(
        synthetic_db["db_dir"], VEC_SEG_ID, "index_metadata.pickle",
    )
    with open(pickle_path, "rb") as f:
        payload = pickle.load(f)
    next_label = max(payload["id_to_label"].values()) + 1
    for hid in extra_ghosts:
        payload["id_to_label"][hid] = next_label
        payload["label_to_id"][next_label] = hid
        next_label += 1
    payload["total_elements_added"] = len(payload["id_to_label"])
    with open(pickle_path, "wb") as f:
        pickle.dump(payload, f)

    return {**synthetic_db, "extra_ghost_ids": list(extra_ghosts)}


def test_audit_recognises_modern_ghost_namespaces(synthetic_db_with_modern_ghosts):
    """End-to-end: audit on a DB with kb_add / kb_session / live_capture
    ghosts classifies them as known_*_ghost; unknown_count == 0. This
    is the central invariant of A3."""
    report = rhi.audit(
        db_path=synthetic_db_with_modern_ghosts["db_path"],
        db_dir=synthetic_db_with_modern_ghosts["db_dir"],
    )
    assert report.hnsw_only_count == 5
    assert report.unknown_count == 0
    assert report.has_unknowns() is False

    classifications = {g.classification for g in report.hnsw_only_ghosts}
    assert "known_kb_add_ghost" in classifications
    assert "known_kb_session_ghost" in classifications
    assert "known_live_capture_ghost" in classifications
    # The 2 original doc-superseded ghosts are still classified
    assert "known_superseded_doc" in classifications


def test_apply_proceeds_with_modern_ghosts_present(synthetic_db_with_modern_ghosts):
    """End-to-end: with the 3 new ghost types present (alongside the
    existing SQL-only orphans), rhi.repair() proceeds without raising.
    The ghosts themselves are not re-embedded — only the SQL-only
    orphans are — but the apply gate no longer blocks on them.

    Pre-A3: same DB would have report.unknown_count >= 3, repair()
    raised RuntimeError, and the CLI returned rc=4. This is the user-
    visible unblock that A3 delivers."""
    report = rhi.audit(
        db_path=synthetic_db_with_modern_ghosts["db_path"],
        db_dir=synthetic_db_with_modern_ghosts["db_dir"],
    )
    coll = FakeCollection()
    result = rhi.repair(
        report,
        embed_fn=_deterministic_embed,
        collection=coll,
        batch_size=10,
    )
    assert result["repaired"] == 4
    assert result["skipped_oversize"] == 0
    upserted_ids = sorted(i for call in coll.upsert_calls for i in call["ids"])
    assert upserted_ids == sorted(
        synthetic_db_with_modern_ghosts["expected_orphan_ids"]
    )


# ============================================================
# Fix 1/2 — benign ghost detection (topic + seq window) [round 2/3]
# ============================================================

OUR_TOPIC = f"persistent://default/default/{COLLECTION_ID}"
_HEALTHY = [_make_doc_chunk(1, "doc_aaaaaaaaaaaa_chunk_0000", "doc_aaaaaaaaaaaa", "h.pdf", 0, "x")]
_GHOST = "doc_dddddddddddd_chunk_0000"
_HNSW = ["doc_aaaaaaaaaaaa_chunk_0000", _GHOST]   # _GHOST è HNSW-only
_ORPHAN = _make_doc_chunk(2, "doc_bbbbbbbbbbbb_chunk_0000", "doc_bbbbbbbbbbbb", "o.pdf", 0, "orphan")
_ORPHAN_ID = _ORPHAN["embedding_id"]


def _build_drift_db(tmp_path, *, embeddings, hnsw_ids, queue_ops=None, registry=None):
    """DB sintetico minimale per i test benigni. Ritorna (db_path, db_dir)."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    db_path = db_dir / "chroma.sqlite3"
    _make_chroma_sqlite(str(db_path), embeddings=embeddings, queue_ops=queue_ops)
    _make_hnsw_pickle(str(db_dir), VEC_SEG_ID, hnsw_ids)
    (db_dir / "doc_registry.json").write_text(json.dumps(registry or {}))
    return str(db_path), str(db_dir)


def _build_and_patch(tmp_path, monkeypatch, **kw):
    db_path, db_dir = _build_drift_db(tmp_path, **kw)
    monkeypatch.setattr(rhi, "BASE_DIR", str(tmp_path))
    monkeypatch.setattr(rhi, "DB_DIR", db_dir)
    monkeypatch.setattr(rhi, "DB_PATH", db_path)
    monkeypatch.setattr(rhi, "BACKUPS_DIR", str(tmp_path / "backups"))
    monkeypatch.setattr(rhi, "MAINTENANCE_LOCK", str(tmp_path / "maintenance.lock"))
    monkeypatch.setattr(rhi, "AUDIT_HISTORY_DIR", str(tmp_path / "logs" / "hnsw_audits"))
    return db_path, db_dir


def _enter_maint_and_backup(tmp_path):
    (tmp_path / "maintenance.lock").write_text("123")
    (tmp_path / "backups").mkdir(exist_ok=True)
    (tmp_path / "backups" / "chroma_now.sqlite3.gz").write_text("fake")


class FakeFlushCollection:
    """Stand-in per il flush path: .get(ids, include) + .upsert(ids, documents, metadatas)."""

    def __init__(self, docs):
        self._docs = docs          # {id: (document, metadata)}
        self.upserts = []

    def get(self, ids, include=None):
        oi, od, om = [], [], []
        for i in ids:
            if i in self._docs:
                oi.append(i); od.append(self._docs[i][0]); om.append(self._docs[i][1])
        return {"ids": oi, "documents": od, "metadatas": om}

    def upsert(self, ids, documents, metadatas):
        self.upserts.append({"ids": list(ids), "documents": list(documents), "metadatas": list(metadatas)})


def _install_flush_mocks(monkeypatch, docs):
    fake_collection = FakeFlushCollection(docs)

    class _FakeClient:
        def __init__(self, *a, **kw):
            pass

        def get_collection(self, name, embedding_function=None):
            return fake_collection

    fake_chromadb = type(sys)("chromadb")
    fake_chromadb.PersistentClient = _FakeClient
    monkeypatch.setitem(sys.modules, "chromadb", fake_chromadb)

    fake_embedding = type(sys)("embedding")
    fake_embedding.get_embeddings = _deterministic_embed
    fake_embedding.info = lambda: {"provider": "fake", "model": "fake", "dim": 4}
    monkeypatch.setitem(sys.modules, "embedding", fake_embedding)
    return fake_collection


# ── audit-level: benignità (topic + finestra seq) ───────────────────────────

def _audit_orphan_with_ops(tmp_path, queue_ops):
    db_path, db_dir = _build_drift_db(
        tmp_path,
        embeddings=[*_HEALTHY, _ORPHAN],
        hnsw_ids=["doc_aaaaaaaaaaaa_chunk_0000"],
        queue_ops=queue_ops,
    )
    return rhi.audit(db_path=db_path, db_dir=db_dir)


def test_orphan_pending_op_in_other_topic_stays_orphan(tmp_path):
    other = "persistent://default/default/other-collection-uuid"
    report = _audit_orphan_with_ops(
        tmp_path,
        [(2, _ORPHAN_ID, other, 1500), (2, "other-id", OUR_TOPIC, 1600)],
    )
    assert [o.embedding_id for o in report.sql_only_orphans] == [_ORPHAN_ID]


def test_orphan_upsert_above_watermark_is_pending_not_orphan(tmp_path):
    report = _audit_orphan_with_ops(tmp_path, [(2, _ORPHAN_ID, OUR_TOPIC, 1500)])
    assert report.sql_only_orphans == []


@pytest.mark.parametrize("seq", [999, 1000])
def test_orphan_upsert_at_or_below_watermark_stays_orphan(tmp_path, seq):
    report = _audit_orphan_with_ops(tmp_path, [(2, _ORPHAN_ID, OUR_TOPIC, seq)])
    assert [o.embedding_id for o in report.sql_only_orphans] == [_ORPHAN_ID]


def test_orphan_delete_above_watermark_is_never_repaired(tmp_path):
    report = _audit_orphan_with_ops(tmp_path, [(3, _ORPHAN_ID, OUR_TOPIC, 1500)])
    coll = FakeCollection()
    result = rhi.repair(report, embed_fn=_deterministic_embed, collection=coll)
    assert report.sql_only_orphans == []
    assert result["repaired"] == 0
    assert coll.upsert_calls == []


@pytest.mark.parametrize("seq", [999, 1000])
def test_orphan_delete_at_or_below_watermark_stays_orphan(tmp_path, seq):
    report = _audit_orphan_with_ops(tmp_path, [(3, _ORPHAN_ID, OUR_TOPIC, seq)])
    assert [o.embedding_id for o in report.sql_only_orphans] == [_ORPHAN_ID]


@pytest.mark.parametrize(
    "ops",
    [
        [(3, _ORPHAN_ID, OUR_TOPIC, 1500), (2, _ORPHAN_ID, OUR_TOPIC, 1600)],
        [(2, _ORPHAN_ID, OUR_TOPIC, 1500), (3, _ORPHAN_ID, OUR_TOPIC, 1600)],
    ],
)
def test_orphan_uses_final_pending_operation(tmp_path, ops):
    report = _audit_orphan_with_ops(tmp_path, ops)
    assert report.sql_only_orphans == []

def test_ghost_benign_when_delete_above_watermark(tmp_path):
    db_path, db_dir = _build_drift_db(
        tmp_path, embeddings=_HEALTHY, hnsw_ids=_HNSW,
        queue_ops=[(3, _GHOST, OUR_TOPIC, 1500)])   # DELETE sopra VEC watermark (1000)
    report = rhi.audit(db_path=db_path, db_dir=db_dir)
    g = next(x for x in report.hnsw_only_ghosts if x.embedding_id == _GHOST)
    assert g.benign is True
    assert g.pending_delete_seq == 1500
    assert report.benign_ghost_count == 1
    assert report.anomalous_ghost_count == 0


def test_ghost_anomalous_when_delete_at_or_below_watermark(tmp_path):
    # DELETE a seq == watermark (1000): Chroma non lo rilegge → NON benigno.
    db_path, db_dir = _build_drift_db(
        tmp_path, embeddings=_HEALTHY, hnsw_ids=_HNSW,
        queue_ops=[(3, _GHOST, OUR_TOPIC, 1000)])
    report = rhi.audit(db_path=db_path, db_dir=db_dir)
    g = next(x for x in report.hnsw_only_ghosts if x.embedding_id == _GHOST)
    assert g.benign is False
    assert report.benign_ghost_count == 0


def test_ghost_anomalous_delete_then_upsert_above_watermark(tmp_path):
    db_path, db_dir = _build_drift_db(
        tmp_path, embeddings=_HEALTHY, hnsw_ids=_HNSW,
        queue_ops=[(3, _GHOST, OUR_TOPIC, 1500), (2, _GHOST, OUR_TOPIC, 1600)])  # net=upsert
    report = rhi.audit(db_path=db_path, db_dir=db_dir)
    g = next(x for x in report.hnsw_only_ghosts if x.embedding_id == _GHOST)
    assert g.benign is False


def test_ghost_benign_upsert_then_delete_above_watermark(tmp_path):
    db_path, db_dir = _build_drift_db(
        tmp_path, embeddings=_HEALTHY, hnsw_ids=_HNSW,
        queue_ops=[(2, _GHOST, OUR_TOPIC, 1500), (3, _GHOST, OUR_TOPIC, 1600)])  # net=delete
    report = rhi.audit(db_path=db_path, db_dir=db_dir)
    g = next(x for x in report.hnsw_only_ghosts if x.embedding_id == _GHOST)
    assert g.benign is True
    assert g.pending_delete_seq == 1600


def test_ghost_delete_in_other_collection_is_not_benign(tmp_path):
    OTHER = "persistent://default/default/other-collection-uuid"
    db_path, db_dir = _build_drift_db(
        tmp_path, embeddings=_HEALTHY, hnsw_ids=_HNSW,
        queue_ops=[
            (3, _GHOST, OTHER, 1500),                              # delete del ghost, topic altrui
            (2, "doc_aaaaaaaaaaaa_chunk_0000", OUR_TOPIC, 1600),   # op sul nostro topic → resolve!=None
        ])
    report = rhi.audit(db_path=db_path, db_dir=db_dir)
    g = next(x for x in report.hnsw_only_ghosts if x.embedding_id == _GHOST)
    assert g.benign is False           # delete su topic altrui IGNORATO
    assert report.benign_ghost_count == 0


def test_ambiguous_topic_yields_no_benign(tmp_path):
    T1 = f"persistent://default/default/{COLLECTION_ID}"
    T2 = f"persistent://other-tenant/other-db/{COLLECTION_ID}"   # stesso suffisso /<cid>
    GHOST2 = "doc_dddddddddddd_chunk_0001"
    db_path, db_dir = _build_drift_db(
        tmp_path, embeddings=_HEALTHY,
        hnsw_ids=["doc_aaaaaaaaaaaa_chunk_0000", _GHOST, GHOST2],
        queue_ops=[(3, _GHOST, T1, 1500), (3, GHOST2, T2, 1600)])
    report = rhi.audit(db_path=db_path, db_dir=db_dir)
    assert report.collection_topic_ambiguous is True
    assert report.benign_ghost_count == 0
    assert report.anomalous_ghost_count == 2


def test_format_report_json_exposes_benign_fields(tmp_path):
    db_path, db_dir = _build_drift_db(
        tmp_path, embeddings=_HEALTHY, hnsw_ids=_HNSW,
        queue_ops=[(3, _GHOST, OUR_TOPIC, 1500)])
    report = rhi.audit(db_path=db_path, db_dir=db_dir)
    j = rhi.format_report_json(report)
    assert "anomalous_ghost_count" in j and "benign_ghost_count" in j and "blocking_unknown_count" in j
    g = next(x for x in j["hnsw_only_ghosts"] if x["embedding_id"] == _GHOST)
    assert g["benign"] is True and g["pending_delete_seq"] == 1500


# ── CLI flush gate + post-condizione ────────────────────────────────────────

def test_flush_cli_refuses_anomalous_ghost(tmp_path, monkeypatch):
    # ghost SENZA delete pendente → anomalo → rc=4 (prima dell'import chromadb)
    _build_and_patch(tmp_path, monkeypatch, embeddings=_HEALTHY, hnsw_ids=_HNSW, queue_ops=None)
    _enter_maint_and_backup(tmp_path)
    rc = rhi.main(["--flush-queue-backlog", "--no-history"])
    assert rc == 4


def test_flush_cli_allows_benign_ghost_reaches_burst(tmp_path, monkeypatch):
    # benigno + drift actionable → gate passa; fake no-op → success False → rc=5 (NON 4)
    _build_and_patch(tmp_path, monkeypatch, embeddings=_HEALTHY, hnsw_ids=_HNSW,
                     queue_ops=[(3, _GHOST, OUR_TOPIC, 2000)])   # seq alto → drift>=800
    _enter_maint_and_backup(tmp_path)
    _install_flush_mocks(monkeypatch, {"doc_aaaaaaaaaaaa_chunk_0000": ("doc", {"source": "document"})})
    rc = rhi.main(["--flush-queue-backlog", "--no-history"])
    assert rc != 4          # gate ammette il benigno
    assert rc == 5          # criterio di successo non soddisfatto (fake no-op)


@pytest.mark.parametrize("mode", ["--apply", "--flush-queue-backlog"])
def test_mutating_modes_refuse_ambiguous_topic(tmp_path, monkeypatch, mode):
    t1 = f"persistent://default/default/{COLLECTION_ID}"
    t2 = f"persistent://other-tenant/other-db/{COLLECTION_ID}"
    _build_and_patch(
        tmp_path,
        monkeypatch,
        embeddings=_HEALTHY,
        hnsw_ids=["doc_aaaaaaaaaaaa_chunk_0000"],
        queue_ops=[(2, "one", t1, 1500), (2, "two", t2, 1600)],
    )
    assert rhi.main([mode, "--no-history"]) == 4


def test_flush_cli_postcondition_fails_when_ghost_remains(synthetic_db, monkeypatch):
    # Simula flush che AVANZA vector_seq e DRENA la queue ma lascia un ghost benigno:
    # tutti i criteri classici passano, solo la post-condizione ghost fallisce → rc=5.
    Path(synthetic_db["lock_path"]).write_text("12345")
    Path(synthetic_db["backups_dir"]).mkdir(exist_ok=True)
    (Path(synthetic_db["backups_dir"]) / "chroma_now.sqlite3.gz").write_text("fake")

    ghost = rhi.GhostChunk(embedding_id=_GHOST, classification="known_superseded_doc",
                           note="", benign=True, pending_delete_seq=1500)

    def _mk_report(queue_total):
        return rhi.DriftReport(
            timestamp="t", timestamp_unix=0, db_path="x",
            metadata_segment=META_SEG_ID, vector_segment=VEC_SEG_ID,
            sql_total=1, hnsw_total=2, queue_total=queue_total,
            queue_oldest_iso=None, queue_newest_iso=None,
            collection_queue_pending_count=queue_total,
            sql_only_orphans=[], hnsw_only_ghosts=[ghost], unknown_count=0)

    reports = iter([_mk_report(400), _mk_report(0)])   # pre (actionable), post (drenata, ghost resta)
    monkeypatch.setattr(rhi, "audit", lambda *a, **k: next(reports))
    seqs = iter([
        {"collection_id": "c", "metadata_segment": META_SEG_ID, "vector_segment": VEC_SEG_ID,
         "vector_seq": 1000, "metadata_seq": 2000, "drift": 1000},   # pre
        {"collection_id": "c", "metadata_segment": META_SEG_ID, "vector_segment": VEC_SEG_ID,
         "vector_seq": 2000, "metadata_seq": 2000, "drift": 0},      # post avanzato
    ])
    monkeypatch.setattr(rhi, "_load_seq_state", lambda con, **k: next(seqs))
    monkeypatch.setattr(rhi, "_load_existing_embedding_ids",
                        lambda con, seg, n: ["doc_aaaaaaaaaaaa_chunk_0000"])
    monkeypatch.setattr(rhi, "flush_queue_backlog",
                        lambda **k: {"processed": 1, "skipped": 0, "errors": 0, "candidate_ids": 1})
    _install_flush_mocks(monkeypatch, {})
    rc = rhi.main(["--flush-queue-backlog", "--no-history"])
    assert rc == 5
