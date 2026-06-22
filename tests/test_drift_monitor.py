"""Test drift VECTOR vs META detection in queue_depth_monitor.py (Path A azione 3).

Costruisce fixture sqlite minimali (collections/segments/max_seq_id) e invoca il
monitor con --skip-queue --dry-run --ignore-snooze, così non tocca DB live né manda
Telegram. Copre: WARNING (>800), CRITICAL (>1500), NORMAL (0), STUCK_VECTOR,
decoding seq_id blob little-endian, e i campi dello snapshot jsonl.
Soglie allineate a queue_depth_monitor.DRIFT_WARNING/DRIFT_CRITICAL (lazy
compaction sotto sync_threshold=1000 è benigna).
"""

import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
MONITOR = REPO / "scripts" / "services" / "queue_depth_monitor.py"
PY = sys.executable


def _write_gate_cache(path, collection_name="tailor_kb_v2", vector_seq=0, drift=0,
                      flush_ran=True, flush_resolved=False, age_hours=2.0):
    """Cache audit del gate come la scrive auto_flush_queue.write_audit_cache."""
    path.write_text(json.dumps({
        "ts_unix": int(time.time() - age_hours * 3600),
        "collection_name": collection_name,
        "vector_seq": vector_seq, "drift": drift,
        "flush_ran": flush_ran, "flush_resolved": flush_resolved,
        "collection_queue_pending_count": 0, "queue_total": 0,
    }))


def _make_db(path, collections):
    """collections: list of {name, vec_seq, meta_seq, seq_as_blob?}."""
    con = sqlite3.connect(str(path))
    cur = con.cursor()
    cur.execute("CREATE TABLE collections (id TEXT, name TEXT)")
    cur.execute("CREATE TABLE segments (id TEXT, type TEXT, scope TEXT, collection TEXT)")
    cur.execute("CREATE TABLE max_seq_id (segment_id TEXT, seq_id)")
    cur.execute(
        "CREATE TABLE embeddings_queue (seq_id INTEGER, created_at TEXT, operation INTEGER, "
        "topic TEXT, id TEXT, vector BLOB, encoding TEXT, metadata TEXT)"
    )
    for i, c in enumerate(collections):
        cid, vseg, mseg = f"col-{i}", f"vec-{i}", f"meta-{i}"
        cur.execute("INSERT INTO collections VALUES (?,?)", (cid, c["name"]))
        cur.execute("INSERT INTO segments VALUES (?,?,?,?)", (vseg, "v", "VECTOR", cid))
        cur.execute("INSERT INTO segments VALUES (?,?,?,?)", (mseg, "m", "METADATA", cid))

        def enc(v):
            return v.to_bytes(8, "little") if c.get("seq_as_blob") else v

        cur.execute("INSERT INTO max_seq_id VALUES (?,?)", (vseg, enc(c["vec_seq"])))
        cur.execute("INSERT INTO max_seq_id VALUES (?,?)", (mseg, enc(c["meta_seq"])))
    con.commit()
    con.close()


def _run(db, drift_snap, drift_state, extra=None):
    cmd = [
        PY, str(MONITOR), "--skip-queue", "--dry-run", "--ignore-snooze",
        "--db-path", str(db),
        "--drift-snap-path", str(drift_snap),
        "--drift-state-path", str(drift_state),
    ]
    if extra:
        cmd += extra
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO))


def test_drift_warning(tmp_path):
    # drift 900: > DRIFT_WARNING(800), < DRIFT_CRITICAL(1500)
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 1000, "meta_seq": 1900}])
    r = _run(db, tmp_path / "d.jsonl", tmp_path / "s.json")
    assert r.returncode == 0, r.stderr
    assert "would alert tailor_kb_v2:WARNING" in r.stdout
    assert "`900`" in r.stdout
    assert "CRITICAL" not in r.stdout
    # WARNING è puramente informativo: niente CTA repair (azione manuale no-op nella
    # zona lazy-compaction; il caso reale è coperto da STUCK_VECTOR confermato).
    assert "repair_hnsw_index.py" not in r.stdout


def test_drift_critical(tmp_path):
    # drift 1600: > DRIFT_CRITICAL(1500)
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 317321, "meta_seq": 318921}])
    r = _run(db, tmp_path / "d.jsonl", tmp_path / "s.json")
    assert r.returncode == 0, r.stderr
    assert "would alert tailor_kb_v2:CRITICAL" in r.stdout
    assert "`1600`" in r.stdout
    # CRITICAL (drift > 1500, ben oltre sync_threshold) NON è zona benigna → CTA invariata.
    assert "repair_hnsw_index.py" in r.stdout


def test_drift_normal(tmp_path):
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb", "vec_seq": 5000, "meta_seq": 5000}])
    r = _run(db, tmp_path / "d.jsonl", tmp_path / "s.json")
    assert r.returncode == 0, r.stderr
    assert "DRIFT_OK tailor_kb drift=0 status=NORMAL" in r.stdout
    assert "would alert" not in r.stdout


def _prior_snap(drift_snap, vector_seq, meta_seq):
    """Snapshot drift precedente (per calcolare vdelta/mdelta nel monitor)."""
    drift_snap.write_text(json.dumps({
        "ts": "2026-05-28 20:00:00", "collection_name": "tailor_kb_v2",
        "vector_seq": vector_seq, "meta_seq": meta_seq, "drift": meta_seq - vector_seq,
        "vector_seq_delta_since_last": None, "meta_seq_delta_since_last": None,
        "status": "WARNING", "stuck_vector": False,
    }) + "\n")


def test_stuck_vector_benign_below_sync_threshold(tmp_path):
    """FP di stamattina: drift 836 (< SYNC_THRESHOLD 1000), vector fermo su 1 finestra.
    Sotto soglia è lazy compaction → A NON scatta → niente STUCK_VECTOR (solo WARNING)."""
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 333672, "meta_seq": 334508}])  # drift 836
    drift_snap = tmp_path / "d.jsonl"
    _prior_snap(drift_snap, vector_seq=333672, meta_seq=334399)  # vdelta 0, mdelta 109
    cache = tmp_path / "gate_cache.json"
    _write_gate_cache(cache, vector_seq=333672, flush_resolved=True)  # realistico: flush poi riuscito
    r = _run(db, drift_snap, tmp_path / "s.json", extra=["--audit-cache-path", str(cache)])
    assert r.returncode == 0, r.stderr
    assert "STUCK_VECTOR" not in r.stdout
    assert "would alert tailor_kb_v2:WARNING" in r.stdout


def test_stuck_vector_confirmed_real_6975(tmp_path):
    """#6975 reale: drift 1100 (>= SYNC_THRESHOLD), vector fermo, e la cache dice che
    l'ultimo flush del gate NON ha risolto (flush_resolved=False) col vector non
    avanzato → STUCK_VECTOR confermato + CTA repair."""
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 333672, "meta_seq": 334772}])  # drift 1100
    drift_snap = tmp_path / "d.jsonl"
    _prior_snap(drift_snap, vector_seq=333672, meta_seq=333872)  # vdelta 0, mdelta 900
    cache = tmp_path / "gate_cache.json"
    _write_gate_cache(cache, vector_seq=333672, drift=1100,
                      flush_ran=True, flush_resolved=False)
    r = _run(db, drift_snap, tmp_path / "s.json", extra=["--audit-cache-path", str(cache)])
    assert r.returncode == 0, r.stderr
    assert "would alert tailor_kb_v2:STUCK_VECTOR" in r.stdout
    assert "STUCK_VECTOR_UNCONFIRMED" not in r.stdout
    assert "repair_hnsw_index.py" in r.stdout  # CTA presente solo qui


def test_stuck_vector_suppressed_when_flush_resolved(tmp_path):
    """drift 1100 + vector fermo, ma la cache dice flush RISOLTO (flush_resolved=True):
    transitorio benigno pre-flush → soppresso (nessun STUCK_VECTOR né UNCONFIRMED)."""
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 333672, "meta_seq": 334772}])
    drift_snap = tmp_path / "d.jsonl"
    _prior_snap(drift_snap, vector_seq=333672, meta_seq=333872)
    cache = tmp_path / "gate_cache.json"
    _write_gate_cache(cache, vector_seq=333672, flush_ran=True, flush_resolved=True)
    r = _run(db, drift_snap, tmp_path / "s.json", extra=["--audit-cache-path", str(cache)])
    assert r.returncode == 0, r.stderr
    assert "STUCK_VECTOR" not in r.stdout  # né confermato né unconfirmed


def test_stuck_vector_suppressed_when_vector_advanced(tmp_path):
    """flush non risolto MA il vector live è avanzato oltre la cache (consumer ripartito)
    → C=False → soppresso."""
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 333672, "meta_seq": 334772}])
    drift_snap = tmp_path / "d.jsonl"
    _prior_snap(drift_snap, vector_seq=333672, meta_seq=333872)
    cache = tmp_path / "gate_cache.json"
    _write_gate_cache(cache, vector_seq=333000, flush_ran=True, flush_resolved=False)  # < current
    r = _run(db, drift_snap, tmp_path / "s.json", extra=["--audit-cache-path", str(cache)])
    assert r.returncode == 0, r.stderr
    assert "STUCK_VECTOR" not in r.stdout


def test_stuck_vector_unconfirmed_cache_absent(tmp_path):
    """drift 1100 + vector fermo ma cache assente → C non valutabile → ricade su A:
    STUCK_VECTOR_UNCONFIRMED (osservabilità), SENZA CTA repair."""
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 333672, "meta_seq": 334772}])
    drift_snap = tmp_path / "d.jsonl"
    _prior_snap(drift_snap, vector_seq=333672, meta_seq=333872)
    absent = tmp_path / "nope.json"
    r = _run(db, drift_snap, tmp_path / "s.json", extra=["--audit-cache-path", str(absent)])
    assert r.returncode == 0, r.stderr
    assert "would alert tailor_kb_v2:STUCK_VECTOR_UNCONFIRMED" in r.stdout
    assert "non confermato" in r.stdout
    # drift 1100 → WARNING (ora senza CTA) + UNCONFIRMED (senza CTA): nessuna CTA repair
    # nell'intero run (< CRITICAL 1500). Conferma che il WARNING non reintroduce la CTA.
    assert "repair_hnsw_index.py" not in r.stdout


def test_no_stuck_when_vector_advances(tmp_path):
    db = tmp_path / "c.sqlite3"
    # vector avanza (1000 -> 1050), meta avanza (1100 -> 1200): NON stuck
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 1050, "meta_seq": 1200}])
    drift_snap = tmp_path / "d.jsonl"
    drift_snap.write_text(json.dumps({
        "ts": "2026-05-28 20:00:00", "collection_name": "tailor_kb_v2",
        "vector_seq": 1000, "meta_seq": 1100, "drift": 100,
        "vector_seq_delta_since_last": None, "meta_seq_delta_since_last": None,
        "status": "WARNING", "stuck_vector": False,
    }) + "\n")
    r = _run(db, drift_snap, tmp_path / "s.json")
    assert r.returncode == 0, r.stderr
    assert "STUCK_VECTOR" not in r.stdout


def test_blob_seq_decoding(tmp_path):
    # drift 1600 (> CRITICAL 1500) con seq_id blob little-endian
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 1000, "meta_seq": 2600,
                   "seq_as_blob": True}])
    r = _run(db, tmp_path / "d.jsonl", tmp_path / "s.json")
    assert r.returncode == 0, r.stderr
    assert "would alert tailor_kb_v2:CRITICAL" in r.stdout
    assert "`1600`" in r.stdout


def test_snapshot_fields_written(tmp_path):
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 1000, "meta_seq": 1900}])
    drift_snap = tmp_path / "d.jsonl"
    _run(db, drift_snap, tmp_path / "s.json")
    lines = [json.loads(x) for x in drift_snap.read_text().splitlines() if x.strip()]
    assert lines, "no snapshot written"
    rec = lines[-1]
    for field in ("ts", "collection_name", "vector_seq", "meta_seq", "drift",
                  "vector_seq_delta_since_last", "meta_seq_delta_since_last", "status"):
        assert field in rec, f"campo mancante: {field}"
    assert rec["drift"] == 900
    assert rec["status"] == "WARNING"


def test_two_collections_independent(tmp_path):
    db = tmp_path / "c.sqlite3"
    _make_db(db, [
        {"name": "tailor_kb", "vec_seq": 5000, "meta_seq": 5000},      # NORMAL
        {"name": "tailor_kb_v2", "vec_seq": 317321, "meta_seq": 318921},  # CRITICAL (drift 1600)
    ])
    r = _run(db, tmp_path / "d.jsonl", tmp_path / "s.json")
    assert r.returncode == 0, r.stderr
    assert "DRIFT_OK tailor_kb drift=0" in r.stdout
    assert "would alert tailor_kb_v2:CRITICAL" in r.stdout
