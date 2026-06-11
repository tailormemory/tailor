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
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
MONITOR = REPO / "scripts" / "services" / "queue_depth_monitor.py"
PY = sys.executable


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


def test_drift_critical(tmp_path):
    # drift 1600: > DRIFT_CRITICAL(1500)
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 317321, "meta_seq": 318921}])
    r = _run(db, tmp_path / "d.jsonl", tmp_path / "s.json")
    assert r.returncode == 0, r.stderr
    assert "would alert tailor_kb_v2:CRITICAL" in r.stdout
    assert "`1600`" in r.stdout


def test_drift_normal(tmp_path):
    db = tmp_path / "c.sqlite3"
    _make_db(db, [{"name": "tailor_kb", "vec_seq": 5000, "meta_seq": 5000}])
    r = _run(db, tmp_path / "d.jsonl", tmp_path / "s.json")
    assert r.returncode == 0, r.stderr
    assert "DRIFT_OK tailor_kb drift=0 status=NORMAL" in r.stdout
    assert "would alert" not in r.stdout


def test_stuck_vector(tmp_path):
    db = tmp_path / "c.sqlite3"
    # vector fermo a 1000, meta avanzato 1100 -> 1900 (drift 900 > WARNING 800)
    _make_db(db, [{"name": "tailor_kb_v2", "vec_seq": 1000, "meta_seq": 1900}])
    drift_snap = tmp_path / "d.jsonl"
    drift_snap.write_text(json.dumps({
        "ts": "2026-05-28 20:00:00", "collection_name": "tailor_kb_v2",
        "vector_seq": 1000, "meta_seq": 1100, "drift": 100,
        "vector_seq_delta_since_last": None, "meta_seq_delta_since_last": None,
        "status": "WARNING", "stuck_vector": False,
    }) + "\n")
    r = _run(db, drift_snap, tmp_path / "s.json")
    assert r.returncode == 0, r.stderr
    assert "would alert tailor_kb_v2:STUCK_VECTOR" in r.stdout
    # drift 900 → anche WARNING (trigger separato)
    assert "would alert tailor_kb_v2:WARNING" in r.stdout


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
