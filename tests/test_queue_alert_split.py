"""Test sdoppiamento alert queue in due segnali distinti (queue_depth_monitor.py).

Sdoppiamento 21/06: il vecchio STUCK singolo (queue_total globale + età-oldest)
generava falsi positivi non azionabili. Ora:
  - STUCK azionabile : collection_pending >= flush-watermark sostenuto da >=48h
                       (stessa metrica del gate auto-flush) → PAGE + CTA repair.
  - WAL global aging : queue_total >= WARNING e NON-decrescente in finestra, con
                       nessuna collection sopra watermark → osservabilità, NO CTA.

Invoca il monitor con --skip-drift --dry-run --ignore-snooze e override:
--db-path (fixture embeddings_queue), --snap-path (storia), --collection-pending
(forza la metrica del gate, salta l'audit), --flush-watermark.
"""

import json
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
MONITOR = REPO / "scripts" / "services" / "queue_depth_monitor.py"
PY = sys.executable
TS_FMT = "%Y-%m-%d %H:%M:%S"
WATERMARK = 300


def _make_queue_db(path, n_rows, oldest="2026-06-18 08:00:00"):
    con = sqlite3.connect(str(path))
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE embeddings_queue (seq_id INTEGER, created_at TEXT, operation INTEGER, "
        "topic TEXT, id TEXT, vector BLOB, encoding TEXT, metadata TEXT)"
    )
    for i in range(n_rows):
        cur.execute("INSERT INTO embeddings_queue (seq_id, created_at) VALUES (?,?)",
                    (i, oldest))
    con.commit()
    con.close()


def _write_history(path, snaps):
    path.write_text("".join(json.dumps(s) + "\n" for s in snaps))


def _hist_snap(hours_ago, queue_total, collection_pending):
    ts = (datetime.now() - timedelta(hours=hours_ago)).strftime(TS_FMT)
    return {"ts": ts, "queue_total": queue_total, "oldest": "2026-06-18 08:00:00",
            "collection_pending": collection_pending}


def _write_cache(path, cqp, age_hours=0.0, queue_total=900):
    """Cache audit del gate come la scrive auto_flush_queue.write_audit_cache."""
    ts_unix = int(time.time() - age_hours * 3600)
    path.write_text(json.dumps({
        "ts": datetime.fromtimestamp(ts_unix).strftime(TS_FMT),
        "ts_unix": ts_unix,
        "collection_queue_pending_count": cqp,
        "queue_total": queue_total,
    }))


def _run(tmp_path, db, cqp, history=None, watermark=WATERMARK, audit_cache=None, extra=None):
    snap = tmp_path / "snap.jsonl"
    if history is not None:
        _write_history(snap, history)
    cmd = [
        PY, str(MONITOR), "--skip-drift", "--dry-run", "--ignore-snooze",
        "--db-path", str(db),
        "--snap-path", str(snap),
        "--state-path", str(tmp_path / "state.json"),
        "--flush-watermark", str(watermark),
    ]
    if audit_cache is not None:
        cmd += ["--audit-cache-path", str(audit_cache)]  # legge la cache, come in prod
    elif cqp is not None:
        cmd += ["--collection-pending", str(cqp)]
    else:
        cmd += ["--skip-audit"]
    if extra:
        cmd += extra
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO))


# ─────────────────────── 1. FP di stamattina: niente STUCK ───────────────────

def test_fp_no_stuck_when_collection_below_watermark(tmp_path):
    """queue_total alto + oldest invariato da >48h, ma collection_pending sotto
    watermark: il vecchio STUCK scattava, ora NO (e queue<1200 → niente aging)."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 700)
    # 48h di storia con collection_pending sempre sotto watermark
    hist = [_hist_snap(h, 700, 50) for h in (60, 50, 40, 24, 12, 1)]
    r = _run(tmp_path, db, cqp=50, history=hist)
    assert r.returncode == 0, r.stderr
    assert "stuck=False" in r.stdout
    assert "would alert" not in r.stdout
    assert r.stdout.startswith("OK") or "OK queue=700" in r.stdout


# ─────────────────────── 2. vero backlog azionabile >=48h ────────────────────

def test_real_actionable_backlog_fires_stuck(tmp_path):
    """collection_pending >= watermark per tutta la finestra >=48h → STUCK + CTA repair."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 900)
    hist = [_hist_snap(h, 900, 400) for h in (60, 50, 40, 24, 12, 1)]  # anchor a 60h, mai sotto
    r = _run(tmp_path, db, cqp=400, history=hist)
    assert r.returncode == 0, r.stderr
    assert "would alert (STUCK)" in r.stdout
    assert "stuck=True" in r.stdout
    assert "repair_hnsw_index.py" in r.stdout  # CTA presente


def test_no_stuck_without_48h_history(tmp_path):
    """collection_pending sopra watermark ADESSO ma senza ancora a >=48h → NO STUCK
    (auto-flush ha ancora margine; STUCK è la persistenza, non l'istantanea)."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 900)
    hist = [_hist_snap(h, 900, 400) for h in (12, 6, 1)]  # solo 12h di storia
    r = _run(tmp_path, db, cqp=400, history=hist)
    assert r.returncode == 0, r.stderr
    assert "stuck=False" in r.stdout
    assert "would alert (STUCK)" not in r.stdout


def test_no_stuck_if_drained_in_window(tmp_path):
    """Se in finestra collection_pending è sceso sotto watermark (auto-flush ha
    drenato) → NON stuck anche se ora è risalito."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 900)
    hist = [_hist_snap(60, 900, 400), _hist_snap(40, 900, 400),
            _hist_snap(24, 900, 20),   # <-- drenato sotto watermark
            _hist_snap(1, 900, 400)]
    r = _run(tmp_path, db, cqp=400, history=hist)
    assert r.returncode == 0, r.stderr
    assert "stuck=False" in r.stdout
    assert "would alert (STUCK)" not in r.stdout


# ─────────────────────── 3. WAL aging: crescita, non età ─────────────────────

def test_wal_aging_fires_on_growth(tmp_path):
    """queue_total >= WARNING(1200) e non-decrescente in finestra, collection sotto
    watermark → WAL_AGING (osservabilità) SENZA CTA repair."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 1300)
    hist = [_hist_snap(12, 1250, 50), _hist_snap(7, 1280, 50)]  # ref a 7h: 1280 <= 1300
    r = _run(tmp_path, db, cqp=50, history=hist)
    assert r.returncode == 0, r.stderr
    assert "would alert (WAL_AGING)" in r.stdout
    assert "aging=True" in r.stdout
    assert "repair_hnsw_index.py" not in r.stdout  # NESSUN CTA repair
    assert "sync_threshold" in r.stdout


def test_wal_aging_critical_severity_no_cta(tmp_path):
    """queue_total >= CRITICAL(1800) → severità CRITICAL, comunque NESSUN CTA repair."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 1900)
    hist = [_hist_snap(12, 1850, 50), _hist_snap(7, 1880, 50)]
    r = _run(tmp_path, db, cqp=50, history=hist)
    assert r.returncode == 0, r.stderr
    assert "would alert (WAL_AGING)" in r.stdout
    assert "CRITICAL" in r.stdout
    assert "repair_hnsw_index.py" not in r.stdout


def test_no_wal_aging_when_decreasing(tmp_path):
    """queue_total >= WARNING ma in CALO rispetto alla finestra (l'indexer drena) →
    NO aging: il trigger è crescita, non profondità statica/età."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 1300)
    hist = [_hist_snap(12, 1500, 50), _hist_snap(7, 1450, 50)]  # ref 1450 > 1300 attuale
    r = _run(tmp_path, db, cqp=50, history=hist)
    assert r.returncode == 0, r.stderr
    assert "aging=False" in r.stdout
    assert "would alert" not in r.stdout


def test_no_wal_aging_without_window_history(tmp_path):
    """Senza snapshot-ancora a >=finestra non si asserisce 'non drena' → NO aging."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 1300)
    hist = [_hist_snap(1, 1280, 50)]  # solo 1h fa, nessuna ancora a >=6h
    r = _run(tmp_path, db, cqp=50, history=hist)
    assert r.returncode == 0, r.stderr
    assert "aging=False" in r.stdout
    assert "would alert" not in r.stdout


# ─────────────────────── 4. mutua esclusione segnali ─────────────────────────

def test_stuck_takes_precedence_over_aging(tmp_path):
    """collection sopra watermark da >=48h E queue_total alto+crescente: scatta SOLO
    STUCK (collection azionabile esiste → WAL-aging gating su 'nessuna collection')."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 1900)
    hist = [_hist_snap(h, 1900, 400) for h in (60, 40, 24, 7, 1)]
    r = _run(tmp_path, db, cqp=400, history=hist)
    assert r.returncode == 0, r.stderr
    assert "would alert (STUCK)" in r.stdout
    assert "WAL_AGING" not in r.stdout
    assert "repair_hnsw_index.py" in r.stdout


# ─────────────────────── 5. audit assente → degrado safe ─────────────────────

def test_audit_absent_no_stuck_aging_still_works(tmp_path):
    """collection_pending non disponibile (--skip-audit): STUCK non valutabile (safe),
    ma WAL-aging globale resta osservabile."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 1300)
    hist = [_hist_snap(12, 1250, None), _hist_snap(7, 1280, None)]
    r = _run(tmp_path, db, cqp=None, history=hist)
    assert r.returncode == 0, r.stderr
    assert "stuck=False" in r.stdout
    assert "would alert (WAL_AGING)" in r.stdout


# ─────────────────── 6. cache audit del gate (no re-audit) ───────────────────

def test_cache_fresh_used_for_stuck(tmp_path):
    """Il monitor legge collection_pending dalla cache del gate (non rifà l'audit):
    cache fresca cqp>=watermark + storia >=48h → STUCK + CTA."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 900)
    cache = tmp_path / "gate_cache.json"
    _write_cache(cache, cqp=400, age_hours=2)  # 2h fa: fresca
    hist = [_hist_snap(h, 900, 400) for h in (60, 40, 24, 1)]
    r = _run(tmp_path, db, cqp=None, history=hist, audit_cache=cache)
    assert r.returncode == 0, r.stderr
    assert "collection_pending=400" in r.stdout
    assert "would alert (STUCK)" in r.stdout
    assert "repair_hnsw_index.py" in r.stdout


def test_cache_stale_no_stuck(tmp_path):
    """Cache oltre CACHE_MAX_AGE_HOURS (13h) → collection_pending non valutabile →
    STUCK non scatta anche con storia 48h sopra watermark (degrado safe)."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 900)  # queue < WARNING → niente aging a mascherare
    cache = tmp_path / "gate_cache.json"
    _write_cache(cache, cqp=400, age_hours=14)  # stale
    hist = [_hist_snap(h, 900, 400) for h in (60, 40, 24, 1)]
    r = _run(tmp_path, db, cqp=None, history=hist, audit_cache=cache)
    assert r.returncode == 0, r.stderr
    assert "collection_pending=None" in r.stdout
    assert "would alert (STUCK)" not in r.stdout
    assert r.stdout.startswith("OK")


def test_cache_absent_no_stuck(tmp_path):
    """Cache inesistente → collection_pending None → STUCK non valutabile."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 900)
    cache = tmp_path / "does_not_exist.json"
    hist = [_hist_snap(h, 900, 400) for h in (60, 40, 24, 1)]
    r = _run(tmp_path, db, cqp=None, history=hist, audit_cache=cache)
    assert r.returncode == 0, r.stderr
    assert "collection_pending=None" in r.stdout
    assert "would alert (STUCK)" not in r.stdout


def test_cache_malformed_no_stuck(tmp_path):
    """Cache JSON illeggibile → collection_pending None → STUCK non valutabile."""
    db = tmp_path / "c.sqlite3"
    _make_queue_db(db, 900)
    cache = tmp_path / "gate_cache.json"
    cache.write_text("{not valid json")
    hist = [_hist_snap(h, 900, 400) for h in (60, 40, 24, 1)]
    r = _run(tmp_path, db, cqp=None, history=hist, audit_cache=cache)
    assert r.returncode == 0, r.stderr
    assert "collection_pending=None" in r.stdout
    assert "would alert (STUCK)" not in r.stdout
