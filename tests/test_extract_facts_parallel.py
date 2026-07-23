"""Tests: dispatch parallelo di extract_facts_nightly.

Copre le quattro invarianti del pool (N worker + writer unico):

  1. accounting PER IDENTITÀ — con N chiamate in volo il risultato che
     torna appartiene al backend che l'ha servita, non a `current_idx`;
  2. semaforo FUORI dal watchdog — il wait_for misura la chiamata, mai
     l'attesa di uno slot;
  3. writer unico — nessun worker tocca facts.sqlite3;
  4. 429 — stop dispatch sul provider, drain degli in-flight, requeue del
     chunk sul backend successivo.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.enrichment import extract_facts_nightly as efn  # noqa: E402


FACT = {"fact": "Emiliano ha scelto sqlite per i fatti.", "category": "decision"}


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------

def _chunks(n, start=0):
    # `source` porta l'id fra parentesi: è l'unico campo del chunk che
    # finisce nel prompt, e serve alle fake call per riconoscere il chunk.
    return [{"id": f"c{i}", "text": "x" * 100, "date": "2026-07-01",
             "source": f"[c{i}]"}
            for i in range(start, start + n)]


def _manager(monkeypatch, backends, worker_cap=4, limit=1000, jitter=lambda: 0.0, clock=None):
    monkeypatch.setattr(efn, "_BACKENDS_CFG", list(backends))
    monkeypatch.setattr(efn, "_API_KEYS",
                        {b["provider"]: "k" for b in backends})
    monkeypatch.setattr(efn, "DAILY_LIMIT_PER_BACKEND", limit)
    kw = {"worker_cap": worker_cap, "jitter": jitter}
    if clock is not None:
        kw["clock"] = clock
    return efn.BackendManager(**kw)


def _two_backends():
    return [
        {"provider": "anthropic", "model": "haiku", "workers": 15},
        {"provider": "google", "model": "flash", "workers": 5},
    ]


def _memdb():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, chunk_id TEXT NOT NULL,
            fact TEXT NOT NULL, category TEXT DEFAULT '',
            entity_tags TEXT DEFAULT '[]', event_date TEXT DEFAULT '',
            confidence REAL DEFAULT 1.0, superseded_by INTEGER DEFAULT NULL,
            superseded_at TEXT DEFAULT '', created_at TEXT NOT NULL,
            expires_at TEXT DEFAULT NULL, derived_from TEXT DEFAULT '',
            relation_type TEXT DEFAULT 'extracted', document_date TEXT DEFAULT '');
        CREATE TABLE extraction_log (
            chunk_id TEXT PRIMARY KEY, facts_count INTEGER DEFAULT 0,
            model TEXT DEFAULT '', extracted_at TEXT NOT NULL);
    """)
    return conn


def _run(manager, chunks, call, workers=4, facts_conn=None):
    state = efn.RunState(total=len(chunks))

    async def go():
        await efn.run_pool(None, chunks, manager, facts_conn, workers,
                           state, "2026-07-23T00:00:00", call=call)

    asyncio.run(go())
    return state


class _Response:
    """Risposta minima con headers, per i rami 429."""

    def __init__(self, status, headers=None, body="{}"):
        self.status = status
        self.headers = headers or {}
        self._body = body

    async def text(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


# ==================================================================
# 1. Accounting per identità
# ==================================================================

def test_accounting_follows_call_identity_under_interleaving(monkeypatch):
    """Il puntatore di dispatch si sposta MENTRE una chiamata è in volo:
    il successo deve restare addebitato a chi lo ha prodotto.

    La fake call di anthropic ruota `current_idx` e poi dorme, così
    quando l'esito arriva al writer "il backend corrente" è già google.
    Con l'accounting su current_idx, la call di anthropic finirebbe nel
    contatore di google (ed è esattamente il finding di Codex)."""
    manager = _manager(monkeypatch, _two_backends(), worker_cap=2)
    served = []

    async def call(session, prompt, backend):
        served.append(backend["name"])
        if backend["name"] == "anthropic":
            manager.current_idx = 1        # rotazione decisa da un altro worker
            await asyncio.sleep(0.05)      # l'esito torna a puntatore spostato
        return [FACT]

    state = _run(manager, _chunks(6), call, workers=2)

    anthropic, google = manager.backends
    assert served.count("anthropic") >= 1
    assert anthropic["calls"] == served.count("anthropic")
    assert google["calls"] == served.count("google")
    assert anthropic["calls"] + google["calls"] == state.processed == 6


def test_outcomes_are_applied_out_of_order_to_the_right_backend(monkeypatch):
    """Interleaving esplicito sul writer: esiti di due backend applicati
    fuori ordine, con il puntatore fermo su un terzo stato."""
    manager = _manager(monkeypatch, _two_backends(), worker_cap=4)
    tracker = efn._WorkTracker(4)
    state = efn.RunState(total=4)
    queue = asyncio.Queue()

    def outcome(chunk_id, result, backend_id, epoch=0):
        b = manager.backends[backend_id]
        return efn.CallOutcome({"id": chunk_id, "date": ""}, result,
                               backend_id, b["name"], b["model"], epoch)

    seq = [
        outcome("c0", [FACT], 0),
        outcome("c1", None, 1),
        outcome("c2", [FACT], 1),
        outcome("c3", None, 0),
    ]
    for item in seq:
        manager.current_idx = 1  # il puntatore non c'entra con l'accounting
        efn._apply_outcome(item, manager, queue, tracker, None, state, "now")

    assert manager.backends[0]["calls"] == 1
    assert manager.backends[1]["calls"] == 1
    assert manager.backends[0]["none_consecutive"] == 1
    assert manager.backends[1]["none_consecutive"] == 0  # azzerato dal successo c2
    assert state.processed == 4 and state.errors == 2


def test_mark_success_is_scoped_to_the_backend_id(monkeypatch):
    manager = _manager(monkeypatch, _two_backends(), worker_cap=4)
    manager.current_idx = 1  # il puntatore è già altrove

    manager.mark_success(0)
    manager.mark_success(0)

    assert manager.backends[0]["calls"] == 2
    assert manager.backends[1]["calls"] == 0


def test_mark_none_streak_is_per_backend(monkeypatch):
    """Due backend che alternano None non devono sommare gli streak."""
    manager = _manager(monkeypatch, _two_backends(), worker_cap=4)

    for _ in range(efn.NONE_CONSECUTIVE_THRESHOLD - 1):
        assert manager.mark_none(0) is False
        assert manager.mark_none(1) is False

    assert manager.backends[0]["none_consecutive"] == efn.NONE_CONSECUTIVE_THRESHOLD - 1
    assert manager.backends[1]["none_consecutive"] == efn.NONE_CONSECUTIVE_THRESHOLD - 1
    assert manager.mark_none(0) is True
    assert manager.backends[1]["none_consecutive"] == efn.NONE_CONSECUTIVE_THRESHOLD - 1


def test_success_on_one_backend_does_not_reset_the_other_streak(monkeypatch):
    manager = _manager(monkeypatch, _two_backends(), worker_cap=4)
    manager.mark_timeout(0)
    manager.mark_none(1)

    manager.mark_success(1)

    assert manager.backends[0]["timeout_consecutive"] == 1
    assert manager.backends[1]["none_consecutive"] == 0


# ==================================================================
# 2. Semaforo fuori dal watchdog
# ==================================================================

def test_semaphore_wait_is_not_inside_the_watchdog(monkeypatch):
    """Cap 1 sul provider e watchdog corto: la somma delle attese in coda
    supera di molto il budget del watchdog, ma nessuna chiamata scade
    perché il timer parte DOPO l'acquisizione dello slot."""
    monkeypatch.setattr(efn, "BACKEND_CALL_TIMEOUT_SECONDS", 0.25)
    manager = _manager(monkeypatch, [
        {"provider": "anthropic", "model": "haiku", "workers": 1},
    ], worker_cap=4)
    assert manager.backends[0]["workers"] == 1  # cap per-provider

    async def call(session, prompt, backend):
        await asyncio.sleep(0.1)     # < watchdog, ma serializzato ×6
        return [FACT]

    state = _run(manager, _chunks(6), call, workers=4)

    assert state.errors == 0, "il semaforo è dentro il timer: TIMEOUT spuri"
    assert state.processed == 6
    assert manager.backends[0]["timeout_consecutive"] == 0


def test_watchdog_still_fires_on_a_slow_provider(monkeypatch):
    """Controprova: la chiamata vera, oltre il budget, scade comunque."""
    monkeypatch.setattr(efn, "BACKEND_CALL_TIMEOUT_SECONDS", 0.05)
    manager = _manager(monkeypatch, _two_backends(), worker_cap=2)

    async def call(session, prompt, backend):
        if backend["name"] == "anthropic":
            await asyncio.sleep(5)
            return [FACT]
        return [FACT]

    state = _run(manager, _chunks(1), call, workers=1)

    assert manager.backends[0]["timeout_consecutive"] == 1
    assert state.requeued == 1
    assert state.processed == 1 and state.errors == 0  # ripreso da google


def test_per_provider_cap_only_lowers_parallelism(monkeypatch):
    """Il numero in config è un tetto, non un target: con --workers 4 un
    backend che dichiara workers: 15 resta a 4."""
    manager = _manager(monkeypatch, _two_backends(), worker_cap=4)
    assert [b["workers"] for b in manager.backends] == [4, 4]

    manager = _manager(monkeypatch, _two_backends(), worker_cap=8)
    assert [b["workers"] for b in manager.backends] == [8, 5]


def test_default_workers_is_four():
    assert efn.DEFAULT_WORKERS == 4


# ==================================================================
# 3. Writer unico
# ==================================================================

class _TracingConn:
    """Proxy sulla connessione che registra quale task esegue SQL."""

    def __init__(self, conn):
        self._conn = conn
        self.callers = set()

    def _note(self):
        task = asyncio.current_task()
        self.callers.add(task.get_name() if task else "sync")

    def cursor(self):
        self._note()
        return _TracingCursor(self._conn.cursor(), self)

    def execute(self, *a, **kw):
        self._note()
        return self._conn.execute(*a, **kw)

    def commit(self):
        self._note()
        return self._conn.commit()


class _TracingCursor:
    def __init__(self, cur, owner):
        self._cur = cur
        self._owner = owner

    def execute(self, *a, **kw):
        self._owner._note()
        return self._cur.execute(*a, **kw)

    def fetchone(self):
        return self._cur.fetchone()


def test_only_the_writer_touches_sqlite(monkeypatch):
    manager = _manager(monkeypatch, _two_backends(), worker_cap=4)
    db = _memdb()
    traced = _TracingConn(db)

    async def call(session, prompt, backend):
        await asyncio.sleep(0)
        return None if "[c3]" in prompt else [FACT]

    state = _run(manager, _chunks(8), call, workers=4, facts_conn=traced)

    assert traced.callers == {"fact-writer"}, f"SQL fuori dal writer: {traced.callers}"
    assert state.processed == 8
    rows = db.execute("SELECT COUNT(*) FROM extraction_log").fetchone()[0]
    assert rows == 8


def test_writer_persists_facts_and_failures(monkeypatch):
    manager = _manager(monkeypatch, _two_backends(), worker_cap=2)
    db = _memdb()

    async def call(session, prompt, backend):
        return None if "[c0]" in prompt else [FACT, {"fact": "corto"}]

    state = _run(manager, _chunks(3), call, workers=2, facts_conn=db)

    # 'corto' scartato: < 10 caratteri
    assert state.facts == 2
    assert db.execute("SELECT COUNT(*) FROM facts").fetchone()[0] == 2
    log = dict(db.execute("SELECT chunk_id, model FROM extraction_log").fetchall())
    assert log["c0"] == "failed_1"          # None -> fail counter, non successo
    assert log["c1"] in ("anthropic", "google")
    assert state.errors == 1


# ==================================================================
# 4. 429: stop dispatch, drain, requeue
# ==================================================================

def test_rate_limit_stops_dispatch_and_requeues_on_next_backend(monkeypatch):
    """Il 429 mette anthropic in cooldown: nessun dispatch NUOVO ci va
    più, e il chunk colpito rientra in coda e riparte su google.

    Un worker solo: così non ci sono chiamate già in volo e l'assert
    isola il dispatch dal drain (coperto da
    test_cooldown_epoch_guard_collapses_a_burst_of_429)."""
    manager = _manager(monkeypatch, _two_backends(), worker_cap=2)
    served = []

    async def call(session, prompt, backend):
        served.append(backend["name"])
        if backend["name"] == "anthropic" and served.count("anthropic") == 1:
            return efn.RateLimited(None)
        return [FACT]

    state = _run(manager, _chunks(4), call, workers=1, facts_conn=None)

    assert state.processed == 4
    assert state.requeued == 1
    assert state.errors == 0
    assert manager.backends[0]["cooldown_until"] > 0      # in cooldown
    assert manager.backends[0]["exhausted"] is False      # non bruciato per la run
    assert manager.backends[0]["rate_limit_hits"] == 1
    assert manager.backends[0]["calls"] == 0              # il 429 non è una call utile
    assert manager.backends[1]["calls"] == 4
    assert served == ["anthropic"] + ["google"] * 4, \
        "dispatch continuato su un provider in cooldown"


def test_cooldown_epoch_guard_collapses_a_burst_of_429(monkeypatch):
    """N worker in volo prendono 429 nella stessa raffica: un solo
    cooldown, un solo hit. I residui portano una epoch superata."""
    now = [1000.0]
    manager = _manager(monkeypatch, _two_backends(), worker_cap=4,
                       clock=lambda: now[0])

    assert manager.mark_rate_limited(0, epoch=0) is True
    first_deadline = manager.backends[0]["cooldown_until"]

    for _ in range(3):  # residui in volo della stessa epoch
        assert manager.mark_rate_limited(0, epoch=0) is False

    assert manager.backends[0]["rate_limit_hits"] == 1
    assert manager.backends[0]["cooldown_until"] == first_deadline
    assert manager.backends[0]["epoch"] == 1


def test_cooldown_expires_and_backend_returns_available(monkeypatch):
    now = [1000.0]
    manager = _manager(monkeypatch, _two_backends(), worker_cap=4,
                       clock=lambda: now[0])

    manager.mark_rate_limited(0, epoch=0)
    assert manager.current()["name"] == "google"
    assert manager.cooldown_eta() > 0

    now[0] += efn.RATE_LIMIT_COOLDOWN_MAX
    assert manager.usable(manager.backends[0]) is True
    assert manager.cooldown_eta() is None


def test_repeated_429_eventually_exhausts_the_backend(monkeypatch):
    now = [1000.0]
    manager = _manager(monkeypatch, _two_backends(), worker_cap=4,
                       clock=lambda: now[0])

    for i in range(efn.RATE_LIMIT_HITS_THRESHOLD):
        now[0] += 100.0
        manager.mark_rate_limited(0, epoch=manager.backends[0]["epoch"])

    assert manager.backends[0]["exhausted"] is True


def test_retry_after_header_drives_the_cooldown(monkeypatch):
    now = [1000.0]
    manager = _manager(monkeypatch, _two_backends(), worker_cap=4,
                       clock=lambda: now[0])

    manager.mark_rate_limited(0, epoch=0, retry_after=42)

    assert manager.backends[0]["cooldown_until"] == 1042.0


@pytest.mark.parametrize("headers,expected", [
    ({"Retry-After": "30"}, 30.0),
    ({"retry-after": "7.5"}, 7.5),
    ({"Retry-After": "99999"}, efn.RATE_LIMIT_COOLDOWN_MAX),
    ({"Retry-After": "Wed, 23 Jul 2026 12:00:00 GMT"}, None),  # -> cooldown jitterato
    ({"Retry-After": "0"}, None),
    ({}, None),
    (None, None),
])
def test_retry_after_parsing(headers, expected):
    assert efn._retry_after_seconds(headers) == expected


def test_jittered_cooldown_when_no_retry_after(monkeypatch):
    now = [0.0]
    manager = _manager(monkeypatch, _two_backends(), worker_cap=4,
                       jitter=lambda: 1.0, clock=lambda: now[0])
    manager.mark_rate_limited(0, epoch=0)
    assert manager.backends[0]["cooldown_until"] == efn.RATE_LIMIT_COOLDOWN_BASE * 1.5

    manager2 = _manager(monkeypatch, _two_backends(), worker_cap=4,
                        jitter=lambda: 0.0, clock=lambda: now[0])
    manager2.mark_rate_limited(0, epoch=0)
    assert manager2.backends[0]["cooldown_until"] == efn.RATE_LIMIT_COOLDOWN_BASE * 0.5


def test_429_response_carries_retry_after_to_the_manager(monkeypatch):
    """Il sentinel 429 dei caller trasporta il Retry-After ed è ancora
    confrontabile con la stringa."""
    class _Session:
        def post(self, url, **kw):
            return _Response(429, {"Retry-After": "13"})

    result = asyncio.run(efn.call_anthropic(_Session(), "p", "m", "k"))

    assert result == "RATE_LIMITED"
    assert result.retry_after == 13.0


@pytest.mark.parametrize("fn,args", [
    (efn.call_anthropic, ("p", "m", "k")),
    (efn.call_openai, ("p", "m", "k")),
    (efn.call_gemini, ("p", "m", "k")),
])
def test_transient_5xx_is_not_charged_to_the_chunk(fn, args, capsys):
    """503/529 = provider giù: sentinel TIMEOUT (rotazione + requeue), mai
    None — un None scriverebbe una riga failed_* sul chunk."""
    class _Session:
        def post(self, url, **kw):
            return _Response(503)

    assert asyncio.run(fn(_Session(), *args)) == "TIMEOUT"


def test_gemini_does_not_retry_internally():
    """Il retry 3× interno è rimosso: un solo POST per chiamata."""
    posts = []

    class _Session:
        def post(self, url, **kw):
            posts.append(url)
            return _Response(503)

    asyncio.run(efn.call_gemini(_Session(), "p", "m", "k"))
    assert len(posts) == 1


def test_gemini_error_log_never_echoes_the_key(capsys):
    class _Session:
        def post(self, url, **kw):
            raise RuntimeError(f"boom {url}")

    result = asyncio.run(efn.call_gemini(_Session(), "p", "m", "sk-secret-key"))
    out = capsys.readouterr().out

    assert result is None
    assert "sk-secret-key" not in out


# ==================================================================
# Terminazione del pool
# ==================================================================

def test_transient_loop_is_bounded(monkeypatch):
    """Un chunk che prende solo esiti transienti non gira all'infinito:
    dopo MAX_TRANSIENT_ATTEMPTS esce come errore, senza failed_*."""
    manager = _manager(monkeypatch, _two_backends(), worker_cap=2)
    db = _memdb()
    calls = []

    async def call(session, prompt, backend):
        calls.append(backend["name"])
        return efn.TIMEOUT

    state = _run(manager, _chunks(1), call, workers=2, facts_conn=db)

    assert len(calls) == efn.MAX_TRANSIENT_ATTEMPTS
    assert state.errors == 1
    assert db.execute("SELECT COUNT(*) FROM extraction_log").fetchone()[0] == 0


def test_pool_drains_when_every_backend_is_exhausted(monkeypatch):
    """Backend tutti esauriti a metà run: i chunk restanti escono come
    'non tentati', senza scrivere nulla e senza appendere il pool."""
    manager = _manager(monkeypatch, _two_backends(), worker_cap=2, limit=2)
    db = _memdb()

    async def call(session, prompt, backend):
        return [FACT]

    state = _run(manager, _chunks(10), call, workers=2, facts_conn=db)

    assert manager.all_exhausted() is True
    assert state.processed == 4               # 2 call per backend
    assert state.unprocessed == 6
    assert db.execute("SELECT COUNT(*) FROM extraction_log").fetchone()[0] == 4


def test_empty_chunk_list_terminates(monkeypatch):
    manager = _manager(monkeypatch, _two_backends(), worker_cap=2)

    async def call(session, prompt, backend):
        raise AssertionError("nessun chunk da processare")

    state = _run(manager, [], call, workers=2)
    assert state.processed == 0


def test_worker_exception_does_not_kill_the_pool(monkeypatch):
    manager = _manager(monkeypatch, _two_backends(), worker_cap=2)
    db = _memdb()

    async def call(session, prompt, backend):
        if "[c0]" in prompt:
            raise RuntimeError("bug nostro")
        return [FACT]

    state = _run(manager, _chunks(4), call, workers=2, facts_conn=db)

    assert state.processed == 3
    assert state.unprocessed == 1
    # il chunk vittima del bug non viene marcato failed_*
    assert db.execute(
        "SELECT COUNT(*) FROM extraction_log WHERE chunk_id='c0'").fetchone()[0] == 0
