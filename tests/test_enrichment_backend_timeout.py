"""Tests: HTTP timeout esplicito sui backend enrichment + rotazione.

Regressione coperta (2026-07-22): la chiamata anthropic in
fact_supersession.py girava senza timeout, il default aiohttp (300s) per
il retry loop a 3 tentativi teneva il worker appeso ~15 minuti su una
connessione TCP stallata, zero CPU, senza mai passare al backend
successivo.

Il session mock qui non risponde mai e onora il ClientTimeout che riceve,
esattamente come farebbe aiohttp: se il chiamante non passa `timeout`,
`session.post` solleva KeyError e il test fallisce.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time

import aiohttp
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib import backend_manager as bm  # noqa: E402
from scripts.enrichment import fact_supersession as fs  # noqa: E402
from scripts.enrichment import extract_facts_nightly as efn  # noqa: E402


FAKE_TIMEOUT = aiohttp.ClientTimeout(total=0.2)


class _HangingPost:
    """Context manager che simula un server che non risponde mai: aspetta
    il budget di timeout e poi solleva, come aiohttp a `total` scaduto."""

    def __init__(self, total: float):
        self.total = total

    async def __aenter__(self):
        await asyncio.sleep(self.total)
        raise asyncio.TimeoutError

    async def __aexit__(self, *exc):
        return False


class HangingSession:
    """session.post che non risponde. Registra i timeout ricevuti: se il
    call site non ne passa uno, `kw["timeout"]` esplode (= il bug)."""

    def __init__(self):
        self.timeouts: list[float] = []
        self.urls: list[str] = []

    def post(self, url, **kw):
        self.urls.append(url)
        self.timeouts.append(kw["timeout"].total)
        return _HangingPost(kw["timeout"].total)


def _patch_cfg(monkeypatch, backends, limit=100):
    monkeypatch.setattr(bm, "get_enrichment_backends", lambda role: list(backends))
    monkeypatch.setattr(bm, "get_enrichment_daily_limit", lambda role: limit)


def _pair():
    return (
        {"id": 1, "fact": "new fact", "event_date": "2026-07-01", "category": "x"},
        {"id": 2, "fact": "old fact", "event_date": "2026-01-01", "category": "x"},
        0.6,
    )


# ============================================================
# 1. Ogni provider passa un timeout esplicito e ritorna "TIMEOUT"
# ============================================================


@pytest.mark.parametrize("call_name", ["call_anthropic", "call_openai", "call_gemini"])
def test_supersession_backends_pass_timeout_and_return_sentinel(monkeypatch, call_name):
    monkeypatch.setattr(fs, "HTTP_TIMEOUT", FAKE_TIMEOUT)
    session = HangingSession()
    fn = getattr(fs, call_name)

    started = time.monotonic()
    result = asyncio.run(fn(
        session, asyncio.Semaphore(2), "new", "old", "2026-07-01", "2026-01-01",
        model="m", api_key="k",
    ))
    elapsed = time.monotonic() - started

    assert session.timeouts == [FAKE_TIMEOUT.total], "timeout esplicito non passato"
    assert result == "TIMEOUT"
    # Nessun retry sullo stesso backend: un solo POST, entro il budget.
    assert len(session.urls) == 1
    assert elapsed < 2.0


@pytest.mark.parametrize("call_name", ["call_anthropic", "call_openai", "call_gemini"])
def test_extract_facts_backends_pass_timeout_and_return_sentinel(monkeypatch, call_name):
    monkeypatch.setattr(efn, "HTTP_TIMEOUT", FAKE_TIMEOUT)
    session = HangingSession()
    fn = getattr(efn, call_name)

    # Nessun semaforo nella firma: lo slot lo acquisisce il consumer, fuori
    # dal watchdog (dispatch parallelo).
    started = time.monotonic()
    result = asyncio.run(fn(session, "prompt", "m", "k"))
    elapsed = time.monotonic() - started

    assert session.timeouts == [FAKE_TIMEOUT.total], "timeout esplicito non passato"
    assert result == "TIMEOUT"
    assert len(session.urls) == 1
    assert elapsed < 2.0


def test_real_default_timeout_is_60s_across_providers():
    """Il budget è uniforme fra i tre backend cloud e fra i due script."""
    assert fs.HTTP_TIMEOUT_SECONDS == 60
    assert efn.HTTP_TIMEOUT_SECONDS == 60
    assert fs.HTTP_TIMEOUT.total == 60
    assert efn.HTTP_TIMEOUT.total == 60


# ============================================================
# 2. Su timeout il worker prosegue sul backend successivo
# ============================================================


def test_worker_proceeds_to_next_backend_on_timeout(monkeypatch):
    """Backend 1 (anthropic) non risponde → run_batch ruota e la coppia
    viene giudicata dal backend 2 (google) entro il timeout."""
    _patch_cfg(monkeypatch, [
        {"provider": "anthropic", "model": "haiku", "workers": 2},
        {"provider": "google", "model": "flash", "workers": 2},
    ])
    monkeypatch.setattr(fs, "HTTP_TIMEOUT", FAKE_TIMEOUT)

    async def fake_gemini(session, semaphore, *args, **kwargs):
        return {"result": "SUPERSEDES", "reason": "ok"}

    monkeypatch.setattr(fs, "call_gemini", fake_gemini)

    manager = bm.BackendManager("fact_supersession",
                                api_keys={"anthropic": "k", "google": "k"})
    session = HangingSession()  # solo anthropic ci arriva davvero

    started = time.monotonic()
    results = asyncio.run(
        fs.run_batch(session, asyncio.Semaphore(2), manager, [_pair()])
    )
    elapsed = time.monotonic() - started

    assert results[0] == {"result": "SUPERSEDES", "reason": "ok"}
    assert elapsed < 5.0, "il worker è rimasto appeso invece di ruotare"
    # anthropic ha ruotato senza essere bruciato per tutta la run
    assert manager.backends[0]["timeouts_consecutive"] == 1
    assert manager.backends[0]["exhausted"] is False
    assert manager.current()["name"] == "google"


def test_timeout_streak_exhausts_backend(monkeypatch):
    """N timeout consecutivi = backend giù per la run; un successo azzera."""
    _patch_cfg(monkeypatch, [
        {"provider": "anthropic", "model": "haiku", "workers": 2},
        {"provider": "google", "model": "flash", "workers": 2},
    ])
    manager = bm.BackendManager("fact_supersession",
                                api_keys={"anthropic": "k", "google": "k"})

    for _ in range(bm.TIMEOUT_CONSECUTIVE_THRESHOLD - 1):
        manager.current_idx = 0
        manager.mark_timeout()
        assert manager.backends[0]["exhausted"] is False

    manager.current_idx = 0
    manager.mark_timeout()
    assert manager.backends[0]["exhausted"] is True
    assert manager.current()["name"] == "google"

    manager.backends[0]["exhausted"] = False
    manager.current_idx = 0
    manager.mark_success()
    assert manager.backends[0]["timeouts_consecutive"] == 0


def test_extract_facts_manager_rotates_on_timeout(monkeypatch, capsys):
    """BackendManager inline di extract_facts_nightly: WARN + switch, e
    exhaustion solo dopo N timeout consecutivi. Accounting per identità:
    il backend lo indica il chiamante, non `current_idx`."""
    monkeypatch.setattr(efn, "_BACKENDS_CFG", [
        {"provider": "anthropic", "model": "haiku", "workers": 2},
        {"provider": "google", "model": "flash", "workers": 2},
    ])
    monkeypatch.setattr(efn, "_API_KEYS", {"anthropic": "k", "google": "k"})
    manager = efn.BackendManager()

    manager.mark_timeout(0)
    out = capsys.readouterr().out
    assert "WARN" in out and "anthropic" in out
    assert manager.backends[0]["exhausted"] is False
    assert manager.current()["name"] == "google"

    for _ in range(efn.TIMEOUT_CONSECUTIVE_THRESHOLD - 1):
        manager.mark_timeout(0)
    assert manager.backends[0]["exhausted"] is True


def test_extract_facts_parse_accepts_single_fact_object():
    parsed = efn._parse_facts_json(
        '{"fact": "User prefers concise status updates.", '
        '"category": "preference", "entity_tags": ["User"]}'
    )

    assert parsed == [{
        "fact": "User prefers concise status updates.",
        "category": "preference",
        "entity_tags": ["User"],
    }]


def test_extract_facts_logs_200_parse_none(capsys):
    result = efn._parse_provider_content(
        "anthropic",
        200,
        '{"content":[{"type":"text","text":"No extractable facts."}]}',
        "No extractable facts.",
    )

    out = capsys.readouterr().out
    assert result is None
    assert "anthropic HTTP 200 produced None" in out
    assert "body[0:200]" in out
    assert "x-api-key" not in out


def test_extract_facts_guarded_backend_timeout(monkeypatch):
    async def never_returns(*args, **kwargs):
        await asyncio.Future()

    monkeypatch.setattr(efn, "BACKEND_CALL_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(efn, "call_backend", never_returns)

    started = time.monotonic()
    result = asyncio.run(efn.call_backend_guarded(
        object(),
        "prompt",
        {"name": "anthropic", "model": "m"},
    ))
    elapsed = time.monotonic() - started

    assert result == "TIMEOUT"
    assert elapsed < 1.0


# ------------------------------------------------------------------
# Regressioni parse 200 -> None (review commit e351fe7)
# ------------------------------------------------------------------

def test_truncated_array_is_not_partial_success():
    """max_tokens che taglia dentro il 2o oggetto non deve promuovere il
    1o fatto a successo: rfind('}') isolerebbe {f1} e il chunk verrebbe
    marcato OK in extraction_log, perdendo gli altri fatti senza retry."""
    assert efn._parse_facts_json(
        '[{"fact":"f1","category":"a"},{"fact":"f2","cat'
    ) is None
    # il singolo fact object legittimo resta supportato
    assert efn._parse_facts_json('{"fact":"f1","category":"a"}') == [
        {"fact": "f1", "category": "a"}
    ]


def test_content_blocks_join_without_separator():
    """I content block si concatenano senza separatore: un '\\n' inserito
    dentro una stringa JSON spezzata a meta' e' un control character
    illegale e fa fallire il parse."""
    data = {"content": [
        {"type": "text", "text": '[{"fact":"prima parte '},
        {"type": "text", "text": 'seconda parte"}]'},
    ]}
    content = efn._anthropic_text(data)
    assert "\n" not in content
    assert efn._parse_facts_json(content) == [{"fact": "prima parte seconda parte"}]

    gem = {"candidates": [{"content": {"parts": [
        {"text": '[{"fact":"prima '}, {"text": 'seconda"}]'},
    ]}}]}
    assert efn._parse_facts_json(efn._gemini_text(gem)) == [{"fact": "prima seconda"}]


@pytest.mark.parametrize("provider,extractor,body", [
    ("google", "_gemini_text",
     '{"candidates":[{"finishReason":"SAFETY"}],"promptFeedback":{"blockReason":"OTHER"}}'),
    ("openai", "_openai_text",
     '{"choices":[{"message":{"role":"assistant","content":null},"finish_reason":"content_filter"}]}'),
    ("anthropic", "_anthropic_text",
     '{"content":[],"stop_reason":"max_tokens"}'),
    ("ollama", "_ollama_text", '{"message":{"role":"assistant"}}'),
])
def test_shape_mismatch_on_200_is_logged_not_silent(provider, extractor, body, capsys):
    """Safety block / refusal / content vuoto su HTTP 200: prima
    sollevavano dentro `except Exception` del caller e tornavano None
    senza una riga di log."""
    result = efn._extract_and_parse(provider, 200, body, getattr(efn, extractor))
    out = capsys.readouterr().out

    assert result is None
    assert f"{provider} HTTP 200 produced None" in out
    assert "content_len=0" in out


def test_non_json_body_on_200_is_logged(capsys):
    result = efn._extract_and_parse("openai", 200, "<html>502 upstream</html>", efn._openai_text)
    out = capsys.readouterr().out

    assert result is None
    assert "body non-JSON" in out


def test_log_reports_finish_reason_and_content_tail(capsys):
    """stop_reason sta dopo il content nel body reale: il preview a 200
    char non lo raggiunge mai, va loggato esplicito. E su un JSON
    troncato e' la coda a dire dove ha tagliato."""
    content = '[{"fact":"' + "x" * 300 + '"},{"fact":"tagliato qui'
    body = json.dumps({
        "id": "msg_01ABC", "type": "message", "role": "assistant",
        "model": "claude-haiku-4-5",
        "content": [{"type": "text", "text": content}],
        "stop_reason": "max_tokens",
        "usage": {"input_tokens": 900, "output_tokens": 1500},
    })

    result = efn._extract_and_parse("anthropic", 200, body, efn._anthropic_text)
    out = capsys.readouterr().out

    assert result is None
    assert "finish=max_tokens" in out
    assert f"content_len={len(content)}" in out
    assert "tagliato qui" in out          # coda visibile
    assert "x-api-key" not in out
    assert "sk-ant" not in out
