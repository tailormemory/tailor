"""RETE DI SICUREZZA TELEGRAM — vale per l'INTERA suite.

Il problema, in una riga: `/etc/tailor/env` è 640 root:staff e `jarvis` è in
staff, quindi qualunque test che finisca dentro `send_telegram` trova token e
chat, e POSTA DAVVERO sul canale di produzione. È già successo (vedi la fixture
autouse in tests/test_reconcile_lexical_index.py) e i test PASSAVANO: asserivano
l'exit code, non l'assenza di effetti.

Quella fixture copre UN file. Questa copre tutto, su due assi:

1. **Env in bianco** — `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` a stringa vuota.
   Chiude i SUBPROCESS: `subprocess.run([...])` senza `env=` fa ereditare
   `os.environ` al figlio (test_queue_alert_split e test_drift_monitor lanciano
   così `queue_depth_monitor`), e con token vuoto la sua `send_telegram`
   corto-circuita PRIMA del POST. Nessun'altra trap in-process arriva lì: il
   figlio è un altro interprete.

2. **Trap sull'egress in-process** (`requests` + `aiohttp`) — un test che POSTA
   verso api.telegram.org SOLLEVA. Non silenzia: un invio reale è un bug del
   test, e un bug del test deve essere rumoroso. Tutto il non-Telegram viene
   delegato all'originale, così un test che parla con un HTTP server locale non
   si accorge di niente.

LIMITE NOTO, verificato non dedotto — `telegram_notify.send_telegram` avvolge il
POST in `except Exception` per contratto ("non solleva MAI": un job non deve
morire per la propria notifica). L'AssertionError della trap ci finisce dentro e
viene declassata a `WARN: Telegram POST raised: AssertionError: ...`. L'invio
resta IMPEDITO — che è il punto — ma per quella famiglia la trap non è rumorosa
quanto altrove. Rendere `send_telegram` trasparente all'AssertionError vorrebbe
dire romperne il contratto in produzione: non si fa da un conftest. Per lei la
difesa vera è l'env in bianco + ENV_FILE morto, che tagliano PRIMA del POST.

`ENV_FILE` neutralizzato è la cintura al punto 1: la famiglia `telegram_notify`
RILEGGE il file quando l'env è vuoto (`ensure_env`), quindi l'env-blank da solo
non basta per lei — si ricaricherebbe il token vero da disco.

`function`-scoped per forza: usa `monkeypatch`, che è function-scoped; una
fixture `session` che lo richiede esplode con ScopeMismatch. Il costo è di
patchare/spatchare a ogni test, ed è irrilevante.
"""

from __future__ import annotations

import inspect
import os
import sys

import pytest
import requests

try:
    import aiohttp
except ImportError:  # aiohttp non è una dipendenza di tutti gli ambienti
    aiohttp = None

sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts", "lib"),
)

import telegram_notify  # noqa: E402

TELEGRAM_HOST = "api.telegram.org"


def _e_telegram(url) -> bool:
    """`url` può essere str, yarl.URL o altro: normalizza prima di guardare."""
    # Match a sottostringa deliberato: è una safety net anti-prod, e un falso
    # positivo tipo `api.telegram.org.evil.test` è accettabile (nessun test
    # legittimo lo colpisce). Precisione via `urlsplit().hostname` è a roadmap.
    return TELEGRAM_HOST in str(url)


def _url_da_requests(args, kwargs):
    """`requests.post(url, ...)` o `requests.post(url=...)`: entrambe le forme."""
    if len(args) > 0:
        return args[0]
    return kwargs.get("url", "")


@pytest.fixture(autouse=True)
def blocca_telegram(monkeypatch):
    # --- 1. env in bianco (chiude i subprocess) --------------------------------
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "")

    # --- 2. cintura: ensure_env() non ricarica il token vero da disco ----------
    monkeypatch.setattr(telegram_notify, "ENV_FILE", "/nonexistent/tailor-env")

    # --- 3. trap requests -----------------------------------------------------
    # Catturare gli originali PRIMA del setattr: leggerli dentro il wrapper
    # significherebbe leggere il wrapper stesso → ricorsione infinita.
    orig_post = requests.post
    orig_get = requests.get

    def post_trappola(*args, **kwargs):
        if _e_telegram(_url_da_requests(args, kwargs)):
            raise AssertionError("POST reale a Telegram da un test")
        return orig_post(*args, **kwargs)

    def get_trappola(*args, **kwargs):
        if _e_telegram(_url_da_requests(args, kwargs)):
            raise AssertionError("GET reale a Telegram da un test")
        return orig_get(*args, **kwargs)

    monkeypatch.setattr(requests, "post", post_trappola)
    monkeypatch.setattr(requests, "get", get_trappola)

    # --- 4. trap aiohttp ------------------------------------------------------
    # Si patcha `ClientSession._request`, non `.post`/`.get`: `_request` è il
    # funnel dove finiscono TUTTI i verbi (post/get/put/delete/patch chiamano
    # lui), quindi una sola patch copre l'intera superficie invece di lasciare
    # scoperto il verbo che nessuno ha pensato di elencare. Prezzo: `_request`
    # è privato (firma non garantita fra minor) e l'URL è il SECONDO posizionale
    # — `_request(self, method, str_or_url, ...)` — non il primo. Da qui il
    # wrapper con firma esplicita invece del solito `args[0]`.
    if aiohttp is None:
        return

    # La firma di `_request` è privata: un cambio fra minor di aiohttp la può
    # riordinare, e il wrapper leggerebbe il posizionale sbagliato — cioè la
    # guardia guarderebbe l'URL nel posto in cui l'URL non è più, lasciando
    # passare tutto. L'assert lo fa fallire QUI, prima di qualunque egress,
    # invece che a valle del tentativo di invio.
    params = list(inspect.signature(aiohttp.ClientSession._request).parameters)
    assert params[:3] == ["self", "method", "str_or_url"], (
        "aiohttp.ClientSession._request ha cambiato firma: aggiornare blocca_telegram"
    )

    orig_request = aiohttp.ClientSession._request

    async def request_trappola(self, method, str_or_url, *args, **kwargs):
        if _e_telegram(str_or_url):
            raise AssertionError(f"{method} reale a Telegram da un test (aiohttp)")
        return await orig_request(self, method, str_or_url, *args, **kwargs)

    monkeypatch.setattr(aiohttp.ClientSession, "_request", request_trappola)
