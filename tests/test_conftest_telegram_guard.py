"""Regression test per la rete di sicurezza in tests/conftest.py.

La fixture `blocca_telegram` è autouse: qui è già attiva senza chiederla, e
questi test verificano che faccia le due cose che le si chiedono — SOLLEVARE
sul traffico verso Telegram, e DELEGARE tutto il resto senza accorgersene.

Net-free: il caso "delega" non colpisce la rete vera. Il sentinel non può
prendere il posto di `requests.post` (la fixture ha già catturato l'originale),
quindi sta un gradino sotto — `Session.request`. Vedi `TestDelega`.
"""

from __future__ import annotations

import asyncio
import os

import pytest
import requests

try:
    import aiohttp
except ImportError:
    aiohttp = None

URL_TELEGRAM = "https://api.telegram.org/bot1234567890:AAFAKEfake_xxxxxxxxxxxxxxxxxxxx/sendMessage"
URL_INNOCUO = "http://127.0.0.1:9/qualcosa"  # porta 9 = discard, mai raggiunta


class TestTrapRequests:
    def test_post_a_telegram_solleva(self):
        with pytest.raises(AssertionError, match="POST reale a Telegram"):
            requests.post(URL_TELEGRAM, json={"chat_id": "1", "text": "ciao"}, timeout=5)

    def test_post_a_telegram_solleva_anche_con_url_kwarg(self):
        with pytest.raises(AssertionError, match="POST reale a Telegram"):
            requests.post(url=URL_TELEGRAM, json={"text": "ciao"}, timeout=5)

    def test_get_a_telegram_solleva(self):
        with pytest.raises(AssertionError, match="GET reale a Telegram"):
            requests.get(URL_TELEGRAM, timeout=5)


class TestTrapAiohttp:
    @pytest.mark.skipif(aiohttp is None, reason="aiohttp non installato")
    def test_post_a_telegram_solleva(self):
        async def prova():
            async with aiohttp.ClientSession() as sess:
                await sess.post(URL_TELEGRAM, json={"text": "ciao"})

        with pytest.raises(AssertionError, match=r"reale a Telegram da un test \(aiohttp\)"):
            asyncio.run(prova())

    @pytest.mark.skipif(aiohttp is None, reason="aiohttp non installato")
    def test_get_a_telegram_solleva(self):
        """Il verbo non-POST prova che la patch sta su `_request` (il funnel) e
        non su `.post`: se fosse su `.post`, questo passerebbe alla rete."""

        async def prova():
            async with aiohttp.ClientSession() as sess:
                await sess.get(URL_TELEGRAM)

        with pytest.raises(AssertionError, match=r"reale a Telegram da un test \(aiohttp\)"):
            asyncio.run(prova())


class TestDelega:
    """Un host non-Telegram NON solleva e finisce DAVVERO nell'originale.

    Il sentinel non può prendere il posto di `requests.post` — la fixture ha già
    catturato l'originale e il wrapper chiama la copia che ha in chiusura. Va
    messo un gradino SOTTO: `Session.request` è dove `requests.post`/`get`
    atterrano entrambi. Così la catena wrapper → originale → sentinel è provata
    per intero e nessun socket viene aperto.
    """

    @pytest.fixture
    def sentinel(self, monkeypatch):
        chiamate = []

        def _sentinel(self, method, url, *args, **kwargs):
            chiamate.append((method, url))
            return "SENTINEL"

        monkeypatch.setattr(requests.sessions.Session, "request", _sentinel)
        return chiamate

    def test_post_host_innocuo_delega_allo_originale(self, sentinel):
        assert requests.post(URL_INNOCUO, json={}) == "SENTINEL"
        assert sentinel == [("post", URL_INNOCUO)]

    def test_get_host_innocuo_delega_allo_originale(self, sentinel):
        assert requests.get(URL_INNOCUO) == "SENTINEL"
        assert sentinel == [("get", URL_INNOCUO)]

    @pytest.mark.skipif(aiohttp is None, reason="aiohttp non installato")
    def test_aiohttp_host_innocuo_delega_allo_originale(self):
        # Qui il sentinel non si applica: sotto `_request` non c'è una seam
        # sincrona da stubbare in una riga. La delega si prova sul TIPO
        # dell'eccezione — `_request` vero verso la porta discard alza
        # ClientConnectorError (che è un ClientError), non AssertionError.
        # La guardia l'ha lasciata passare: è quello che si vuole dimostrare.
        async def prova():
            async with aiohttp.ClientSession() as sess:
                await sess.get(URL_INNOCUO)

        with pytest.raises(aiohttp.ClientError):
            asyncio.run(prova())


class TestEnvInBianco:
    def test_token_e_chat_vuoti(self):
        """I subprocess ereditano questo: token vuoto → send_telegram esce
        prima del POST anche in un interprete che la trap non raggiunge."""
        assert os.environ.get("TELEGRAM_BOT_TOKEN") == ""
        assert os.environ.get("TELEGRAM_CHAT_ID") == ""

    def test_env_file_neutralizzato(self):
        import telegram_notify

        assert not os.path.exists(telegram_notify.ENV_FILE)

    def test_send_telegram_corto_circuita(self):
        """L'integrazione delle due reti: con env in bianco ed ENV_FILE morto,
        l'helper vero ritorna False senza mai arrivare a `requests`."""
        from telegram_notify import send_telegram

        righe = []
        assert send_telegram("test", log=lambda lv, m: righe.append(f"{lv}: {m}")) is False
        assert any("Telegram skipped" in r for r in righe)
