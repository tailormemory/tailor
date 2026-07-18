"""Regression test per scripts/lib/telegram_notify.py.

Il punto: il bot token vive DENTRO l'URL (`api.telegram.org/bot<TOKEN>/…`) e
`requests` incorpora l'URL nel messaggio delle sue eccezioni. Loggare `{e}`
nudo scrive il token in chiaro nei log al primo blip di rete — e i log stanno
nel working tree, leggibili da qualunque sessione agent (CLAUDE.md §2).

Questi test NON toccano la rete: `requests` è iniettato in `sys.modules` con un
fake, perché `send_telegram` lo importa lazy al proprio interno.

CONCURRENCY WARNING — come tests/test_repair_hnsw_index.py: l'iniezione via
`monkeypatch.setitem(sys.modules, "requests", ...)` è process-global. pytest la
reverte fra i test in esecuzione seriale, ma NON eseguire questo file sotto
pytest-xdist o altro runner parallelo senza prima rendere i fake fixture-scoped.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts", "lib"))

import telegram_notify  # noqa: E402
from telegram_notify import redact, send_telegram  # noqa: E402

# Token FINTO con la forma reale (`<id>:<secret>`). Non è mai esistito.
TOKEN_FINTO = "1234567890:AAFAKEfaketokenNONreale_xxxxxxxxxxxxxxxxx"
CHAT_FINTA = "-1009999999999"


@pytest.fixture(autouse=True)
def env_finto(monkeypatch):
    """Token finto in ambiente: `ensure_env` esce subito e non legge
    /etc/tailor/env, quindi nessun test può toccare il segreto vero."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", TOKEN_FINTO)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", CHAT_FINTA)


class _Resp:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


def _fake_requests(monkeypatch, post):
    modulo = type(sys)("requests")
    modulo.post = post
    monkeypatch.setitem(sys.modules, "requests", modulo)


def _cattura(monkeypatch):
    righe = []
    return righe, lambda level, message: righe.append(f"{level}: {message}")


class TestRedact:
    def test_toglie_il_token_esatto(self):
        assert TOKEN_FINTO not in redact(f"url: /bot{TOKEN_FINTO}/send", TOKEN_FINTO)

    def test_toglie_un_token_sconosciuto_per_forma(self):
        # Token diverso da quello noto (vecchio, o URL costruito altrove).
        altro = "9876543210:BBotherTOKENshapedvalue_yyyyyyyyyyyyyyy"
        out = redact(f"url: /bot{altro}/send", TOKEN_FINTO)
        assert altro not in out
        assert "bot<REDACTED>" in out

    def test_token_vuoto_non_distrugge_il_testo(self):
        # `str.replace("", x)` inserirebbe x fra OGNI carattere.
        assert redact("messaggio innocuo", "") == "messaggio innocuo"

    def test_non_redige_parzialmente(self):
        out = redact(f"/bot{TOKEN_FINTO}/sendMessage", TOKEN_FINTO)
        # Né intero né a pezzi: nemmeno il secret troncato deve sopravvivere.
        assert TOKEN_FINTO not in out
        assert "AAFAKEfaketoken" not in out
        assert "1234567890" not in out

    def test_lascia_intatto_il_resto(self):
        out = redact(f"ConnectionError su /bot{TOKEN_FINTO}/sendMessage timeout=15", TOKEN_FINTO)
        assert "ConnectionError" in out
        assert "timeout=15" in out


class TestNessunLeakDelToken:
    """Il ramo che ha leakato davvero: eccezione di requests con l'URL dentro."""

    def test_connection_error_non_logga_il_token(self, monkeypatch):
        righe, log = _cattura(monkeypatch)

        def post(url, **kw):
            # Riproduce il messaggio REALE di requests, verificato: l'URL
            # (token incluso) finisce dentro str(e).
            raise ConnectionError(
                f"HTTPSConnectionPool(host='api.telegram.org', port=443): "
                f"Max retries exceeded with url: /bot{TOKEN_FINTO}/sendMessage"
            )

        _fake_requests(monkeypatch, post)

        assert send_telegram("testo", log=log) is False
        assert len(righe) == 1
        assert TOKEN_FINTO not in righe[0]
        assert "<REDACTED>" in righe[0]
        assert "ConnectionError" in righe[0]  # la diagnostica resta utile

    def test_retry_error_non_logga_il_token(self, monkeypatch):
        righe, log = _cattura(monkeypatch)
        chiamate = []

        def post(url, **kw):
            chiamate.append(kw.get("json", {}).get("parse_mode"))
            if len(chiamate) == 1:
                return _Resp(400, "Bad Request: can't parse entities")
            raise ConnectionError(f"boom with url: /bot{TOKEN_FINTO}/sendMessage")

        _fake_requests(monkeypatch, post)

        assert send_telegram("testo _rotto_", log=log) is False
        assert chiamate == ["Markdown", None]  # fallback a plain text
        assert all(TOKEN_FINTO not in r for r in righe)

    def test_non_200_non_logga_il_token(self, monkeypatch):
        righe, log = _cattura(monkeypatch)
        # Corpo lungo col token oltre il carattere 200: la redazione deve
        # avvenire PRIMA del troncamento, o il token sopravvive spezzato.
        corpo = "x" * 300 + f" /bot{TOKEN_FINTO}/sendMessage"
        _fake_requests(monkeypatch, lambda url, **kw: _Resp(500, corpo))

        assert send_telegram("testo", log=log) is False
        assert all(TOKEN_FINTO not in r for r in righe)

    def test_stderr_di_default_non_logga_il_token(self, monkeypatch, capsys):
        # Senza `log` esplicito il fallback è stderr: stesso obbligo.
        def post(url, **kw):
            raise ConnectionError(f"url: /bot{TOKEN_FINTO}/sendMessage")

        _fake_requests(monkeypatch, post)

        send_telegram("testo")

        err = capsys.readouterr().err
        assert TOKEN_FINTO not in err
        assert "<REDACTED>" in err

    def test_url_costruito_contiene_il_token_ma_non_e_loggato(self, monkeypatch):
        """Il token DEVE stare nell'URL (è l'API) — il punto è non stamparlo."""
        righe, log = _cattura(monkeypatch)
        visti = {}

        def post(url, **kw):
            visti["url"] = url
            return _Resp(200)

        _fake_requests(monkeypatch, post)

        assert send_telegram("ok", log=log) is True
        assert TOKEN_FINTO in visti["url"]  # l'URL è giusto...
        assert righe == []  # ...e non è stato loggato niente


class TestEccezioneRealeDiRequests:
    """Il cuore del leak: il token NON è nel messaggio che passo io — è
    nell'URL, che `requests` incorpora da solo dentro str(e).

    Niente mock qui: si provoca l'eccezione VERA. Nessuna rete esterna —
    127.0.0.1:1 rifiuta la connessione all'istante, senza DNS.
    """

    URL_MORTO = f"http://127.0.0.1:1/bot{TOKEN_FINTO}/sendMessage"

    def _eccezione_vera(self):
        import requests

        try:
            requests.post(self.URL_MORTO, json={}, timeout=2)
        except Exception as e:  # noqa: BLE001
            return e
        pytest.fail("127.0.0.1:1 doveva rifiutare la connessione")

    def test_la_premessa_regge_il_token_e_dentro_str_e(self):
        # Se un giorno requests smettesse di incorporare l'URL, questo test
        # fallisce e ci dice che la redazione non serve più — invece di
        # lasciarci a redigere per superstizione.
        e = self._eccezione_vera()
        assert TOKEN_FINTO in f"{e}", "requests non incorpora più l'URL: rivalutare redact()"

    def test_redact_toglie_il_token_dallesenzione_reale(self):
        e = self._eccezione_vera()
        grezzo = f"TG error (sendMessage): {e}"
        assert TOKEN_FINTO in grezzo  # pre-condizione: il leak esiste

        pulito = redact(grezzo, TOKEN_FINTO)

        assert TOKEN_FINTO not in pulito
        assert "AAFAKEfaketoken" not in pulito  # nemmeno un pezzo
        assert "1234567890" not in pulito
        assert "<REDACTED>" in pulito
        assert "TG error (sendMessage)" in pulito  # diagnostica intatta

    def test_redact_regge_anche_senza_conoscere_il_token(self):
        # Il ramo regex: copre un token vecchio/ruotato o un URL costruito
        # altrove, di cui il chiamante non ha il valore.
        e = self._eccezione_vera()
        pulito = redact(f"{e}", token="")
        assert TOKEN_FINTO not in pulito

    def test_send_telegram_reale_non_logga_il_token(self, monkeypatch):
        """End-to-end: send_telegram contro un host morto, requests VERO."""
        righe, log = _cattura(monkeypatch)
        monkeypatch.setattr(telegram_notify, "API_URL", "http://127.0.0.1:1/bot{token}/sendMessage")

        assert send_telegram("testo", log=log) is False

        assert len(righe) == 1
        assert TOKEN_FINTO in self.URL_MORTO  # il token ERA nell'URL usato...
        assert TOKEN_FINTO not in righe[0]  # ...ma non nel log
        assert "<REDACTED>" in righe[0]


class TestNessunLeakNelRepo:
    """Guard sorgente sui 7 sender Telegram del repo.

    `telegram_bot.py` NON è importabile in un test (fa `sys.exit(1)` senza
    token e dirotta SIGTERM/SIGINT a import-time, rubando gli handler a
    pytest), e `mcp_server.py` costa un mondo da importare. Il guard è quindi
    sul SORGENTE: grezzo, ma fallisce davvero se qualcuno rimette un `{e}` nudo.
    """

    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def _src(self, rel):
        with open(os.path.join(self.ROOT, rel)) as f:
            return f.read()

    @pytest.mark.parametrize(
        "rel",
        [
            "scripts/services/telegram_bot.py",
            "scripts/services/model_advisor.py",
            "mcp_server.py",
        ],
    )
    def test_ogni_sender_residuo_importa_redact(self, rel):
        """Chi manda ancora per conto proprio deve almeno redigere.

        Sono i tre non consolidati: `telegram_bot` ha un choke-point di log
        tutto suo, `model_advisor` e `mcp_server` ritornano stringhe verso
        l'LLM invece di loggare. Gli altri cinque non compaiono più qui perché
        non hanno più un sender proprio — vedi il test qui sotto.
        """
        assert "telegram_notify import redact" in self._src(rel)

    @pytest.mark.parametrize(
        "rel",
        [
            "scripts/services/auto_flush_queue.py",
            "scripts/services/queue_depth_monitor.py",
            "scripts/services/chromadb_version_check.py",
            "scripts/services/heartbeat.py",
            "scripts/services/reminder_checker.py",
        ],
    )
    def test_i_cinque_daemon_non_hanno_un_sender_proprio(self, rel):
        """Il guard anti-leak per questi cinque non è più testuale: è che il
        codice che potrebbe leakare non esiste più nel file.

        Ricomparire con un `def send_telegram` locale significherebbe rifare la
        copia — e la copia è precisamente ciò che portava il token in chiaro
        nei log al primo blip di rete.
        """
        src = self._src(rel)
        assert "from telegram_notify import send_telegram" in src
        assert "def send_telegram" not in src
        assert "api.telegram.org" not in src

    def test_telegram_bot_redige_nel_choke_point(self):
        # Il log() è l'unica via per stderr: se redige lì, tg_api è coperto.
        src = self._src("scripts/services/telegram_bot.py")
        assert "redact(str(msg), BOT_TOKEN)" in src
        # La forma vecchia, che ha prodotto le 2.801 righe, non deve tornare.
        assert 'print(f"[{now()}] {msg}", file=sys.stderr)' not in src

    def test_telegram_bot_non_bypassa_con_print_exc(self):
        src = self._src("scripts/services/telegram_bot.py")
        # print_exc scrive su stderr saltando log(): deve passare da redact.
        assert "traceback.print_exc(file=sys.stderr)" not in src
        assert "redact(traceback.format_exc(), BOT_TOKEN)" in src

    def test_canonico_senza_eccezione_nuda(self):
        """Le due forme che leakavano nel trittico, ora in un posto solo.

        Prima erano tre asserzioni parametrizzate su tre sorgenti; il POST e il
        retry vivono adesso qui dentro, quindi qui basta guardarli una volta.
        """
        src = self._src("scripts/lib/telegram_notify.py")
        assert 'f"Telegram POST raised: {type(e).__name__}: {e}")' not in src
        assert 'f"Telegram retry POST raised: {type(e).__name__}: {e}")' not in src
        assert 'redact(f"Telegram POST raised' in src
        assert 'redact(f"Telegram retry POST raised' in src
        # `resp.text[:200]` nudo taglierebbe il token a metà: irredimibile.
        assert "body={resp.text[:200]}" not in src
        assert "redact(resp.text, token)[:200]" in src

    def test_mcp_server_non_ritorna_leccezione_nuda(self):
        # È il RETURN di un tool MCP: finisce nel contesto dell'LLM, non in un
        # log. Il leak lì è peggio, non meglio.
        src = self._src("mcp_server.py")
        assert 'return f"Telegram send error: {str(e)}"' not in src
        assert 'redact(f"Telegram send error: {str(e)}", TELEGRAM_BOT_TOKEN)' in src

    def test_heartbeat_logga_su_file_non_su_stderr(self):
        """Heartbeat prima ingoiava tutto con `except Exception: pass`; ora il
        canonico i WARN li scrive davvero, e la destinazione conta.

        Gira ogni 5 minuti sotto launchd: con `log=None` finirebbero su stderr,
        cioè nel log di sistema, a ogni ciclo. L'adapter esiste solo per questo
        (la `log(msg)` del job è mono-argomento e non combacia con la firma).
        """
        src = self._src("scripts/services/heartbeat.py")
        assert "def _tg_log(level, message):" in src
        assert "send_telegram(msg, log=_tg_log)" in src


class TestContratto:
    def test_non_solleva_mai_se_requests_manca(self, monkeypatch):
        righe, log = _cattura(monkeypatch)
        monkeypatch.setitem(sys.modules, "requests", None)  # import -> ImportError
        assert send_telegram("testo", log=log) is False
        assert "requests non disponibile" in righe[0]

    def test_token_assente_salta_senza_sollevare(self, monkeypatch):
        righe, log = _cattura(monkeypatch)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
        monkeypatch.setattr(telegram_notify, "ENV_FILE", "/nonesistente/env")
        assert send_telegram("testo", log=log) is False
        assert any("empty" in r or "non leggibile" in r for r in righe)

    def test_ensure_env_non_termina_il_processo(self, monkeypatch):
        """`env_loader.load_env` fa sys.exit(1): non deve poter uccidere il job."""
        righe, log = _cattura(monkeypatch)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
        monkeypatch.setattr(telegram_notify, "ENV_FILE", "/nonesistente/env")
        telegram_notify.ensure_env(log)  # non solleva SystemExit
        assert any("non leggibile" in r for r in righe)
