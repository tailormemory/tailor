"""Invio Telegram best-effort, condiviso fra i job di manutenzione.

Porting fedele di `scripts/services/auto_flush_queue.py:113` (Markdown con
fallback a plain text su parse-error, best-effort, non solleva mai). Nasce come
helper condiviso perché `send_telegram` oggi è copiaincollato in SEI posti con
firme divergenti — `auto_flush_queue` e `chromadb_version_check` ritornano bool,
`heartbeat` e `reminder_checker` None, `mcp_server:3350` una stringa:

    mcp_server.py:3350, scripts/services/heartbeat.py:98,
    scripts/services/chromadb_version_check.py:85,
    scripts/services/queue_depth_monitor.py:444,
    scripts/services/auto_flush_queue.py:113,
    scripts/services/reminder_checker.py:51

Questo modulo NON li migra (sono daemon in produzione: è un cambio a sé, con il
suo giro di test). È il punto di arrivo quando si deciderà di consolidarli.

CONTRATTO: `send_telegram` non solleva MAI e non termina MAI il processo. Un job
di manutenzione non deve poter morire per colpa della propria notifica.
"""

from __future__ import annotations

import os
import re
import sys

ENV_FILE = "/etc/tailor/env"
API_URL = "https://api.telegram.org/bot{token}/sendMessage"
TIMEOUT_S = 15

# Forma di un bot token dentro un URL: `bot<id>:<secret>`.
_TOKEN_IN_URL_RE = re.compile(r"bot\d{5,}:[A-Za-z0-9_-]{20,}")


def redact(text: str, token: str = "") -> str:
    """Toglie il bot token da un testo destinato a log/stderr.

    NON è una precauzione teorica: il token sta DENTRO l'URL, e `requests`
    incorpora l'URL nel messaggio delle proprie eccezioni —

        ConnectionError: HTTPSConnectionPool(host='api.telegram.org', port=443):
        Max retries exceeded with url: /bot<TOKEN>/sendMessage (Caused by ...)

    — quindi loggare `{e}` nudo scrive il token in chiaro al primo blip di rete.
    Verificato empiricamente, non dedotto. È il bug che hanno OGGI tutte e sei
    le copie di send_telegram (auto_flush_queue.py:124, queue_depth_monitor.py,
    chromadb_version_check.py, heartbeat.py, reminder_checker.py, mcp_server.py).

    Due reti: sostituzione esatta del token noto + regex sul pattern, che copre
    anche un token vecchio/diverso o un URL costruito altrove. Mai redazione
    parziale: il token non compare NEMMENO troncato (§1: niente varianti).
    """
    if len(token) > 0:
        # Guard obbligatoria: `str.replace("", x)` inserisce x fra OGNI
        # carattere. Con token vuoto (env non caricato) distruggerebbe il testo.
        text = text.replace(token, "<REDACTED>")
    return _TOKEN_IN_URL_RE.sub("bot<REDACTED>", text)


def _log(log, level: str, message: str) -> None:
    """`log` è un callable(level, message) opzionale (i job hanno il proprio).

    Il chiamante redige PRIMA di passare qui: questa funzione non sa quale sia
    il token, e un redattore che si fida del chiamante è meglio di uno che
    ricalcola l'env a ogni riga.
    """
    if log is None:
        print(f"{level}: {message}", file=sys.stderr, flush=True)
    else:
        log(level, message)


def ensure_env(log=None) -> None:
    """Carica /etc/tailor/env se il token non è già in ambiente.

    I LaunchDaemon lo iniettano via wrapper `run_*.sh` (CLAUDE.md §1), quindi di
    norma è già lì e questa è una no-op. Serve ai run manuali senza `source`.

    `env_loader.load_env()` fa `sys.exit(1)` sul file illeggibile: qui NON deve
    poter succedere, o una notifica ucciderebbe il job che la manda. Doppia rete:
    `os.access` prima (evita il caso comune), `except SystemExit` come backstop
    (copre la race in cui il file sparisce fra il check e la open).
    """
    if len(os.environ.get("TELEGRAM_BOT_TOKEN", "")) > 0:
        return
    if not os.access(ENV_FILE, os.R_OK):
        _log(log, "WARN", f"Telegram: {ENV_FILE} non leggibile, notifica disattivata")
        return
    try:
        lib_dir = os.path.dirname(os.path.abspath(__file__))
        if lib_dir not in sys.path:
            sys.path.insert(0, lib_dir)
        from env_loader import load_env

        load_env()
    except (SystemExit, OSError, ImportError) as e:
        _log(log, "WARN", f"Telegram: load_env fallita ({type(e).__name__}), notifica disattivata")


def send_telegram(text: str, log=None) -> bool:
    """Manda `text`. Ritorna True se consegnato. Non solleva mai.

    `requests` è importato QUI, non a livello di modulo: così importare questo
    helper non impone la dipendenza a chi non notifica mai, e un venv incompleto
    degrada in "notifica saltata" invece di rompere l'import del job.
    """
    ensure_env(log)
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat = os.environ.get("TELEGRAM_CHAT_ID", "")
    if len(token) == 0 or len(chat) == 0:
        _log(log, "WARN", "Telegram skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID empty")
        return False

    try:
        import requests
    except ImportError:
        _log(log, "WARN", "Telegram skipped: requests non disponibile")
        return False

    url = API_URL.format(token=token)  # <- il token vive QUI: mai loggare l'url
    body = {"chat_id": chat, "text": text, "parse_mode": "Markdown"}
    try:
        resp = requests.post(url, json=body, timeout=TIMEOUT_S)
    except Exception as e:
        _log(log, "WARN", redact(f"Telegram POST raised: {type(e).__name__}: {e}", token))
        return False
    if resp.status_code == 200:
        return True
    # Il testo può contenere path/underscore che rompono il parser Markdown:
    # ritenta in plain text invece di perdere il messaggio.
    if resp.status_code == 400 and "can't parse" in resp.text.lower():
        body.pop("parse_mode", None)
        try:
            resp = requests.post(url, json=body, timeout=TIMEOUT_S)
        except Exception as e:
            _log(log, "WARN", redact(f"Telegram retry POST raised: {type(e).__name__}: {e}", token))
            return False
        if resp.status_code == 200:
            return True
    # Redigere PRIMA di troncare: `[:200]` potrebbe tagliare il token a metà,
    # e un token spezzato non combacia più né con replace né con la regex.
    _log(
        log,
        "WARN",
        f"Telegram non-200: status={resp.status_code} "
        f"body={redact(resp.text, token)[:200]}",
    )
    return False
