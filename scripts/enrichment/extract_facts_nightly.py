"""
TAILOR — 17b: Fact Extraction Nightly Runner (multi-backend rotation)
Processes chunks in daily batches, rotating between backends to stay within rate limits.

Logica:
  1. Priorità ai backend in ordine di config (il primo è il preferito)
  2. Su 429 il provider entra in cooldown: stop dispatch, drain degli
     in-flight, il chunk torna in coda e riparte sul backend successivo
  3. Quando tutti i backend sono esauriti, si ferma
  4. La notte dopo riprende — incrementale

Dispatch: pool di N worker su una coda di chunk + WRITER UNICO.
I worker fanno SOLO la chiamata al provider; il writer è l'unico a toccare
facts.sqlite3/extraction_log, ad aggiornare lo stato dei backend e a
calcolare processed/errors/facts/ETA. Vedi §"PARALLEL DISPATCH".

Integrated in the nightly pipeline as step 5e.

Uso:
  python scripts/enrichment/extract_facts_nightly.py              # Run notturno (max 20k chunk)
  python scripts/enrichment/extract_facts_nightly.py --limit 5000 # Limita a N chunk
  python scripts/enrichment/extract_facts_nightly.py --workers 8  # Parallelismo (default 4)
  python scripts/enrichment/extract_facts_nightly.py --test 10    # Test mode (no write)
"""

import json
import re
import os
import sys
import time
import random
import asyncio
import sqlite3
import argparse
import subprocess
import aiohttp
from dataclasses import dataclass, field
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.lib.backend_manager import gemini_thinking_config
from scripts.lib.config import get_enrichment_backends, get_enrichment_daily_limit
from scripts.lib.config import get as cfg
_USER_LANG = cfg("user", "language") or "en"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(BASE_DIR, "db")
CHROMA_DB_PATH = os.path.join(DB_DIR, "chroma.sqlite3")
FACTS_DB_PATH = os.path.join(DB_DIR, "facts.sqlite3")

# Read backends and daily limit from config (supports old and new YAML format)
_BACKENDS_CFG = get_enrichment_backends("fact_extraction")
DAILY_LIMIT_PER_BACKEND = get_enrichment_daily_limit("fact_extraction")

def get_key(name):
    key = os.environ.get(name, "")
    if key:
        return key
    try:
        result = subprocess.run(
            ["/usr/bin/plutil", "-extract", f"EnvironmentVariables.{name}", "raw",
             "/Library/LaunchDaemons/com.tailor.mcp.plist"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return ""

# API keys — loaded dynamically based on which providers are in config
_KEY_NAMES = {"google": "GOOGLE_API_KEY", "openai": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY"}
_API_KEYS = {}
for _prov in set(b["provider"] for b in _BACKENDS_CFG):
    _kname = _KEY_NAMES.get(_prov)
    if _kname:
        _API_KEYS[_prov] = get_key(_kname)

EXTRACTION_PROMPT = """Extract atomic facts from this knowledge base chunk. Each fact should be a single, self-contained statement that could be independently verified or updated.

Text (date: {date}, source: {source}):
{text}

Respond with ONLY a JSON array. No other text, no markdown, no explanation.
[
  {{"fact": "concise standalone statement", "category": "category", "entity_tags": ["entity1"], "event_date": "YYYY-MM-DD or empty string", "expires_at": "YYYY-MM-DD or empty string"}},
  ...
]

Categories: decision, preference, status, relationship, financial, event, technical, health, personal, legal

Rules:
- Extract ONLY factual statements, not filler or meta-info
- Each fact must make sense on its own
- Include entity names in entity_tags
- event_date: date the fact refers to, NOT the document date. Empty if not inferable.
- expires_at: ISO date (YYYY-MM-DD) after which this fact is no longer operationally relevant.
  Set ONLY for time-bound facts (meetings, deadlines, flights, appointments, reservations).
  Leave EMPTY ("") for permanent facts (decisions, preferences, relationships, status, financial positions).
  Example: "Meeting with Alex on 2026-04-06 at 3pm" -> expires_at: "2026-04-07"
  Example: "User lives in Rome" -> expires_at: ""
  When in doubt, leave it EMPTY -- never expire a fact you are unsure about.
- Aim for 3-15 facts per chunk. Return [] for noise/filler.
- Write facts in {language}."""


_EXPIRES_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def _clean_expires(val):
    """Normalizza il campo expires_at prodotto dall'LLM.

    Accetta SOLO un ISO date YYYY-MM-DD; qualunque altro output
    (stringa vuota, "tomorrow", "Q3", None, non-stringa) -> "" =
    fatto permanente. Secondo strato di sicurezza contro la
    misclassificazione: un output malformato non deve mai tradursi in
    un expiry accidentale che nasconde il fatto dal search. Il filtro
    MCP (get_facts_for_chunks) tratta "" e NULL come 'mai scade'."""
    if not isinstance(val, str):
        return ""
    val = val.strip()
    return val if _EXPIRES_RE.match(val) else ""


def _has_unclosed_array(content):
    """True se il content contiene un array JSON aperto e mai chiuso.

    Euristica per distinguere una risposta troncata a max_tokens
    (`[{f1},{f2` ) da un singolo fact object legittimo (`{f1}`). Senza
    questa guardia il fallback graffe isolerebbe il 1° oggetto via
    rfind("}") e lo promuoverebbe a successo parziale: gli altri fatti
    verrebbero persi senza retry, con il chunk marcato OK in
    extraction_log. Conta anche le parentesi dentro le stringhe, ma il
    path è raggiunto solo dopo che il parse completo è già fallito."""
    return "[" in content and content.count("[") > content.count("]")


def _parse_facts_json(content):
    content = content.replace("```json", "").replace("```", "").strip()
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        facts = parsed.get("facts")
        if isinstance(facts, list):
            return facts
        if isinstance(parsed.get("fact"), str):
            return [parsed]

    start = content.find("[")
    end = content.rfind("]") + 1
    if start >= 0 and end > start:
        parsed = json.loads(content[start:end])
        if isinstance(parsed, list):
            return parsed
    start = content.find("{")
    end = content.rfind("}") + 1
    if start >= 0 and end > start:
        parsed = json.loads(content[start:end])
        if isinstance(parsed, dict) and isinstance(parsed.get("facts"), list):
            return parsed["facts"]
        if (isinstance(parsed, dict) and isinstance(parsed.get("fact"), str)
                and not _has_unclosed_array(content)):
            return [parsed]
    return None


def _preview(text, limit=200):
    text = " ".join(str(text).split())
    return text[:limit]


def _preview_tail(text, limit=200):
    """Coda del content. Su un JSON troncato è la coda che dice dove ha
    tagliato; la testa mostra solo che l'array era ben formato."""
    text = " ".join(str(text).split())
    return text[-limit:]


def _finish_reason(data):
    """stop_reason (anthropic) / finish_reason (openai) / finishReason
    (gemini) / done_reason (ollama).

    Nei body reali sta DOPO il content: un preview a 200 char non lo
    raggiunge mai, ed è la causa più probabile di un 200 -> None
    (max_tokens, safety block). Va loggato esplicitamente."""
    if not isinstance(data, dict):
        return ""
    for key in ("stop_reason", "done_reason"):
        val = data.get(key)
        if isinstance(val, str):
            return val
    choices = data.get("choices")
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        val = choices[0].get("finish_reason")
        if isinstance(val, str):
            return val
    candidates = data.get("candidates")
    if isinstance(candidates, list) and candidates and isinstance(candidates[0], dict):
        val = candidates[0].get("finishReason")
        if isinstance(val, str):
            return val
    return ""


# SICUREZZA — invariante: questo logger è raggiungibile SOLO dopo
# resp.status == 200. È ciò che tiene fuori dai log il body 401 di OpenAI,
# che echeggia la key mascherata ("Incorrect API key provided: sk-proj-***").
# Gli header (x-api-key, Authorization) restano locali ai caller e non
# arrivano mai qui. Se in futuro si vuole loggare anche i 4xx, va prima
# redatto il body: NON spostare la chiamata nel ramo non-200 così com'è.
def _log_parse_none(provider, status, body_text, content_text="", reason="parse returned None", data=None):
    content_text = "" if content_text is None else str(content_text)
    print(
        f"  ⚠️  {provider} HTTP {status} produced None ({reason}); "
        f"finish={_finish_reason(data) or '?'}; content_len={len(content_text)}; "
        f"body[0:200]={_preview(body_text)!r}; "
        f"content[0:200]={_preview(content_text)!r}; "
        f"content[-200:]={_preview_tail(content_text)!r}",
        flush=True,
    )


# I content block di Anthropic/Gemini si concatenano SENZA separatore per
# contratto API. Un "\n".join spezza le stringhe JSON divise a metà fra due
# blocchi ("prima parte \nseconda parte" = control character illegale).
def _anthropic_text(data):
    blocks = data.get("content", []) if isinstance(data, dict) else []
    if not isinstance(blocks, list):
        return ""
    texts = [b["text"] for b in blocks
             if isinstance(b, dict) and isinstance(b.get("text"), str)]
    return "".join(texts).strip()


def _gemini_text(data):
    candidates = data.get("candidates") if isinstance(data, dict) else None
    if not isinstance(candidates, list) or not candidates:
        return ""
    content = candidates[0].get("content") if isinstance(candidates[0], dict) else None
    parts = content.get("parts") if isinstance(content, dict) else None
    if not isinstance(parts, list):
        return ""
    texts = [p["text"] for p in parts
             if isinstance(p, dict) and isinstance(p.get("text"), str)]
    return "".join(texts).strip()


def _openai_text(data):
    choices = data.get("choices") if isinstance(data, dict) else None
    if not isinstance(choices, list) or not choices:
        return ""
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    return content.strip() if isinstance(content, str) else ""


def _ollama_text(data):
    message = data.get("message") if isinstance(data, dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    return content.strip() if isinstance(content, str) else ""


def _extract_and_parse(provider, status, body_text, extractor):
    """Body -> content -> facts, con log su OGNI ramo che produce None.

    Prima di questo wrapper, json.loads(body) e l'estrazione del content
    stavano fuori dal path strumentato: un body non-JSON su 200, un safety
    block Gemini (candidates senza content) o un refusal OpenAI
    (content=null) sollevavano dentro il generico `except Exception` del
    caller e tornavano None senza una riga di log — esattamente la classe
    "200 -> None" che l'instrumentazione deve coprire."""
    try:
        data = json.loads(body_text)
    except (ValueError, TypeError) as e:
        _log_parse_none(provider, status, body_text, "", f"body non-JSON: {e}")
        return None
    try:
        content = extractor(data)
    except Exception as e:
        _log_parse_none(provider, status, body_text, "",
                        f"content extraction: {type(e).__name__}: {e}", data=data)
        return None
    if not content:
        _log_parse_none(provider, status, body_text, content, "content vuoto", data=data)
        return None
    return _parse_provider_content(provider, status, body_text, content, data=data)


def _parse_provider_content(provider, status, body_text, content_text, data=None):
    try:
        parsed = _parse_facts_json(content_text)
    except (ValueError, TypeError) as e:
        _log_parse_none(provider, status, body_text, content_text, f"json decode: {e}", data=data)
        return None
    if parsed is None:
        _log_parse_none(provider, status, body_text, content_text, data=data)
    return parsed


# ============================================================
# BACKEND CALLERS
# ============================================================

# NOTE: shared implementation now lives at scripts/lib/backend_manager.py
# (used by fact_supersession.py and derive_facts.py). This in-line copy
# is preserved temporarily to avoid expanding test surface in the
# v1.2.3.x cost-leak fix PR. Migration tracked in
# docs/v1.2.4_resume_state.md "v1.2.5 backlog".
NONE_CONSECUTIVE_THRESHOLD = 3  # rotate backend after this many consecutive None responses
TIMEOUT_CONSECUTIVE_THRESHOLD = 3  # exhaust backend after this many consecutive HTTP timeouts
RATE_LIMIT_HITS_THRESHOLD = 5  # exhaust backend after this many 429 cooldowns in one run

# Parallelismo di default. 4 e non 15: il numero alto in config è un cap
# per-provider storico, non un target. Il parallelismo globale lo decide
# --workers; il valore di config lo può solo ABBASSARE (min dei due).
DEFAULT_WORKERS = 4

# Tentativi massimi per chunk sugli esiti transienti (429/timeout/5xx).
# Oltre questa soglia il chunk viene contato come errore della run ma NON
# marcato failed_* : resta eleggibile per la run successiva.
MAX_TRANSIENT_ATTEMPTS = 3

# Cooldown provider-level su 429 quando il provider non manda Retry-After.
# Breve e jitterato: N worker che sbattono sullo stesso 429 non devono
# ripartire tutti insieme allo scadere.
RATE_LIMIT_COOLDOWN_BASE = 20.0   # -> 10-30s jitterati
RATE_LIMIT_COOLDOWN_MAX = 300.0   # tetto anche per un Retry-After assurdo

# Budget di output per anthropic. 1500 tagliava: 13 troncamenti osservati,
# tutti con stop_reason=max_tokens e content_len ~4050 — l'array JSON
# restava aperto, _parse_facts_json tornava None e il chunk finiva a
# failed_* pur avendo prodotto fatti validi. Solo anthropic: gemini ha già
# maxOutputTokens 4000, openai non ha mostrato troncamenti.
ANTHROPIC_MAX_TOKENS = 4000

# 5xx = il provider è giù, non il chunk illeggibile. Vanno trattati come
# TIMEOUT (rotazione + requeue), mai come None: un None scriverebbe una
# riga failed_* addebitando al chunk un'indisponibilità del backend.
_TRANSIENT_STATUS = frozenset({500, 502, 503, 504, 529})

# Explicit HTTP timeout, uniform across cloud providers (google, openai,
# anthropic). Ollama is local and may pay a cold model-load, so it keeps
# a wider budget. A timeout must surface as the "TIMEOUT" sentinel — never
# as None (which would count the chunk as unparseable) and never as an
# open-ended wait (observed 2026-07-22: worker hung 15+ min on a stalled
# TCP connection, zero CPU, because no timeout was in force).
HTTP_TIMEOUT_SECONDS = 60
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
OLLAMA_TIMEOUT = aiohttp.ClientTimeout(total=120)
# Watchdog uniforme: 70s cloud / 130s ollama, HTTP timeout + 10s di
# margine. Copre gli await fuori dal path socket (DNS, pool, connector).
BACKEND_CALL_TIMEOUT_SECONDS = HTTP_TIMEOUT_SECONDS + 10
OLLAMA_CALL_TIMEOUT_SECONDS = int(OLLAMA_TIMEOUT.total or 120) + 10

_TIMEOUT_EXC = (asyncio.TimeoutError, aiohttp.ServerTimeoutError)


# ---- Sentinelle di esito ---------------------------------------------
# "TIMEOUT"/"RATE_LIMITED"/"NO_BACKEND" restano stringhe: i confronti
# esistenti (`result == "TIMEOUT"`) continuano a valere. RateLimited è una
# str che trasporta anche il Retry-After del provider —
# `RateLimited(30) == "RATE_LIMITED"` è True, ma il writer può leggere
# `.retry_after` per dimensionare il cooldown.
TIMEOUT = "TIMEOUT"
NO_BACKEND = "NO_BACKEND"        # nessun backend utilizzabile: chunk non tentato
INTERNAL_ERROR = "INTERNAL_ERROR"  # bug nostro nel worker: mai colpa del chunk


class RateLimited(str):
    def __new__(cls, retry_after=None):
        obj = super().__new__(cls, "RATE_LIMITED")
        obj.retry_after = retry_after
        return obj


def _retry_after_seconds(headers):
    """Retry-After -> secondi, o None.

    Solo la forma numerica (quella che mandano openai/anthropic). La forma
    HTTP-date torna None: si ricade sul cooldown jitterato, che è
    l'obiettivo comunque — meglio un cooldown breve certo che una data
    parsata male."""
    if not headers:
        return None
    raw = None
    try:
        raw = headers.get("Retry-After")
    except AttributeError:
        return None
    if raw is None:
        try:
            raw = next((v for k, v in headers.items() if k.lower() == "retry-after"), None)
        except Exception:
            return None
    if raw is None:
        return None
    try:
        val = float(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return min(val, RATE_LIMIT_COOLDOWN_MAX) if val > 0 else None


class BackendManager:
    """Rotazione backend con accounting PER IDENTITÀ.

    Con N chiamate in volo "il backend corrente" non ha più significato:
    quando un risultato torna, `current_idx` può già puntare altrove (una
    rotazione decisa da un altro worker). Ogni mark_* prende quindi
    `backend_id` — l'identità del backend che ha SERVITO quella chiamata —
    e aggiorna quel backend, mai `self.current_idx`. `current_idx` resta
    solo il puntatore di dispatch (quale backend serve il prossimo chunk).

    `epoch` = generazione dello stato di un backend, incrementata a ogni
    transizione (cooldown 429, exhaustion). Un 429 che arriva con una
    epoch superata è il residuo in volo della stessa raffica che ha già
    aperto il cooldown: va ignorato, altrimenti N worker impilerebbero N
    cooldown e N hit per un singolo evento di rate limit.
    """

    def __init__(self, worker_cap=DEFAULT_WORKERS, clock=time.monotonic, jitter=random.random):
        self._clock = clock
        self._jitter = jitter
        self.backends = []
        for bcfg in _BACKENDS_CFG:
            provider = bcfg["provider"]
            model = bcfg["model"]
            api_key = _API_KEYS.get(provider, "")
            # Ollama doesn't need an API key
            if provider != "ollama" and not api_key:
                print(f"  ⚠️  Skipping {provider}/{model} — no API key found")
                continue
            try:
                cap = int(bcfg.get("workers") or worker_cap)
            except (TypeError, ValueError):
                cap = worker_cap
            self.backends.append({
                "id": len(self.backends),
                "name": provider, "model": model, "calls": 0,
                "limit": DAILY_LIMIT_PER_BACKEND, "exhausted": False,
                # Cap per-provider: il valore di config non alza più il
                # parallelismo, lo limita soltanto.
                "workers": max(1, min(worker_cap, cap)),
                "none_consecutive": 0,
                "timeout_consecutive": 0,
                "rate_limit_hits": 0,
                "epoch": 0,
                "cooldown_until": 0.0,
                "semaphore": None,  # creato da init_semaphores(), dentro il loop
            })
        self.current_idx = 0

    # -- lifecycle -------------------------------------------------
    def init_semaphores(self):
        """Un semaforo per provider, capienza = cap per-provider. Va
        chiamato dentro il loop asyncio che poi lo usa."""
        for b in self.backends:
            b["semaphore"] = asyncio.Semaphore(b["workers"])

    # -- selezione -------------------------------------------------
    def usable(self, b, now=None):
        now = self._clock() if now is None else now
        return not b["exhausted"] and b["cooldown_until"] <= now

    def current(self):
        """Primo backend utilizzabile ORA, in ordine di priorità a partire
        dal puntatore di dispatch. None se nessuno lo è (esauriti o tutti
        in cooldown). Non muta lo stato: il puntatore lo spostano solo le
        mark_*."""
        if not self.backends:
            return None
        now = self._clock()
        n = len(self.backends)
        for off in range(n):
            b = self.backends[(self.current_idx + off) % n]
            if self.usable(b, now):
                return b
        return None

    def cooldown_eta(self):
        """Secondi al primo backend che esce dal cooldown, o None se non
        c'è nulla da aspettare (i restanti sono esauriti per davvero)."""
        now = self._clock()
        etas = [b["cooldown_until"] - now for b in self.backends
                if not b["exhausted"] and b["cooldown_until"] > now]
        return min(etas) if etas else None

    def _rotate_from(self, backend_id):
        """Sposta il puntatore di dispatch solo se puntava a questo
        backend: un altro worker può averlo già spostato."""
        if self.backends and self.current_idx == backend_id:
            self.current_idx = (self.current_idx + 1) % len(self.backends)

    def _exhaust(self, b, reason):
        if not b["exhausted"]:
            b["exhausted"] = True
            b["epoch"] += 1
            print(f"  ⚠️  {b['name']} esaurito ({reason}). Switching...", flush=True)
        self._rotate_from(b["id"])

    # -- accounting per identità -----------------------------------
    def mark_success(self, backend_id):
        b = self.backends[backend_id]
        b["calls"] += 1
        b["none_consecutive"] = 0  # reset None streak
        b["timeout_consecutive"] = 0  # backend is responding again
        if b["calls"] >= b["limit"]:
            self._exhaust(b, f"limite di {b['limit']} call")

    def mark_rate_limited(self, backend_id, epoch, retry_after=None):
        """429 -> cooldown provider-level. True se ha aperto un cooldown
        nuovo, False se è il residuo di una epoch già gestita.

        Il cooldown è ciò che ferma il dispatch verso quel provider
        (current() lo salta) mentre le chiamate già partite drenano; i
        chunk toccati tornano in coda e ripartono sul backend successivo.
        Diversamente dal 429 del path sequenziale, il backend NON viene
        bruciato per tutta la run: si riprende da solo allo scadere, e
        viene esaurito solo dopo RATE_LIMIT_HITS_THRESHOLD cooldown."""
        b = self.backends[backend_id]
        if epoch != b["epoch"]:
            return False
        b["rate_limit_hits"] += 1
        b["none_consecutive"] = 0  # reset (rate limit is orthogonal to parse-fail)
        if retry_after:
            cooldown = min(float(retry_after), RATE_LIMIT_COOLDOWN_MAX)
            src = "Retry-After"
        else:
            cooldown = RATE_LIMIT_COOLDOWN_BASE * (0.5 + self._jitter())
            src = "jitter"
        b["cooldown_until"] = self._clock() + cooldown
        b["epoch"] += 1
        print(f"  ⚠️  {b['name']} rate limited (429) — cooldown {cooldown:.0f}s ({src}), "
              f"stop dispatch + drain in volo", flush=True)
        self._rotate_from(backend_id)
        if b["rate_limit_hits"] >= RATE_LIMIT_HITS_THRESHOLD:
            self._exhaust(b, f"{b['rate_limit_hits']}× 429")
        return True

    def mark_timeout(self, backend_id, epoch=None):
        """Timeout HTTP: WARN + rotazione del puntatore, così il chunk
        riparte subito sul provider successivo. Il backend è esaurito solo
        dopo TIMEOUT_CONSECUTIVE_THRESHOLD timeout consecutivi — un
        singolo stallo di rete non deve bruciare un provider per la run.

        Nessuna guardia su epoch, a differenza del 429: lo streak conta
        per chiamata, e N chiamate in volo che scadono tutte sono
        esattamente l'evidenza che il backend non risponde."""
        b = self.backends[backend_id]
        b["timeout_consecutive"] += 1
        if b["timeout_consecutive"] >= TIMEOUT_CONSECUTIVE_THRESHOLD:
            print(f"  ⚠️  WARN {b['name']} timeout {b['timeout_consecutive']}× consecutive "
                  f"({HTTP_TIMEOUT_SECONDS}s)", flush=True)
            self._exhaust(b, "timeout streak")
        else:
            print(f"  ⚠️  WARN {b['name']} HTTP timeout ({HTTP_TIMEOUT_SECONDS}s) — "
                  f"switching backend, chunk back in queue", flush=True)
            self._rotate_from(backend_id)

    def mark_none(self, backend_id):
        """Record a None response from the backend that served the call.
        After NONE_CONSECUTIVE_THRESHOLD consecutive None responses,
        rotate to the next backend (without exhausting it — the backend
        may recover later). Returns True if rotation occurred.

        Rationale: a backend that returns None on N successive chunks
        is likely in a transient degraded state (timeout, malformed
        responses, content filter triggers) or fundamentally unable to
        parse the chunk pattern. Either way, trying the next backend
        is cheap and may succeed (see indagine 2026-05-25: 17 chunks
        permanently failing on anthropic without ever being attempted
        on gemini/openai)."""
        b = self.backends[backend_id]
        b["none_consecutive"] += 1
        if b["none_consecutive"] >= NONE_CONSECUTIVE_THRESHOLD:
            print(
                f"  🔄 {b['name']} returned None {b['none_consecutive']}× consecutive — rotating",
                flush=True,
            )
            b["none_consecutive"] = 0  # reset counter so next rotation tracks fresh streak
            self._rotate_from(backend_id)
            return True
        return False

    def all_exhausted(self):
        return all(b["exhausted"] for b in self.backends)

    def status(self):
        now = self._clock()
        parts = []
        for b in self.backends:
            if b["exhausted"]:
                status = "❌"
            elif b["cooldown_until"] > now:
                status = f"⏳{b['cooldown_until'] - now:.0f}s"
            else:
                status = "✅"
            parts.append(f"{b['name']}:{b['calls']}/{b['limit']}{status}")
        return " | ".join(parts)


# I caller NON prendono più il semaforo: lo slot lo acquisisce il consumer
# PRIMA di armare il watchdog (vedi _run_one). Con l'acquisizione dentro il
# timer, il wait_for misurava anche l'attesa in coda e faceva scattare
# TIMEOUT su chiamate mai partite.
#
# Nessun retry interno, su nessun provider: rotazione backend + requeue del
# chunk coprono il caso transiente, e un retry same-backend ricostruisce
# esattamente lo stallo che il timeout esiste per tagliare.
async def call_gemini(session, prompt, model, api_key):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    gen_cfg = {"temperature": 0.1, "maxOutputTokens": 4000}
    thinking = gemini_thinking_config(model)
    if thinking:  # dict vuoto = il model non accetta thinkingConfig
        gen_cfg["thinkingConfig"] = thinking
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": gen_cfg,
    }
    try:
        async with session.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=HTTP_TIMEOUT) as resp:
            if resp.status == 429:
                return RateLimited(_retry_after_seconds(resp.headers))
            if resp.status in _TRANSIENT_STATUS:
                print(f"  ⚠️  gemini {model} HTTP {resp.status} (transient) — rotating", flush=True)
                return TIMEOUT
            if resp.status != 200:
                # Senza questo il fallimento è silenzioso: il gating dei
                # modelli lato Google (404 "no longer available to new
                # users") era diagnosticabile solo via curl manuale.
                # NB: mai loggare l'URL — contiene ?key=.
                err_body = (await resp.text())[:200]
                print(f"  ⚠️  gemini {model} HTTP {resp.status}: {err_body}", flush=True)
                return None
            body_text = await resp.text()
            # Nessun retry sul safety block / shape inattesa: è una
            # risposta non recuperabile, non un errore transitorio.
            return _extract_and_parse("google", resp.status, body_text, _gemini_text)
    except _TIMEOUT_EXC:
        return TIMEOUT
    except Exception as e:
        # Solo il tipo: il messaggio di alcune eccezioni aiohttp riporta
        # l'URL, che qui contiene ?key=.
        print(f"  ⚠️  gemini {model} {type(e).__name__}", flush=True)
        return None


async def call_openai(session, prompt, model, api_key):
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model, "max_tokens": 1500, "temperature": 0.1,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"}
    }
    try:
        async with session.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers, timeout=HTTP_TIMEOUT) as resp:
            if resp.status == 429:
                return RateLimited(_retry_after_seconds(resp.headers))
            if resp.status in _TRANSIENT_STATUS:
                print(f"  ⚠️  openai {model} HTTP {resp.status} (transient) — rotating", flush=True)
                return TIMEOUT
            if resp.status != 200:
                return None
            body_text = await resp.text()
            return _extract_and_parse("openai", resp.status, body_text, _openai_text)
    except _TIMEOUT_EXC:
        return TIMEOUT
    except Exception:
        return None


async def call_anthropic(session, prompt, model, api_key):
    headers = {
        "x-api-key": api_key,
        "content-type": "application/json",
        "anthropic-version": "2023-06-01"
    }
    payload = {
        "model": model,
        "max_tokens": ANTHROPIC_MAX_TOKENS,
        "temperature": 0.1,
        "messages": [{"role": "user", "content": prompt}]
    }
    try:
        async with session.post("https://api.anthropic.com/v1/messages", json=payload, headers=headers, timeout=HTTP_TIMEOUT) as resp:
            if resp.status == 429:
                return RateLimited(_retry_after_seconds(resp.headers))
            if resp.status in _TRANSIENT_STATUS:
                print(f"  ⚠️  anthropic {model} HTTP {resp.status} (transient) — rotating", flush=True)
                return TIMEOUT
            if resp.status != 200:
                return None
            body_text = await resp.text()
            return _extract_and_parse("anthropic", resp.status, body_text, _anthropic_text)
    except _TIMEOUT_EXC:
        return TIMEOUT
    except Exception:
        return None


# ============================================================
# CHUNK LOADING
# ============================================================

# Regex for detecting code-heavy lines
_CODE_LINE_RE = re.compile(
    r'<[a-z/][^>]*>|[{}];|class="|style="|function\s|const\s|let\s|var\s'
    r'|import\s.+from|def\s|return\s|console\.|addEventListener|querySelector'
    r'|getElementById|onClick|onChange|padding:|margin:|font-|border:|display:'
    r'|background:|width:|height:|\.(css|js|py|html)$'
)

def is_code_heavy(text, threshold=0.6):
    """Returns True if >threshold of lines look like code/markup."""
    lines = text.split('\n')
    if len(lines) < 3:
        return False
    code_lines = sum(1 for l in lines if _CODE_LINE_RE.search(l))
    return (code_lines / len(lines)) > threshold


def is_empty_chunk(text, threshold=0.05):
    """Returns True if the chunk is almost entirely non-alphanumeric:
    empty spreadsheet grids serialized as '| | | |', separator/whitespace
    noise. No extractable facts. See /tmp/document_zero_facts_analysis.md
    (2026-05-29): 91.3% of document zero-facts chunks have alnum ratio
    <5%, dominated by sparse xlsx grids (BP 2WATCH INVESTORS.xlsx etc)."""
    if not text:
        return True
    return sum(c.isalnum() for c in text) / max(len(text), 1) < threshold


def load_chunks(limit=20000):
    # Already-extracted chunks. Exclude transient failures (model='failed_1',
    # 'failed_2') so they get retried up to 3 attempts total. Permanent
    # failures (model='failed_persistent', set after 3 None results) stay
    # in the exclusion set to prevent infinite retry loops on chunks the
    # LLM cannot parse (e.g. mojibake conversation dumps, address-list
    # emails, multi-language insurance tables — see indagine 2026-05-25).
    extracted = set()
    if os.path.exists(FACTS_DB_PATH):
        conn = sqlite3.connect(f"file:{FACTS_DB_PATH}?mode=ro", uri=True)
        for r in conn.execute(
            "SELECT chunk_id FROM extraction_log "
            "WHERE model = 'failed_persistent' OR model NOT LIKE 'failed_%'"
        ):
            extracted.add(r[0])
        conn.close()

    conn = sqlite3.connect(f"file:{CHROMA_DB_PATH}?mode=ro", uri=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT e.embedding_id, fts.c0,
               MAX(CASE WHEN m.key='date' THEN m.string_value END),
               MAX(CASE WHEN m.key='source' THEN m.string_value END)
        FROM embeddings e
        JOIN embedding_fulltext_search_content fts ON fts.id = e.id
        JOIN embedding_metadata m ON m.id = e.id
        WHERE m.key IN ('date','source')
        GROUP BY e.embedding_id
    """)
    chunks = []
    skipped_code = []
    skipped_empty = []
    for eid, text, date, source in cur.fetchall():
        if eid in extracted:
            continue
        if not text or len(text.strip()) < 50:
            continue
        # Skip near-empty chunks (sparse xlsx grids, separator-only) — no facts
        if is_empty_chunk(text):
            skipped_empty.append(eid)
            continue
        # Skip pure code/markup chunks (>60% code lines — no useful facts)
        if is_code_heavy(text):
            skipped_code.append(eid)
            continue
        chunks.append({"id": eid, "text": text[:3000], "date": date or "", "source": source or ""})
        if len(chunks) >= limit:
            break
    conn.close()

    # Mark skipped chunks (code-heavy + near-empty) in extraction_log so they're
    # not retried and are excluded from the coverage denominator.
    if (skipped_code or skipped_empty) and os.path.exists(FACTS_DB_PATH):
        fconn = sqlite3.connect(FACTS_DB_PATH, timeout=30)
        fconn.execute("PRAGMA journal_mode=WAL")
        now_str = datetime.now().isoformat()
        for cid in skipped_code:
            fconn.execute(
                "INSERT OR IGNORE INTO extraction_log (chunk_id, model, facts_count, extracted_at) VALUES (?, ?, ?, ?)",
                (cid, "skipped_code", 0, now_str)
            )
        for cid in skipped_empty:
            fconn.execute(
                "INSERT OR IGNORE INTO extraction_log (chunk_id, model, facts_count, extracted_at) VALUES (?, ?, ?, ?)",
                (cid, "skipped_empty", 0, now_str)
            )
        fconn.commit()
        fconn.close()
        if skipped_code:
            print(f"  Skipped {len(skipped_code):,} code-heavy chunks (marked in extraction_log)")
        if skipped_empty:
            print(f"  Skipped {len(skipped_empty):,} near-empty chunks (marked in extraction_log)")

    return chunks


# ============================================================
# DISPATCH AL PROVIDER
# ============================================================

async def call_backend(session, prompt, backend):
    """Dispatch to the correct provider's API call."""
    provider = backend["name"]
    model = backend["model"]
    api_key = _API_KEYS.get(provider, "")
    if provider == "google":
        return await call_gemini(session, prompt, model, api_key)
    elif provider == "anthropic":
        return await call_anthropic(session, prompt, model, api_key)
    elif provider == "openai":
        return await call_openai(session, prompt, model, api_key)
    elif provider == "ollama":
        return await call_ollama(session, prompt, model)
    else:
        print(f"  ❌ Unknown provider: {provider}")
        return None


async def call_backend_guarded(session, prompt, backend, call=None):
    """Dispatch with an outer watchdog: 70s cloud / 130s ollama.

    The provider functions pass aiohttp timeouts, but the watchdog also
    covers any await outside the socket read path (connector/DNS/pool bugs,
    future code drift, or local Ollama oddities). This is the guard that keeps
    an idle event loop from sleeping forever after a None/error path.

    Il semaforo per-provider è GIÀ acquisito dal chiamante quando si arriva
    qui: dentro questo wait_for c'è solo la chiamata al provider, mai
    l'attesa di uno slot.
    """
    timeout = (
        OLLAMA_CALL_TIMEOUT_SECONDS
        if backend["name"] == "ollama"
        else BACKEND_CALL_TIMEOUT_SECONDS
    )
    fn = call or call_backend
    try:
        return await asyncio.wait_for(fn(session, prompt, backend), timeout=timeout)
    except asyncio.TimeoutError:
        print(
            f"  ⚠️  WARN {backend['name']} backend await exceeded {timeout}s — "
            "treating as TIMEOUT",
            flush=True,
        )
        return TIMEOUT


async def call_ollama(session, prompt, model):
    """Call Ollama local API."""
    ollama_url = os.environ.get("OLLAMA_URL", cfg("ollama", "base_url") or "http://localhost:11434") + "/api/chat"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {"temperature": 0.1}
    }
    try:
        async with session.post(ollama_url, json=payload, timeout=OLLAMA_TIMEOUT) as resp:
            if resp.status in _TRANSIENT_STATUS:
                print(f"  ⚠️  ollama {model} HTTP {resp.status} (transient) — rotating", flush=True)
                return TIMEOUT
            if resp.status != 200:
                return None
            body_text = await resp.text()
            return _extract_and_parse("ollama", resp.status, body_text, _ollama_text)
    except _TIMEOUT_EXC:
        return TIMEOUT
    except Exception:
        return None


# ============================================================
# PARALLEL DISPATCH — N worker + writer unico
# ============================================================
#
# Ruoli, rigidi:
#   worker  — prende un chunk, sceglie un backend, acquisisce lo slot,
#             chiama il provider sotto watchdog, mette l'esito su result_q.
#             NON tocca sqlite, NON aggiorna contatori, NON decide rotazioni.
#   writer  — unico consumer di result_q: aggiorna lo stato dei backend per
#             identità, scrive facts/extraction_log, tiene
#             processed/errors/facts/ETA, decide requeue e commit.
#
# Un solo scrittore = niente lock su sqlite, niente conteggi che si
# sovrappongono, un solo punto in cui leggere la logica di esito.

_STOP = object()  # sentinella di chiusura per le due code


@dataclass
class CallOutcome:
    """Esito di UNA chiamata, con l'identità del backend che l'ha servita.

    provider/model sono quelli EFFETTIVI della chiamata: con N in volo,
    rileggerli dal manager al ritorno darebbe il backend sbagliato (è
    l'errore che questa struttura esiste per rendere impossibile).

    `ack` è l'Event con cui il writer dice al worker "esito contabilizzato".
    Il worker lo aspetta prima di prendere il chunk successivo: senza,
    dopo un 429 continuerebbe a dispatchare sul provider appena messo in
    cooldown (il writer non ha ancora girato), e sforerebbe i limiti per
    lo stesso motivo. Non serializza le chiamate — gli altri worker sono
    già in volo — costa solo un giro di event loop per chunk."""
    chunk: dict
    result: object
    backend_id: int
    provider: str
    model: str
    epoch: int
    ack: object = None


@dataclass
class RunState:
    total: int = 0
    processed: int = 0
    errors: int = 0
    facts: int = 0
    requeued: int = 0
    unprocessed: int = 0      # chunk mai tentati (backend finiti): nessuna riga scritta
    start_time: float = field(default_factory=time.time)


class _WorkTracker:
    """Conteggio esplicito del lavoro in sospeso.

    Non si usa Queue.join(): il requeue lo decide il writer, DOPO che il
    worker ha già fatto task_done(). Con join() la coda potrebbe risultare
    finita l'istante prima che il writer rimetta dentro il chunk, e il pool
    chiuderebbe lasciando lavoro fuori. Qui il contatore scende solo quando
    il writer chiude definitivamente un chunk."""

    def __init__(self, pending):
        self.pending = pending
        self.done = asyncio.Event()
        if pending <= 0:
            self.done.set()

    def complete(self, n=1):
        self.pending -= n
        if self.pending <= 0:
            self.done.set()


async def _lease_backend(manager, poll=1.0):
    """Backend utilizzabile per il prossimo chunk, aspettando l'eventuale
    cooldown. None quando non ne resta nessuno (tutti esauriti)."""
    while True:
        backend = manager.current()
        if backend is not None:
            return backend
        eta = manager.cooldown_eta()
        if eta is None:
            return None
        await asyncio.sleep(min(max(eta, 0.05), poll))


async def _run_one(session, chunk, manager, call=None):
    backend = await _lease_backend(manager)
    if backend is None:
        return CallOutcome(chunk, NO_BACKEND, -1, "", "", -1)
    # Identità congelata QUI: epoch e id appartengono alla chiamata, non
    # allo stato che il manager avrà quando la risposta torna.
    epoch = backend["epoch"]
    prompt = EXTRACTION_PROMPT.format(
        language=_USER_LANG, date=chunk["date"], source=chunk["source"], text=chunk["text"]
    )
    # Semaforo FUORI dal watchdog: prima lo slot, poi si arma il timer.
    async with backend["semaphore"]:
        result = await call_backend_guarded(session, prompt, backend, call=call)
    return CallOutcome(chunk, result, backend["id"], backend["name"], backend["model"], epoch)


async def _chunk_worker(name, session, chunk_q, result_q, manager, call=None):
    while True:
        chunk = await chunk_q.get()
        try:
            if chunk is _STOP:
                return
            try:
                outcome = await _run_one(session, chunk, manager, call=call)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Un bug nostro non deve né uccidere il pool né addebitare
                # un failed_* al chunk.
                print(f"  ❌ {name}: {type(e).__name__}: {e}", flush=True)
                outcome = CallOutcome(chunk, INTERNAL_ERROR, -1, "", "", -1)
            outcome.ack = asyncio.Event()
            await result_q.put(outcome)
            # Prossimo dispatch solo su stato aggiornato: vedi CallOutcome.ack.
            await outcome.ack.wait()
        finally:
            chunk_q.task_done()


def _write_failure(facts_conn, chunk, now_str):
    """Bump del fail counter (facts_count negativo). Dopo 3 None
    consecutivi il chunk diventa 'failed_persistent' e smette di essere
    ricaricato — vedi indagine 2026-05-25: 17 chunk ritentati ogni notte
    per settimane, sempre None da Claude Haiku."""
    if not facts_conn:
        return
    cur = facts_conn.cursor()
    row = cur.execute(
        "SELECT facts_count FROM extraction_log WHERE chunk_id = ?", (chunk["id"],)
    ).fetchone()
    prev = row[0] if row and row[0] < 0 else 0
    new_count = prev - 1
    new_model = "failed_persistent" if new_count <= -3 else f"failed_{abs(new_count)}"
    cur.execute(
        "INSERT OR REPLACE INTO extraction_log "
        "(chunk_id, facts_count, model, extracted_at) VALUES (?, ?, ?, ?)",
        (chunk["id"], new_count, new_model, now_str)
    )


def _write_facts(facts_conn, chunk, facts, provider, now_str):
    """Valida e scrive i fatti. Ritorna quanti ne ha tenuti."""
    valid = []
    for f in facts:
        if not isinstance(f, dict):
            continue
        fact_text = f.get("fact", "").strip()
        if not fact_text or len(fact_text) < 10:
            continue
        valid.append(f)
    if not facts_conn:
        return len(valid)
    cur = facts_conn.cursor()
    for f in valid:
        cur.execute("""
            INSERT INTO facts (chunk_id, fact, category, entity_tags, event_date, expires_at, confidence, created_at, document_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (chunk["id"], f["fact"][:500], f.get("category", "")[:50].lower(),
              json.dumps(f.get("entity_tags", [])[:10]),
              f.get("event_date", "") or "", _clean_expires(f.get("expires_at", "")),
              1.0, now_str, chunk.get("date", "")))
    cur.execute("""
        INSERT OR REPLACE INTO extraction_log (chunk_id, facts_count, model, extracted_at)
        VALUES (?, ?, ?, ?)
    """, (chunk["id"], len(valid), provider, now_str))
    return len(valid)


def _requeue_or_drop(item, manager, chunk_q, tracker, state, reason):
    """Esito transiente (429/timeout/5xx): il chunk NON è colpevole. Torna
    in coda e riparte sul prossimo backend utilizzabile; nessuna riga
    failed_* viene scritta, così resta eleggibile anche per la run
    successiva."""
    chunk = item.chunk
    chunk["attempts"] = chunk.get("attempts", 0) + 1
    if chunk["attempts"] < MAX_TRANSIENT_ATTEMPTS and not manager.all_exhausted():
        state.requeued += 1
        chunk_q.put_nowait(chunk)
        return
    print(f"  ⚠️  chunk {chunk['id']} lasciato indietro dopo {chunk['attempts']} "
          f"tentativi ({reason}) — nessun failed_* scritto", flush=True)
    state.errors += 1
    state.processed += 1
    tracker.complete()


def _apply_outcome(item, manager, chunk_q, tracker, facts_conn, state, now_str):
    """Unico punto in cui un esito diventa stato. Gira solo nel writer."""
    result = item.result

    if result == "RATE_LIMITED":
        manager.mark_rate_limited(item.backend_id, item.epoch,
                                  getattr(result, "retry_after", None))
        _requeue_or_drop(item, manager, chunk_q, tracker, state, "rate limited")
        return

    if result == TIMEOUT:
        manager.mark_timeout(item.backend_id, item.epoch)
        _requeue_or_drop(item, manager, chunk_q, tracker, state, "timeout")
        return

    if result in (NO_BACKEND, INTERNAL_ERROR):
        # Mai tentato davvero: conta come errore della run, ma il chunk
        # resta pulito in extraction_log.
        state.errors += 1
        state.unprocessed += 1
        tracker.complete()
        return

    if result is None:
        manager.mark_none(item.backend_id)
        state.errors += 1
        state.processed += 1
        _write_failure(facts_conn, item.chunk, now_str)
        tracker.complete()
        return

    manager.mark_success(item.backend_id)
    state.facts += _write_facts(facts_conn, item.chunk, result, item.provider, now_str)
    state.processed += 1
    tracker.complete()


async def _result_writer(result_q, chunk_q, manager, tracker, facts_conn, state, now_str):
    logged_bucket = 0
    while True:
        item = await result_q.get()
        if item is _STOP:
            return
        try:
            _apply_outcome(item, manager, chunk_q, tracker, facts_conn, state, now_str)
        finally:
            # Il worker riparte solo ora: un esito non contabilizzato non
            # deve mai influenzare il dispatch successivo.
            if item.ack is not None:
                item.ack.set()

        # Commit + progress ogni 100 chunk chiusi.
        bucket = state.processed // 100
        if bucket > logged_bucket:
            logged_bucket = bucket
            if facts_conn:
                facts_conn.commit()
            elapsed = time.time() - state.start_time
            rate = state.processed / elapsed if elapsed > 0 else 0
            remaining = state.total - state.processed
            eta = remaining / rate if rate > 0 else 0
            print(f"  [{state.processed:,}/{state.total:,}] {rate:.1f}/s | "
                  f"Facts={state.facts:,} ERR={state.errors} RQ={state.requeued} | "
                  f"{manager.status()} | ETA {eta/60:.0f}m", flush=True)


async def run_pool(session, chunks, manager, facts_conn, worker_count, state, now_str, call=None):
    """Avvia N worker + il writer e ritorna quando ogni chunk è chiuso."""
    manager.init_semaphores()
    chunk_q = asyncio.Queue()
    result_q = asyncio.Queue()
    for chunk in chunks:
        chunk_q.put_nowait(chunk)
    tracker = _WorkTracker(len(chunks))

    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            _result_writer(result_q, chunk_q, manager, tracker, facts_conn, state, now_str),
            name="fact-writer",
        )
        workers = [
            tg.create_task(
                _chunk_worker(f"worker-{i}", session, chunk_q, result_q, manager, call=call),
                name=f"fact-worker-{i}",
            )
            for i in range(worker_count)
        ]
        await tracker.done.wait()
        for _ in workers:
            chunk_q.put_nowait(_STOP)
        # Il writer chiude solo dopo i worker: nessun esito resta in coda.
        await asyncio.gather(*workers)
        result_q.put_nowait(_STOP)
    return state


# ============================================================
# MAIN
# ============================================================

async def main():
    parser = argparse.ArgumentParser(description="TAILOR Nightly Fact Extraction (multi-backend)")
    parser.add_argument("--limit", type=int, default=20000, help="Max chunk da processare (default 20k)")
    parser.add_argument("--test", type=int, default=0, help="Test mode (no write)")
    parser.add_argument("--workers", type=int, default=0,
                        help=f"Worker paralleli (0 = default {DEFAULT_WORKERS}); "
                             "il valore in config resta solo come cap per-provider")
    args = parser.parse_args()

    worker_count = args.workers if args.workers > 0 else DEFAULT_WORKERS
    manager = BackendManager(worker_cap=worker_count)
    if not manager.backends:
        print("❌ No backend available. Check enrichment.fact_extraction.backends in config/tailor.yaml")
        sys.exit(1)

    # Init facts DB
    if not os.path.exists(FACTS_DB_PATH):
        print("❌ facts.sqlite3 does not exist. Run 17_extract_facts.py --stats first")
        sys.exit(1)

    print(f"🔍 Loading chunks...")
    chunks = load_chunks(args.limit)
    if args.test:
        chunks = chunks[:args.test]

    total = len(chunks)
    if total == 0:
        print("✅ No chunks to process.")
        return

    print(f"📋 {total:,} chunks to process")
    print(f"  Backend: {manager.status()}")

    facts_conn = None
    if not args.test:
        facts_conn = sqlite3.connect(FACTS_DB_PATH, timeout=30)
        facts_conn.execute("PRAGMA journal_mode=WAL")
        facts_conn.execute("PRAGMA busy_timeout=30000")

    caps = ", ".join(f"{b['name']}:{b['workers']}" for b in manager.backends)
    print(f"  Workers: {worker_count} (cap per-provider: {caps})")
    state = RunState(total=total)
    now_str = datetime.now().isoformat()

    async with aiohttp.ClientSession() as session:
        print(f'  Session created, {worker_count} worker su {len(chunks):,} chunks...', flush=True)
        await run_pool(session, chunks, manager, facts_conn, worker_count, state, now_str)

    if facts_conn:
        facts_conn.commit()
        facts_conn.close()

    elapsed = time.time() - state.start_time
    print(f"\n{'═' * 55}")
    print(f"Completed in {elapsed/60:.1f} minutes")
    print(f"  Chunks processed: {state.processed:,}")
    print(f"  Facts extracted: {state.facts:,}")
    print(f"  Errors: {state.errors:,}")
    print(f"  Requeued: {state.requeued:,}")
    if state.unprocessed:
        print(f"  ⚠️  Non tentati (backend esauriti): {state.unprocessed:,}")
    print(f"  Backend: {manager.status()}")
    if args.test:
        print(f"  ⚠️  TEST mode — no changes")


if __name__ == "__main__":
    asyncio.run(main())
