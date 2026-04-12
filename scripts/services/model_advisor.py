#!/usr/bin/env python3
"""
TAILOR — Model Advisor
Periodically checks for new/updated models across providers.
Filters by hardware constraints, notifies via Telegram + dashboard.

Usage:
  python3 scripts/services/model_advisor.py          # run check now
  python3 scripts/services/model_advisor.py --force   # ignore schedule_days
"""

import os, sys, json, sqlite3, subprocess, asyncio, logging, argparse, re
from datetime import datetime, timezone, date
from pathlib import Path

# ── Project imports ───────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from scripts.lib.config import get_model_advisor_config, get_model_advisor_max_size, get as cfg_get

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ModelAdvisor] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("model_advisor")

# ── Paths ─────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "db" / "model_advisor.sqlite3"

# ── API key resolution ────────────────────────────────────────
def get_key(name: str) -> str:
    """Resolve API key from env or macOS plist."""
    key = os.environ.get(name, "")
    if key:
        return key
    try:
        result = subprocess.run(
            ["/usr/bin/plutil", "-extract", f"EnvironmentVariables.{name}", "raw",
             "/Library/LaunchDaemons/com.tailor.mcp.plist"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return ""


# ── Database ──────────────────────────────────────────────────
def init_db():
    """Create model_advisor tables if missing."""
    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            model_id TEXT NOT NULL,
            digest TEXT DEFAULT '',
            size_bytes INTEGER DEFAULT 0,
            created_at TEXT DEFAULT '',
            extra TEXT DEFAULT '{}',
            snapshot_date TEXT NOT NULL,
            UNIQUE(provider, model_id, snapshot_date)
        );
        CREATE TABLE IF NOT EXISTS advisories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            type TEXT NOT NULL,       -- update | new | deprecated
            provider TEXT NOT NULL,
            model_id TEXT NOT NULL,
            detail TEXT DEFAULT '',
            size_gb REAL DEFAULT 0,
            notified INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_snap_provider_date ON snapshots(provider, snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_adv_timestamp ON advisories(timestamp);
        CREATE TABLE IF NOT EXISTS recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            model_id TEXT NOT NULL,
            params_b REAL DEFAULT 0,
            ram_q4_gb REAL DEFAULT 0,
            on_ollama INTEGER DEFAULT 0,
            downloads INTEGER DEFAULT 0,
            family TEXT DEFAULT '',
            updated_at TEXT NOT NULL
        );
    """)
    db.commit()
    return db


# ══════════════════════════════════════════════════════════════
# Provider Checkers
# ══════════════════════════════════════════════════════════════

import aiohttp

TIMEOUT = aiohttp.ClientTimeout(total=30)

# ── RAM estimation for GGUF Q4 quantization ───────────────────
# Rule of thumb: params_billions * 0.6 ≈ GB in RAM (Q4_K_M)
Q4_BYTES_PER_PARAM = 0.6

# HuggingFace org names for tracked families (official repos)
_HF_FAMILY_ORGS = {
    "qwen":     ["Qwen"],
    "llama":    ["meta-llama"],
    "gemma":    ["google"],
    "phi":      ["microsoft"],
    "mistral":  ["mistralai"],
    "deepseek": ["deepseek-ai"],
}


async def fetch_ollama_installed() -> list[dict]:
    """Get locally installed Ollama models."""
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as s:
            ollama_url = os.environ.get("OLLAMA_URL", "http://localhost:11434")
            async with s.get(f"{ollama_url}/api/tags") as r:
                data = await r.json()
                return [
                    {
                        "model_id": m["name"],
                        "digest": m.get("digest", "")[:12],
                        "size_bytes": m.get("size", 0),
                        "created_at": m.get("modified_at", ""),
                    }
                    for m in data.get("models", [])
                ]
    except Exception as e:
        log.warning(f"Ollama local list failed: {e}")
        return []


async def _fetch_ollama_registry_names() -> set[str]:
    """Fetch all model names from Ollama registry for cross-reference."""
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as s:
            async with s.get("https://ollama.com/api/tags") as r:
                data = await r.json()
                # Normalize: "qwen3.5:397b" → "qwen3.5" (base name)
                names = set()
                for m in data.get("models", []):
                    name = m.get("name", "").lower()
                    base = name.split(":")[0]
                    names.add(base)
                return names
    except Exception as e:
        log.warning(f"Ollama registry fetch failed: {e}")
        return set()


def _extract_param_count(model_id: str, safetensors: dict) -> float | None:
    """Extract parameter count in billions from HF model metadata."""
    # Try safetensors.total first
    total = safetensors.get("total", 0)
    if total > 0:
        return total / 1e9

    # Try parsing from model name (e.g. "Qwen3.5-9B", "Llama-3.2-3B")
    match = re.search(r'(\d+\.?\d*)[Bb]', model_id.split("/")[-1])
    if match:
        return float(match.group(1))

    return None


def _estimate_q4_ram_gb(params_b: float) -> float:
    """Estimate RAM needed for Q4_K_M quantization."""
    return round(params_b * Q4_BYTES_PER_PARAM, 1)


async def fetch_ollama_registry(families: list[str], max_size_gb: float) -> list[dict]:
    """Discover Ollama-compatible models via HuggingFace API + Ollama cross-reference.

    Strategy:
    1. Query HuggingFace for each family's official org models
    2. Extract parameter count from safetensors metadata
    3. Estimate Q4 RAM and filter by max_size_gb
    4. Cross-reference with Ollama registry to confirm availability
    """
    # Step 1: Get Ollama registry names for cross-reference
    ollama_names = await _fetch_ollama_registry_names()
    log.info(f"  Ollama registry: {len(ollama_names)} base models")

    results = []
    seen = set()

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        for family, orgs in _HF_FAMILY_ORGS.items():
            if family not in families:
                continue

            for org in orgs:
                try:
                    # Search HF for this org's models, sorted by downloads
                    url = (
                        f"https://huggingface.co/api/models"
                        f"?author={org}"
                        f"&sort=downloads&direction=-1"
                        f"&limit=50"
                    )
                    async with session.get(url) as r:
                        if r.status != 200:
                            log.warning(f"  HuggingFace API error for {org}: {r.status}")
                            continue
                        hf_models = await r.json()

                    for m in hf_models:
                        hf_id = m.get("id", "")
                        safetensors = m.get("safetensors", {})

                        # Extract param count
                        params_b = _extract_param_count(hf_id, safetensors)
                        if params_b is None or params_b < 0.5:
                            continue  # skip tiny or unresolvable models

                        # Estimate RAM and filter
                        ram_gb = _estimate_q4_ram_gb(params_b)
                        if ram_gb > max_size_gb:
                            continue

                        # Derive Ollama-style name for cross-reference
                        # e.g. "Qwen/Qwen3.5-9B" → "qwen3.5"
                        short_name = hf_id.split("/")[-1].lower()
                        # Remove param suffix: "qwen3.5-9b" → "qwen3.5"
                        base_name = re.sub(r'-\d+\.?\d*b$', '', short_name)
                        # Also try without dots: "qwen35" for fuzzy match
                        base_nodot = base_name.replace(".", "")

                        on_ollama = (
                            base_name in ollama_names
                            or base_nodot in ollama_names
                            or short_name in ollama_names
                        )

                        # Build unique key
                        key = f"{family}/{hf_id}"
                        if key in seen:
                            continue
                        seen.add(key)

                        size_bytes = int(params_b * 1e9)
                        results.append({
                            "model_id": hf_id,
                            "digest": "",
                            "size_bytes": size_bytes,
                            "created_at": m.get("lastModified", "")[:10],
                            "extra": json.dumps({
                                "family": family,
                                "params_b": round(params_b, 1),
                                "ram_q4_gb": ram_gb,
                                "on_ollama": on_ollama,
                                "downloads": m.get("downloads", 0),
                                "likes": m.get("likes", 0),
                                "pipeline_tag": m.get("pipeline_tag", ""),
                            }),
                        })

                except Exception as e:
                    log.warning(f"  HuggingFace fetch failed for {org}: {e}")

    # Sort by downloads (most popular first)
    results.sort(key=lambda x: json.loads(x.get("extra", "{}")).get("downloads", 0), reverse=True)
    return results


async def fetch_anthropic_models() -> list[dict]:
    """List Anthropic models via /v1/models."""
    key = get_key("ANTHROPIC_API_KEY")
    if not key:
        log.info("Anthropic: no API key, skipping")
        return []
    try:
        headers = {"x-api-key": key, "anthropic-version": "2023-06-01"}
        async with aiohttp.ClientSession(timeout=TIMEOUT) as s:
            async with s.get("https://api.anthropic.com/v1/models", headers=headers) as r:
                data = await r.json()
                return [
                    {
                        "model_id": m["id"],
                        "digest": "",
                        "size_bytes": 0,
                        "created_at": m.get("created_at", ""),
                    }
                    for m in data.get("data", [])
                ]
    except Exception as e:
        log.warning(f"Anthropic model list failed: {e}")
        return []


async def fetch_google_models() -> list[dict]:
    """List Google Gemini models via API."""
    key = get_key("GOOGLE_API_KEY")
    if not key:
        log.info("Google: no API key, skipping")
        return []
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models?key={key}"
        async with aiohttp.ClientSession(timeout=TIMEOUT) as s:
            async with s.get(url) as r:
                data = await r.json()
                return [
                    {
                        "model_id": m["name"].replace("models/", ""),
                        "digest": "",
                        "size_bytes": 0,
                        "created_at": "",
                        "extra": json.dumps({
                            "display_name": m.get("displayName", ""),
                            "input_token_limit": m.get("inputTokenLimit", 0),
                            "output_token_limit": m.get("outputTokenLimit", 0),
                        }),
                    }
                    for m in data.get("models", [])
                ]
    except Exception as e:
        log.warning(f"Google model list failed: {e}")
        return []


async def fetch_openai_models() -> list[dict]:
    """List OpenAI models via /v1/models. Filters to relevant model families."""
    key = get_key("OPENAI_API_KEY")
    if not key:
        log.info("OpenAI: no API key, skipping")
        return []
    RELEVANT_PREFIXES = ("gpt-4", "gpt-3.5", "o1", "o3", "o4")
    try:
        headers = {"Authorization": f"Bearer {key}"}
        async with aiohttp.ClientSession(timeout=TIMEOUT) as s:
            async with s.get("https://api.openai.com/v1/models", headers=headers) as r:
                data = await r.json()
                return [
                    {
                        "model_id": m["id"],
                        "digest": "",
                        "size_bytes": 0,
                        "created_at": str(m.get("created", "")),
                    }
                    for m in data.get("data", [])
                    if any(m["id"].startswith(p) for p in RELEVANT_PREFIXES)
                ]
    except Exception as e:
        log.warning(f"OpenAI model list failed: {e}")
        return []


# ══════════════════════════════════════════════════════════════
# Advisor Logic — Compare snapshots, generate advisories
# ══════════════════════════════════════════════════════════════

def get_previous_snapshot(db: sqlite3.Connection, provider: str) -> dict[str, dict]:
    """Get the most recent snapshot for a provider. Returns {model_id: row}."""
    cur = db.execute(
        """SELECT model_id, digest, size_bytes, created_at, extra
           FROM snapshots
           WHERE provider = ? AND snapshot_date = (
               SELECT MAX(snapshot_date) FROM snapshots WHERE provider = ?
           )""",
        (provider, provider),
    )
    return {row[0]: {"digest": row[1], "size_bytes": row[2], "created_at": row[3], "extra": row[4]} for row in cur}


def save_snapshot(db: sqlite3.Connection, provider: str, models: list[dict], today: str):
    """Save current model list as today's snapshot."""
    for m in models:
        db.execute(
            """INSERT OR IGNORE INTO snapshots (provider, model_id, digest, size_bytes, created_at, extra, snapshot_date)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (provider, m["model_id"], m.get("digest", ""), m.get("size_bytes", 0),
             m.get("created_at", ""), m.get("extra", "{}"), today),
        )
    db.commit()


def compare_and_advise(
    provider: str,
    current: list[dict],
    previous: dict[str, dict],
    installed_ids: set[str] | None = None,
) -> list[dict]:
    """Compare current models against previous snapshot. Returns advisories."""
    advisories = []
    current_ids = {m["model_id"] for m in current}

    for m in current:
        mid = m["model_id"]
        prev = previous.get(mid)

        if prev is None:
            # New model — for Ollama, include RAM estimate in detail
            extra = {}
            try:
                extra = json.loads(m.get("extra", "{}"))
            except Exception:
                pass
            ram_info = f", ~{extra['ram_q4_gb']}GB Q4" if "ram_q4_gb" in extra else ""
            ollama_info = " [on Ollama]" if extra.get("on_ollama") else ""
            params_info = f" ({extra['params_b']}B params)" if "params_b" in extra else ""

            advisories.append({
                "type": "new",
                "provider": provider,
                "model_id": mid,
                "size_gb": extra.get("ram_q4_gb", round(m.get("size_bytes", 0) / (1024 ** 3), 1)),
                "detail": f"New model: {mid}{params_info}{ram_info}{ollama_info}",
            })
        elif m.get("digest") and prev.get("digest") and m["digest"] != prev["digest"]:
            # Updated (digest changed) — only flag if installed
            if installed_ids is None or mid in installed_ids:
                advisories.append({
                    "type": "update",
                    "provider": provider,
                    "model_id": mid,
                    "size_gb": round(m.get("size_bytes", 0) / (1024 ** 3), 1),
                    "detail": f"Updated: {mid} (digest {prev['digest']} → {m['digest']})",
                })

    # Deprecated: was in previous, not in current, and was installed
    for mid in previous:
        if mid not in current_ids:
            if installed_ids is None or mid in installed_ids:
                advisories.append({
                    "type": "deprecated",
                    "provider": provider,
                    "model_id": mid,
                    "size_gb": 0,
                    "detail": f"Model removed from registry: {mid}",
                })

    return advisories


def save_advisories(db: sqlite3.Connection, advisories: list[dict], timestamp: str):
    """Save advisories to DB."""
    for a in advisories:
        db.execute(
            """INSERT INTO advisories (timestamp, type, provider, model_id, detail, size_gb)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (timestamp, a["type"], a["provider"], a["model_id"], a["detail"], a.get("size_gb", 0)),
        )
    db.commit()


# ══════════════════════════════════════════════════════════════
# Notifications
# ══════════════════════════════════════════════════════════════

def generate_recommendations(db, ollama_models, installed_ids, timestamp):
    """Generate recommendations: models that fit in RAM but aren't installed.
    
    Sorted by downloads (popularity). Includes all models from tracked families
    on HuggingFace that fit the hardware constraints.
    """
    db.execute("DELETE FROM recommendations")
    
    # Normalize installed model names for matching
    # e.g. "qwen3.5:latest" -> "qwen3.5", "nomic-embed-text:latest" -> "nomic-embed-text"
    installed_normalized = set()
    for inst in installed_ids:
        base = inst.split(":")[0].lower()  # remove :latest, :9b etc
        installed_normalized.add(base)
    
    for m in ollama_models:
        mid = m["model_id"]
        # Normalize HF name: "Qwen/Qwen3.5-9B" -> "qwen3.5-9b" -> base "qwen3.5"
        short = mid.split("/")[-1].lower()
        base = re.sub(r'[-_]\d+\.?\d*b$', '', short)  # remove param suffix
        
        # Check if any installed model matches
        if base in installed_normalized or short in installed_normalized:
            continue
        
        extra = {}
        try:
            extra = json.loads(m.get("extra", "{}"))
        except Exception:
            pass
        
        # Skip non-text models (embeddings, TTS, ASR, rerankers, VL-only)
        pipeline = extra.get("pipeline_tag", "")
        skip_tags = ("feature-extraction", "automatic-speech-recognition", 
                     "text-to-audio", "text-to-speech")
        if pipeline in skip_tags:
            continue
        # Skip by name pattern
        name_lower = mid.lower()
        if any(x in name_lower for x in ("-awq", "-gptq", "-fp8", "-gguf", 
                                          "embedding", "reranker", "-tts", "-asr")):
            continue
        
        on_ollama = 1 if extra.get("on_ollama") else 0
        db.execute(
            "INSERT INTO recommendations (provider, model_id, params_b, ram_q4_gb, on_ollama, downloads, family, updated_at) VALUES (?,?,?,?,?,?,?,?)",
            ("ollama", mid, extra.get("params_b", 0), extra.get("ram_q4_gb", 0),
             on_ollama, extra.get("downloads", 0), extra.get("family", ""), timestamp))
    db.commit()
    count = db.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0]
    log.info(f"Generated {count} recommendations")


async def notify_telegram(advisories: list[dict]):
    """Send advisory summary via Telegram."""
    tg = cfg_get("telegram") or {}
    token = tg.get("bot_token", "")
    chat_id = tg.get("chat_id", "")
    if not token or not chat_id:
        log.info("Telegram not configured, skipping notification")
        return

    updates = [a for a in advisories if a["type"] == "update"]
    new_models = [a for a in advisories if a["type"] == "new"]
    deprecated = [a for a in advisories if a["type"] == "deprecated"]

    lines = ["🔍 *Model Advisor Report*\n"]

    if updates:
        lines.append("*Updates:*")
        for a in updates:
            size = f" ({a['size_gb']}GB)" if a["size_gb"] else ""
            lines.append(f"  ↻ `{a['provider']}/{a['model_id']}`{size}")

    if new_models:
        lines.append("\n*New models:*")
        for a in new_models:
            size = f" ({a['size_gb']}GB)" if a["size_gb"] else ""
            lines.append(f"  ✦ `{a['provider']}/{a['model_id']}`{size}")

    if deprecated:
        lines.append("\n*Removed:*")
        for a in deprecated:
            lines.append(f"  ✕ `{a['provider']}/{a['model_id']}`")

    lines.append(f"\n_{len(advisories)} change{'s' if len(advisories) != 1 else ''} detected._")
    text = "\n".join(lines)

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        async with aiohttp.ClientSession(timeout=TIMEOUT) as s:
            async with s.post(url, json=payload) as r:
                if r.status == 200:
                    log.info(f"Telegram notification sent ({len(advisories)} advisories)")
                else:
                    body = await r.text()
                    log.warning(f"Telegram send failed: {r.status} {body[:200]}")
    except Exception as e:
        log.warning(f"Telegram notification failed: {e}")


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

async def run_check(force: bool = False):
    """Run the full model advisory check."""
    config = get_model_advisor_config()

    if not config.get("enabled") and not force:
        log.info("Model Advisor disabled in config. Use --force to override.")
        return

    # Check schedule (unless forced)
    if not force:
        today_weekday = date.today().weekday()  # 0=Mon
        schedule = config.get("schedule_days", [1, 4])
        if today_weekday not in schedule:
            log.info(f"Not a scheduled day (today={today_weekday}, schedule={schedule}). Use --force.")
            return

    max_size_gb = get_model_advisor_max_size()
    providers = config.get("providers", {})
    notify_cfg = config.get("notify", {})
    timestamp = datetime.now(timezone.utc).isoformat()
    today = date.today().isoformat()

    log.info(f"Starting check — max model size: {max_size_gb}GB, date: {today}")

    db = init_db()
    all_advisories = []

    # ── Ollama (via HuggingFace + cross-reference) ────────────
    ollama_cfg = providers.get("ollama", {})
    installed_ids = set()
    if ollama_cfg.get("enabled"):
        log.info("Checking Ollama (via HuggingFace)...")

        # Installed models (local)
        installed = await fetch_ollama_installed()
        installed_ids = {m["model_id"] for m in installed}
        log.info(f"  Installed: {len(installed)} models")

        if ollama_cfg.get("discover_new") or ollama_cfg.get("track_installed"):
            families = ollama_cfg.get("families", [])
            registry = await fetch_ollama_registry(families, max_size_gb)

            # Count how many are on Ollama
            on_ollama = sum(1 for m in registry
                           if json.loads(m.get("extra", "{}")).get("on_ollama"))
            log.info(f"  HuggingFace: {len(registry)} models under {max_size_gb}GB "
                     f"({on_ollama} on Ollama)")

            prev = get_previous_snapshot(db, "ollama")
            advs = compare_and_advise(
                "ollama", registry, prev,
                installed_ids=installed_ids if ollama_cfg.get("track_installed") else None,
            )
            all_advisories.extend(advs)
            save_snapshot(db, "ollama", registry, today)
            generate_recommendations(db, registry, installed_ids, timestamp)

    # ── Anthropic ─────────────────────────────────────────────
    if providers.get("anthropic", {}).get("enabled"):
        log.info("Checking Anthropic...")
        models = await fetch_anthropic_models()
        log.info(f"  Found: {len(models)} models")
        prev = get_previous_snapshot(db, "anthropic")
        advs = compare_and_advise("anthropic", models, prev)
        all_advisories.extend(advs)
        save_snapshot(db, "anthropic", models, today)

    # ── Google ────────────────────────────────────────────────
    if providers.get("google", {}).get("enabled"):
        log.info("Checking Google...")
        models = await fetch_google_models()
        log.info(f"  Found: {len(models)} models")
        prev = get_previous_snapshot(db, "google")
        advs = compare_and_advise("google", models, prev)
        all_advisories.extend(advs)
        save_snapshot(db, "google", models, today)

    # ── OpenAI ────────────────────────────────────────────────
    if providers.get("openai", {}).get("enabled"):
        log.info("Checking OpenAI...")
        models = await fetch_openai_models()
        log.info(f"  Found: {len(models)} models")
        prev = get_previous_snapshot(db, "openai")
        advs = compare_and_advise("openai", models, prev)
        all_advisories.extend(advs)
        save_snapshot(db, "openai", models, today)

    # ── Results ───────────────────────────────────────────────
    if all_advisories:
        save_advisories(db, all_advisories, timestamp)
        log.info(f"Found {len(all_advisories)} advisories:")
        for a in all_advisories:
            log.info(f"  [{a['type']}] {a['provider']}/{a['model_id']}: {a['detail']}")

        if notify_cfg.get("telegram"):
            await notify_telegram(all_advisories)
    else:
        log.info("No changes detected across all providers.")

    db.close()
    return all_advisories


def main():
    parser = argparse.ArgumentParser(description="TAILOR Model Advisor")
    parser.add_argument("--force", action="store_true", help="Run regardless of schedule/enabled")
    args = parser.parse_args()
    asyncio.run(run_check(force=args.force))


if __name__ == "__main__":
    main()
