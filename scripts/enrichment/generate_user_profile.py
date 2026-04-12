#!/usr/bin/env python3
"""
TAILOR — User Profile Generator
Generates a structured user profile (static + dynamic) from the KB.

Output: db/user_profile.json

Uso:
  python3 scripts/12_generate_user_profile.py           # Generate/update profile
  python3 scripts/12_generate_user_profile.py --dry-run  # Show material without generating
  python3 scripts/12_generate_user_profile.py --view      # Show current profile

Il profilo ha due sezioni:
  - static: stable facts (identity, family, companies, residence, setup)
  - dynamic: recent context (active projects, decisions, last month focus)

Sources (in priority order):
  1. db/user_profile_overrides.json — fatti confermati manualmente dall'utente
  2. KB (conv_summary, doc_summary, entity index) — dati automatici
  3. Claude preferences context — personality and operational preferences

Backend: Claude Haiku
Frequenza consigliata: settimanale, o post-pipeline notturna
"""

import os
import sys
import json
import time
import sqlite3
import requests
from datetime import datetime, timedelta

# ============================================================
# CONFIGURAZIONE
# ============================================================

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "db")
CHROMA_DB = os.path.join(DB_DIR, "chroma.sqlite3")
PROFILE_PATH = os.path.join(DB_DIR, "user_profile.json")
ENTITY_DB = os.path.join(DB_DIR, "entity_index.sqlite3")
OVERRIDES_PATH = os.path.join(DB_DIR, "user_profile_overrides.json")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.lib.config import get_enrichment
_sum_cfg = get_enrichment("summaries")
MODEL = _sum_cfg["model"]

RECENT_DAYS = 30
MAX_CONTEXT_CHARS = 60000

# Contesto fisso — loaded from config/tailor.yaml
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
from config import get as cfg

# User name from config (for prompt templates)
from config import get as cfg
_USER_NAME = cfg("user", "name") or "the user"
_USER_LANG = cfg("user", "language") or "en"
USER_CONTEXT = cfg("user", "bio") or "No user bio configured in config/tailor.yaml"


# ============================================================
# DB HELPERS
# ============================================================

def get_segment_id():
    conn = sqlite3.connect(f"file:{CHROMA_DB}?mode=ro", uri=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT s.id FROM segments s JOIN collections c ON s.collection = c.id 
        WHERE c.name = 'tailor_kb' AND s.scope = 'METADATA'
    """)
    row = cur.fetchone()
    conn.close()
    if not row:
        print("❌ Collection non trovata")
        sys.exit(1)
    return row[0]


def get_summaries_by_category(segment_id, category, min_date=None, limit=40):
    conn = sqlite3.connect(f"file:{CHROMA_DB}?mode=ro", uri=True)
    cur = conn.cursor()

    if min_date:
        cur.execute("""
            SELECT e.embedding_id, 
                   MAX(CASE WHEN em.key = 'title' THEN em.string_value END) as title,
                   MAX(CASE WHEN em.key = 'source' THEN em.string_value END) as source,
                   MAX(CASE WHEN em.key = 'date' THEN em.string_value END) as date
            FROM embeddings e
            JOIN embedding_metadata em ON e.id = em.id
            WHERE e.segment_id = ?
            AND e.id IN (SELECT id FROM embedding_metadata WHERE key = 'category' AND string_value = ?)
            AND e.id IN (SELECT id FROM embedding_metadata WHERE key = 'date' AND string_value >= ?)
            GROUP BY e.embedding_id
            ORDER BY date DESC
            LIMIT ?
        """, (segment_id, category, min_date, limit))
    else:
        cur.execute("""
            SELECT e.embedding_id,
                   MAX(CASE WHEN em.key = 'title' THEN em.string_value END) as title,
                   MAX(CASE WHEN em.key = 'source' THEN em.string_value END) as source,
                   MAX(CASE WHEN em.key = 'date' THEN em.string_value END) as date
            FROM embeddings e
            JOIN embedding_metadata em ON e.id = em.id
            WHERE e.segment_id = ?
            AND e.id IN (SELECT id FROM embedding_metadata WHERE key = 'category' AND string_value = ?)
            GROUP BY e.embedding_id
            ORDER BY date DESC
            LIMIT ?
        """, (segment_id, category, limit))

    rows = cur.fetchall()
    results = []
    for row in rows:
        chunk_id = row[0]
        cur.execute("""
            SELECT fts.c0 FROM embedding_fulltext_search_content fts
            JOIN embeddings e ON fts.id = e.id
            WHERE e.embedding_id = ? AND e.segment_id = ?
        """, (chunk_id, segment_id))
        text_row = cur.fetchone()
        if text_row and text_row[0]:
            results.append({
                "id": chunk_id, "title": row[1] or "", "source": row[2] or "",
                "date": row[3] or "", "text": text_row[0][:2000]
            })
    conn.close()
    return results


def get_top_entities(limit=30):
    if not os.path.exists(ENTITY_DB):
        return []
    conn = sqlite3.connect(f"file:{ENTITY_DB}?mode=ro", uri=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT entity, entity_type, COUNT(*) as cnt
        FROM entity_index GROUP BY entity COLLATE NOCASE, entity_type
        ORDER BY cnt DESC LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return [{"entity": r[0], "type": r[1], "count": r[2]} for r in rows]


def load_overrides():
    if not os.path.exists(OVERRIDES_PATH):
        return {}
    with open(OVERRIDES_PATH) as f:
        return json.load(f)


# ============================================================
# LLM
# ============================================================

PROFILE_PROMPT = """You are a system that generates structured user profiles for a personal Knowledge Base.

You are given data about {_USER_NAME} from multiple sources, in priority order:
1. MANUAL OVERRIDES — facts confirmed directly by the user. These have ABSOLUTE PRIORITY.
2. USER CONTEXT — information from user preferences.
3. KB ENTITIES — people, companies, projects most frequent in the Knowledge Base.
4. RECENT ACTIVITY — conversation and document summaries from the last 30-60 days.

CRITICAL RULES:
- MANUAL OVERRIDES always take precedence over KB data. If an override says a person is a "former collaborator", do not include them among active people even if they have thousands of KB mentions.
- Frequency in the KB does NOT indicate current relevance. People with many mentions may be former collaborators.
- For the "static" section: use overrides + user context as the base, enrich with KB data only where there is no conflict.
- For the "dynamic" section: use ONLY recent data (last 30 days). Decisions and projects must be current.
- NEVER fabricate. If data is not available, use an empty array.

Generate a JSON profile with this structure:

{
  "static": {
    "identity": "Who is the user in 2-3 sentences",
    "family": ["Family fact"],
    "companies": ["Company: role and description"],
    "key_people": ["Name: current role/relationship with the user"],
    "former_key_people": ["Name: former role — no longer active"],
    "locations": ["Place: context"],
    "health": "Health/fitness status in 1-2 sentences",
    "tech_setup": "Tech setup in 1-2 sentences",
    "preferences": ["Preference 1", "Preference 2"]
  },
  "dynamic": {
    "current_focus": "What they are working on NOW",
    "active_projects": ["Project: status"],
    "recent_decisions": ["Date — Decision"],
    "upcoming": ["Upcoming item"]
  }
}

Respond in {_USER_LANG}.
Respond ONLY with valid JSON.""".replace("{_USER_NAME}", _USER_NAME).replace("{_USER_LANG}", _USER_LANG)


def generate_profile(overrides, entities, recent_conv, recent_docs):
    parts = []

    # 1. Manual overrides (highest priority)
    if overrides:
        parts.append("=== OVERRIDE MANUALI (PRIORITÀ ASSOLUTA) ===")
        static_ov = overrides.get("static_overrides", {})
        for key, value in static_ov.items():
            if isinstance(value, list):
                parts.append(f"\n{key}:")
                for item in value:
                    parts.append(f"  - {item}")
            else:
                parts.append(f"\n{key}: {value}")

    # 1b. Manual facts (dal tool update_profile)
    manual_facts = overrides.get('manual_facts', [])
    if manual_facts:
        parts.append('\n=== FATTI MANUALI RECENTI (dall utente, PRIORITÀ ASSOLUTA) ===')
        for mf in manual_facts[-20:]:  # ultimi 20
            action = mf.get('action', 'add')
            parts.append(f"  [{action.upper()}] {mf['fact']} (registrato: {mf.get('added_at', 'N/A')[:10]})")

    # 2. Contesto utente
    parts.append("\n=== CONTESTO UTENTE (dalle preferenze) ===")
    parts.append(USER_CONTEXT.strip())

    # 3. KB entities
    parts.append("\n=== MOST FREQUENT KB ENTITIES ===")
    for e in entities:
        parts.append(f"  [{e['type']}] {e['entity']} ({e['count']} menzioni)")

    # 4. Recent activity
    parts.append(f"\n=== CONVERSAZIONI RECENTI (ultimi {RECENT_DAYS} giorni) ===")
    for c in recent_conv:
        parts.append(f"--- {c['date']} | {c['title'][:80]} | {c['source']} ---")
        parts.append(c['text'][:1200])

    parts.append("\n=== DOCUMENTI RECENTI (ultimi 60 giorni) ===")
    for c in recent_docs:
        parts.append(f"--- {c['date']} | {c['title'][:80]} ---")
        parts.append(c['text'][:1000])

    context = "\n".join(parts)
    if len(context) > MAX_CONTEXT_CHARS:
        context = context[:MAX_CONTEXT_CHARS] + "\n[...troncato...]"

    print(f"  Contesto: {len(context):,} chars (~{len(context)//4:,} token)")

    try:
        r = requests.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": MODEL,
                "max_tokens": 4096,
                "system": PROFILE_PROMPT,
                "messages": [{"role": "user", "content": context}],
            },
            timeout=90
        )

        if r.status_code != 200:
            print(f"❌ API {r.status_code}: {r.text[:300]}")
            return None

        raw = r.json()["content"][0]["text"].strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(l for l in lines if not l.startswith("```")).strip()

        profile = json.loads(raw)
        profile["generated_at"] = datetime.now().isoformat()
        profile["data_window_days"] = RECENT_DAYS
        profile["model"] = MODEL
        profile["has_overrides"] = bool(overrides.get("static_overrides"))
        return profile

    except json.JSONDecodeError as e:
        print(f"❌ JSON error: {e}")
        print(f"  Raw: {raw[:500]}")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


# ============================================================
# MAIN
# ============================================================

def main():
    global ANTHROPIC_API_KEY

    if "--view" in sys.argv:
        if not os.path.exists(PROFILE_PATH):
            print("❌ No profile found. Run: python3 scripts/enrichment/generate_user_profile.py")
            return
        with open(PROFILE_PATH) as f:
            print(json.dumps(json.load(f), indent=2, ensure_ascii=False))
        return

    dry_run = "--dry-run" in sys.argv

    if not ANTHROPIC_API_KEY:
        import subprocess
        try:
            result = subprocess.run(
                ["plutil", "-extract", "EnvironmentVariables.ANTHROPIC_API_KEY", "raw",
                 "/Library/LaunchDaemons/com.tailor.mcp.plist"],
                capture_output=True, text=True
            )
            if result.returncode == 0:
                ANTHROPIC_API_KEY = result.stdout.strip()
        except Exception:
            pass

    if not ANTHROPIC_API_KEY and not dry_run:
        print("❌ ANTHROPIC_API_KEY non trovata")
        sys.exit(1)

    print("=" * 60)
    print("TAILOR — User Profile Generator")
    print("=" * 60)
    start = time.time()

    segment_id = get_segment_id()
    cutoff = (datetime.now() - timedelta(days=RECENT_DAYS)).strftime("%Y-%m-%d")
    cutoff_60 = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")

    print("\nRaccolta dati...")

    print("  Override manuali...")
    overrides = load_overrides()
    has_ov = bool(overrides.get("static_overrides"))
    print(f"  → {'SI' if has_ov else 'NO'} ({len(overrides.get('static_overrides', {}))} fields)" if has_ov else "  → none")

    print("  Top entities...")
    entities = get_top_entities(30)
    print(f"  → {len(entities)}")

    print("  Conv summary recenti (30gg)...")
    recent_conv = get_summaries_by_category(segment_id, "conv_summary", cutoff, 40)
    print(f"  → {len(recent_conv)}")

    print("  Doc summary recenti (60gg)...")
    recent_docs = get_summaries_by_category(segment_id, "doc_summary", cutoff_60, 20)
    print(f"  → {len(recent_docs)}")

    if dry_run:
        print("\n=== DRY RUN ===")
        if has_ov:
            print(f"\nOverride campi: {list(overrides['static_overrides'].keys())}")
        print(f"\nTop 10 entities:")
        for e in entities[:10]:
            print(f"  [{e['type']}] {e['entity']} ({e['count']})")
        print(f"\nConv recenti ({len(recent_conv)}):")
        for c in recent_conv[:10]:
            print(f"  {c['date']} | {c['title'][:60]} | {c['source']}")
        print(f"\nDoc recenti ({len(recent_docs)}):")
        for c in recent_docs[:5]:
            print(f"  {c['date']} | {c['title'][:60]}")
        return

    print("\nGenerating profile via Claude Haiku...")
    profile = generate_profile(overrides, entities, recent_conv, recent_docs)

    if not profile:
        print("❌ Generation failed")
        sys.exit(1)

    with open(PROFILE_PATH, "w") as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)

    elapsed = time.time() - start
    s = profile.get("static", {})
    d = profile.get("dynamic", {})

    print(f"\n{'='*60}")
    print(f"PROFILO GENERATO — {elapsed:.1f}s")
    print(f"{'='*60}")
    print(f"File: {PROFILE_PATH}")
    print(f"Override applicati: {'SI' if profile.get('has_overrides') else 'NO'}")
    print(f"\nStatic: {s.get('identity','')[:100]}")
    print(f"  Aziende: {len(s.get('companies',[]))} | Persone attive: {len(s.get('key_people',[]))} | Ex: {len(s.get('former_key_people',[]))}")
    print(f"\nDynamic: {d.get('current_focus','')[:100]}")
    print(f"  Progetti: {len(d.get('active_projects',[]))} | Decisioni: {len(d.get('recent_decisions',[]))}")


if __name__ == "__main__":
    main()
