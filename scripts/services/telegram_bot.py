#!/usr/bin/env python3
"""
TAILOR — Telegram Bot v5

Architecture:
- Qwen (local) for intent classification (fast, free)
- Pluggable LLM brain for general responses (Claude/GPT/Gemini/Ollama)
- Tool use: LLM calls KB tools autonomously (Tier 1) or gets pre-fetched context (Tier 2)
- Session buffer: SQLite-backed conversation history with auto session capture
- Config: YAML-based, LLM-agnostic, plug & play

v5 changes from v4:
- LLM brain replaces Qwen for response generation
- Multi-turn conversation context via session buffer
- Automatic session capture to KB on timeout
- Tool use loop: LLM decides what to search
- All config in config/tailor.yaml
"""

import os, sys, json, time, uuid, signal, requests, traceback
from datetime import datetime, timedelta
from filelock import FileLock

# ── Path setup ────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, "scripts"))

from lib.config import load_config, get
from lib.session_store import init_db, get_or_create_session, add_message, get_history, get_expired_sessions, get_session_messages, close_session, get_session_count
from lib.llm_client import get_brain, get_classifier, TOOLS

# ── Load config ───────────────────────────────────────────────
load_config()

BOT_TOKEN = get("telegram", "bot_token", "")
CHAT_ID = str(get("telegram", "chat_id", ""))
POLL_TIMEOUT = get("telegram", "poll_timeout", 30)
PERSONA_NAME = get("persona", "name", "Tailor")
SYSTEM_PROMPT = get("persona", "system_prompt", "You are a helpful assistant.")
_USER_LANG = get("user", "language") or "en"

# i18n — translate user-facing strings
try:
    from lib.i18n import t
except ImportError:
    t = lambda x, **kw: x  # fallback: no translation
AUTO_CAPTURE = get("sessions", "auto_capture", True)

DB_DIR = os.path.join(ROOT_DIR, "db")
REMINDERS_FILE = os.path.join(DB_DIR, "reminders.json")
LOCK_FILE = REMINDERS_FILE + ".lock"

if not BOT_TOKEN or not CHAT_ID:
    print("ERROR: telegram.bot_token or telegram.chat_id missing in config.", file=sys.stderr)
    sys.exit(1)

# ── Globals ───────────────────────────────────────────────────
last_update_id = 0
running = True
pending_note_deletions = []
pending_reminder_deletions = []

def signal_handler(sig, frame):
    global running; running = False
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def log(msg): print(f"[{now()}] {msg}", file=sys.stderr)

# ═══════════════════════════════════════════════════════════════
# Telegram API
# ═══════════════════════════════════════════════════════════════

def tg_api(method, data=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=data, timeout=POLL_TIMEOUT+10) if data else requests.get(url, timeout=POLL_TIMEOUT+10)
        return resp.json()
    except Exception as e:
        log(f"TG error ({method}): {e}"); return None

def send_message(text, parse_mode="Markdown"):
    # Telegram has 4096 char limit
    if len(text) > 4000:
        text = text[:4000] + "\n\n_[troncato]_"
    r = tg_api("sendMessage", {"chat_id": CHAT_ID, "text": text, "parse_mode": parse_mode})
    if r and not r.get("ok") and "can't parse" in r.get("description","").lower():
        tg_api("sendMessage", {"chat_id": CHAT_ID, "text": text})

def send_typing(): tg_api("sendChatAction", {"chat_id": CHAT_ID, "action": "typing"})

# ═══════════════════════════════════════════════════════════════
# Reminder System (kept from v4 — works well, no reason to change)
# ═══════════════════════════════════════════════════════════════

def load_reminders():
    with FileLock(LOCK_FILE, timeout=5):
        if not os.path.exists(REMINDERS_FILE): return []
        with open(REMINDERS_FILE,"r") as f: return json.load(f)

def save_reminders(rem):
    with FileLock(LOCK_FILE, timeout=5):
        with open(REMINDERS_FILE,"w") as f: json.dump(rem, f, indent=2, ensure_ascii=False)

def add_reminder(text, remind_at):
    rem = load_reminders()
    r = {"id": int(datetime.now().timestamp()*1000), "text": text, "remind_at": remind_at,
         "created_at": datetime.now().isoformat(), "sent": False}
    rem.append(r); save_reminders(rem); return r

def add_recurring_reminder(text, days, time_str):
    rem = load_reminders()
    r = {"id": int(datetime.now().timestamp()*1000), "type": "recurring", "label": text[:60],
         "action": "send_text", "message": f"\u23f0 *Reminder*\n\n{text}",
         "days": days, "time": time_str, "last_sent": ""}
    rem.append(r); save_reminders(rem); return r

def get_active_reminders(): return [r for r in load_reminders() if not r.get("sent")]

def delete_reminder_by_index(idx):
    rem = load_reminders(); active = [r for r in rem if not r.get("sent")]
    if idx<1 or idx>len(active): return False
    tid = active[idx-1]["id"]; rem = [r for r in rem if r["id"]!=tid]; save_reminders(rem); return True

# ═══════════════════════════════════════════════════════════════
# Intent Classification (Qwen, local, fast)
# ═══════════════════════════════════════════════════════════════

INTENT_SYSTEM = (
    "Intent classifier. Classify into ONE category:\n"
    "- \"create_reminder\": wants to create a reminder\n"
    "- \"delete_reminder\": wants to delete a reminder\n"
    "- \"list_reminders\": wants to see reminders\n"
    "- \"create_note\": wants to save a note\n"
    "- \"delete_note\": wants to delete a note\n"
    "- \"search_notes\": wants to search notes\n"
    "- \"confirm_delete\": confirms deletion with a number\n"
    "- \"show_status\": wants system status\n"
    "- \"general\": anything else\n\n"
    "Respond ONLY JSON: {\"intent\": \"...\", \"query\": \"...\"}"
)

REMINDER_SYSTEM = (
    "Parse a reminder request. Determine if it's ONE-TIME or RECURRING.\n\n"
    "RECURRING indicators: \"every\", \"daily\", \"weekly\", \"every monday\", \"every day\", \"weekdays\".\n"
    "ONE-TIME indicators: \"tomorrow\", \"in X hours\", \"at 3pm\", a specific date.\n\n"
    "For ONE-TIME, respond JSON:\n"
    "  {\"type\": \"once\", \"text\": \"what to remember\", \"datetime\": \"ISO 8601\"}\n"
    "  \"tomorrow\"=next day, \"in X days/hours/minutes\"=now+X, no time=09:00\n"
    "  \"morning\"=08:00, \"lunch\"=13:00, \"afternoon\"=15:00, \"evening\"=20:00\n\n"
    "For RECURRING, respond JSON:\n"
    "  {\"type\": \"recurring\", \"text\": \"what to remember\", \"days\": [0,1,2...], \"time\": \"HH:MM\"}\n"
    "  days: 0=Monday...6=Sunday\n"
    "  \"every day\"=[0,1,2,3,4,5,6], \"weekdays\"=[0,1,2,3,4]\n"
    "  If no time specified, default \"09:00\"\n\n"
    "The user may write in any language. Parse intent regardless of language.\n"
    "Respond ONLY JSON, nothing else."
)

NOTE_SYSTEM = (
    "Note classifier. Generate:\n1. \"title\": concise title (max 60 chars)\n"
    "2. \"category\": decision/preference/fact/project/person/financial/health/legal/business/technical/personal\n"
    "Respond ONLY JSON."
)

def ask_classifier(system: str, user_prompt: str) -> str:
    """Use the local classifier (Qwen) for fast intent/parsing."""
    try:
        return get_classifier().chat(system, [{"role": "user", "content": user_prompt}], max_tokens=400)
    except Exception as e:
        log(f"Classifier error: {e}"); return ""

def ask_classifier_json(system: str, user_prompt: str) -> dict | None:
    raw = ask_classifier(system, user_prompt)
    if not raw: return None
    try:
        s = raw.index("{"); e = raw.rindex("}") + 1
        return json.loads(raw[s:e])
    except: return None

def classify_intent(text: str) -> tuple[str, str]:
    t = text.strip()
    # Command shortcuts
    if t.startswith("/status"): return "show_status", ""
    if t.startswith("/reminders"): return "list_reminders", ""
    if t.startswith("/start"): return "start", ""
    if t.startswith("/stats"): return "show_status", ""
    
    # Pending deletion flow
    if (pending_note_deletions or pending_reminder_deletions) and len(t) < 30:
        has_num = any(c.isdigit() for c in t)
        cwords = ["si","ok","prima","primo","seconda","secondo","terza","terzo","quarta","quarto","cancella"]
        if has_num or any(w in t.lower().replace("\u00ec","i") for w in cwords):
            return "confirm_delete", t
    
    p = ask_classifier_json(INTENT_SYSTEM, f"Message: \"{t}\"")
    if p and "intent" in p:
        return p["intent"], p.get("query", t)
    return "general", t

# ═══════════════════════════════════════════════════════════════
# Specific Intent Handlers (reminder, notes, status)
# ═══════════════════════════════════════════════════════════════

def handle_create_reminder(text):
    send_typing()
    dt_now = datetime.now()
    days_en = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
    day_names = ["MON","TUE","WED","THU","FRI","SAT","SUN"]
    p = ask_classifier_json(REMINDER_SYSTEM, f"Now: {dt_now.strftime('%Y-%m-%d %H:%M:%S')} ({days_en[dt_now.weekday()]})\nMessage: \"{text}\"")
    if not p or "text" not in p:
        return t("I didn't understand what/when. Try:\nremind me to call Marco tomorrow at 10")
    
    rtype = p.get("type", "once")
    if rtype == "recurring":
        days = p.get("days", [])
        time_str = p.get("time", "09:00")
        if not days: return t("I didn't understand the days.")
        add_recurring_reminder(p["text"], days, time_str)
        day_labels = ", ".join(day_names[d] for d in sorted(days))
        return f"\u2705 *{t("Recurring reminder set")}*\n\n\U0001f4dd {p['text']}\n\U0001f4c5 {day_labels} {time_str}"
    else:
        dt_str = p.get("datetime", "")
        if not dt_str: return t("I didn't understand when.")
        try:
            dt = datetime.fromisoformat(dt_str)
            if dt < dt_now - timedelta(minutes=5):
                return t(f"Date ({dt.strftime('%d/%m/%Y %H:%M')}) is in the past.")
        except: return f"{t('Invalid date')}: {dt_str}"
        add_reminder(p["text"], dt_str)
        return f"\u2705 *{t("Reminder set")}*\n\n\U0001f4dd {p['text']}\n\U0001f4c5 {dt.strftime('%d/%m/%Y %H:%M')}"

def handle_delete_reminder(query):
    global pending_reminder_deletions
    send_typing(); active = get_active_reminders()
    if not active: return "No active reminders."
    if query:
        ql = query.lower()
        matches = [(i,r) for i,r in enumerate(active) if ql in r.get("text","").lower() or ql in r.get("label","").lower()]
        if not matches: return f"No reminder matching _{query}_."
        pending_reminder_deletions = [i for i,_ in matches]; target = [r for _,r in matches]
    else:
        pending_reminder_deletions = list(range(len(active))); target = active
    lines = ["\U0001f5d1 *Quale reminder cancello?*\n"]
    for i,r in enumerate(target,1):
        try: ds = datetime.fromisoformat(r.get("remind_at","")).strftime("%d/%m %H:%M")
        except: ds = r.get("time","?")
        lines.append(f"*{i}.* {r.get('text', r.get('label','?'))} — {ds}")
    lines.append("\n" + t("Reply with the number.")); return "\n".join(lines)

def handle_list_reminders():
    all_rem = load_reminders()
    day_names = ["MON","TUE","WED","THU","FRI","SAT","SUN"]
    once_active = [r for r in all_rem if r.get("type","once")=="once" and not r.get("sent")]
    recurring = [r for r in all_rem if r.get("type")=="recurring"]
    if not once_active and not recurring: return "No active reminders."
    lines = [f"\U0001f4cb *{t('Active reminders')}:*\n"]
    if once_active:
        lines.append("*One-shot:*")
        for i,r in enumerate(once_active,1):
            try: ds = datetime.fromisoformat(r["remind_at"]).strftime("%d/%m/%Y %H:%M")
            except: ds = "?"
            lines.append(f"  *{i}.* {r['text']} — {ds}")
    if recurring:
        lines.append("\n*Ricorrenti:*")
        for r in recurring:
            label = r.get("label", r.get("text","")[:40])
            ds = ", ".join(day_names[d] for d in r.get("days",[]))
            lines.append(f"  \u2022 {label} — {ds} {r.get('time','')}")
    return "\n".join(lines)

def handle_create_note(text):
    send_typing()
    if len(text.strip()) < 5: return t("Note too short.")
    p = ask_classifier_json(NOTE_SYSTEM, f"Nota: \"{text}\"")
    title = p["title"][:80] if p and "title" in p else text[:60]
    cat = p.get("category","general") if p else "general"
    # Use tool executor directly
    from lib.tool_executor import execute_tool
    result = execute_tool("kb_add", {"content": text, "title": title, "category": cat})
    if "errore" in result.lower() or "error" in result.lower():
        return t("Save error") + f": {result[:200]}"
    return f"\U0001f4dd *{t("Note saved")}*\n\n\u2022 {title}\n\u2022 {t("Category")}: {cat}"

def handle_delete_note(query):
    global pending_note_deletions
    send_typing()
    from lib.tool_executor import execute_tool
    # Search notes saved from Telegram
    result = execute_tool("kb_hybrid_search", {"query": query, "n_results": 5, "source_filter": "telegram"})
    # Parse results to get IDs — this is text output, extract what we can
    # For now, simplified: tell user to use Claude for complex operations
    return t("KB search for") + f" _{query}_:\n\n{result[:1500]}"

def handle_confirm_delete(query):
    global pending_note_deletions, pending_reminder_deletions
    num = None
    for w in query.split():
        try: num = int(w); break
        except: continue
    if num is None:
        ords = {"prima":1,"primo":1,"seconda":2,"secondo":2,"terza":3,"terzo":3,"quarta":4,"quarto":4}
        for w in query.lower().split():
            if w in ords: num = ords[w]; break
    if num is None: return t("Reply with a number.")
    if pending_reminder_deletions:
        if num<1 or num>len(pending_reminder_deletions): return f"Scegli tra 1 e {len(pending_reminder_deletions)}."
        if delete_reminder_by_index(pending_reminder_deletions[num-1]+1):
            pending_reminder_deletions = []; return t("\u2705 Reminder deleted.")
        return t("Deletion error.")
    return "No pending deletions."

def handle_show_status():
    try:
        from lib.tool_executor import execute_tool
        stats_raw = execute_tool("get_user_profile", {})
        sess = get_session_count()
        brain = get_brain()
        return (f"\U0001f7e2 *{PERSONA_NAME} v5*\n\n"
                f"\u2022 Brain: {brain.provider}/{brain.model}\n"
                f"\u2022 Tool use: {'Tier 1' if brain.tool_use else 'Tier 2'}\n"
                f"\u2022 Sessioni TG: {sess['total_sessions']} ({sess['active_sessions']} attive)\n"
                f"\u2022 Messaggi TG: {sess['total_messages']}\n"
                f"\u2022 Ora: {now()}")
    except Exception as e: return f"\u26a0\ufe0f {e}"

# ═══════════════════════════════════════════════════════════════
# General Handler — LLM Brain with Tool Use
# ═══════════════════════════════════════════════════════════════

def handle_general(text: str, session_id: str) -> str:
    """The main handler: LLM brain answers using KB tools."""
    send_typing()
    brain = get_brain()
    
    # Build conversation history from session
    history = get_history(session_id)
    
    # For Tier 2 (no tool use): pre-fetch context
    if not brain.tool_use:
        from lib.tool_executor import execute_tool
        context = execute_tool("kb_hybrid_search", {"query": text, "n_results": 3})
        profile = execute_tool("get_user_profile", {})
        enhanced_system = f"{SYSTEM_PROMPT}\n\n--- PROFILO UTENTE ---\n{profile[:1500]}\n\n--- CONTESTO KB ---\n{context[:3000]}"
        # Use history + current message
        msgs = history + [{"role": "user", "content": text}]
        return brain.chat(enhanced_system, msgs) or f"{PERSONA_NAME} non disponibile."
    
    # Tier 1: LLM with tool use
    msgs = history + [{"role": "user", "content": text}]
    response = brain.chat_with_tools(SYSTEM_PROMPT, msgs)
    return response or f"{PERSONA_NAME} non disponibile."

# ═══════════════════════════════════════════════════════════════
# Session Capture — auto-save to KB on timeout
# ═══════════════════════════════════════════════════════════════

def check_and_capture_sessions():
    """Find expired sessions, generate summaries, save to KB."""
    if not AUTO_CAPTURE:
        return
    expired = get_expired_sessions()
    for sess in expired:
        sid = sess["session_id"]
        messages = get_session_messages(sid)
        if len(messages) < 2:
            close_session(sid, "too_short")
            continue
        
        # Build conversation text for summary
        conv_text = "\n".join([f"[{m['role']}] {m['content']}" for m in messages])
        if len(conv_text) > 8000:
            conv_text = conv_text[:8000] + "\n[truncated]"
        
        try:
            brain = get_brain()
            summary_prompt = (
                "Analyze this Telegram conversation and generate a structured summary.\n"
                "Respond ONLY in this JSON format:\n"
                "{\n"
                "  \"summary\": \"Riassunto in 2-3 frasi\",\n"
                "  \"key_facts\": \"Fatti chiave separati da newline\",\n"
                "  \"decisions\": \"Decisioni prese (o vuoto)\",\n"
                "  \"action_items\": \"Azioni da fare (o vuoto)\"\n"
                "}\n\n"
                f"Conversazione:\n{conv_text}"
            )
            raw = brain.chat("You are an assistant that summarizes conversations. Respond ONLY JSON.", 
                           [{"role": "user", "content": summary_prompt}], max_tokens=500)
            
            # Parse JSON from response
            try:
                s = raw.index("{"); e = raw.rindex("}") + 1
                parsed = json.loads(raw[s:e])
            except:
                parsed = {"summary": raw[:500], "key_facts": "", "decisions": "", "action_items": ""}
            
            # Save to KB via tool executor
            from lib.tool_executor import execute_tool
            
            # Import kb_update_session directly since it's not in our standard tools
            sys.path.insert(0, ROOT_DIR)
            sys.path.insert(0, os.path.join(ROOT_DIR, "scripts"))
            import mcp_server as mcp
            result = mcp.kb_update_session(
                summary=parsed.get("summary", ""),
                key_facts=parsed.get("key_facts", ""),
                decisions=parsed.get("decisions", ""),
                action_items=parsed.get("action_items", ""),
                source="telegram"
            )
            
            close_session(sid, parsed.get("summary", "captured"))
            log(f"Session {sid} captured: {parsed.get('summary', '')[:80]}")
        except Exception as e:
            log(f"Session capture error for {sid}: {e}")
            close_session(sid, f"error: {e}")

# ═══════════════════════════════════════════════════════════════
# Message Router
# ═══════════════════════════════════════════════════════════════

def route_message(text: str, session_id: str) -> str:
    intent, query = classify_intent(text)
    log(f"Intent: {intent} | Query: {query[:60]}")
    
    handlers = {
        "create_reminder": lambda: handle_create_reminder(query),
        "delete_reminder": lambda: handle_delete_reminder(query),
        "list_reminders": lambda: handle_list_reminders(),
        "create_note": lambda: handle_create_note(query),
        "delete_note": lambda: handle_delete_note(query),
        "search_notes": lambda: handle_general(query, session_id),  # Let brain handle search
        "confirm_delete": lambda: handle_confirm_delete(query),
        "show_status": lambda: handle_show_status(),
        "general": lambda: handle_general(query, session_id),
        "start": lambda: (f"\U0001f7e2 *{PERSONA_NAME} v5 {t('active')}.*\n\n"
                         f"Brain: {get_brain().provider}/{get_brain().model}\n\n"
                         f"{t('Write naturally')}:\n"
                         f"\u2022 _remind me..._ — reminder\n"
                         f"\u2022 _note: ..._ — {t('save to KB')}\n"
                         f"\u2022 {t('Any question')} — {t('I answer with KB context')}"),
    }
    return handlers.get(intent, lambda: handle_general(text, session_id))()

# ═══════════════════════════════════════════════════════════════
# Main Poll Loop
# ═══════════════════════════════════════════════════════════════

def poll():
    global last_update_id
    
    # Initialize
    init_db()
    brain = get_brain()
    log(f"{PERSONA_NAME} Telegram Bot v5 avviato.")
    log(f"Brain: {brain.provider}/{brain.model} | Tool use: {'Tier 1' if brain.tool_use else 'Tier 2'}")
    
    last_capture_check = time.time()
    
    while running:
        try:
            # Check for expired sessions every 60 seconds
            if time.time() - last_capture_check > 60:
                check_and_capture_sessions()
                last_capture_check = time.time()
            
            data = tg_api("getUpdates", {"offset": last_update_id+1, "timeout": POLL_TIMEOUT, "allowed_updates": ["message"]})
            if not data or not data.get("ok"):
                time.sleep(5); continue
            
            for upd in data.get("result", []):
                last_update_id = upd["update_id"]
                msg = upd.get("message", {})
                cid = str(msg.get("chat", {}).get("id", ""))
                text = msg.get("text", "")
                if cid != CHAT_ID or not text:
                    continue
                
                log(f"Ricevuto: {text[:100]}")
                
                try:
                    # Get or create session
                    session_id = get_or_create_session()
                    
                    # Save user message to session
                    add_message(session_id, "user", text)
                    
                    # Route and get response
                    response = route_message(text, session_id)
                    
                    # Save assistant response to session
                    add_message(session_id, "assistant", response)
                    
                    # Send to Telegram
                    send_message(response)
                    
                except Exception as e:
                    log(f"Error: {e}"); traceback.print_exc(file=sys.stderr)
                    send_message(f"\u26a0\ufe0f {str(e)[:200]}")
                    
        except Exception as e:
            log(f"Poll error: {e}"); time.sleep(10)

if __name__ == "__main__":
    poll()
