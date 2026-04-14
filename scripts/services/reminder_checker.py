#!/usr/bin/env python3
"""
TAILOR — Reminder Checker v2 (unified)
Runs every minute via cron. Handles:
- once: reminder one-shot (data/ora specifica)
- recurring: recurring reminders (days + time)
  - text/message: static text to send
  - action "run_script": runs a script and sends the output

Reads db/reminders.json, sends due reminders via Telegram.
"""

import os, sys, json, subprocess, requests
from datetime import datetime, timedelta
from filelock import FileLock

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(BASE_DIR))
REMINDERS_FILE = os.path.join(PROJECT_DIR, "db", "reminders.json")
LOCK_FILE = REMINDERS_FILE + ".lock"
VENV_PYTHON = os.path.join(PROJECT_DIR, ".venv", "bin", "python3")

if not BOT_TOKEN or not CHAT_ID:
    print("ERROR: env vars missing.", file=sys.stderr); sys.exit(1)

def load_reminders():
    with FileLock(LOCK_FILE, timeout=5):
        if not os.path.exists(REMINDERS_FILE): return []
        with open(REMINDERS_FILE, "r") as f: return json.load(f)

def save_reminders(rem):
    with FileLock(LOCK_FILE, timeout=5):
        with open(REMINDERS_FILE, "w") as f: json.dump(rem, f, indent=2, ensure_ascii=False)

def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}, timeout=10)
        data = resp.json()
        if data.get("ok"): return True
        if "can't parse" in data.get("description","").lower():
            requests.post(url, json={"chat_id": CHAT_ID, "text": text}, timeout=10)
            return True
        return False
    except Exception as e:
        print(f"TG error: {e}", file=sys.stderr); return False

def run_script(script_path, args=None):
    """Run a Python script and capture stdout as message."""
    cmd = [VENV_PYTHON, script_path]
    if args: cmd.extend(args if isinstance(args, list) else [args])
    try:
        env = {**os.environ, "TELEGRAM_BOT_TOKEN": BOT_TOKEN, "TELEGRAM_CHAT_ID": CHAT_ID}
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, env=env, cwd=PROJECT_DIR)
        return result.returncode == 0
    except Exception as e:
        print(f"Script error ({script_path}): {e}", file=sys.stderr)
        return False

def check_once(r, now):
    if r.get("sent"): return False
    try: return now >= datetime.fromisoformat(r["remind_at"])
    except Exception: return False

def check_recurring(r, now):
    days = r.get("days", [])
    time_str = r.get("time", "")
    if not days or not time_str: return False
    if now.weekday() not in days: return False
    try: hour, minute = map(int, time_str.split(":"))
    except Exception: return False
    if now.hour != hour or now.minute != minute: return False
    last_sent = r.get("last_sent", "")
    if last_sent:
        try:
            ls = datetime.fromisoformat(last_sent)
            if ls.date() == now.date() and ls.hour == hour and ls.minute == minute: return False
        except Exception: pass
    return True

def main():
    reminders = load_reminders()
    if not reminders: return

    now = datetime.now()
    updated = False

    for r in reminders:
        rtype = r.get("type", "once")

        if rtype == "once":
            if check_once(r, now):
                text = f"⏰ *Reminder*\n\n{r['text']}"
                if send_telegram(text):
                    r["sent"] = True; r["sent_at"] = now.isoformat(); updated = True
                    print(f"Sent once: {r['text'][:60]}")

        elif rtype == "recurring":
            if check_recurring(r, now):
                action = r.get("action", "send_text")

                if action == "run_script":
                    # The script sends the message directly
                    script = r.get("script", "")
                    args = r.get("args", [])
                    if script and run_script(os.path.join(PROJECT_DIR, script), args):
                        r["last_sent"] = now.isoformat(); updated = True
                        print(f"Ran script: {script} {args}")
                    else:
                        print(f"Script failed: {script}", file=sys.stderr)

                else:  # send_text
                    text = r.get("message", r.get("text", ""))
                    if text and send_telegram(text):
                        r["last_sent"] = now.isoformat(); updated = True
                        print(f"Sent recurring: {r.get('label','')[:60]}")

    if updated:
        cutoff = (now - timedelta(days=7)).isoformat()
        reminders = [r for r in reminders if not (
            r.get("type","once") == "once" and r.get("sent") and r.get("sent_at","") < cutoff
        )]
        save_reminders(reminders)

if __name__ == "__main__":
    main()
