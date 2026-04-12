#!/usr/bin/env python3
"""
TAILOR — Heartbeat Monitor
Checks that critical services are running. Telegram alert if down.
Cron: every 5 minutes.

Servizi monitorati:
- MCP Server (porta 8787)
- Ollama (porta 11434)
- Telegram Bot (processo 12_telegram_bot.py)

State file: db/checkpoints/heartbeat_state.json — avoids repeated alerts.
"""

import os, sys, json, subprocess, socket, requests
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
from config import get as cfg
APP_NAME = cfg("branding", "app_name") or "TAILOR"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATE_FILE = os.path.join(BASE_DIR, "db", "checkpoints", "heartbeat_state.json")
LOG_PATH = os.path.join(BASE_DIR, "logs", "heartbeat.log")

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")

# Load services from config, fallback to defaults
_svc_cfg = cfg("services") or []
if _svc_cfg:
    SERVICES = []
    for s in _svc_cfg:
        entry = {"name": s["name"], "check": s.get("check", "process"), "icon": s.get("icon", "⚙")}
        if s.get("port"):
            entry["port"] = s["port"]
        if s.get("process"):
            entry["pattern"] = s["process"]
        SERVICES.append(entry)
else:
    SERVICES = [
        {"name": "MCP Server", "check": "port", "port": 8787, "icon": "🖥"},
        {"name": "Ollama", "check": "port", "port": 11434, "icon": "🧠"},
        {"name": "Telegram Bot", "check": "process", "pattern": "telegram_bot", "icon": "🤖"},
    ]


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as f:
        f.write(f"[{ts}] {msg}\n")


def check_port(port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect(("127.0.0.1", port))
        s.close()
        return True
    except:
        return False


def check_process(pattern):
    try:
        result = subprocess.run(["pgrep", "-f", pattern], capture_output=True, timeout=5)
        return result.returncode == 0
    except:
        return False


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def send_telegram(text):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "Markdown"},
            timeout=10
        )
    except:
        pass


def main():
    state = load_state()
    alerts = []
    recoveries = []

    for svc in SERVICES:
        name = svc["name"]
        icon = svc["icon"]

        if svc["check"] == "port":
            alive = check_port(svc["port"])
        elif svc["check"] == "process":
            alive = check_process(svc["pattern"])
        else:
            alive = False

        was_down = state.get(name, {}).get("down", False)

        if not alive and not was_down:
            # Just went down — alert
            alerts.append(f"{icon} *{name}*: DOWN")
            state[name] = {"down": True, "since": datetime.now().isoformat()}
            log(f"ALERT: {name} DOWN")

        elif not alive and was_down:
            # Still down — no repeated alert
            log(f"STILL DOWN: {name}")

        elif alive and was_down:
            # Back up — recovery alert
            since = state.get(name, {}).get("since", "?")
            recoveries.append(f"{icon} *{name}*: " + t(f"restored (was down since {since})"))
            state[name] = {"down": False}
            log(f"RECOVERED: {name}")

        else:
            # All good
            state[name] = {"down": False}

    save_state(state)

    if alerts:
        msg = f"🚨 *{APP_NAME} Alert*\n\n" + "\n".join(alerts)
        send_telegram(msg)

    if recoveries:
        msg = f"✅ *{APP_NAME} Recovery*\n\n" + "\n".join(recoveries)
        send_telegram(msg)


if __name__ == "__main__":
    main()
