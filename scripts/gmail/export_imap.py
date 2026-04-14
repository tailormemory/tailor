#!/usr/bin/env python3
"""
TAILOR — IMAP Email Export
Fetches emails via IMAP, saves in same JSONL format as Gmail export.
Reads config from tailor.yaml email section.

Usage:
  python export_imap.py                    # Incremental (last 7 days)
  python export_imap.py --since 2026/01/01
  python export_imap.py --full
"""
import os, sys, json, imaplib, email, re, argparse
from email.header import decode_header
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
from config import get as cfg, load_config
DATA_DIR = os.path.join(BASE_DIR, "data")

SENDER_BLACKLIST = {
    "noreply","no-reply","donotreply","do-not-reply",
    "notifications","notification","mailer-daemon","postmaster",
    "newsletter","news","marketing","promo","promotions",
    "updates","update",
}

def decode_str(s):
    if not s: return ""
    parts = decode_header(s)
    r = []
    for d, ch in parts:
        if isinstance(d, bytes):
            r.append(d.decode(ch or "utf-8", errors="replace"))
        else: r.append(str(d))
    return " ".join(r).strip()

# PART 2 follows via append

def extract_body(msg):
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in cd:
                p = part.get_payload(decode=True)
                if p: body += p.decode(part.get_content_charset() or "utf-8", errors="replace")
            elif ct == "text/html" and not body and "attachment" not in cd:
                p = part.get_payload(decode=True)
                if p:
                    h = p.decode(part.get_content_charset() or "utf-8", errors="replace")
                    body = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', h)).strip()
    else:
        p = msg.get_payload(decode=True)
        if p:
            t = p.decode(msg.get_content_charset() or "utf-8", errors="replace")
            if msg.get_content_type() == "text/html":
                t = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', t)).strip()
            body = t
    return body.strip()

def is_blacklisted(addr):
    if not addr: return False
    local = addr.lower().split("@")[0] if "@" in addr.lower() else addr.lower()
    return any(bl in local for bl in SENDER_BLACKLIST)

def connect_imap(host, port, username, password):
    port = int(port)
    conn = imaplib.IMAP4_SSL(host, port) if port == 993 else imaplib.IMAP4(host, port)
    conn.login(username, password)
    return conn

def fetch_emails(conn, since_date=None, mailbox="INBOX"):
    conn.select(mailbox, readonly=True)
    criteria = f'(SINCE "{since_date.strftime("%d-%b-%Y")}")' if since_date else "ALL"
    status, data = conn.search(None, criteria)
    if status != "OK": print(f"  Search failed: {status}"); return []
    msg_ids = data[0].split()
    if not msg_ids: return []
    print(f"  Found {len(msg_ids)} messages in {mailbox}")
    results = []; errors = 0
    email_addrs = cfg("email", "addresses") or []
    for i, mid in enumerate(msg_ids):
        try:
            st, md = conn.fetch(mid, "(RFC822)")
            if st != "OK": errors += 1; continue
            msg = email.message_from_bytes(md[0][1])
            from_a = decode_str(msg.get("From", ""))
            if is_blacklisted(from_a): continue
            body = extract_body(msg)
            if not body or len(body) < 20: continue
            if len(body) > 50000: body = body[:50000] + "\n[...truncated]"
            msg_id = msg.get("Message-ID", mid.decode()).strip("<>")
            is_sent = any(addr.lower() in from_a.lower() for addr in email_addrs) if email_addrs else False
            results.append({
                "id": msg_id,
                "thread_id": (msg.get("In-Reply-To","").strip("<>") or msg_id),
                "date": msg.get("Date",""),
                "from": from_a,
                "to": decode_str(msg.get("To","")),
                "cc": decode_str(msg.get("Cc","")),
                "subject": decode_str(msg.get("Subject","")),
                "body": body, "labels": [mailbox],
                "is_sent": is_sent,
                "snippet": body[:200].replace("\n"," "),
                "account": "imap",
            })
            if (i+1)%100==0: print(f"    [{i+1}/{len(msg_ids)}] {len(results)} kept")
        except Exception as e:
            errors += 1
            if errors < 5: print(f"    Error: {e}")
    print(f"  Fetched: {len(results)}, {errors} errors")
    return results

def main():
    load_config()
    provider = cfg("email", "provider")
    if provider != "imap":
        print(f"Email provider is '{provider}', not 'imap'. Skipping.")
        return
    imap_cfg = cfg("email", "imap") or {}
    host = imap_cfg.get("host", "")
    port = imap_cfg.get("port", 993)
    username = imap_cfg.get("username", "")
    password = imap_cfg.get("password", "")
    mailboxes = imap_cfg.get("mailboxes", ["INBOX", "Sent"])
    if not host or not username or not password:
        print("ERROR: IMAP credentials incomplete"); return

    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="YYYY/MM/DD")
    ap.add_argument("--full", action="store_true")
    args = ap.parse_args()

    output = os.path.join(DATA_DIR, "gmail_export_imap.jsonl")
    if args.full: since = None; print("Full IMAP export")
    elif args.since: since = datetime.strptime(args.since, "%Y/%m/%d")
    else: since = datetime.now() - timedelta(days=7); print("Incremental (7 days)")

    existing = set()
    if os.path.exists(output):
        with open(output) as f:
            for ln in f:
                try: existing.add(json.loads(ln)["id"])
                except Exception: pass
    print(f"Existing: {len(existing)} emails")

    print(f"Connecting to {host}:{port}...")
    try: conn = connect_imap(host, port, username, password)
    except Exception as e: print(f"ERROR: {e}"); return

    all_new = []
    try:
        for mbox in mailboxes:
            print(f"\n--- {mbox} ---")
            try:
                emails = fetch_emails(conn, since_date=since, mailbox=mbox)
                new = [e for e in emails if e["id"] not in existing]
                print(f"  New: {len(new)}")
                all_new.extend(new)
                for e in new: existing.add(e["id"])
            except Exception as e: print(f"  ERROR: {e}")
    finally:
        try: conn.logout()
        except Exception: pass

    if all_new:
        with open(output, "a") as f:
            for r in all_new: f.write(json.dumps(r, ensure_ascii=False)+"\n")
        print(f"\nAppended {len(all_new)} emails")
    else: print("\nNo new emails")

if __name__ == "__main__":
    main()
