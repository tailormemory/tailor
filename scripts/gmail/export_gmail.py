"""
TAILOR — Gmail Export
Downloads emails from Gmail API excluding noisy categories.
Saves to data/gmail_export.jsonl (incremental).

Usage:
  python3 scripts/gmail/export_gmail.py                    # Incremental export
  python3 scripts/gmail/export_gmail.py --stats            # Stats only
  python3 scripts/gmail/export_gmail.py --full             # Full re-export
  python3 scripts/gmail/export_gmail.py --account default

Incremental export: tracks the last downloaded message and resumes from there.
Can be interrupted and resumed without data loss.
"""

import os
import sys
import json
import time
import base64
import email
import re
from datetime import datetime
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# === CONFIGURATION ===
import sys as _sys
_sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
from config import get as cfg

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CREDENTIALS_DIR = os.path.join(BASE_DIR, "credentials")
DATA_DIR = os.path.join(BASE_DIR, "data")
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# Account configuration — loaded from tailor.yaml, with sensible defaults
def _build_accounts():
    gmail_cfg = cfg("email", "gmail") or {}
    creds = gmail_cfg.get("credentials", os.path.join(CREDENTIALS_DIR, "gmail_credentials.json"))
    token = gmail_cfg.get("token", os.path.join(CREDENTIALS_DIR, "gmail_token.json"))
    # Resolve relative paths
    if creds.startswith("./"): creds = os.path.join(BASE_DIR, creds[2:])
    if token.startswith("./"): token = os.path.join(BASE_DIR, token[2:])
    return {
        "default": {
            "credentials": creds,
            "token": token,
            "output": os.path.join(DATA_DIR, "gmail_export.jsonl"),
            "checkpoint": os.path.join(DATA_DIR, "gmail_export_checkpoint.json"),
        },
    }

ACCOUNTS = _build_accounts()

# Gmail query: exclude noisy categories
# -category: excludes automatic Gmail categories
GMAIL_QUERY = "-category:promotions -category:social -category:updates -category:forums -in:spam -in:trash"

# Blacklist sender domain (newsletter, notifiche automatiche)
_SENDER_BLACKLIST_DEFAULT = {
    "noreply", "no-reply", "donotreply", "do-not-reply",
    "notifications", "notification", "mailer-daemon", "postmaster",
    "newsletter", "news", "marketing", "promo", "promotions",
    "updates", "update", "info@github.com", "builds@travis-ci.org",
    "notify", "alert", "alerts",
}
_sender_extra = set(cfg("email", "sender_blacklist_extra") or [])
SENDER_BLACKLIST = _SENDER_BLACKLIST_DEFAULT | _sender_extra

# Blacklist domain (aggiungere pattern di domini rumorosi)
_DOMAIN_BLACKLIST_DEFAULT = {
    "facebookmail.com", "linkedin.com", "twitter.com", "x.com",
    "pinterest.com", "instagram.com", "tiktok.com",
    "accounts.google.com", "googleusercontent.com",
    "mailchimp.com", "sendgrid.net", "mailgun.org",
    "amazonses.com", "bounce.google.com",
    "steampowered.com", "playstation.com", "xbox.com",
    "spotify.com", "netflix.com", "apple.com",
}
_domain_extra = set(cfg("email", "domain_blacklist_extra") or [])
DOMAIN_BLACKLIST = _DOMAIN_BLACKLIST_DEFAULT | _domain_extra

# Soglia minima di lunghezza body (sotto = probabilmente rumore)
MIN_BODY_LENGTH = 50

# Batch size per list API
LIST_BATCH_SIZE = 500

# Rate limiting
REQUESTS_PER_SECOND = 5


def authenticate(account_config):
    """Autentica con Gmail API usando token salvato."""
    token_file = account_config["token"]
    creds_file = account_config["credentials"]

    if not os.path.exists(token_file):
        print(f"ERROR: Token not found: {token_file}")
        print("Esegui prima: python3 test_gmail_auth.py")
        sys.exit(1)

    creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    if not creds.valid:
        if creds.expired and creds.refresh_token:
            print("Token scaduto, rinnovo...")
            creds.refresh(Request())
            with open(token_file, "w") as f:
                f.write(creds.to_json())
        else:
            print("ERROR: Token invalid. Re-run the Gmail auth script.")
            sys.exit(1)

    return build("gmail", "v1", credentials=creds)


def is_sender_blacklisted(from_header):
    """Check if sender is in blacklist."""
    if not from_header:
        return False

    from_lower = from_header.lower()

    # Check email local part (before @)
    email_match = re.search(r'[\w.-]+@[\w.-]+', from_lower)
    if email_match:
        email_addr = email_match.group()
        local_part = email_addr.split("@")[0]
        domain = email_addr.split("@")[1]

        # Blacklist locale
        for bl in SENDER_BLACKLIST:
            if bl in local_part:
                return True

        # Blacklist dominio
        for bl_domain in DOMAIN_BLACKLIST:
            if domain == bl_domain or domain.endswith("." + bl_domain):
                return True

    return False


def extract_body(payload):
    """Estrae il body di testo dall'email (ricorsivo per MIME multipart)."""
    body_text = ""

    if "parts" in payload:
        for part in payload["parts"]:
            body_text += extract_body(part)
    else:
        mime_type = payload.get("mimeType", "")
        if mime_type == "text/plain":
            data = payload.get("body", {}).get("data", "")
            if data:
                try:
                    decoded = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                    body_text += decoded
                except Exception:
                    pass
        elif mime_type == "text/html" and not body_text:
            # Fallback: strip HTML if no text/plain available
            data = payload.get("body", {}).get("data", "")
            if data:
                try:
                    decoded = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                    # Rimuovi tag HTML in modo grezzo
                    clean = re.sub(r'<[^>]+>', ' ', decoded)
                    clean = re.sub(r'\s+', ' ', clean).strip()
                    body_text += clean
                except Exception:
                    pass

    return body_text


def get_headers_dict(headers_list):
    """Converte la lista di header Gmail in un dizionario."""
    return {h["name"].lower(): h["value"] for h in (headers_list or [])}


def load_checkpoint(checkpoint_file):
    """Load checkpoint for incremental export."""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            return json.load(f)
    return {"exported_ids": set(), "total_exported": 0, "last_run": None}


def save_checkpoint(checkpoint_file, checkpoint):
    """Salva checkpoint."""
    # Converti set in list per JSON
    data = {
        "exported_ids_count": len(checkpoint["exported_ids"]),
        "total_exported": checkpoint["total_exported"],
        "last_run": datetime.now().isoformat(),
        # Keep only last 10k IDs to prevent checkpoint file bloat
        "recent_ids": list(checkpoint["exported_ids"])[-10000:],
    }
    with open(checkpoint_file, "w") as f:
        json.dump(data, f, indent=2)


def show_stats(account_name, account_config):
    """Mostra statistiche dell'export."""
    output_file = account_config["output"]
    checkpoint_file = account_config["checkpoint"]

    print(f"\n=== Stats export Gmail: {account_name} ===")

    if os.path.exists(output_file):
        count = 0
        size = os.path.getsize(output_file)
        with open(output_file, "r") as f:
            for line in f:
                count += 1
        print(f"Emails exported: {count:,}")
        print(f"Dimensione file: {size / 1024 / 1024:.1f} MB")
    else:
        print("No export found.")

    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            cp = json.load(f)
        print(f"Ultimo run: {cp.get('last_run', 'N/A')}")
        print(f"IDs tracciati: {cp.get('exported_ids_count', 0):,}")


def export_emails(service, account_name, account_config, full=False, since=None):
    """Export incrementale delle email."""
    output_file = account_config["output"]
    checkpoint_file = account_config["checkpoint"]

    # Load checkpoint
    if full and os.path.exists(output_file):
        print("Full export: rimuovo file precedente...")
        os.remove(output_file)
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)

    checkpoint = load_checkpoint(checkpoint_file)
    if "recent_ids" in checkpoint:
        checkpoint["exported_ids"] = set(checkpoint.get("recent_ids", []))
    else:
        checkpoint["exported_ids"] = set()

    # Build query with date filter if requested
    query = GMAIL_QUERY
    if since:
        query = f"{GMAIL_QUERY} after:{since}"

    # Profilo
    profile = service.users().getProfile(userId="me").execute()
    email_addr = profile["emailAddress"]
    total_messages = profile.get("messagesTotal", 0)
    print(f"\nAccount: {email_addr}")
    print(f"Email totali nell'account: {total_messages:,}")
    print(f"Emails already exported: {checkpoint['total_exported']:,}")
    print(f"Query filter: {query}")

    # List messages with filter
    print("\nDownloading message list...")
    all_message_ids = []
    page_token = None
    page_count = 0

    while True:
        try:
            kwargs = {
                "userId": "me",
                "q": query,
                "maxResults": LIST_BATCH_SIZE,
            }
            if page_token:
                kwargs["pageToken"] = page_token

            results = service.users().messages().list(**kwargs).execute()
            messages = results.get("messages", [])
            all_message_ids.extend([m["id"] for m in messages])

            page_count += 1
            print(f"  Pagina {page_count}: {len(messages)} messaggi (totale: {len(all_message_ids):,})", end="\r")

            page_token = results.get("nextPageToken")
            if not page_token:
                break

            time.sleep(1 / REQUESTS_PER_SECOND)

        except Exception as e:
            print(f"\n  Errore lista messaggi: {e}")
            time.sleep(5)
            continue

    print(f"\nMessages after Gmail filter: {len(all_message_ids):,}")

    # Filter already-exported IDs
    new_ids = [mid for mid in all_message_ids if mid not in checkpoint["exported_ids"]]
    print(f"New to download: {len(new_ids):,}")

    if not new_ids:
        print("No new messages to export.")
        return

    # Download and process emails
    exported = 0
    skipped_blacklist = 0
    skipped_short = 0
    skipped_error = 0
    start_time = time.time()

    with open(output_file, "a", encoding="utf-8") as out_f:
        for i, msg_id in enumerate(new_ids):
            try:
                # Download full email
                msg = service.users().messages().get(
                    userId="me", id=msg_id, format="full"
                ).execute()

                headers = get_headers_dict(msg.get("payload", {}).get("headers", []))
                from_header = headers.get("from", "")
                subject = headers.get("subject", "(nessun oggetto)")
                date_header = headers.get("date", "")
                to_header = headers.get("to", "")
                cc_header = headers.get("cc", "")

                # Sender blacklist filter
                if is_sender_blacklisted(from_header):
                    skipped_blacklist += 1
                    checkpoint["exported_ids"].add(msg_id)
                    continue

                # Estrai body
                body = extract_body(msg.get("payload", {}))

                # Minimum length filter
                if len(body.strip()) < MIN_BODY_LENGTH:
                    skipped_short += 1
                    checkpoint["exported_ids"].add(msg_id)
                    continue

                # Tronca body lunghi (max 10k chars per email)
                if len(body) > 10000:
                    body = body[:10000] + "\n[...troncato...]"

                # Labels
                label_ids = msg.get("labelIds", [])

                # Check if sent by the user
                is_sent = "SENT" in label_ids

                # Record
                record = {
                    "id": msg_id,
                    "thread_id": msg.get("threadId", ""),
                    "date": date_header,
                    "from": from_header,
                    "to": to_header,
                    "cc": cc_header,
                    "subject": subject,
                    "body": body.strip(),
                    "labels": label_ids,
                    "is_sent": is_sent,
                    "snippet": msg.get("snippet", ""),
                    "account": account_name,
                }

                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                exported += 1
                checkpoint["exported_ids"].add(msg_id)
                checkpoint["total_exported"] += 1

                # Progress
                elapsed = time.time() - start_time
                rate = exported / elapsed if elapsed > 0 else 0
                eta = (len(new_ids) - i) / rate / 60 if rate > 0 else 0
                print(
                    f"  [{i+1:,}/{len(new_ids):,}] "
                    f"Esportate: {exported:,} | "
                    f"Skip blacklist: {skipped_blacklist:,} | "
                    f"Skip corte: {skipped_short:,} | "
                    f"Errori: {skipped_error:,} | "
                    f"{rate:.1f} email/s | "
                    f"ETA: {eta:.0f} min",
                    end="\r"
                )

                # Checkpoint every 500 emails
                if exported % 500 == 0:
                    out_f.flush()
                    save_checkpoint(checkpoint_file, checkpoint)

                # Rate limiting
                time.sleep(1 / REQUESTS_PER_SECOND)

            except Exception as e:
                skipped_error += 1
                if "rateLimitExceeded" in str(e) or "userRateLimitExceeded" in str(e):
                    print(f"\n  Rate limit hit, pausa 30s...")
                    time.sleep(30)
                elif "quotaExceeded" in str(e):
                    print(f"\n  Quota giornaliera esaurita. Riprendi domani.")
                    break
                else:
                    # Skip singola email con errore
                    checkpoint["exported_ids"].add(msg_id)
                    time.sleep(1)
                continue

    # Salva checkpoint finale
    save_checkpoint(checkpoint_file, checkpoint)

    elapsed = time.time() - start_time
    print(f"\n\n{'='*50}")
    print(f"Export completato in {elapsed/60:.1f} minuti")
    print(f"  Emails exported: {exported:,}")
    print(f"  Skip blacklist: {skipped_blacklist:,}")
    print(f"  Skip troppo corte: {skipped_short:,}")
    print(f"  Errori: {skipped_error:,}")
    print(f"  Totale nel file: {checkpoint['total_exported']:,}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export Gmail to TAILOR KB")
    parser.add_argument("--account", default="default",
                        help="Account to export (default: default)")
    parser.add_argument("--stats", action="store_true", help="Mostra solo statistiche")
    parser.add_argument("--full", action="store_true", help="Re-export completo (cancella precedente)")
    parser.add_argument("--since", type=str, help="Export solo email dopo questa data (formato: YYYY/MM/DD, es: 2026/03/24)")
    args = parser.parse_args()

    account_config = ACCOUNTS[args.account]

    if args.stats:
        show_stats(args.account, account_config)
    else:
        service = authenticate(account_config)
        export_emails(service, args.account, account_config, full=args.full, since=args.since)