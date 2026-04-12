"""
Gmail Triage v5 — via OpenAI Batch API (mini-batch sequenziali)
Classifies emails as "useful" or "noise" using the Batch API.
Mini-batches of 5k emails, submitted one at a time to stay under the
2M enqueued token limit.

Main workflow:
  python3 scripts/7_triage_gmail.py run              # Does everything: prepare + submit + retrieve + merge
  python3 scripts/7_triage_gmail.py run --resume     # Resumes from where it left off

Individual commands (for debug):
  python3 scripts/7_triage_gmail.py prepare           # Generate mini-batch files
  python3 scripts/7_triage_gmail.py submit-next       # Submit next pending batch
  python3 scripts/7_triage_gmail.py retrieve          # Check running batch status
  python3 scripts/7_triage_gmail.py merge             # Final results merge
  python3 scripts/7_triage_gmail.py --stats           # Statistics
"""

import os
import sys
import json
import time
import glob
import requests
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

INPUT_FILE = os.environ.get("EMAIL_EXPORT_FILE", os.path.join(DATA_DIR, "gmail_export.jsonl"))
BATCH_STATE_FILE = os.path.join(DATA_DIR, "gmail_triage_batch_state.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "gmail_triage.jsonl")

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE = "https://api.openai.com/v1"

BATCH_MAX = 5000  # ~1.45M token, sotto il limite di 2M enqueued token
POLL_INTERVAL = 30  # seconds between each check

TRIAGE_SYSTEM = "Email classifier. Respond ONLY with valid JSON."

TRIAGE_PROMPT_TEMPLATE = """Classifica questa email come UTILE o RUMORE per un imprenditore.

UTILE = comunicazioni personali o di business, decisioni, trattative, contratti, questioni legali/fiscali, discussioni su progetti, feedback da clienti/partner, email inviate dall'utente, corrispondenza con persone reali.

RUMORE = report automatici (analytics, monitoring, stats), conferme d'ordine/spedizione, ricevute, notifiche di servizio, alert automatici, newsletter, email di sistema, conferme prenotazioni, reminder automatici, email di benvenuto, conferme pagamento.

Rispondi SOLO con: {{"useful": true}} o {{"useful": false}}

From: {sender}
Subject: {subject}
Snippet: {snippet}"""


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    log_file = os.path.join(LOG_DIR, "gmail_triage.log")
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(log_file, "a") as f:
        f.write(line + "\n")


def auth_headers():
    return {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}


def load_emails():
    if not os.path.exists(INPUT_FILE):
        print(f"No email export file found: {INPUT_FILE}")
        print("Run the email export first (export_gmail.py or export_imap.py)")
        sys.exit(0)
    emails = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                emails.append(json.loads(line))
    return emails


def make_batch_line(email, idx):
    prompt = TRIAGE_PROMPT_TEMPLATE.format(
        sender=email.get("from", "")[:100],
        subject=email.get("subject", "")[:200],
        snippet=email.get("snippet", "")[:300],
    )
    return {
        "custom_id": email.get("id", f"email_{idx}"),
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": TRIAGE_SYSTEM},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.0,
            "max_tokens": 20,
            "response_format": {"type": "json_object"},
        },
    }


def load_state():
    if os.path.exists(BATCH_STATE_FILE):
        with open(BATCH_STATE_FILE, "r") as f:
            return json.load(f)
    return None


def save_state(state):
    with open(BATCH_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ============================================================
# PREPARE — generate mini-batches of 5k emails
# ============================================================

def prepare():
    emails = load_emails()
    total = len(emails)
    log(f"Email nel file export: {total:,}")

    # Remove previous batch files
    for f in glob.glob(os.path.join(DATA_DIR, "gmail_triage_batch_input_*.jsonl")):
        os.remove(f)
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    if os.path.exists(BATCH_STATE_FILE):
        os.remove(BATCH_STATE_FILE)

    # Split into mini-batches
    num_batches = (total + BATCH_MAX - 1) // BATCH_MAX
    log(f"Mini-batch da generare: {num_batches} (max {BATCH_MAX:,} email per batch)")

    state = {
        "created_at": datetime.now().isoformat(),
        "total_emails": total,
        "batch_size": BATCH_MAX,
        "batches": [],
    }

    for batch_idx in range(num_batches):
        start = batch_idx * BATCH_MAX
        end = min(start + BATCH_MAX, total)
        batch_emails = emails[start:end]

        batch_file = os.path.join(DATA_DIR, f"gmail_triage_batch_input_{batch_idx + 1:03d}.jsonl")
        with open(batch_file, "w", encoding="utf-8") as f:
            for i, email in enumerate(batch_emails):
                f.write(json.dumps(make_batch_line(email, start + i), ensure_ascii=False) + "\n")

        size_mb = os.path.getsize(batch_file) / 1024 / 1024
        log(f"  Batch {batch_idx + 1}/{num_batches}: {len(batch_emails):,} email, {size_mb:.1f} MB")

        state["batches"].append({
            "batch_num": batch_idx + 1,
            "input_file": batch_file,
            "email_count": len(batch_emails),
            "status": "prepared",
            "batch_id": None,
            "file_id": None,
            "output_file_id": None,
        })

    save_state(state)
    log(f"\n{num_batches} mini-batch preparati. Prossimo step: run o submit-next")
    return state


# ============================================================
# SUBMIT-NEXT — submit next pending batch
# ============================================================

def submit_next(state):
    """Submit next batch with status 'prepared'. Returns True if one was submitted."""
    for batch_info in state["batches"]:
        if batch_info["status"] != "prepared":
            continue

        batch_file = batch_info["input_file"]
        batch_num = batch_info["batch_num"]
        total_batches = len(state["batches"])

        log(f"[{batch_num}/{total_batches}] Upload {os.path.basename(batch_file)}...")
        with open(batch_file, "rb") as f:
            resp = requests.post(
                f"{OPENAI_BASE}/files",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                files={"file": (os.path.basename(batch_file), f, "application/jsonl")},
                data={"purpose": "batch"},
                timeout=300,
            )

        if resp.status_code != 200:
            log(f"  ERRORE upload: {resp.status_code} {resp.text[:300]}")
            batch_info["status"] = "upload_failed"
            batch_info["error"] = resp.text[:300]
            save_state(state)
            return False

        file_id = resp.json()["id"]
        batch_info["file_id"] = file_id
        log(f"  File caricato: {file_id}")

        log(f"  Avvio batch job...")
        resp = requests.post(
            f"{OPENAI_BASE}/batches",
            headers=auth_headers(),
            json={
                "input_file_id": file_id,
                "endpoint": "/v1/chat/completions",
                "completion_window": "24h",
                "metadata": {"description": f"TAILOR Gmail triage batch {batch_num}/{total_batches}"},
            },
            timeout=60,
        )

        if resp.status_code != 200:
            log(f"  ERRORE batch submit: {resp.status_code} {resp.text[:300]}")
            batch_info["status"] = "submit_failed"
            batch_info["error"] = resp.text[:300]
            save_state(state)
            return False

        batch_data = resp.json()
        batch_info["batch_id"] = batch_data["id"]
        batch_info["status"] = batch_data["status"]
        batch_info["submitted_at"] = datetime.now().isoformat()
        save_state(state)

        log(f"  Batch inviato: {batch_data['id']} — Status: {batch_data['status']}")
        return True

    return False  # No batch to send


# ============================================================
# WAIT — wait for running batch to complete
# ============================================================

def wait_for_current(state):
    """Wait for currently running batch to complete. Returns True when done."""
    for batch_info in state["batches"]:
        if batch_info["status"] in ("prepared", "completed", "failed", "expired",
                                     "cancelled", "upload_failed", "submit_failed"):
            continue

        # This batch is running
        batch_id = batch_info["batch_id"]
        batch_num = batch_info["batch_num"]
        total_batches = len(state["batches"])

        while True:
            try:
                resp = requests.get(
                    f"{OPENAI_BASE}/batches/{batch_id}",
                    headers=auth_headers(),
                    timeout=30,
                )
            except Exception as e:
                log(f"  Errore connessione: {e}. Retry in {POLL_INTERVAL}s...")
                time.sleep(POLL_INTERVAL)
                continue

            if resp.status_code != 200:
                log(f"  Errore check: {resp.status_code}. Retry in {POLL_INTERVAL}s...")
                time.sleep(POLL_INTERVAL)
                continue

            batch_data = resp.json()
            status = batch_data["status"]
            req_counts = batch_data.get("request_counts", {})
            completed = req_counts.get("completed", 0)
            failed = req_counts.get("failed", 0)
            total = req_counts.get("total", 0)

            batch_info["status"] = status

            if status == "completed":
                batch_info["output_file_id"] = batch_data.get("output_file_id")
                batch_info["completed_at"] = datetime.now().isoformat()
                batch_info["request_counts"] = req_counts
                save_state(state)
                log(f"  [{batch_num}/{total_batches}] COMPLETATO — {completed:,}/{total:,} OK, {failed:,} fallite")
                return True

            elif status in ("failed", "expired", "cancelled"):
                errors = batch_data.get("errors", {}).get("data", [])
                error_msgs = [err.get("message", "N/A") for err in errors[:3]]
                batch_info["error"] = "; ".join(error_msgs)
                save_state(state)
                log(f"  [{batch_num}/{total_batches}] FALLITO: {batch_info['error']}")
                return False

            else:
                # Still in progress
                progress = f"{completed:,}/{total:,}" if total > 0 else "..."
                log(f"  [{batch_num}/{total_batches}] {status} — {progress} — polling in {POLL_INTERVAL}s")
                save_state(state)
                time.sleep(POLL_INTERVAL)

    return True  # No batch running


# ============================================================
# DOWNLOAD — download results from all completed batches
# ============================================================

def download_results(state):
    """Download results from all completed batches. Returns dict {email_id: result}."""
    all_results = {}

    for batch_info in state["batches"]:
        if batch_info["status"] != "completed":
            continue
        output_file_id = batch_info.get("output_file_id")
        if not output_file_id:
            continue
        if batch_info.get("downloaded"):
            # Already downloaded in a previous run — reload from local file
            local_file = batch_info.get("output_local_file")
            if local_file and os.path.exists(local_file):
                with open(local_file, "r") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        record = json.loads(line)
                        result = _parse_batch_result(record)
                        all_results[record.get("custom_id", "")] = result
                continue

        batch_num = batch_info["batch_num"]
        log(f"  Scaricamento batch {batch_num}...")

        try:
            resp = requests.get(
                f"{OPENAI_BASE}/files/{output_file_id}/content",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                timeout=300,
            )
        except Exception as e:
            log(f"    ERRORE download batch {batch_num}: {e}")
            continue

        if resp.status_code != 200:
            log(f"    ERRORE download batch {batch_num}: HTTP {resp.status_code}")
            continue

        # Salva risultato locale
        local_file = os.path.join(DATA_DIR, f"gmail_triage_batch_output_{batch_num:03d}.jsonl")
        with open(local_file, "w", encoding="utf-8") as f:
            f.write(resp.text)

        batch_info["downloaded"] = True
        batch_info["output_local_file"] = local_file
        save_state(state)

        for line in resp.text.strip().split("\n"):
            if not line.strip():
                continue
            record = json.loads(line)
            result = _parse_batch_result(record)
            all_results[record.get("custom_id", "")] = result

        log(f"    Batch {batch_num}: {len(resp.text.strip().split(chr(10))):,} risultati")

    return all_results


def _parse_batch_result(record):
    """Parse a single batch output record."""
    if record.get("error"):
        return {"useful": None, "error": str(record["error"])}

    try:
        response_body = record.get("response", {}).get("body", {})
        choices = response_body.get("choices", [])
        if choices:
            content = choices[0]["message"]["content"].strip()
            parsed = json.loads(content)
            return {"useful": parsed.get("useful", False)}
        else:
            return {"useful": None, "error": "no choices"}
    except Exception as e:
        return {"useful": None, "error": str(e)}


# ============================================================
# MERGE — merge risultati nel file finale
# ============================================================

def merge(state, all_results):
    """Merge results with original emails and write output file."""
    emails = load_emails()
    useful_count = 0
    noise_count = 0
    error_count = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for email in emails:
            email_id = email.get("id", "")
            result = all_results.get(email_id, {})

            email["useful"] = result.get("useful")
            if result.get("error"):
                email["triage_error"] = result["error"]
                error_count += 1
            elif email["useful"] is True:
                useful_count += 1
            elif email["useful"] is False:
                noise_count += 1
            else:
                error_count += 1

            f.write(json.dumps(email, ensure_ascii=False) + "\n")

    classified = useful_count + noise_count
    log(f"Merge completato:")
    log(f"  Utili: {useful_count:,} ({useful_count / max(1, classified) * 100:.1f}%)")
    log(f"  Rumore: {noise_count:,}")
    log(f"  Errori: {error_count:,}")
    log(f"  Totale: {len(emails):,}")
    log(f"  File: {OUTPUT_FILE}")

    state["completed_at"] = datetime.now().isoformat()
    state["results"] = {
        "useful": useful_count,
        "noise": noise_count,
        "errors": error_count,
        "total": len(emails),
    }
    save_state(state)


# ============================================================
# RUN — workflow completo automatico
# ============================================================

def run(resume=False):
    state = load_state()

    if resume and state and state.get("batches"):
        log(f"Ripresa da stato esistente: {len(state['batches'])} batch, "
            f"creato {state.get('created_at', 'N/A')}")
    else:
        if state and state.get("completed_at"):
            log("WARNING: triage already completed. Use 'prepare' to start over.")
            return
        log("Avvio triage completo...")
        state = prepare()

    total_batches = len(state["batches"])
    completed_count = sum(1 for b in state["batches"] if b["status"] == "completed")
    failed_count = sum(1 for b in state["batches"]
                       if b["status"] in ("failed", "expired", "cancelled", "upload_failed", "submit_failed"))

    log(f"\nStato: {completed_count} completati, {failed_count} falliti, "
        f"{total_batches - completed_count - failed_count} da fare\n")

    # Loop: submit a batch, wait, repeat
    while True:
        # Conta quanti mancano
        remaining = sum(1 for b in state["batches"] if b["status"] == "prepared")
        in_progress = sum(1 for b in state["batches"]
                         if b["status"] not in ("prepared", "completed", "failed", "expired",
                                                 "cancelled", "upload_failed", "submit_failed"))

        if remaining == 0 and in_progress == 0:
            break

        # If a batch is running, wait for it to complete
        if in_progress > 0:
            wait_for_current(state)
            continue

        # Prima di inviare: verifica che la coda OpenAI sia libera
        queue_wait = 0
        while queue_wait < 30:
            try:
                resp = requests.get(
                    f"{OPENAI_BASE}/batches?limit=20",
                    headers=auth_headers(),
                    timeout=30,
                )
                if resp.status_code == 200:
                    active = [b for b in resp.json().get("data", [])
                              if b["status"] in ("validating", "in_progress", "finalizing")]
                    if not active:
                        break
                    log(f"  Coda OpenAI: {len(active)} batch attivi. Aspetto 60s...")
                    time.sleep(60)
                    queue_wait += 1
                else:
                    time.sleep(30)
                    queue_wait += 1
            except Exception:
                time.sleep(30)
                queue_wait += 1

        if queue_wait >= 30:
            log("  OpenAI queue did not clear after 30 minutes. Exiting.")
            break

        # Invia il prossimo
        submitted = submit_next(state)
        if not submitted:
            # Check if failure is due to token limit — if so, wait and retry
            last_failed = None
            for b in state["batches"]:
                if b["status"] in ("failed", "submit_failed") and b.get("error") and "token limit" in b.get("error", "").lower():
                    last_failed = b
            
            if last_failed:
                log(f"  Token limit reached. Resetting batch #{last_failed['batch_num']} and waiting 120s...")
                last_failed["status"] = "prepared"
                last_failed["batch_id"] = None
                last_failed["file_id"] = None
                last_failed["error"] = None
                save_state(state)
                time.sleep(120)
                continue
            else:
                break

        # Wait for completion
        wait_for_current(state)

    # Batch summary
    completed_count = sum(1 for b in state["batches"] if b["status"] == "completed")
    failed_count = sum(1 for b in state["batches"]
                       if b["status"] in ("failed", "expired", "cancelled", "upload_failed", "submit_failed"))

    log(f"\n{'='*60}")
    log(f"Tutti i batch processati: {completed_count} completati, {failed_count} falliti")

    if completed_count == 0:
        log("No batch completed. Nothing to download.")
        return

    # Download risultati
    log(f"\nDownload risultati...")
    all_results = download_results(state)
    log(f"Risultati scaricati: {len(all_results):,}")

    # Merge
    log(f"\nMerge finale...")
    merge(state, all_results)

    log(f"\nTriage completato!")


# ============================================================
# STATS
# ============================================================

def show_stats():
    print(f"\n=== Stats triage Gmail ===")

    if os.path.exists(BATCH_STATE_FILE):
        with open(BATCH_STATE_FILE, "r") as f:
            state = json.load(f)

        batches = state.get("batches", [])
        total_b = len(batches)
        completed_b = sum(1 for b in batches if b["status"] == "completed")
        failed_b = sum(1 for b in batches
                       if b["status"] in ("failed", "expired", "cancelled", "upload_failed", "submit_failed"))
        prepared_b = sum(1 for b in batches if b["status"] == "prepared")
        in_progress_b = total_b - completed_b - failed_b - prepared_b

        print(f"Batch: {total_b} total")
        print(f"  Completati: {completed_b}")
        print(f"  In corso: {in_progress_b}")
        print(f"  Preparati: {prepared_b}")
        print(f"  Falliti: {failed_b}")
        print(f"Creato: {state.get('created_at', 'N/A')}")
        if state.get("completed_at"):
            print(f"Completato: {state['completed_at']}")

        # Show last 5 batches for detail
        if batches:
            active = [b for b in batches if b["status"] not in ("prepared", "completed")]
            if active:
                print(f"\nActive batches:")
                for b in active[:5]:
                    print(f"  #{b['batch_num']}: {b['status']} — {b.get('batch_id', 'N/A')[:20]}...")

        if state.get("results"):
            r = state["results"]
            classified = r["useful"] + r["noise"]
            print(f"\nRisultati finali:")
            print(f"  Utili: {r['useful']:,} ({r['useful'] / max(1, classified) * 100:.1f}%)")
            print(f"  Rumore: {r['noise']:,}")
            print(f"  Errori: {r['errors']:,}")
            print(f"  Totale: {r['total']:,}")
    else:
        print("No triage in progress.")

    if os.path.exists(OUTPUT_FILE) and not (os.path.exists(BATCH_STATE_FILE) and load_state().get("results")):
        useful = noise = errors = 0
        with open(OUTPUT_FILE, "r") as f:
            for line in f:
                d = json.loads(line)
                if d.get("useful") is True:
                    useful += 1
                elif d.get("useful") is False:
                    noise += 1
                else:
                    errors += 1
        classified = useful + noise
        print(f"\nRisultati (da file):")
        print(f"  Utili: {useful:,} ({useful / max(1, classified) * 100:.1f}%)")
        print(f"  Rumore: {noise:,}")
        print(f"  Errori: {errors:,}")

    if os.path.exists(INPUT_FILE):
        input_count = sum(1 for _ in open(INPUT_FILE))
        print(f"\nEmail export: {input_count:,}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python3 scripts/7_triage_gmail.py run              # Workflow completo")
        print("  python3 scripts/7_triage_gmail.py run --resume     # Riprende da checkpoint")
        print("  python3 scripts/7_triage_gmail.py prepare          # Only generate mini-batches")
        print("  python3 scripts/7_triage_gmail.py submit-next      # Submit next batch")
        print("  python3 scripts/7_triage_gmail.py retrieve         # Check running batch")
        print("  python3 scripts/7_triage_gmail.py merge            # Final results merge")
        print("  python3 scripts/7_triage_gmail.py --stats          # Statistics")
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd == "--stats":
        show_stats()
        sys.exit(0)

    if not OPENAI_API_KEY:
        print("ERROR: OPENAI_API_KEY not set.")
        sys.exit(1)

    if cmd == "run":
        resume = "--resume" in sys.argv
        run(resume=resume)
    elif cmd == "prepare":
        prepare()
    elif cmd == "submit-next":
        state = load_state()
        if not state:
            print("ERROR: no state found. Run 'prepare' first.")
            sys.exit(1)
        submit_next(state)
    elif cmd == "retrieve":
        state = load_state()
        if not state:
            print("ERROR: no state found. Run 'run' first.")
            sys.exit(1)
        wait_for_current(state)
    elif cmd == "merge":
        state = load_state()
        if not state:
            print("ERROR: no state found.")
            sys.exit(1)
        all_results = download_results(state)
        merge(state, all_results)
    else:
        print(f"Comando sconosciuto: {cmd}")
        sys.exit(1)