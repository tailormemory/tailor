"""
TAILOR — Gmail Chunking
Reads triaged emails, filters, deduplicates by thread, strips quoted text,
and generates chunks ready for KB ingest.

Filters applied:
  1. Only emails with useful=true (from LLM triage)
  2. Only emails where the configured user is sender or sole TO recipient
  3. Dedup by thread: keep only the latest email per thread
  4. Strip quoted text (lines starting with >, "On ... wrote:", "Il ... ha scritto:")
  5. No body length filter

Chunking:
  - Target ~1200 chars per chunk
  - Overlap 200 chars between consecutive chunks
  - Embedding prefix: "Email | From: X | To: Y | Subject: Z"

Usage:
  python3 scripts/gmail/chunk_gmail.py                # Generate chunks
  python3 scripts/gmail/chunk_gmail.py --stats        # Stats only
  python3 scripts/gmail/chunk_gmail.py --sample 5     # Show 5 sample chunks

Input:  data/gmail_triage.jsonl
Output: data/chunks_gmail.jsonl
"""

import json
import os
import re
import sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")

sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
from config import get as cfg

INPUT_FILE = os.environ.get("EMAIL_TRIAGE_FILE", os.path.join(DATA_DIR, "gmail_triage.jsonl"))
OUTPUT_FILE = os.path.join(DATA_DIR, "chunks_gmail.jsonl")

# Chunking config
TARGET_CHUNK_CHARS = 1200
MAX_CHUNK_CHARS = 2000
OVERLAP_CHARS = 200


# ============================================================
# USER EMAIL ADDRESSES (from config)
# ============================================================

_USER_EMAILS = set()
try:
    _addrs = cfg("email", "addresses") or []
    _USER_EMAILS = set(e.lower() for e in _addrs if e)
except Exception:
    pass

if not _USER_EMAILS:
    print("WARNING: No email addresses configured in tailor.yaml under email.addresses")
    print("         Email filtering (sent/received detection) will not work correctly.")

def is_user_sender(email):
    """Is the configured user the sender?"""
    fr = (email.get("from") or "").lower()
    return any(addr in fr for addr in _USER_EMAILS)


def is_user_sole_to(email):
    """Is the configured user the sole TO recipient?"""
    to = (email.get("to") or "").lower()
    to_addrs = [a.strip() for a in to.split(",") if "@" in a]
    return len(to_addrs) == 1 and any(addr in to for addr in _USER_EMAILS)


def should_keep(email):
    """Apply filters: useful + (sender OR sole TO)."""
    if not email.get("useful"):
        return False
    return is_user_sender(email) or is_user_sole_to(email)


# ============================================================
# DEDUP PER THREAD
# ============================================================

def dedup_by_thread(emails):
    """Keep only the latest email per thread_id."""
    threads = {}
    for email in emails:
        tid = email.get("thread_id", email.get("id", ""))
        date = email.get("date") or email.get("internal_date") or ""
        if tid not in threads or date > threads[tid].get("date", ""):
            threads[tid] = email
    return list(threads.values())


# ============================================================
# STRIP QUOTED TEXT
# ============================================================

# Load custom signature patterns from config
_CUSTOM_SIGNATURE_PATTERNS = []
try:
    _patterns = cfg("email", "signature_patterns_extra") or []
    _CUSTOM_SIGNATURE_PATTERNS = [p for p in _patterns if p]
except Exception:
    pass


def strip_quoted(body):
    """Remove quoted text, email signatures, disclaimers, and forwarded content."""
    lines = body.split("\n")
    clean = []
    for line in lines:
        stripped = line.strip()

        # Quoted lines
        if stripped.startswith(">"):
            continue

        # Reply header (English)
        if re.match(r"^On .+ wrote:\s*$", stripped):
            continue

        # Reply header (Italian)
        if re.match(r"^Il .+ ha scritto:\s*$", stripped):
            continue

        # Forwarded/original message separators
        if re.match(r"^-{3,}\s*(Original Message|Forwarded message|Messaggio originale)", stripped, re.IGNORECASE):
            break
        if re.match(r"^_{3,}\s*$", stripped):
            break

        clean.append(line)

    text = "\n".join(clean).strip()

    # Remove signature block
    
    text = re.split(r"\n-- ?\n", text)[0].strip()

    # Built-in signature/disclaimer patterns (universal)
    
    
    signature_patterns = [
    
        # Legal disclaimers (Italian)
        r"(?:Questa e-?mail|La presente e-?mail|La presente comunicazione).*(?:destinat[aoi]|riservat[aoi]|intend)",
        r"(?:l'utilizzo e la distribuzione|severamente proibita)",
        r"(?:Se non siete il destinatario|Se ha ricevuto per errore|Se Lei non è il destinatario)",
        # Legal disclaimers (English)
        r"(?:This e-?mail|This message).*(?:confidential|intended solely|privileged)",
        r"(?:If you are not the intended recipient|If you have received this)",
        r"(?:strictly prohibited|unauthorized use|unauthorized disclosure)",
        r"(?:AVVISO DI RISERVATEZZA|CONFIDENTIALITY NOTICE|DISCLAIMER|AVVERTENZA)",
        # Generic signature images/logos
        r"\[https?://.*?(?:signature|logo|banner).*?\]",
        # "Sent from" blocks
        r"(?:Sent from|Inviato da|Envoyé de) (?:my |Mail for |)(?:iPhone|iPad|Android|Samsung|Outlook|Gmail)",
        # Typical corporate signature patterns (tel/fax with numbers)
        r"(?:^|\n)(?:Tel|Phone|Fax|Mobile|Cell|Office|Direct)[.:]?\s*[\+\d\(\)\-\s]{8,}",
    ]

    # Add custom patterns from config
    for custom_pat in _CUSTOM_SIGNATURE_PATTERNS:
        signature_patterns.append(re.escape(custom_pat))

    for pattern in signature_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            before = text[:match.start()].strip()
            # Cut if there's enough content before the match
            if len(before) > 50:
                text = before
                break  # Primo match taglia tutto — non continuare

    # Remove consecutive URL blocks (typical of image-based signatures)
    
    lines = text.split("\n")
    clean_lines = []
    url_streak = 0
    url_streak_start = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        is_url_line = bool(re.match(r"^[\s·•\-]*\[?https?://", stripped)) or (
            stripped.startswith("[") and "http" in stripped
        ) or (
            "·" in stripped and "http" in stripped
        ) or bool(re.match(r"^[\s·•\-]*\[.*\]\s*$", stripped))
        is_empty = not stripped

        if is_url_line:
            if url_streak == 0:
                url_streak_start = len(clean_lines)
            url_streak += 1
        elif is_empty and url_streak > 0:
            url_streak += 1
        else:
            if url_streak >= 2:
                # Rimuovi le righe URL accumulate
                clean_lines = clean_lines[:url_streak_start]
            url_streak = 0
            clean_lines.append(line)

    
    
    final_text = "\n".join(clean_lines).strip()

    # Late patterns: cut from first match onwards
    late_patterns = [
        # Disclaimers in any position
        r"(?:Questa e-?mail e tutti i suoi allegati|This e-?mail and any)",
        r"(?:L'utilizzo e la distribuzione|The use and distribution)",
        r"(?:Se non siete il|If you are not the intended)",

        # "Get Outlook for" blocks
        r"(?:Get Outlook for|Scarica Outlook per)",
    ]

    # Add custom patterns to late patterns too
    for custom_pat in _CUSTOM_SIGNATURE_PATTERNS:
        late_patterns.append(re.escape(custom_pat))

    for pattern in late_patterns:
        match = re.search(pattern, final_text, re.IGNORECASE)
        if match:
            before = final_text[:match.start()].strip()
            if len(before) > 50:
                final_text = before

    return final_text


# ============================================================
# CHUNKING (aligned with 2_chunk.py v2)
# ============================================================

def split_text_at_boundaries(text, max_chars):
    """Split long text at natural boundaries."""
    if len(text) <= max_chars:
        return [text]

    segments = []
    remaining = text

    while len(remaining) > max_chars:
        chunk = remaining[:max_chars]
        cut_at = -1

        for sep, offset in [("\n\n", 2), ("\n", 1), (". ", 2), (" ", 1)]:
            pos = chunk.rfind(sep)
            if pos > max_chars * 0.3:
                cut_at = pos + offset
                break

        if cut_at == -1:
            cut_at = max_chars

        segments.append(remaining[:cut_at].rstrip())
        remaining = remaining[cut_at:].lstrip()

    if remaining.strip():
        segments.append(remaining.strip())

    return segments


def chunk_email(email, stripped_body):
    """Generate chunks for a single email."""
    chunks = []

    if not stripped_body:
        return chunks

    
    email_id = email.get("id", "")
    thread_id = email.get("thread_id", email_id)
    from_addr = email.get("from", "")
    to_addr = email.get("to", "")
    subject = email.get("subject", "")
    date = email.get("date", "")
    snippet = email.get("snippet", "")

    
    from_short = from_addr.split("<")[0].strip().strip('"') if "<" in from_addr else from_addr
    to_short = to_addr.split("<")[0].strip().strip('"') if "<" in to_addr else to_addr

    
    parts = split_text_at_boundaries(stripped_body, TARGET_CHUNK_CHARS)
    prev_chunk_text = ""

    for idx, part in enumerate(parts):
        
        overlap_text = ""
        if prev_chunk_text and OVERLAP_CHARS > 0:
            overlap_text = prev_chunk_text[-OVERLAP_CHARS:].strip()
            space_pos = overlap_text.find(" ")
            if space_pos > 0:
                overlap_text = overlap_text[space_pos + 1:]

        if overlap_text:
            full_text = f"[...contesto precedente:] {overlap_text}\n\n{part}"
        else:
            full_text = part

        chunk_id = f"email_{email_id}_chunk_{idx:04d}"

        meta = {
            "conv_id": f"email_thread_{thread_id}",
            "title": subject or "(nessun oggetto)",
            "date": date,
            "create_time": 0,  # Will be computed if needed
            "chunk_index": idx,
            "char_count": len(full_text),
            "turn_count": 1,
            "source": "email",
            "email_from": from_addr[:200],
            "email_to": to_addr[:200],
            "email_id": email_id,
            "thread_id": thread_id,
        }

        chunks.append({
            "chunk_id": chunk_id,
            "text": full_text,
            "embed_prefix": f"Email | From: {from_short[:50]} | To: {to_short[:50]} | Subject: {subject[:100]}",
            "metadata": meta,
        })

        prev_chunk_text = part

    return chunks


# ============================================================
# MAIN
# ============================================================

def process():
    print(f"TAILOR Gmail Chunking")
    print(f"Input: {INPUT_FILE}")

    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: file not found: {INPUT_FILE}")
        sys.exit(1)

    # Step 1: Load and filter
    print(f"\nStep 1: Loading and filtering...")
    kept = []
    total = 0
    useful = 0

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            email = json.loads(line)
            total += 1
            if email.get("useful"):
                useful += 1
            if should_keep(email):
                kept.append(email)

    print(f"  Email totali: {total:,}")
    print(f"  Utili (triage): {useful:,}")
    print(f"  After filter (sender + sole TO): {len(kept):,}")

    # Step 2: Dedup per thread
    print(f"\nStep 2: Dedup per thread...")
    deduped = dedup_by_thread(kept)
    print(f"  Thread unici: {len(deduped):,} (da {len(kept):,} email)")

    # Step 3: Strip quoted + chunk
    print(f"\nStep 3: Strip quoted text + chunking...")
    all_chunks = []
    empty_after_strip = 0
    total_raw_chars = 0
    total_stripped_chars = 0

    for email in deduped:
        body = email.get("body", "")
        total_raw_chars += len(body)

        stripped = strip_quoted(body)
        total_stripped_chars += len(stripped)

        if not stripped.strip():
            empty_after_strip += 1
            continue

        chunks = chunk_email(email, stripped)
        all_chunks.extend(chunks)

    strip_pct = (1 - total_stripped_chars / max(1, total_raw_chars)) * 100
    print(f"  Chars raw: {total_raw_chars:,}")
    print(f"  Chars dopo strip: {total_stripped_chars:,} (-{strip_pct:.0f}%)")
    print(f"  Email vuote dopo strip: {empty_after_strip:,}")
    print(f"  Chunk generati: {len(all_chunks):,}")

    # Step 4: Salva
    print(f"\nStep 4: Salvataggio...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for chunk in all_chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + "\n")

    output_mb = os.path.getsize(OUTPUT_FILE) / 1024 / 1024
    print(f"  File: {OUTPUT_FILE}")
    print(f"  Dimensione: {output_mb:.1f} MB")

    # Stats finali
    chunk_sizes = [c["metadata"]["char_count"] for c in all_chunks]
    print(f"\n{'='*50}")
    print(f"Riepilogo:")
    print(f"  Email input: {total:,}")
    print(f"  Emails filtered: {len(kept):,}")
    print(f"  Thread unici: {len(deduped):,}")
    print(f"  Chunk output: {len(all_chunks):,}")
    print(f"  Chars totali: {sum(chunk_sizes):,}")
    print(f"  Dimensione media chunk: {sum(chunk_sizes) // max(1, len(chunk_sizes)):,} chars")

    return all_chunks


def show_stats():
    """Show stats without generating chunks."""
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: file not found: {INPUT_FILE}")
        return

    total = useful = kept_count = 0
    kept = []

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            email = json.loads(line)
            total += 1
            if email.get("useful"):
                useful += 1
            if should_keep(email):
                kept_count += 1
                kept.append(email)

    deduped = dedup_by_thread(kept)
    total_raw = sum(len(e.get("body", "")) for e in deduped)
    total_stripped = sum(len(strip_quoted(e.get("body", ""))) for e in deduped)

    print(f"\n=== Stats Gmail Chunking ===")
    print(f"Email totali: {total:,}")
    print(f"Utili (triage): {useful:,}")
    print(f"Filtrate (sender + sole TO): {kept_count:,}")
    print(f"Thread unici: {len(deduped):,}")
    print(f"Chars raw: {total_raw:,}")
    print(f"Chars dopo strip: {total_stripped:,} (-{(1 - total_stripped / max(1, total_raw)) * 100:.0f}%)")
    print(f"Chunk stimati: ~{total_stripped // 1200:,}")

    if os.path.exists(OUTPUT_FILE):
        count = sum(1 for _ in open(OUTPUT_FILE))
        size_mb = os.path.getsize(OUTPUT_FILE) / 1024 / 1024
        print(f"\nFile output esistente: {count:,} chunk ({size_mb:.1f} MB)")


def show_sample(n):
    """Show N sample chunks."""
    if not os.path.exists(OUTPUT_FILE):
        print("File output non trovato. Esegui prima senza --sample.")
        return

    print(f"\n=== Sample di {n} chunk ===\n")
    with open(OUTPUT_FILE, "r") as f:
        for i, line in enumerate(f):
            if i >= n:
                break
            chunk = json.loads(line)
            meta = chunk["metadata"]
            print(f"--- Chunk {i + 1}: {chunk['chunk_id']} ---")
            print(f"Subject: {meta['title']}")
            print(f"From: {meta['email_from'][:60]}")
            print(f"Date: {meta['date']}")
            print(f"Chars: {meta['char_count']}")
            print(f"Text preview: {chunk['text'][:200]}...")
            print()


if __name__ == "__main__":
    if "--stats" in sys.argv:
        show_stats()
    elif "--sample" in sys.argv:
        idx = sys.argv.index("--sample")
        n = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 5
        show_sample(n)
    else:
        process()