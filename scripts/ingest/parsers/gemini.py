"""
TAILOR — Gemini conversation parser.
Reads Google Gemini exports from multiple possible formats:
  1. Google Takeout: conversations.json with array of conversations
  2. Google Takeout: per-conversation JSON files in a directory
  3. AI Chat Exporter / Gemini Exporter: per-conversation JSON files
  4. HTML export: conversations in structured HTML

The Google Takeout format is unstable and varies over time.
This parser is intentionally lenient — it tries multiple strategies
and skips what it can't parse instead of failing.

Output format per record:
  {id, title, text, create_time, update_time, date, source, message_count, char_count}
"""

import json
import os
import glob
import re
from datetime import datetime, timezone


MIN_CHARS = 100


def _parse_timestamp(ts_str):
    """Convert ISO/various timestamp formats to (unix_ts, date_str)."""
    if not ts_str:
        return 0, ""
    # Handle numeric timestamps
    if isinstance(ts_str, (int, float)):
        try:
            return float(ts_str), datetime.fromtimestamp(ts_str).strftime("%Y-%m-%d")
        except (OSError, ValueError):
            return 0, ""
    try:
        ts_str = str(ts_str).replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp(), dt.strftime("%Y-%m-%d")
    except Exception:
        return 0, ""


def _role_label(role: str) -> str:
    """Normalize role to USER/ASSISTANT label."""
    role = role.lower().strip()
    if role in ("user", "human"):
        return "USER"
    if role in ("model", "assistant", "gemini"):
        return "ASSISTANT"
    return ""


def _entries_to_text(entries: list) -> str:
    """Convert a list of message entries to formatted text."""
    lines = []
    for entry in entries:
        role = entry.get("role", "")
        text = entry.get("text", "")

        # Handle content array (some formats use parts/content)
        if not text:
            parts = entry.get("parts", entry.get("content", []))
            if isinstance(parts, list):
                text_parts = []
                for p in parts:
                    if isinstance(p, str):
                        text_parts.append(p)
                    elif isinstance(p, dict) and p.get("text"):
                        text_parts.append(p["text"])
                text = "\n".join(text_parts)
            elif isinstance(parts, str):
                text = parts

        text = text.strip()
        if not text:
            continue

        # Skip thinking/thought entries
        if entry.get("isThought"):
            continue

        label = _role_label(role)
        if not label:
            continue

        lines.append(f"[{label}]: {text}")

    return "\n\n".join(lines)


# ── Strategy 1: Single JSON with conversations array ─────────

def _parse_conversations_json(filepath: str) -> list[dict]:
    """Parse a single JSON file with a 'conversations' array."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Could be {"conversations": [...]} or just [...]
    if isinstance(data, dict):
        convs = data.get("conversations", [])
    elif isinstance(data, list):
        convs = data
    else:
        return []

    records = []
    for conv in convs:
        if not isinstance(conv, dict):
            continue

        conv_id = conv.get("id", conv.get("conversationId", conv.get("uuid", "")))
        title = conv.get("title", conv.get("name", "Untitled"))
        created = conv.get("create_time", conv.get("created_at", conv.get("timestamp", "")))
        updated = conv.get("update_time", conv.get("updated_at", created))

        create_ts, create_date = _parse_timestamp(created)
        update_ts, _ = _parse_timestamp(updated)

        # Try different message field names
        entries = (conv.get("entries", [])
                   or conv.get("messages", [])
                   or conv.get("chat_messages", []))

        # AI Studio format: chunkedPrompt.chunks
        if not entries:
            chunked = conv.get("chunkedPrompt", {})
            entries = chunked.get("chunks", [])

        text = _entries_to_text(entries)
        if len(text) < MIN_CHARS:
            continue

        if not conv_id:
            conv_id = f"gemini_{abs(hash(title + str(create_ts)))}"

        records.append({
            "id": conv_id,
            "title": title,
            "create_time": create_ts,
            "update_time": update_ts,
            "date": create_date,
            "source": "gemini",
            "text": text,
            "message_count": len(entries),
            "char_count": len(text),
        })

    return records


# ── Strategy 2: Per-conversation JSON files ───────────────────

def _parse_individual_jsons(directory: str) -> list[dict]:
    """Parse individual JSON files (one per conversation)."""
    files = glob.glob(os.path.join(directory, "*.json"))
    if not files:
        return []

    records = []
    for filepath in sorted(files):
        fname = os.path.basename(filepath)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        # Single conversation object
        if isinstance(data, dict):
            # Could be a conversations.json wrapper
            if "conversations" in data:
                records.extend(_parse_conversations_json(filepath))
                continue

            conv_id = data.get("id", data.get("conversationId", os.path.splitext(fname)[0]))
            title = data.get("title", data.get("name", fname))
            created = data.get("create_time", data.get("created_at", data.get("timestamp", "")))
            updated = data.get("update_time", data.get("updated_at", created))

            create_ts, create_date = _parse_timestamp(created)
            update_ts, _ = _parse_timestamp(updated)

            entries = (data.get("entries", [])
                       or data.get("messages", [])
                       or data.get("chat_messages", []))

            # AI Studio / chunkedPrompt format
            if not entries:
                chunked = data.get("chunkedPrompt", {})
                entries = chunked.get("chunks", [])

            text = _entries_to_text(entries)
            if len(text) < MIN_CHARS:
                continue

            records.append({
                "id": conv_id,
                "title": title,
                "create_time": create_ts,
                "update_time": update_ts,
                "date": create_date,
                "source": "gemini",
                "text": text,
                "message_count": len(entries),
                "char_count": len(text),
            })

        # Array of conversations
        elif isinstance(data, list):
            records.extend(_parse_conversations_json(filepath))

    return records


# ── Strategy 3: HTML export ───────────────────────────────────

def _parse_html_files(directory: str) -> list[dict]:
    """Parse HTML export files with conversation content."""
    files = glob.glob(os.path.join(directory, "*.html"))
    if not files:
        return []

    records = []
    for filepath in sorted(files):
        fname = os.path.basename(filepath)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                html = f.read()
        except UnicodeDecodeError:
            continue

        # Skip tiny placeholder files (like the 11-byte ones from empty exports)
        if len(html) < MIN_CHARS:
            continue

        # Strip HTML tags for basic text extraction
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL)
        text = re.sub(r'<[^>]+>', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()

        if len(text) < MIN_CHARS:
            continue

        conv_id = f"gemini_html_{os.path.splitext(fname)[0]}"

        records.append({
            "id": conv_id,
            "title": fname.replace(".html", ""),
            "create_time": 0,
            "update_time": 0,
            "date": "",
            "source": "gemini",
            "text": text,
            "message_count": 0,
            "char_count": len(text),
        })

    return records


# ── Main parse function ───────────────────────────────────────

def parse(source_config: dict, data_dir: str) -> list[dict]:
    """
    Parse Gemini export files into normalized records.
    Tries multiple strategies to handle Google's unstable export format.

    Args:
        source_config: dict from YAML (source_dir, source_files, etc.)
        data_dir: base data directory

    Returns:
        List of normalized records ready for chunking.
    """
    source_dir = source_config.get("source_dir", "gemini_export")
    if not os.path.isabs(source_dir):
        source_dir = os.path.join(data_dir, source_dir)

    if not os.path.exists(source_dir):
        print(f"  [gemini] Export directory not found: {source_dir}")
        return []

    print(f"  [gemini] Parsing from {source_dir}")

    records = []

    # Strategy 1: Look for conversations.json
    conv_json = os.path.join(source_dir, "conversations.json")
    if os.path.exists(conv_json):
        print(f"    Found conversations.json")
        found = _parse_conversations_json(conv_json)
        print(f"    Parsed: {len(found)} conversations")
        records.extend(found)

    # Strategy 2: Individual JSON files (skip conversations.json if already parsed)
    individual = _parse_individual_jsons(source_dir)
    # Deduplicate by id
    existing_ids = {r["id"] for r in records}
    new = [r for r in individual if r["id"] not in existing_ids]
    if new:
        print(f"    Individual JSONs: {len(new)} additional conversations")
        records.extend(new)

    # Strategy 3: Check subdirectories (Takeout nests under Gemini Apps/)
    for subdir_name in ["Gemini Apps", "Gemini", "Bard"]:
        subdir = os.path.join(source_dir, subdir_name)
        if os.path.isdir(subdir):
            print(f"    Found subdirectory: {subdir_name}/")
            sub_records = _parse_individual_jsons(subdir)
            existing_ids = {r["id"] for r in records}
            new = [r for r in sub_records if r["id"] not in existing_ids]
            if new:
                print(f"    {subdir_name}/: {len(new)} conversations")
                records.extend(new)

            # Also try HTML in subdirectories
            html_records = _parse_html_files(subdir)
            existing_ids = {r["id"] for r in records}
            new = [r for r in html_records if r["id"] not in existing_ids]
            if new:
                print(f"    {subdir_name}/ HTML: {len(new)} conversations")
                records.extend(new)

    # Strategy 4: HTML files in root dir
    html_records = _parse_html_files(source_dir)
    existing_ids = {r["id"] for r in records}
    new = [r for r in html_records if r["id"] not in existing_ids]
    if new:
        print(f"    HTML files: {len(new)} conversations")
        records.extend(new)

    if not records:
        print(f"  [gemini] No conversations found. Make sure your Gemini export contains conversation data.")
        print(f"           Google Takeout sometimes exports empty/config-only files.")
        print(f"           Consider using 'Gemini Conversation Saver' Chrome extension for full exports.")
    else:
        print(f"  [gemini] Total: {len(records)} records")

    return records
