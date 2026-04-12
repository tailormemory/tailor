"""
TAILOR — ChatGPT conversation parser.
Reads conversations-*.json from ChatGPT data export.
Outputs normalized JSONL records compatible with the unified chunker.

Output format per record:
  {id, title, text, create_time, update_time, date, source, message_count, char_count}

The 'text' field contains the full conversation formatted as:
  [USER]: message
  [ASSISTANT]: message
"""

import json
import os
import glob
from datetime import datetime


MIN_CHARS = 100  # Skip conversations shorter than this


def _extract_messages(conv):
    """Extract messages from a ChatGPT conversation in chronological order."""
    mapping = conv.get("mapping", {})
    messages = []

    for node_id, node in mapping.items():
        msg = node.get("message")
        if not msg:
            continue

        role = msg.get("author", {}).get("role", "")
        if role not in ("user", "assistant"):
            continue

        content = msg.get("content", {})
        parts = content.get("parts", [])
        text_parts = [p for p in parts if isinstance(p, str)]
        text = "\n".join(text_parts).strip()

        if not text:
            continue

        create_time = msg.get("create_time", 0)
        metadata = msg.get("metadata", {})
        model = metadata.get("model_slug", "")

        messages.append({
            "role": role,
            "text": text,
            "timestamp": create_time or 0,
            "model": model
        })

    messages.sort(key=lambda m: m["timestamp"])
    return messages


def _messages_to_text(messages):
    """Convert structured messages to formatted text block."""
    lines = []
    for msg in messages:
        prefix = "USER" if msg["role"] == "user" else "ASSISTANT"
        lines.append(f"[{prefix}]: {msg['text']}")
    return "\n\n".join(lines)


def parse(source_config: dict, data_dir: str) -> list[dict]:
    """
    Parse ChatGPT export files into normalized records.

    Args:
        source_config: dict from YAML with keys like source_files, etc.
        data_dir: base data directory (e.g. /path/to/tailor/data)

    Returns:
        List of normalized records ready for chunking.
    """
    # Find source files
    patterns = source_config.get("source_files", ["conversations-*.json"])
    files = []
    for pattern in patterns:
        if os.path.isabs(pattern):
            files.extend(sorted(glob.glob(pattern)))
        else:
            files.extend(sorted(glob.glob(os.path.join(data_dir, pattern))))

    if not files:
        print(f"  [chatgpt] No files matching {patterns} in {data_dir}")
        return []

    print(f"  [chatgpt] Found {len(files)} file(s)")

    records = []
    skipped = 0

    for filepath in files:
        fname = os.path.basename(filepath)
        with open(filepath, "r", encoding="utf-8") as f:
            convs = json.load(f)

        file_count = 0
        for conv in convs:
            conv_id = conv.get("conversation_id", conv.get("id", "")) or f"chatgpt_{hash(conv.get('title','') + str(conv.get('create_time',0))) & 0xFFFFFFFF:08x}"
            title = conv.get("title", "(senza titolo)")
            create_time = conv.get("create_time", 0)
            update_time = conv.get("update_time", 0)

            messages = _extract_messages(conv)
            if not messages:
                skipped += 1
                continue

            text = _messages_to_text(messages)
            if len(text) < MIN_CHARS:
                skipped += 1
                continue

            # Date string from create_time
            date_str = ""
            if create_time:
                try:
                    date_str = datetime.fromtimestamp(create_time).strftime("%Y-%m-%d")
                except (OSError, ValueError):
                    date_str = ""

            records.append({
                "id": conv_id,
                "title": title,
                "create_time": create_time,
                "update_time": update_time,
                "date": date_str,
                "source": "chatgpt",
                "text": text,
                "message_count": len(messages),
                "char_count": len(text),
            })
            file_count += 1

        print(f"    {fname}: {file_count} conversations")

    print(f"  [chatgpt] Parsed: {len(records)}, skipped: {skipped}")
    return records
