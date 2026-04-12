"""
TAILOR — Claude conversation parser.
Reads Claude data export (conversations.json, projects.json, memories.json).
Outputs normalized JSONL records compatible with the unified chunker.

Output format per record:
  {id, title, text, create_time, update_time, date, source, message_count, char_count}
"""

import json
import os
from datetime import datetime, timezone


MIN_CHARS = 100


def _parse_timestamp(ts_str):
    """Convert ISO timestamp to (unix_ts, date_str)."""
    if not ts_str:
        return 0, ""
    try:
        ts_str = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp(), dt.strftime("%Y-%m-%d")
    except Exception:
        return 0, ""


def _extract_message_text(msg):
    """Extract text from a Claude message (handles content array structure)."""
    text = msg.get("text", "")
    if text:
        return text.strip()

    content = msg.get("content", [])
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                t = block.get("text", "").strip()
                if t:
                    parts.append(t)
        return "\n".join(parts).strip()

    return ""


def _messages_to_text(chat_messages):
    """Convert Claude message list to formatted text block."""
    lines = []
    for msg in chat_messages:
        sender = msg.get("sender", "")
        text = _extract_message_text(msg)
        if not text:
            continue
        if sender == "human":
            lines.append(f"[USER]: {text}")
        elif sender == "assistant":
            lines.append(f"[ASSISTANT]: {text}")
    return "\n\n".join(lines)


def _parse_conversations(export_dir):
    """Parse conversations.json — normal conversations outside projects."""
    filepath = os.path.join(export_dir, "conversations.json")
    if not os.path.exists(filepath):
        print(f"    conversations.json not found")
        return []

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"    conversations.json: {len(data)} conversations")

    records = []
    skipped = 0

    for conv in data:
        uuid = conv.get("uuid", "")
        name = conv.get("name", "Senza titolo")
        summary = conv.get("summary", "")
        created_at = conv.get("created_at", "")
        updated_at = conv.get("updated_at", created_at)
        chat_messages = conv.get("chat_messages", [])

        create_ts, create_date = _parse_timestamp(created_at)
        update_ts, _ = _parse_timestamp(updated_at)

        text = _messages_to_text(chat_messages)

        if summary and len(summary) > 50:
            text = f"[SUMMARY]: {summary}\n\n{text}" if text else f"[SUMMARY]: {summary}"

        if len(text) < MIN_CHARS:
            skipped += 1
            continue

        records.append({
            "id": uuid,
            "title": name,
            "create_time": create_ts,
            "update_time": update_ts,
            "date": create_date,
            "source": "claude",
            "text": text,
            "message_count": len(chat_messages),
            "char_count": len(text),
        })

    print(f"    Parsed: {len(records)}, skipped: {skipped}")
    return records


def _parse_projects(export_dir):
    """Parse projects.json — project docs and prompt templates."""
    filepath = os.path.join(export_dir, "projects.json")
    if not os.path.exists(filepath):
        print(f"    projects.json not found")
        return []

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"    projects.json: {len(data)} projects")

    records = []
    skipped = 0

    for project in data:
        proj_uuid = project.get("uuid", "")
        proj_name = project.get("name", "Unnamed project")
        proj_desc = project.get("description", "")
        prompt_template = project.get("prompt_template", "")
        created_at = project.get("created_at", "")
        docs = project.get("docs", [])

        create_ts, create_date = _parse_timestamp(created_at)

        # Project prompt template
        if prompt_template and len(prompt_template) >= MIN_CHARS:
            text = f"[PROGETTO: {proj_name}]\n"
            if proj_desc:
                text += f"[DESCRIZIONE]: {proj_desc}\n\n"
            text += f"[SYSTEM PROMPT]:\n{prompt_template}"

            records.append({
                "id": f"{proj_uuid}_prompt",
                "title": f"System Prompt - {proj_name}",
                "create_time": create_ts,
                "update_time": create_ts,
                "date": create_date,
                "source": "claude",
                "text": text,
                "message_count": 1,
                "char_count": len(text),
            })

        # Project docs
        for doc in docs:
            doc_uuid = doc.get("uuid", "")
            doc_filename = doc.get("file_name", doc.get("filename", "document"))
            doc_created = doc.get("created_at", created_at)
            doc_content = doc.get("extracted_content", doc.get("content", ""))

            if not doc_content or len(doc_content) < MIN_CHARS:
                skipped += 1
                continue

            doc_ts, doc_date = _parse_timestamp(doc_created)
            text = f"[PROGETTO: {proj_name}]\n[FILE: {doc_filename}]\n\n{doc_content}"

            records.append({
                "id": f"{proj_uuid}_doc_{doc_uuid}",
                "title": f"{proj_name} - {doc_filename}",
                "create_time": doc_ts,
                "update_time": doc_ts,
                "date": doc_date,
                "source": "claude",
                "text": text,
                "message_count": 1,
                "char_count": len(text),
            })

    print(f"    Parsed: {len(records)}, skipped: {skipped}")
    return records


def _parse_memories(export_dir):
    """Parse memories.json — Claude memories."""
    filepath = os.path.join(export_dir, "memories.json")
    if not os.path.exists(filepath):
        print(f"    memories.json not found")
        return []

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        memories = data
    elif isinstance(data, dict):
        memories = data.get("memories", data.get("items", [data]))
    else:
        memories = []

    memory_lines = []
    for mem in memories:
        if isinstance(mem, dict):
            conv_memory = mem.get("conversations_memory", "")
            if conv_memory:
                memory_lines.append(conv_memory.strip())
                continue
            content = mem.get("content", mem.get("text", mem.get("value", "")))
            created = mem.get("created_at", mem.get("timestamp", ""))
            _, mem_date = _parse_timestamp(created)
            if content:
                prefix = f"[{mem_date}] " if mem_date else ""
                memory_lines.append(f"{prefix}{content}")
        elif isinstance(mem, str) and mem.strip():
            memory_lines.append(mem.strip())

    if not memory_lines:
        print(f"    No memories found")
        return []

    now = datetime.now()
    text = "=== CLAUDE MEMORIES ===\n\n" + "\n\n".join(memory_lines)

    print(f"    Memories: {len(memory_lines)} entries")
    return [{
        "id": "claude_memories",
        "title": "Claude Memories",
        "create_time": now.timestamp(),
        "update_time": now.timestamp(),
        "date": now.strftime("%Y-%m-%d"),
        "source": "claude",
        "text": text,
        "message_count": len(memory_lines),
        "char_count": len(text),
    }]


def parse(source_config: dict, data_dir: str) -> list[dict]:
    """
    Parse Claude export files into normalized records.

    Args:
        source_config: dict from YAML with keys like source_dir, etc.
        data_dir: base data directory
    
    Returns:
        List of normalized records ready for chunking.
    """
    source_dir = source_config.get("source_dir", "claude_export")
    if not os.path.isabs(source_dir):
        source_dir = os.path.join(data_dir, source_dir)

    if not os.path.exists(source_dir):
        print(f"  [claude] Export directory not found: {source_dir}")
        return []

    print(f"  [claude] Parsing from {source_dir}")

    records = []
    records.extend(_parse_conversations(source_dir))
    records.extend(_parse_projects(source_dir))
    records.extend(_parse_memories(source_dir))

    print(f"  [claude] Total: {len(records)} records")
    return records
