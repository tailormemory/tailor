# ⚠️ AUTO-FORGETTING — PREPARED, NOT ACTIVE

**Status**: infrastructure ready, `expires_at` field NOT yet populated.
**Date**: April 6, 2026
**Why this file exists**: so we don't forget about forgetting (pun intended).

## What's in place

1. ✅ Column `expires_at TEXT DEFAULT NULL` added to `facts.sqlite3`
2. ✅ Index `idx_facts_expires` created
3. ✅ `get_facts_for_chunks()` in MCP server filters facts with `expires_at < now()`
4. ❌ The fact extraction prompt (extract_facts / extract_facts_nightly) does NOT populate `expires_at`
5. ❌ No GC active — not needed while the field is empty

## What's needed to activate it

### Step 1: Update the fact extraction prompt
In `scripts/enrichment/extract_facts.py` and `scripts/enrichment/extract_facts_nightly.py`, add to the LLM prompt:
```
- expires_at: ISO date after which this fact is no longer operationally relevant.
  Set ONLY for time-bound facts (meetings, deadlines, flights, appointments).
  Leave empty for permanent facts (decisions, preferences, relationships, status).
  Example: "Meeting with Alex tomorrow at 3pm" (date: 2026-04-06) → expires_at: "2026-04-07"
  Example: "User lives in Rome" → expires_at: empty
```

And add the `expires_at` field to the JSON output schema.

### Step 2: Retroactive batch on existing facts
Once extraction reaches 100% coverage (~155k chunks), run a batch to classify
the ~241k+ existing facts and populate `expires_at` where applicable.

Suggested approach: SQL query to find facts with `event_date` in the past + temporal
keywords (meeting, call, deadline, flight, appointment), then LLM to confirm.

### Step 3: GC in the nightly pipeline
Add a step in `sync_and_ingest.sh` that counts expired facts (for reporting).
No DELETE needed — the MCP filter already excludes them.

## Why it's NOT active now (April 6, 2026)

Empirical test performed: expired temporal facts do NOT pollute searches with the
current KB (~241k facts, ~22k chunks covered). ONNX re-ranking + supersession
handle noise sufficiently well.

Main risk of premature activation: if the LLM misclassifies a permanent fact
as temporal, we lose it from searches. Better to wait until extraction is
complete and test classification quality on a sample.

## When to activate

Suggested triggers (any one is enough):
- [ ] Fact extraction completed at 100% (~155k chunks)
- [ ] Total facts exceed 500k and searches show measurable noise
- [ ] Someone (user or AI assistant) notices results polluted by expired temporal facts

## Files to modify

| File | Action |
|------|--------|
| `scripts/enrichment/extract_facts.py` | Add `expires_at` to prompt and parsing |
| `scripts/enrichment/extract_facts_nightly.py` | Same |
| `scripts/enrichment/fact_supersession.py` | No changes needed |
| `mcp_server.py` | ✅ Already done (filter in get_facts_for_chunks) |
| `db/facts.sqlite3` | ✅ Already done (column + index) |
| `sync_and_ingest.sh` | Add expired facts counter to TG report |
