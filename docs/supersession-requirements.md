# Supersession — Design Requirements

> **Status (April 2026):** Chunk-level supersession pipeline is archived at
> `archive/supersession.py` and not currently in use. Fact-level supersession
> (`scripts/enrichment/fact_supersession.py`) is active and operates on a
> separate table (`db/facts.sqlite3`) without touching ChromaDB chunk metadata.
>
> This document captures requirements for any future work that revives or
> replaces the chunk-level pipeline.

---

## Background

TAILOR ingests content from heterogeneous sources with different authority levels:

- **Documents** (`source=document`): PDFs, docx, xlsx the user actively maintains. Source of truth.
- **Document summaries** (`conv_id` prefixed with `doc_summary_`): auto-generated condensations of a document. Derivative.
- **Conversation summaries** (`conv_id` prefixed with `conv_summary_`): auto-generated condensations of chat sessions. Derivative.
- **Conversations** (`source` in `{claude, chatgpt, email, telegram}`): raw sessions. Working memory.

These sources have fundamentally different semantics. **A conversation discussing a document is not an updated version of that document** — the user may decide things during a chat, but until those decisions are written back to the source document, the document remains authoritative.

## Historical bug (fixed 2026-04-21)

The old chunk-level supersession pipeline used semantic similarity + LLM-based temporal reasoning to decide whether a newer chunk "superseded" an older one. It applied this decision uniformly, ignoring `source` type.

Result: conversation-summary chunks were flagged as superseding document chunks. 17 chunks across 5 documents became invisible in default KB searches because a downstream Claude/ChatGPT conversation *about* the document was treated as a *replacement* for it.

Documents affected at the time of fix:
- `piano_integrazione_marzo2026.docx` — 8 chunks
- `piano_integrazione_aprile2026.docx` — 4 chunks
- `Divorzio/Avv. Brunelli/Trattativa per riduzione assegno di mantenimento.docx` — 3 chunks
- `Salute & Fitness/2025/Piano Integrazione Set-Dic 2025.docx` — 1 chunk
- `Red Pill Ventures/.../Avviso convoc ass straord 24 giugno 22.pdf` — 1 chunk

Fix applied: `DELETE FROM embedding_metadata WHERE key IN ('superseded_by', 'superseded_at')` scoped to the affected chunks. Legitimate `doc → doc_summary` supersessions (729 chunks) were preserved.

## Requirement for any future chunk-level supersession

When the decision logic evaluates whether chunk B should supersede chunk A, it **MUST** enforce the following source hierarchy:

```
document  >  doc_summary  >  conv_summary  >  chatgpt | claude | email | telegram
```

### Rule

A chunk can only be superseded by a chunk of **equal or higher** source rank. Specifically:

- `source=document` → can be superseded ONLY by:
  - Another `source=document` chunk, OR
  - A `doc_summary` with the same `file_path` (i.e. a summary *of the same document*, not of a different one).
- `source=document` MUST NEVER be superseded by `source in {claude, chatgpt, email, telegram}`.
- Cross-document supersession (doc A superseded by doc B where `file_path(A) != file_path(B)`) requires explicit semantic linkage beyond similarity — e.g. filename pattern match like `piano_integrazione_marzo2026.docx → piano_integrazione_aprile2026.docx`. Until such linkage exists, reject.

### Rationale

The user's mental model:

- **Documents are the source of truth**, updated manually after discussions conclude.
- **Conversations are working memory**, a space to decide and reason — not a write target.
- A conversation deciding a plan-update does not, in itself, update the plan. The user subsequently edits the document.

This matches how humans actually use knowledge bases with an assistant: the chat is scratch space, the file is canon.

## Acceptance criteria for revived code

Before merging any revived chunk-level supersession code:

1. **Unit test** asserting the source-hierarchy rule rejects cross-type supersessions (e.g. `conv_summary → document` MUST return `INDEPENDENT`).
2. **Enforcement at decision time** (pre-write). Do not rely on post-hoc filtering in search — the metadata itself must never be written incorrectly.
3. **Explicit logging** when a candidate is rejected for source-hierarchy reasons, so drift can be audited in nightly reports.
4. **Documentation** updated to reflect the semantic contract (this file, plus inline comments in the supersession decision function).

## Non-requirements

- This rule does NOT apply to fact-level supersession (`scripts/enrichment/fact_supersession.py`). Fact-level supersession operates on atomic facts extracted from chunks, regardless of source — a fact updated in a conversation legitimately supersedes the same fact extracted from an older document, because the semantic unit is the fact itself, not the chunk's visibility.
- This rule does NOT prevent a document from being superseded by an updated version of itself or by a summary of itself (both are same-authority operations within the document lane).

## References

- Archived implementation: `archive/supersession.py` (pre-April 2026 reorg).
- Data migration fix: applied 2026-04-21 during a debugging session on an unrelated bug (supplement reminder scheduling).
- Active fact-level pipeline: `scripts/enrichment/fact_supersession.py`.
