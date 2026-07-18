#!/usr/bin/env python3
"""
validate_corpus.py — Validatore standalone del corpus di retrieval (schema v3).

READ-ONLY. Non tocca mcp_server.py, config/, scripts/lib/. Zero scritture su db/,
zero HTTP verso :8787. Exit 0 se tutto valido, 1 se almeno una riga FAIL.

Cosa controlla, riga per riga:
  - JSONL parsabile riga per riga
  - tutti i campi dello schema presenti + tipo giusto
  - enum rispettati (category, origin, expected_failure_mode, gold_type, lang, status)
  - id unici su tutto il file
  - n ∈ {3, 5, 10}
  - status=ready  → gold NON vuoto ; status=draft → gold [] ammesso
  - primary_gold  → null oppure ∈ gold
  - holdout=true su status=draft → WARNING (non errore)
  - esistenza dei chunk_id in KB → SOLO per righe ready

Accesso KB (esistenza chunk_id):
  Replica il pattern read-only già in uso in
  scripts/maintenance/reconcile_lexical_index.py — sqlite3 su chroma.sqlite3 con
  `mode=ro` + busy_timeout, scoping al segmento METADATA della collection
  `tailor_kb_v2`. NIENTE PersistentClient: un secondo PersistentClient sul path
  di produzione, concorrente col processo MCP, causa SIGSEGV (chromadb). Il
  pattern esistente lo evita leggendo direttamente il segmento sqlite in sola
  lettura, e questo validatore fa lo stesso.

Uso:
    cd /path/to/tailor && ./.venv/bin/python eval/validate_corpus.py
    ./.venv/bin/python eval/validate_corpus.py --corpus eval/retrieval_corpus.jsonl
    ./.venv/bin/python eval/validate_corpus.py --no-kb   # salta l'esistenza KB
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

# ============================================================
# SCHEMA v3
# ============================================================

REQUIRED_FIELDS = (
    "id",
    "query",
    "category",
    "origin",
    "expected_failure_mode",
    "gold",
    "primary_gold",
    "gold_type",
    "n",
    "lang",
    "holdout",
    "status",
    "source_doc_id",
    "added_at",
    "note",
)

ENUMS = {
    "category": {"retrieval_miss", "entity_ok", "semantic_control"},
    "origin": {"observed", "designed", "regression"},
    "expected_failure_mode": {
        "meta_overrank",
        "cross_lingua",
        "reranker_misorder",
        "chunk_boundary",
        "unknown",
    },
    # Solo due valori: il tipo deve rispecchiare la cardinalità di `gold`
    # (vedi check di coerenza in validate_shape). "doc_summary"/"entity"/
    # "semantic" non sono più ammessi — non compaiono nel corpus e non
    # esprimono una cardinalità.
    "gold_type": {"single_chunk", "multi_chunk"},
    "lang": {"it", "en", "cross"},
    "status": {"draft", "ready"},
}

VALID_N = {3, 5, 10}

# ============================================================
# KB (read-only) — stesso pattern di reconcile_lexical_index.py
# ============================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CHROMA_DB = os.path.join(BASE_DIR, "db", "chroma.sqlite3")
DEFAULT_CORPUS = os.path.join(BASE_DIR, "eval", "retrieval_corpus.jsonl")

COLLECTION_NAME = "tailor_kb_v2"
DATABASE_NAME = "default_database"


def open_kb_readonly(chroma_path: str):
    """Apre la KB in sola lettura, coordinandosi con i writer (mode=ro).

    `mode=ro` prende lock condivisi → letture consistenti anche mentre MCP
    scrive; busy_timeout attende un commit in corso invece di errorare. NIENTE
    PersistentClient (caricherebbe l'HNSW + i writer chromadb → SIGSEGV race).
    """
    conn = sqlite3.connect(f"file:{chroma_path}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def get_metadata_segment(conn) -> str:
    """Segment_id METADATA di tailor_kb_v2, qualificato per database.

    Fail-loud: fetchall() + abort se i risultati sono più di uno (collection
    duplicata), mai un fetchone() cieco. Fallback allo schema legacy senza
    `databases`. Speculare a reconcile_lexical_index.get_metadata_segment().
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT s.id
            FROM segments s
            JOIN collections c ON s.collection = c.id
            JOIN databases d ON c.database_id = d.id
            WHERE c.name = ? AND d.name = ? AND s.scope = 'METADATA'
            """,
            (COLLECTION_NAME, DATABASE_NAME),
        )
        rows = cursor.fetchall()
        qualifier = f"database {DATABASE_NAME}"
    except sqlite3.OperationalError:
        cursor.execute(
            """
            SELECT s.id
            FROM segments s
            JOIN collections c ON s.collection = c.id
            WHERE c.name = ? AND s.scope = 'METADATA'
            """,
            (COLLECTION_NAME,),
        )
        rows = cursor.fetchall()
        qualifier = "senza qualifica di database (schema legacy)"

    if len(rows) > 1:
        raise RuntimeError(
            f"Segmento METADATA ambiguo per {COLLECTION_NAME} ({qualifier}): "
            f"{len(rows)} risultati {[r[0] for r in rows]} — non scelgo a caso"
        )
    if len(rows) == 0:
        raise RuntimeError(
            f"Collection {COLLECTION_NAME} / segmento METADATA non trovati ({qualifier})"
        )
    return rows[0][0]


def existing_chunk_ids(conn, segment_id: str, chunk_ids) -> set:
    """Sottoinsieme di `chunk_ids` presenti in KB come embedding_id nel segmento
    METADATA. Batch di IN(...) per non fare N query. Sola lettura.
    """
    present = set()
    ids = list(chunk_ids)
    cursor = conn.cursor()
    for start in range(0, len(ids), 500):
        batch = ids[start : start + 500]
        placeholders = ",".join("?" for _ in batch)
        cursor.execute(
            f"""
            SELECT DISTINCT embedding_id
            FROM embeddings
            WHERE segment_id = ? AND embedding_id IN ({placeholders})
            """,
            (segment_id, *batch),
        )
        present.update(row[0] for row in cursor.fetchall())
    return present


# ============================================================
# VALIDAZIONE (logica pura, nessun I/O)
# ============================================================


def validate_shape(rec: dict) -> list:
    """Errori strutturali di una riga (campi, tipi, enum, invarianti). Pura.

    NON include l'esistenza KB (serve I/O). Ritorna lista di messaggi (vuota =
    struttura OK).
    """
    errors = []

    missing = [f for f in REQUIRED_FIELDS if f not in rec]
    if missing:
        errors.append(f"campi mancanti: {', '.join(missing)}")
        # Senza i campi non ha senso proseguire i check che li assumono presenti.
        return errors

    # tipi base
    if not isinstance(rec["id"], str) or not rec["id"]:
        errors.append("id deve essere stringa non vuota")
    if not isinstance(rec["query"], str) or not rec["query"]:
        errors.append("query deve essere stringa non vuota")
    if not isinstance(rec["note"], str):
        errors.append("note deve essere stringa")
    if not isinstance(rec["added_at"], str) or not rec["added_at"]:
        errors.append("added_at deve essere stringa non vuota")

    # enum
    for field, allowed in ENUMS.items():
        val = rec.get(field)
        if val not in allowed:
            errors.append(f"{field}={val!r} non in {sorted(allowed)}")

    # n
    n = rec.get("n")
    # bool è sottotipo di int in Python: escludi esplicitamente True/False.
    if isinstance(n, bool) or not isinstance(n, int) or n not in VALID_N:
        errors.append(f"n={n!r} non in {sorted(VALID_N)}")

    # holdout bool
    if not isinstance(rec["holdout"], bool):
        errors.append(f"holdout={rec['holdout']!r} deve essere bool")

    # source_doc_id: string o null
    sdi = rec["source_doc_id"]
    if sdi is not None and not isinstance(sdi, str):
        errors.append(f"source_doc_id={sdi!r} deve essere stringa o null")

    # gold: lista di stringhe
    gold = rec["gold"]
    if not isinstance(gold, list) or any(not isinstance(g, str) for g in gold):
        errors.append("gold deve essere lista di stringhe")
        gold = []  # evita crash nei check successivi

    # gold_type ↔ len(gold): il tipo dichiara la cardinalità, non può mentire.
    # gold vuoto (draft) non vincola nulla: la cardinalità non è ancora fissata.
    gold_type = rec.get("gold_type")
    if gold_type in ENUMS["gold_type"] and len(gold) > 0:
        expected_type = "single_chunk" if len(gold) == 1 else "multi_chunk"
        if gold_type != expected_type:
            errors.append(
                f"gold_type={gold_type!r} incoerente con len(gold)={len(gold)}: "
                f"atteso {expected_type!r}"
            )

    # status ↔ gold
    status = rec.get("status")
    if status == "ready" and len(gold) == 0:
        errors.append("status=ready ma gold è vuoto")
    # status=draft con gold [] è ammesso: nessun errore.

    # primary_gold: null oppure ∈ gold
    pg = rec["primary_gold"]
    if pg is not None:
        if not isinstance(pg, str):
            errors.append(f"primary_gold={pg!r} deve essere stringa o null")
        elif pg not in gold:
            errors.append(f"primary_gold={pg!r} non presente in gold")

    return errors


def validate_warnings(rec: dict) -> list:
    """Warning non bloccanti di una riga. Pura."""
    warnings = []
    if rec.get("holdout") is True and rec.get("status") == "draft":
        warnings.append("holdout=true su status=draft (gold non ancora fissato)")
    if rec.get("status") == "ready" and rec.get("source_doc_id") is None:
        warnings.append(
            "status=ready con source_doc_id=null — legittimo per un gold di tipo "
            "summary il cui doc sorgente non è in KB (cfr. miss_analisi_sangue_2025); "
            "promemoria per i consumer che assumono ready ⇒ source_doc_id"
        )
    return warnings


# ============================================================
# ORCHESTRAZIONE + REPORT
# ============================================================


def load_lines(path: str):
    """Yield (lineno, raw, parsed_or_None, parse_error_or_None). Righe vuote saltate."""
    with open(path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            if raw.strip() == "":
                continue
            try:
                yield lineno, raw, json.loads(raw), None
            except json.JSONDecodeError as exc:
                yield lineno, raw, None, str(exc)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Validatore read-only del corpus retrieval (v3)")
    parser.add_argument("--corpus", default=DEFAULT_CORPUS, help="path del .jsonl")
    parser.add_argument("--chroma-db", default=DEFAULT_CHROMA_DB, help="chroma.sqlite3 (read-only)")
    parser.add_argument(
        "--no-kb", action="store_true", help="salta la verifica di esistenza chunk_id in KB"
    )
    args = parser.parse_args(argv)

    if not os.path.exists(args.corpus):
        print(f"❌ corpus non trovato: {args.corpus}", file=sys.stderr)
        return 1

    # Pass 1: parse + shape. Accumula per riga.
    records = []  # (lineno, rec_or_None, errors, warnings)
    seen_ids = {}
    n_fail = n_warn = n_ok = n_ready = 0

    for lineno, _raw, rec, parse_err in load_lines(args.corpus):
        if parse_err is not None:
            records.append((lineno, None, [f"JSON non parsabile: {parse_err}"], []))
            continue

        if not isinstance(rec, dict):
            records.append((lineno, None, [f"riga non è un oggetto JSON (è {type(rec).__name__})"], []))
            continue

        errors = validate_shape(rec)
        warnings = validate_warnings(rec)

        # id unici (cross-riga)
        rid = rec.get("id")
        if isinstance(rid, str) and rid:
            if rid in seen_ids:
                errors.append(f"id duplicato (già alla riga {seen_ids[rid]})")
            else:
                seen_ids[rid] = lineno

        records.append((lineno, rec, errors, warnings))

    # Pass 2: esistenza KB, SOLO righe ready strutturalmente valide con gold.
    kb_error = None
    kb_missing = {}  # lineno -> [chunk_id mancanti]
    if not args.no_kb:
        ready_targets = {}  # lineno -> set(chunk_id)
        for lineno, rec, errors, _w in records:
            if rec is None:
                continue
            if rec.get("status") == "ready" and not errors and rec.get("gold"):
                ready_targets[lineno] = set(rec["gold"])

        if ready_targets:
            if not os.path.exists(args.chroma_db):
                kb_error = f"KB non trovata: {args.chroma_db}"
            else:
                try:
                    conn = open_kb_readonly(args.chroma_db)
                    try:
                        segment_id = get_metadata_segment(conn)
                        all_ids = set().union(*ready_targets.values())
                        present = existing_chunk_ids(conn, segment_id, all_ids)
                    finally:
                        conn.close()
                    for lineno, wanted in ready_targets.items():
                        absent = sorted(wanted - present)
                        if absent:
                            kb_missing[lineno] = absent
                except (sqlite3.DatabaseError, RuntimeError) as exc:
                    kb_error = f"{type(exc).__name__}: {exc}"

    # Report per riga
    print("=" * 64)
    print("VALIDATE CORPUS — retrieval_corpus schema v3")
    print("=" * 64)
    print(f"file: {args.corpus}")
    print(f"KB:   {'SKIP (--no-kb)' if args.no_kb else args.chroma_db}")
    if kb_error is not None:
        print(f"⚠️  verifica KB non eseguita: {kb_error}")
    print("-" * 64)

    for lineno, rec, errors, warnings in records:
        # applica l'esito KB (solo se la verifica ha girato)
        row_errors = list(errors)
        if lineno in kb_missing:
            row_errors.append(
                "chunk_id assenti in KB: " + ", ".join(kb_missing[lineno])
            )

        rid = rec.get("id") if rec else "?"
        status = rec.get("status") if rec else "?"
        if status == "ready":
            n_ready += 1

        # nota KB per righe ready non verificate a causa di kb_error
        kb_note = ""
        if (
            not args.no_kb
            and kb_error is not None
            and rec is not None
            and status == "ready"
            and not errors
            and rec.get("gold")
        ):
            kb_note = "  [KB non verificata]"

        if row_errors:
            n_fail += 1
            print(f"L{lineno:<3} FAIL  {rid}")
            for e in row_errors:
                print(f"          → {e}")
        elif warnings:
            n_warn += 1
            print(f"L{lineno:<3} WARN  {rid}{kb_note}")
            for w in warnings:
                print(f"          → {w}")
        else:
            n_ok += 1
            verified = ""
            if status == "ready" and not args.no_kb and kb_error is None and rec.get("gold"):
                verified = f"  ({len(rec['gold'])} chunk_id verificati in KB)"
            print(f"L{lineno:<3} OK    {rid}{verified}{kb_note}")

    # Summary
    print("-" * 64)
    total = len(records)
    print(
        f"righe: {total}  |  OK: {n_ok}  WARN: {n_warn}  FAIL: {n_fail}  "
        f"|  ready: {n_ready}  draft: {total - n_ready}"
    )

    if n_fail > 0:
        print(f"❌ VALIDAZIONE FALLITA — {n_fail} riga/e FAIL")
        return 1
    if kb_error is not None and not args.no_kb:
        # KB doveva essere verificata ma non ha girato: non è un pass pulito.
        print(f"❌ VALIDAZIONE INCOMPLETA — verifica KB non eseguita ({kb_error})")
        return 1
    print("✅ VALIDAZIONE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
