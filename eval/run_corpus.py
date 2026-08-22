#!/usr/bin/env python3
"""Harness di valutazione del corpus di retrieval — read-only.

Legge `eval/retrieval_corpus.jsonl`, interroga `/api/diag/hybrid-search` una
riga alla volta e stampa recall e latenze. Nessuna scrittura su KB o indici:
l'endpoint diagnostico usa l'accessor read-only e non tocca la collection.

PERCHE' UN PROCESSO SEPARATO — `exec_command` gira DENTRO il processo MCP
(asyncio single-threaded): una HTTP verso :8787 fatta da lì si auto-blocca
(l'event loop che deve servire la richiesta è lo stesso che la attende). Questo
script va lanciato da terminale:

    ./.venv/bin/python eval/run_corpus.py
    ./.venv/bin/python eval/run_corpus.py --json /tmp/run.json

Solo stdlib: gira anche col python3 di sistema.

METRICA UFFICIALE = exact chunk match. Se il gold è `_chunk_0001` e arriva
`_chunk_0000` NON è un hit. Il match a livello di documento è una lente
diagnostica separata (colonne `doc_*`), mai sommata all'exact: serve a
distinguere "documento sbagliato" da "documento giusto, chunk sbagliato" —
il caso reale è hit_persehais_redundancy, dove arriva
email_19fb937fbc0eb7bb_chunk_0000 col gold su _chunk_0001.

Gli aggregati sono nominati per esteso (`mean_exact_recall_at_row_n`) e mai
`recall@n`: n varia per riga (12 righe a 10, 1 a 5), quindi un "@10" globale
sarebbe una bugia. Macro = ogni query pesa 1. Micro = gold trovati / gold totali.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import quote

DEFAULT_BASE_URL = "http://127.0.0.1:8787"
DEFAULT_TIMEOUT = 30.0
DEFAULT_N = 10
SCHEMA_VERSION = 1

# I rank riportati sono 1-based (il diag li espone 0-based). Esplicitato anche
# in metadata.rank_base per non lasciare ambiguità a chi diffa il JSON.
RANK_BASE = 1

_CHUNK_SUFFIX_RE = re.compile(r"^(?P<doc>.+)_chunk_\d+$")


class ServerUnreachable(Exception):
    """Nessuna risposta HTTP dal server: problema d'ambiente, non del corpus."""


class RowFailure(Exception):
    """Fallimento della singola riga: timeout, HTTP != 200, JSON invalido."""


# --------------------------------------------------------------------------
# Corpus
# --------------------------------------------------------------------------

def load_corpus(path):
    """Legge il JSONL. Ritorna (rows, warnings). Le righe non-JSON sono un
    problema di dati, non di run: sollevano subito invece di falsare i conti."""
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"{path}:{lineno}: JSON invalido ({e})") from e
            if not isinstance(obj, dict):
                raise ValueError(f"{path}:{lineno}: la riga non è un oggetto JSON")
            obj["_lineno"] = lineno
            rows.append(obj)
    return rows


def dedupe_preserving_order(items):
    """Dedup stabile. Ritorna (unici, n_scartati)."""
    seen = set()
    out = []
    dropped = 0
    for it in items:
        if it in seen:
            dropped += 1
            continue
        seen.add(it)
        out.append(it)
    return out, dropped


def doc_id_of(chunk_id):
    """`doc_xxx_chunk_0003` → `doc_xxx`. Un id senza suffisso _chunk_N resta
    se stesso: meglio un doc-match degenere che una KeyError."""
    m = _CHUNK_SUFFIX_RE.match(chunk_id)
    return m.group("doc") if m else chunk_id


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

def build_url(base_url, query, n):
    """URL-encoding OBBLIGATORIO: 7 righe su 13 contengono `?`, `'` o `.`.
    Un `?` non codificato tronca la query string e il server risponde 200 su
    una query mutilata — numeri sbagliati, zero errori sollevati."""
    return "{}/api/diag/hybrid-search?q={}&n={}".format(
        base_url.rstrip("/"), quote(query, safe=""), int(n))


def fetch_diag(url, timeout, first_request=False):
    """GET + parse. Alza ServerUnreachable solo se non è arrivata NESSUNA
    risposta HTTP alla prima richiesta; un 500 significa server vivo → RowFailure."""
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", "replace")[:200]
        except Exception:
            pass
        raise RowFailure(f"HTTP {e.code}{(': ' + body) if body else ''}") from e
    except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as e:
        reason = getattr(e, "reason", e)
        if first_request:
            raise ServerUnreachable(str(reason)) from e
        raise RowFailure(f"{type(e).__name__}: {reason}") from e

    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise RowFailure(f"risposta non JSON ({e})") from e
    if not isinstance(data, dict):
        raise RowFailure("risposta JSON non è un oggetto")
    if "stages" not in data or not isinstance(data.get("stages"), dict):
        err = data.get("error")
        raise RowFailure(f"risposta senza stages{(': ' + str(err)) if err else ''}")
    return data


# --------------------------------------------------------------------------
# Lettura difensiva degli stage
# --------------------------------------------------------------------------

def stage_dict(resp, name):
    st = resp.get("stages", {}).get(name)
    return st if isinstance(st, dict) else {}


def stage_ids(resp, name):
    """chunk_id di uno stage. Non assume lista di stringhe: gli items sono
    oggetti `{chunk_id, rank, score, ...}`, ma una futura versione del diag
    potrebbe appiattirli. Ritorna (ids, n_malformati)."""
    items = stage_dict(resp, name).get("items")
    if not isinstance(items, list):
        return [], 0
    ids = []
    malformed = 0
    for it in items:
        if isinstance(it, str):
            ids.append(it)
        elif isinstance(it, dict):
            cid = it.get("chunk_id")
            if isinstance(cid, str) and cid:
                ids.append(cid)
            else:
                malformed += 1
        else:
            malformed += 1
    return ids, malformed


def stage_count(resp, name):
    c = stage_dict(resp, name).get("count")
    return c if isinstance(c, int) else None


def _rank_of(needle, ordered):
    try:
        return ordered.index(needle) + RANK_BASE
    except ValueError:
        return None


# --------------------------------------------------------------------------
# Valutazione di una riga
# --------------------------------------------------------------------------

def evaluate(row, resp):
    """Calcola le metriche di una riga. Nessun accesso di rete qui: prende la
    risposta già deserializzata, così i test girano senza server."""
    warnings = []

    gold_raw = row.get("gold") or []
    if not isinstance(gold_raw, list):
        gold_raw = []
        warnings.append("gold_not_a_list")
    gold = [g for g in gold_raw if isinstance(g, str) and g]
    if len(gold) != len(gold_raw):
        warnings.append("gold_entries_skipped")
    gold, gold_dupes = dedupe_preserving_order(gold)
    if gold_dupes:
        warnings.append(f"gold_duplicates_dropped:{gold_dupes}")
    if not gold:
        warnings.append("gold_empty")

    primary = row.get("primary_gold")
    if not isinstance(primary, str) or not primary:
        primary = None
        if gold:
            warnings.append("primary_gold_missing")
    elif primary not in gold:
        # Non è fatale: il rank del primary si misura comunque, ma la riga va
        # segnalata perché exact_hits e primary_exact_rank stanno misurando
        # insiemi diversi.
        warnings.append("primary_gold_not_in_gold")

    # --- n effettivo -------------------------------------------------------
    n_raw = row.get("n")
    if isinstance(n_raw, bool) or not isinstance(n_raw, int):
        n = DEFAULT_N
        warnings.append("n_missing_default_%d" % DEFAULT_N)
    elif n_raw <= 0:
        n = DEFAULT_N
        warnings.append(f"n_invalid:{n_raw}_default_{DEFAULT_N}")
    else:
        n = n_raw

    # --- final_ranked ------------------------------------------------------
    final_all, malformed = stage_ids(resp, "final_ranked")
    if malformed:
        warnings.append(f"final_ranked_malformed_items:{malformed}")
    final_all, final_dupes = dedupe_preserving_order(final_all)
    if final_dupes:
        # I duplicati falsano rank e recall: vanno tolti PRIMA del taglio a n.
        warnings.append(f"final_ranked_duplicates_dropped:{final_dupes}")
    if n > len(final_all):
        warnings.append(f"n_gt_final_ranked:{n}>{len(final_all)}")
    top = final_all[:n]
    top_docs = {doc_id_of(c) for c in top}
    top_doc_order = []
    for c in top:
        d = doc_id_of(c)
        if d not in top_doc_order:
            top_doc_order.append(d)

    # --- exact vs doc-level (mai sommati) ----------------------------------
    top_set = set(top)
    exact_hits = sum(1 for g in gold if g in top_set)
    doc_hits = sum(1 for g in gold if doc_id_of(g) in top_docs)

    primary_exact_rank = _rank_of(primary, top) if primary else None
    primary_doc_rank = _rank_of(doc_id_of(primary), top_doc_order) if primary else None

    # --- provenienza -------------------------------------------------------
    sem_ids, _ = stage_ids(resp, "semantic_candidates")
    ent_pre_ids, _ = stage_ids(resp, "entity_candidates_pre_cap")
    ent_post_ids, _ = stage_ids(resp, "entity_candidates_post_cap")
    lex_ids, _ = stage_ids(resp, "lexical_candidates")
    pool_ids, _ = stage_ids(resp, "rerank_pool")
    sem_set, pre_set, post_set = set(sem_ids), set(ent_pre_ids), set(ent_post_ids)
    lex_set, pool_set = set(lex_ids), set(pool_ids)

    gold_detail = []
    for g in gold:
        # Lista, non stringa: lo stesso chunk può arrivare da più rami
        # (["entity","lexical"]). Ordinata per rendere il JSON diffabile.
        prov = sorted(
            name for name, ids in (("semantic", sem_set),
                                   ("entity", post_set),   # post_cap = "ha raggiunto il pool"
                                   ("lexical", lex_set))
            if g in ids
        )
        gold_detail.append({
            "chunk_id": g,
            "provenance": prov,
            # tagliato dal cap entity: visto dalla SQL ma mai fetchato
            "entity_pre_cap_only": (g in pre_set) and (g not in post_set),
            "in_rerank_pool": g in pool_set,
            "in_final_topn": g in top_set,
            "final_rank": _rank_of(g, top),
            "doc_in_final_topn": doc_id_of(g) in top_docs,
        })

    # --- lexical_derived_dropped: LETTO, mai inferito ----------------------
    lex_stage = stage_dict(resp, "lexical_candidates")
    ldd = lex_stage.get("lexical_derived_dropped")
    if not isinstance(ldd, int) or isinstance(ldd, bool):
        ldd = None  # campo assente/non numerico → None, non 0 e non calcolato
        warnings.append("lexical_derived_dropped_missing")

    server_counts = resp.get("counts") if isinstance(resp.get("counts"), dict) else {}
    counts = {
        "raw_candidates": server_counts.get("raw_candidates"),
        "after_supersede": server_counts.get("after_supersede"),
        "final_semantic": server_counts.get("final_semantic"),
        "final_entity": server_counts.get("final_entity"),
        "final_lexical": server_counts.get("final_lexical"),
        "semantic_candidates": stage_count(resp, "semantic_candidates"),
        "entity_pre_cap": stage_count(resp, "entity_candidates_pre_cap"),
        "entity_post_cap": stage_count(resp, "entity_candidates_post_cap"),
        "lexical_candidates": stage_count(resp, "lexical_candidates"),
        "rerank_pool": stage_count(resp, "rerank_pool"),
        "final_ranked": stage_count(resp, "final_ranked"),
    }

    timings_raw = resp.get("timings") if isinstance(resp.get("timings"), dict) else {}
    timings = {}
    for k in ("lexical_ms", "entity_extract_ms", "rerank_ms", "total_ms"):
        v = timings_raw.get(k)
        timings[k] = round(float(v), 2) if isinstance(v, (int, float)) and not isinstance(v, bool) else None

    return {
        "n": n,
        "gold": gold,
        "primary_gold": primary,
        "exact_hits": exact_hits,
        "doc_hits": doc_hits,
        "gold_total": len(gold),
        "primary_exact_rank": primary_exact_rank,
        "primary_doc_rank": primary_doc_rank,
        "final_ranked_ids": top,
        "gold_provenance": gold_detail,
        "counts": counts,
        "lexical_derived_dropped": ldd,
        "lexical_error": lex_stage.get("error"),
        "rerank_backend": resp.get("rerank_backend"),
        "warnings": warnings,
        "timings_ms": timings,
    }


# --------------------------------------------------------------------------
# Aggregati
# --------------------------------------------------------------------------

def _median(values):
    vals = [v for v in values if isinstance(v, (int, float))]
    return round(statistics.median(vals), 2) if vals else None


def aggregate(results):
    """Macro = media delle recall per riga (ogni query pesa 1).
    Micro = gold trovati totali / gold totali (le righe con più gold pesano di più).
    Le righe in errore e quelle senza gold non entrano in nessuna delle due."""
    scored = [r for r in results
              if r["status"] == "ok" and r["metrics"]["gold_total"] > 0]
    exact_rates = [r["metrics"]["exact_hits"] / r["metrics"]["gold_total"] for r in scored]
    doc_rates = [r["metrics"]["doc_hits"] / r["metrics"]["gold_total"] for r in scored]
    gold_found = sum(r["metrics"]["exact_hits"] for r in scored)
    gold_total = sum(r["metrics"]["gold_total"] for r in scored)
    with_primary = [r for r in scored if r["metrics"]["primary_gold"]]

    def _mean(xs):
        return round(sum(xs) / len(xs), 4) if xs else None

    return {
        "rows_scored": len(scored),
        "mean_exact_recall_at_row_n": _mean(exact_rates),
        "mean_doc_recall_at_row_n": _mean(doc_rates),
        "micro_exact_recall_at_row_n": round(gold_found / gold_total, 4) if gold_total else None,
        "micro_exact_gold_found": gold_found,
        "micro_exact_gold_total": gold_total,
        "primary_exact_hit_rate": _mean(
            [1.0 if r["metrics"]["primary_exact_rank"] else 0.0 for r in with_primary]),
        "primary_doc_hit_rate": _mean(
            [1.0 if r["metrics"]["primary_doc_rank"] else 0.0 for r in with_primary]),
        # VOLATILI: utili per vedere una regressione di costo, MAI criterio di
        # regressione funzionale (dipendono da carico macchina e cache).
        "timings_median_ms": {
            k: _median([r["metrics"]["timings_ms"].get(k) for r in scored])
            for k in ("lexical_ms", "entity_extract_ms", "rerank_ms", "total_ms")
        },
    }


# --------------------------------------------------------------------------
# Output umano
# --------------------------------------------------------------------------

def _fmt(v, dash="-"):
    return dash if v is None else str(v)


def render_table(results, out=None):
    # `out` risolto a chiamata, non come default: sys.stdout legato al momento
    # del def sarebbe quello vero anche sotto redirezione/cattura.
    out = out or sys.stdout
    header = ("{:<26} {:>3} {:>7} {:>7} {:>5} {:>5} {:>8} {:>8} {:>8}"
              .format("id", "n", "exact", "doc", "pEx", "pDoc", "lexDrop", "lex_ms", "tot_ms"))
    print(header, file=out)
    print("-" * len(header), file=out)
    for r in results:
        rid = r["id"][:26]
        if r["status"] != "ok":
            print("{:<26} {:>3} {:>7} {:>7} {:>5} {:>5} {:>8} {:>8} {:>8}   ERRORE: {}"
                  .format(rid, _fmt(r.get("n_requested")), "-", "-", "-", "-", "-", "-", "-",
                          r["error"]), file=out)
            continue
        m = r["metrics"]
        t = m["timings_ms"]
        line = ("{:<26} {:>3} {:>7} {:>7} {:>5} {:>5} {:>8} {:>8} {:>8}".format(
            rid, m["n"],
            "{}/{}".format(m["exact_hits"], m["gold_total"]),
            "{}/{}".format(m["doc_hits"], m["gold_total"]),
            _fmt(m["primary_exact_rank"]), _fmt(m["primary_doc_rank"]),
            _fmt(m["lexical_derived_dropped"]),
            _fmt(t["lexical_ms"]), _fmt(t["total_ms"])))
        flags = list(m["warnings"])
        if m["lexical_error"]:
            flags.append("lexical_branch_error")
        pre_only = [g["chunk_id"] for g in m["gold_provenance"] if g["entity_pre_cap_only"]]
        if pre_only:
            flags.append(f"entity_pre_cap_only:{len(pre_only)}")
        if flags:
            line += "   [" + " ".join(flags) + "]"
        print(line, file=out)


def render_summary(agg, meta, out=None):
    out = out or sys.stdout
    print("", file=out)
    print("AGGREGATO (macro: ogni query pesa 1; exact e doc mai sommati)", file=out)
    print("  righe: {} totali / {} ready / {} valutate / {} in errore"
          .format(meta["rows_total"], meta["rows_ready"],
                  meta["rows_evaluated"], meta["rows_failed"]), file=out)
    print("  mean_exact_recall_at_row_n  = {}  (metrica ufficiale, {} righe)"
          .format(_fmt(agg["mean_exact_recall_at_row_n"]), agg["rows_scored"]), file=out)
    print("  mean_doc_recall_at_row_n    = {}  (lente diagnostica, non sommare)"
          .format(_fmt(agg["mean_doc_recall_at_row_n"])), file=out)
    print("  micro_exact_recall_at_row_n = {}  ({}/{} gold)"
          .format(_fmt(agg["micro_exact_recall_at_row_n"]),
                  agg["micro_exact_gold_found"], agg["micro_exact_gold_total"]), file=out)
    print("  primary_exact_hit_rate      = {}   primary_doc_hit_rate = {}"
          .format(_fmt(agg["primary_exact_hit_rate"]), _fmt(agg["primary_doc_hit_rate"])), file=out)
    tm = agg["timings_median_ms"]
    print("  mediane ms (VOLATILI, non criterio di regressione funzionale): "
          "lexical={} entity_extract={} rerank={} total={}"
          .format(_fmt(tm["lexical_ms"]), _fmt(tm["entity_extract_ms"]),
                  _fmt(tm["rerank_ms"]), _fmt(tm["total_ms"])), file=out)


# --------------------------------------------------------------------------
# Run
# --------------------------------------------------------------------------

def run_corpus(rows, base_url, timeout, fetch=None):
    """Interroga il diag riga per riga. `fetch` è iniettabile: i test passano
    un fake e non serve nessun server. Risolto a chiamata (non come default
    dell'argomento) così un monkeypatch del modulo ha effetto."""
    fetch = fetch or fetch_diag
    results = []
    seen_ids = {}
    duplicate_ids = []
    first = True
    for row in rows:
        rid = row.get("id") or "<row line {}>".format(row.get("_lineno"))
        seen_ids[rid] = seen_ids.get(rid, 0) + 1
        if seen_ids[rid] > 1 and rid not in duplicate_ids:
            duplicate_ids.append(rid)
        query = row.get("query")
        n_req = row.get("n")
        n_req = n_req if isinstance(n_req, int) and not isinstance(n_req, bool) and n_req > 0 else DEFAULT_N
        base = {"id": rid, "line": row.get("_lineno"), "query": query,
                "n_requested": n_req, "occurrence": seen_ids[rid]}
        if not isinstance(query, str) or not query:
            results.append(dict(base, status="error", error="query mancante o vuota", url=None))
            continue
        url = build_url(base_url, query, n_req)
        try:
            resp = fetch(url, timeout, first_request=first)
        except ServerUnreachable:
            raise
        except RowFailure as e:
            results.append(dict(base, status="error", error=str(e), url=url))
            continue
        finally:
            first = False
        results.append(dict(base, status="ok", error=None, url=url,
                            metrics=evaluate(row, resp)))
    return results, duplicate_ids


def build_json_report(results, agg, meta):
    """Formato stabile e diffabile: niente payload dei candidati, niente score
    float, niente stage non usati. I timings ci sono ma sono volatili."""
    rows = []
    for r in results:
        entry = {
            "id": r["id"],
            "line": r["line"],
            "occurrence": r["occurrence"],
            "query": r["query"],
            "url": r["url"],
            "status": r["status"],
            "error": r["error"],
        }
        if r["status"] == "ok":
            m = r["metrics"]
            entry.update({
                "n": m["n"],
                "gold": m["gold"],
                "primary_gold": m["primary_gold"],
                "final_ranked_ids": m["final_ranked_ids"],
                "exact_hits": m["exact_hits"],
                "doc_hits": m["doc_hits"],
                "gold_total": m["gold_total"],
                "primary_exact_rank": m["primary_exact_rank"],
                "primary_doc_rank": m["primary_doc_rank"],
                "gold_provenance": m["gold_provenance"],
                "counts": m["counts"],
                "lexical_derived_dropped": m["lexical_derived_dropped"],
                "lexical_error": m["lexical_error"],
                "rerank_backend": m["rerank_backend"],
                "warnings": m["warnings"],
                "timings_ms": m["timings_ms"],
            })
        rows.append(entry)
    return {"schema_version": SCHEMA_VERSION, "metadata": meta,
            "aggregate": agg, "rows": rows}


def main(argv=None):
    p = argparse.ArgumentParser(
        description="Valuta il corpus di retrieval contro /api/diag/hybrid-search (read-only).")
    default_corpus = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "retrieval_corpus.jsonl")
    p.add_argument("--corpus", default=default_corpus, help="path del JSONL (default: eval/retrieval_corpus.jsonl)")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"default {DEFAULT_BASE_URL}")
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="secondi per richiesta (default 30)")
    p.add_argument("--json", dest="json_out", default=None, help="scrive il report JSON su questo path")
    args = p.parse_args(argv)

    try:
        all_rows = load_corpus(args.corpus)
    except (OSError, ValueError) as e:
        print(f"ERRORE: corpus non leggibile: {e}", file=sys.stderr)
        return 2

    ready = [r for r in all_rows if r.get("status") == "ready"]
    skipped = len(all_rows) - len(ready)
    print(f"corpus: {args.corpus}  righe={len(all_rows)} ready={len(ready)} saltate={skipped}")
    print(f"target: {args.base_url}  timeout={args.timeout}s\n")

    started = time.time()
    try:
        results, duplicate_ids = run_corpus(ready, args.base_url, args.timeout)
    except ServerUnreachable as e:
        print(f"ERRORE: server non raggiungibile su {args.base_url} ({e}).\n"
              f"       È un problema d'ambiente, non un risultato del corpus: "
              f"nessuna metrica prodotta.\n"
              f"       Verifica che l'MCP sia up (launchctl list | grep tailor) "
              f"e riprova.", file=sys.stderr)
        return 2

    failed = [r for r in results if r["status"] != "ok"]
    meta = {
        "base_url": args.base_url,
        "corpus": os.path.abspath(args.corpus),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "timeout_s": args.timeout,
        "rank_base": RANK_BASE,
        "rows_total": len(all_rows),
        "rows_ready": len(ready),
        "rows_skipped_not_ready": skipped,
        "rows_evaluated": len(results) - len(failed),
        "rows_failed": len(failed),
        "duplicate_ids": duplicate_ids,
    }
    agg = aggregate(results)

    render_table(results)
    render_summary(agg, meta)
    if duplicate_ids:
        print("  ATTENZIONE: id duplicati nel corpus: " + ", ".join(duplicate_ids))
    no_gold = [r["id"] for r in results if r["status"] == "ok" and r["metrics"]["gold_total"] == 0]
    if no_gold:
        print("  ATTENZIONE: righe senza gold, escluse dagli aggregati: " + ", ".join(no_gold))
    print(f"  wall clock: {time.time() - started:.1f}s")

    if args.json_out:
        report = build_json_report(results, agg, meta)
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        print(f"  report JSON: {args.json_out}")

    if failed:
        print(f"\n{len(failed)} righe fallite: " + ", ".join(r["id"] for r in failed), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
