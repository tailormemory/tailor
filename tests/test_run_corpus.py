"""Harness di valutazione del corpus (`eval/run_corpus.py`).

Nessun server reale e nessuna KB: le risposte del diag sono fixture in-memory e
`urllib.request.urlopen` è monkeypatchato. Quello che si verifica qui è la
CONTABILITA' delle metriche, non il retrieval:

- exact chunk match come metrica ufficiale, doc-level come lente separata
  (il caso reale: gold `_chunk_0001`, arriva `_chunk_0000`);
- provenienza multi-stage come lista;
- `entity_pre_cap_only` per il gold tagliato dal cap;
- URL-encoding di `?` e `'` (7 righe su 13 li contengono: un `?` non
  codificato tronca la query string e produce numeri sbagliati in silenzio);
- exit code: 0 successo, 2 server down, 1 righe fallite.
"""

from __future__ import annotations

import io
import json
import os
import sys
import urllib.error
import urllib.request

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "eval"))

import run_corpus  # noqa: E402


# --------------------------------------------------------------------------
# Fixture: risposta diag minima, stessa forma di _hybrid_diagnostic
# --------------------------------------------------------------------------

def _items(ids):
    return [{"chunk_id": c, "rank": i, "score": 0.5, "source_type": "semantic",
             "doc_type": "email"} for i, c in enumerate(ids)]


def diag_response(final, semantic=(), entity_pre=(), entity_post=(), lexical=(),
                  pool=None, derived_dropped=3, lexical_error=None, timings=None):
    pool = list(final) if pool is None else list(pool)
    return {
        "query": "q", "n_results": len(final), "source_filter": "",
        "include_superseded": False, "rerank_backend": "onnx",
        "counts": {"raw_candidates": 130, "after_supersede": 120,
                   "final_semantic": 5, "final_entity": 3, "final_lexical": 2},
        "stages": {
            "semantic_candidates": {"count": len(semantic), "items": _items(semantic)},
            "entity_candidates_pre_cap": {"count": len(entity_pre), "items": _items(entity_pre),
                                          "rows_appended": len(entity_pre),
                                          "unique_count": len(entity_pre)},
            "entity_candidates_post_cap": {"count": len(entity_post), "items": _items(entity_post)},
            "lexical_candidates": {"count": len(lexical), "items": _items(lexical),
                                   "lexical_derived_dropped": derived_dropped,
                                   "error": lexical_error},
            "after_supersede": {"count": len(pool), "items": _items(pool)},
            "rerank_pool": {"count": len(pool), "items": _items(pool)},
            "final_ranked": {"count": len(final), "items": _items(final)},
        },
        "timings": dict(timings or {"semantic_ms": 12.0, "entity_extract_ms": 3.5,
                                    "entity_fetch_ms": 8.0, "lexical_ms": 4.25,
                                    "supersede_ms": 0.5, "pool_ms": 0.1,
                                    "rerank_ms": 90.0, "total_ms": 118.0}),
    }


def corpus_row(**kw):
    row = {"id": "row1", "query": "una query", "gold": ["doc_a_chunk_0001"],
           "primary_gold": "doc_a_chunk_0001", "n": 10, "status": "ready", "_lineno": 1}
    row.update(kw)
    return row


# --------------------------------------------------------------------------
# exact vs doc-level
# --------------------------------------------------------------------------

def test_exact_miss_ma_doc_hit_sul_chunk_sbagliato():
    """Il caso hit_persehais_redundancy: gold `_chunk_0001`, arriva `_chunk_0000`.
    Documento giusto, chunk sbagliato → exact 0, doc 1. Mai sommati."""
    row = corpus_row(gold=["email_19fb937fbc0eb7bb_chunk_0001"],
                     primary_gold="email_19fb937fbc0eb7bb_chunk_0001")
    resp = diag_response(final=["altro_chunk_0000", "email_19fb937fbc0eb7bb_chunk_0000"])
    m = run_corpus.evaluate(row, resp)

    assert m["exact_hits"] == 0
    assert m["doc_hits"] == 1
    assert m["primary_exact_rank"] is None
    assert m["primary_doc_rank"] == 2          # 1-based, sui doc distinti del top-n
    assert m["gold_provenance"][0]["in_final_topn"] is False
    assert m["gold_provenance"][0]["doc_in_final_topn"] is True


def test_exact_hit_conta_una_volta_sola():
    row = corpus_row(gold=["doc_a_chunk_0000", "doc_a_chunk_0001"],
                     primary_gold="doc_a_chunk_0001")
    resp = diag_response(final=["doc_a_chunk_0001", "doc_b_chunk_0000"])
    m = run_corpus.evaluate(row, resp)
    assert m["exact_hits"] == 1
    assert m["doc_hits"] == 2                  # entrambi i gold stanno in doc_a
    assert m["primary_exact_rank"] == 1


def test_doc_id_senza_suffisso_chunk():
    assert run_corpus.doc_id_of("doc_a_chunk_0012") == "doc_a"
    assert run_corpus.doc_id_of("qualcosa_senza_suffisso") == "qualcosa_senza_suffisso"


# --------------------------------------------------------------------------
# provenienza
# --------------------------------------------------------------------------

def test_provenienza_multi_stage_e_lista():
    """Lo stesso chunk può arrivare da più rami: la provenienza è una lista."""
    g = "doc_a_chunk_0001"
    resp = diag_response(final=[g], semantic=[g], entity_pre=[g], entity_post=[g], lexical=[g])
    m = run_corpus.evaluate(corpus_row(), resp)
    prov = m["gold_provenance"][0]
    assert prov["provenance"] == ["entity", "lexical", "semantic"]   # ordinata → diffabile
    assert prov["entity_pre_cap_only"] is False
    assert prov["in_rerank_pool"] is True


def test_entity_pre_cap_only_quando_il_cap_taglia_il_gold():
    """Gold visto dalla SQL entity ma mai fetchato: non ha raggiunto il pool.
    `post_cap` è la fonte di verità per la provenienza 'entity'."""
    g = "doc_a_chunk_0001"
    resp = diag_response(final=["doc_z_chunk_0000"], entity_pre=[g], entity_post=["doc_z_chunk_0000"])
    m = run_corpus.evaluate(corpus_row(), resp)
    prov = m["gold_provenance"][0]
    assert prov["entity_pre_cap_only"] is True
    assert prov["provenance"] == []            # pre_cap NON conta come provenienza
    assert m["exact_hits"] == 0


# --------------------------------------------------------------------------
# lexical_derived_dropped: letto, mai inferito
# --------------------------------------------------------------------------

def test_lexical_derived_dropped_letto_dal_diag():
    resp = diag_response(final=["doc_a_chunk_0001"], derived_dropped=10)
    m = run_corpus.evaluate(corpus_row(), resp)
    assert m["lexical_derived_dropped"] == 10


def test_lexical_derived_dropped_assente_e_none_non_zero():
    resp = diag_response(final=["doc_a_chunk_0001"])
    del resp["stages"]["lexical_candidates"]["lexical_derived_dropped"]
    m = run_corpus.evaluate(corpus_row(), resp)
    assert m["lexical_derived_dropped"] is None
    assert "lexical_derived_dropped_missing" in m["warnings"]


def test_lexical_error_propagato():
    resp = diag_response(final=["doc_a_chunk_0001"], lexical_error="OperationalError: no such table")
    m = run_corpus.evaluate(corpus_row(), resp)
    assert m["lexical_error"] == "OperationalError: no such table"


# --------------------------------------------------------------------------
# robustezza sui dati
# --------------------------------------------------------------------------

def test_final_ranked_come_lista_di_stringhe():
    resp = diag_response(final=["doc_a_chunk_0001"])
    resp["stages"]["final_ranked"]["items"] = ["doc_a_chunk_0001"]
    m = run_corpus.evaluate(corpus_row(), resp)
    assert m["exact_hits"] == 1


def test_chunk_duplicati_in_final_ranked_non_falsano_rank():
    """Un duplicato prima del gold sposterebbe il rank di uno e gonfierebbe il top-n."""
    resp = diag_response(final=["doc_x_chunk_0000", "doc_x_chunk_0000", "doc_a_chunk_0001"])
    m = run_corpus.evaluate(corpus_row(n=2), resp)
    assert m["primary_exact_rank"] == 2        # senza dedup sarebbe fuori dal top-2
    assert m["final_ranked_ids"] == ["doc_x_chunk_0000", "doc_a_chunk_0001"]
    assert any(w.startswith("final_ranked_duplicates_dropped:1") for w in m["warnings"])


def test_gold_vuoto_non_esplode_ed_e_escluso_dagli_aggregati():
    resp = diag_response(final=["doc_a_chunk_0000"])
    m = run_corpus.evaluate(corpus_row(gold=[], primary_gold=None), resp)
    assert m["gold_total"] == 0 and m["exact_hits"] == 0
    assert "gold_empty" in m["warnings"]
    agg = run_corpus.aggregate([{"id": "row1", "status": "ok", "metrics": m}])
    assert agg["rows_scored"] == 0
    assert agg["mean_exact_recall_at_row_n"] is None


def test_primary_gold_fuori_da_gold_e_segnalato_ma_misurato():
    row = corpus_row(gold=["doc_a_chunk_0000"], primary_gold="doc_b_chunk_0000")
    resp = diag_response(final=["doc_b_chunk_0000"])
    m = run_corpus.evaluate(row, resp)
    assert "primary_gold_not_in_gold" in m["warnings"]
    assert m["primary_exact_rank"] == 1
    assert m["exact_hits"] == 0


@pytest.mark.parametrize("n_val,expected,flag", [
    (None, 10, "n_missing_default_10"),
    (0, 10, "n_invalid:0_default_10"),
    (-3, 10, "n_invalid:-3_default_10"),
])
def test_n_mancante_o_non_positivo_ricade_sul_default(n_val, expected, flag):
    row = corpus_row(n=n_val)
    m = run_corpus.evaluate(row, diag_response(final=["doc_a_chunk_0001"]))
    assert m["n"] == expected
    assert flag in m["warnings"]


def test_n_maggiore_dei_risultati_disponibili():
    m = run_corpus.evaluate(corpus_row(n=10), diag_response(final=["doc_a_chunk_0001"]))
    assert m["exact_hits"] == 1
    assert any(w.startswith("n_gt_final_ranked:10>1") for w in m["warnings"])


def test_id_duplicati_nel_corpus_segnalati():
    rows = [corpus_row(id="dup"), corpus_row(id="dup", _lineno=2)]
    results, dups = run_corpus.run_corpus(
        rows, "http://x", 1.0,
        fetch=lambda url, t, first_request=False: diag_response(final=["doc_a_chunk_0001"]))
    assert dups == ["dup"]
    assert [r["occurrence"] for r in results] == [1, 2]


# --------------------------------------------------------------------------
# aggregati
# --------------------------------------------------------------------------

def test_macro_e_micro_divergono_come_devono():
    """Macro: ogni query pesa 1. Micro: gold trovati / gold totali."""
    m1 = run_corpus.evaluate(
        corpus_row(gold=["a_chunk_0000"], primary_gold="a_chunk_0000"),
        diag_response(final=["a_chunk_0000"]))                       # 1/1
    m2 = run_corpus.evaluate(
        corpus_row(gold=["b_chunk_000%d" % i for i in range(4)], primary_gold="b_chunk_0000"),
        diag_response(final=["b_chunk_0000"]))                       # 1/4
    agg = run_corpus.aggregate([{"id": "r1", "status": "ok", "metrics": m1},
                                {"id": "r2", "status": "ok", "metrics": m2}])
    assert agg["mean_exact_recall_at_row_n"] == 0.625                # (1 + 0.25) / 2
    assert agg["micro_exact_recall_at_row_n"] == 0.4                 # 2 / 5
    assert agg["micro_exact_gold_found"] == 2 and agg["micro_exact_gold_total"] == 5
    assert agg["timings_median_ms"]["total_ms"] == 118.0


def test_righe_in_errore_escluse_dagli_aggregati():
    m = run_corpus.evaluate(corpus_row(), diag_response(final=["doc_a_chunk_0001"]))
    agg = run_corpus.aggregate([{"id": "ok", "status": "ok", "metrics": m},
                                {"id": "ko", "status": "error", "error": "HTTP 500"}])
    assert agg["rows_scored"] == 1
    assert agg["mean_exact_recall_at_row_n"] == 1.0


# --------------------------------------------------------------------------
# URL
# --------------------------------------------------------------------------

def test_url_encoding_di_apostrofo_e_punto_interrogativo():
    """Senza encoding il `?` tronca la query string: il server risponde 200 su
    una query mutilata e i numeri sono sbagliati senza nessun errore."""
    url = run_corpus.build_url("http://127.0.0.1:8787",
                               "Come ho gestito la redundancy di Alex Persehais?", 10)
    assert url == ("http://127.0.0.1:8787/api/diag/hybrid-search"
                   "?q=Come%20ho%20gestito%20la%20redundancy%20di%20Alex%20Persehais%3F&n=10")
    assert url.count("?") == 1

    url2 = run_corpus.build_url("http://127.0.0.1:8787/", "chi ha l'usufrutto sulla casa di Ninfa", 5)
    assert "%27usufrutto" in url2 and "'" not in url2
    assert url2.startswith("http://127.0.0.1:8787/api/diag/")     # niente doppio slash


def test_url_encoding_e_reversibile_lato_server():
    from urllib.parse import unquote
    q = "dettagli dell'acquisto di via Marche 72 int. 425 & altro=1?"
    url = run_corpus.build_url("http://h", q, 10)
    qs = url.split("?", 1)[1]
    params = dict(p.split("=", 1) for p in qs.split("&") if "=" in p)   # parsing del server
    assert unquote(params["q"]) == q
    assert params["n"] == "10"


# --------------------------------------------------------------------------
# HTTP: server down, riga in errore, JSON invalido
# --------------------------------------------------------------------------

class _FakeResp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


def _install_urlopen(monkeypatch, handler):
    """handler(url) → bytes | Exception da sollevare. Registra le URL chiamate."""
    calls = []

    def fake_urlopen(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else req
        calls.append((url, timeout))
        out = handler(url)
        if isinstance(out, Exception):
            raise out
        return _FakeResp(out)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    return calls


def test_server_down_alla_prima_richiesta_alza_server_unreachable(monkeypatch):
    _install_urlopen(monkeypatch, lambda u: urllib.error.URLError(ConnectionRefusedError(61, "refused")))
    with pytest.raises(run_corpus.ServerUnreachable):
        run_corpus.fetch_diag("http://127.0.0.1:8787/x", 1.0, first_request=True)


def test_connessione_persa_a_meta_run_e_solo_una_riga_fallita(monkeypatch):
    _install_urlopen(monkeypatch, lambda u: urllib.error.URLError("connection reset"))
    with pytest.raises(run_corpus.RowFailure):
        run_corpus.fetch_diag("http://127.0.0.1:8787/x", 1.0, first_request=False)


def test_http_500_e_una_riga_fallita_non_un_server_down(monkeypatch):
    """Un 500 significa server VIVO: non deve abortire il run."""
    def handler(url):
        return urllib.error.HTTPError(url, 500, "err", {},
                                      io.BytesIO(b'{"error": "diagnostic failed"}'))
    _install_urlopen(monkeypatch, handler)
    with pytest.raises(run_corpus.RowFailure, match="HTTP 500"):
        run_corpus.fetch_diag("http://x/y", 1.0, first_request=True)


def test_json_invalido_e_una_riga_fallita(monkeypatch):
    _install_urlopen(monkeypatch, lambda u: b"<html>proxy error</html>")
    with pytest.raises(run_corpus.RowFailure, match="non JSON"):
        run_corpus.fetch_diag("http://x/y", 1.0, first_request=False)


def test_risposta_senza_stages_e_una_riga_fallita(monkeypatch):
    _install_urlopen(monkeypatch, lambda u: b'{"error": "invalid parameters"}')
    with pytest.raises(run_corpus.RowFailure, match="senza stages"):
        run_corpus.fetch_diag("http://x/y", 1.0, first_request=False)


# --------------------------------------------------------------------------
# main(): skip non-ready, exit code, --json
# --------------------------------------------------------------------------

CORPUS_LINES = [
    {"id": "riga_ready", "query": "chi ha l'usufrutto sulla casa di Ninfa?",
     "gold": ["doc_a_chunk_0001"], "primary_gold": "doc_a_chunk_0001", "n": 5, "status": "ready"},
    {"id": "riga_draft", "query": "query mai eseguita",
     "gold": ["doc_z_chunk_0000"], "primary_gold": "doc_z_chunk_0000", "n": 10, "status": "draft"},
    {"id": "riga_ready_2", "query": "a quanto ho venduto i mobili di via Marche 103?",
     "gold": ["doc_b_chunk_0000"], "primary_gold": "doc_b_chunk_0000", "n": 10, "status": "ready"},
]


@pytest.fixture
def corpus_file(tmp_path):
    p = tmp_path / "corpus.jsonl"
    p.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in CORPUS_LINES) + "\n",
                 encoding="utf-8")
    return str(p)


def _ok_handler(url):
    if "Ninfa" in url:
        return json.dumps(diag_response(final=["doc_a_chunk_0001"], lexical=["doc_a_chunk_0001"],
                                        derived_dropped=10)).encode()
    return json.dumps(diag_response(final=["doc_b_chunk_0000"], semantic=["doc_b_chunk_0000"],
                                    derived_dropped=8)).encode()


def test_main_salta_le_righe_non_ready_ed_esce_zero(monkeypatch, capsys, corpus_file):
    calls = _install_urlopen(monkeypatch, _ok_handler)
    rc = run_corpus.main(["--corpus", corpus_file, "--base-url", "http://127.0.0.1:8787"])
    assert rc == 0
    assert len(calls) == 2                                   # la riga draft non è mai interrogata
    out = capsys.readouterr().out
    assert "riga_draft" not in out
    assert "ready=2 saltate=1" in out
    assert "mean_exact_recall_at_row_n  = 1.0" in out


def test_main_timeout_passato_a_urlopen(monkeypatch, corpus_file):
    calls = _install_urlopen(monkeypatch, _ok_handler)
    run_corpus.main(["--corpus", corpus_file, "--timeout", "7.5"])
    assert {t for _, t in calls} == {7.5}


def test_main_server_down_esce_2_senza_metriche(monkeypatch, capsys, corpus_file):
    _install_urlopen(monkeypatch, lambda u: urllib.error.URLError(ConnectionRefusedError(61, "refused")))
    rc = run_corpus.main(["--corpus", corpus_file])
    assert rc == 2
    cap = capsys.readouterr()
    assert "non raggiungibile" in cap.err
    assert "mean_exact_recall_at_row_n" not in cap.out       # nessuna metrica prodotta


def test_main_riga_fallita_esce_1_ma_valuta_le_altre(monkeypatch, capsys, corpus_file):
    def handler(url):
        if "Ninfa" in url:
            return _ok_handler(url)
        return urllib.error.HTTPError(url, 500, "err", {}, io.BytesIO(b"{}"))
    _install_urlopen(monkeypatch, handler)
    rc = run_corpus.main(["--corpus", corpus_file])
    assert rc == 1
    cap = capsys.readouterr()
    assert "ERRORE: HTTP 500" in cap.out
    assert "1 righe fallite: riga_ready_2" in cap.err
    assert "riga_ready" in cap.out


def test_main_corpus_mancante_esce_2(monkeypatch, capsys, tmp_path):
    rc = run_corpus.main(["--corpus", str(tmp_path / "assente.jsonl")])
    assert rc == 2
    assert "corpus non leggibile" in capsys.readouterr().err


def test_json_report_deterministico_modulo_timings(monkeypatch, tmp_path, corpus_file):
    """Due run sulla stessa fixture → stesso JSON, tolti i campi volatili."""
    def run(dest, base_ms):
        def handler(url):
            resp = json.loads(_ok_handler(url))
            resp["timings"] = {k: v + base_ms for k, v in resp["timings"].items()}
            return json.dumps(resp).encode()
        _install_urlopen(monkeypatch, handler)
        assert run_corpus.main(["--corpus", corpus_file, "--json", str(dest)]) == 0
        return json.loads(dest.read_text(encoding="utf-8"))

    a = run(tmp_path / "a.json", 0.0)
    b = run(tmp_path / "b.json", 37.0)

    def strip_volatile(rep):
        rep = json.loads(json.dumps(rep))
        rep["metadata"].pop("timestamp")
        rep["aggregate"].pop("timings_median_ms")
        for r in rep["rows"]:
            r.pop("timings_ms", None)
        return rep

    assert strip_volatile(a) == strip_volatile(b)
    assert a["aggregate"]["timings_median_ms"] != b["aggregate"]["timings_median_ms"]
    # e il formato deve restare diffabile a colpo d'occhio: una chiave per riga
    assert (tmp_path / "a.json").read_text(encoding="utf-8").count("\n") > 20


def test_json_report_contiene_i_campi_richiesti(monkeypatch, tmp_path, corpus_file):
    _install_urlopen(monkeypatch, _ok_handler)
    dest = tmp_path / "out.json"
    assert run_corpus.main(["--corpus", corpus_file, "--json", str(dest)]) == 0
    rep = json.loads(dest.read_text(encoding="utf-8"))

    meta = rep["metadata"]
    assert meta["base_url"] == run_corpus.DEFAULT_BASE_URL
    assert meta["corpus"] == os.path.abspath(corpus_file)
    assert (meta["rows_total"], meta["rows_ready"], meta["rows_failed"]) == (3, 2, 0)
    assert meta["rank_base"] == 1 and meta["timestamp"]

    agg = rep["aggregate"]
    assert "mean_exact_recall_at_row_n" in agg and "mean_doc_recall_at_row_n" in agg
    assert "micro_exact_recall_at_row_n" in agg
    assert "recall@n" not in json.dumps(agg)          # mai il nome nudo: n varia per riga

    row = rep["rows"][0]
    assert row["id"] == "riga_ready" and row["n"] == 5
    assert row["final_ranked_ids"] == ["doc_a_chunk_0001"]
    assert (row["exact_hits"], row["doc_hits"]) == (1, 1)
    assert (row["primary_exact_rank"], row["primary_doc_rank"]) == (1, 1)
    assert row["gold_provenance"][0]["provenance"] == ["lexical"]
    assert row["lexical_derived_dropped"] == 10
    assert row["counts"]["rerank_pool"] == 1
    # esclude payload dei candidati e score
    assert "stages" not in row and "score" not in json.dumps(row)
