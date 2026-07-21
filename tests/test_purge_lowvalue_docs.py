"""Guardie del purge scripts/maintenance/purge_lowvalue_docs.py.

Tre invarianti, verificate sugli effetti (righe in db, id cancellati) e non
sugli exit code:

  (a) la denylist esclude per path ESATTO e non per prefisso/substring, e
      scan_folders() la rispetta;
  (b) il piano sui derivati si calcola sui facts VIVI e, una volta persistito,
      vince sul ricalcolo — e' l'unica cosa che tiene insieme un resume dopo
      che meta' dei facts vittima sono gia' spariti;
  (c) la quarantena toglie i derivati dal retrieval senza cancellarli, e
      rimette a NULL i superseded_by rimasti senza superseder.

Nessun test tocca db/: facts.sqlite3 e' ricreato in tmp_path e chromadb non
viene mai istanziato.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import types

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "maintenance"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))

import ingest_docs as ing                        # noqa: E402
import purge_lowvalue_docs as purge              # noqa: E402


# ============================================================================
# (a) DENYLIST — path esatti
# ============================================================================
def test_denylist_ignora_commenti_e_righe_vuote(tmp_path):
    f = tmp_path / "deny.txt"
    f.write_text("# commento\n\n/a/b.xlsx\n  /c/d.pdf  \n#/e/f.xlsx\n")

    assert ing.load_denylist(str(f)) == {"/a/b.xlsx", "/c/d.pdf"}


def test_denylist_assente_non_e_un_errore(tmp_path):
    assert ing.load_denylist(str(tmp_path / "manca.txt")) == set()


def test_denylist_e_esatta_non_substring(tmp_path, monkeypatch):
    """Un file col nome simile NON deve sparire: e' la scelta di design."""
    root = tmp_path / "w"
    (root / "sub").mkdir(parents=True)
    denied = root / "IC - January 2016.xlsx"
    similar = root / "IC - January 2016 RIVISTO.xlsx"
    other = root / "sub" / "IC - January 2016.xlsx"   # stesso nome, altro path
    for p in (denied, similar, other):
        p.write_bytes(b"x")

    deny = tmp_path / "deny.txt"
    deny.write_text(str(denied) + "\n")

    monkeypatch.setattr(ing, "WATCH_FOLDERS", [str(root)])
    monkeypatch.setattr(ing, "DENYLIST", ing.load_denylist(str(deny)))

    found = {f["filepath"] for f in ing.scan_folders()}

    assert str(denied) not in found, "il path denylistato deve sparire"
    assert str(similar) in found, "un nome simile NON e' denylistato"
    assert str(other) in found, "stesso basename ad altro path NON e' denylistato"


# ============================================================================
# (b) PIANO DERIVATI
# ============================================================================
VICTIM_HASH = "aaaaaaaaaaaa"
OTHER_HASH = "bbbbbbbbbbbb"


@pytest.fixture
def facts_db(tmp_path, monkeypatch):
    """facts.sqlite3 minimo: 2 facts vittima, 1 salvo, 2 derived, 1 superseded."""
    db = tmp_path / "facts.sqlite3"
    con = sqlite3.connect(db)
    con.execute("""CREATE TABLE facts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chunk_id TEXT NOT NULL,
        fact TEXT NOT NULL, category TEXT DEFAULT '', entity_tags TEXT DEFAULT '[]',
        event_date TEXT DEFAULT '', confidence REAL DEFAULT 1.0,
        superseded_by INTEGER DEFAULT NULL, superseded_at TEXT DEFAULT '',
        created_at TEXT NOT NULL, expires_at TEXT DEFAULT NULL,
        derived_from TEXT DEFAULT '', relation_type TEXT DEFAULT 'extracted',
        document_date TEXT DEFAULT '')""")

    def add(chunk_id, rel="extracted", df="", sup=None):
        cur = con.execute(
            "INSERT INTO facts (chunk_id, fact, created_at, relation_type,"
            " derived_from, superseded_by) VALUES (?,'f','2026-01-01',?,?,?)",
            (chunk_id, rel, df, sup))
        return cur.lastrowid

    v1 = add(f"doc_{VICTIM_HASH}_chunk_0000")
    v2 = add(f"doc_{VICTIM_HASH}_chunk_0001")
    safe = add(f"doc_{OTHER_HASH}_chunk_0000")
    d_hit = add("derived_20260101_0", rel="derived", df=json.dumps([v1, safe]))
    d_clean = add("derived_20260101_1", rel="derived", df=json.dumps([safe]))
    sup_victim = add(f"doc_{OTHER_HASH}_chunk_0001", sup=v2)   # superseder muore
    sup_alive = add(f"doc_{OTHER_HASH}_chunk_0002", sup=safe)  # superseder vive
    con.commit()
    con.close()

    monkeypatch.setattr(purge, "FACTS_DB", str(db))
    monkeypatch.setattr(purge, "DERIVED_PLAN_PATH", str(tmp_path / "plan.json"))
    monkeypatch.setattr(purge, "LOGS_DIR", str(tmp_path))
    return {"db": str(db), "v1": v1, "v2": v2, "safe": safe, "d_hit": d_hit,
            "d_clean": d_clean, "sup_victim": sup_victim, "sup_alive": sup_alive}


def test_piano_derivati_seleziona_solo_i_contaminati(facts_db):
    plan = purge.build_derived_plan([VICTIM_HASH])

    assert plan["victim_facts"] == 2
    ids = [q["id"] for q in plan["quarantine"]]
    assert ids == [facts_db["d_hit"]], "solo il derived che cita un fact vittima"

    q = plan["quarantine"][0]
    assert q["docs"] == [VICTIM_HASH]
    assert q["kept_refs"] == [facts_db["safe"]], "i ref vivi si conservano"
    assert q["lost"] == 1

    assert plan["superseded_reset"] == [facts_db["sup_victim"]], \
        "solo il fact il cui superseder viene cancellato"


def test_piano_persistito_vince_sul_ricalcolo(facts_db):
    """Il resume NON deve ricalcolare su facts gia' potati."""
    first = purge.load_or_build_derived_plan([VICTIM_HASH], log=lambda m: None)
    assert len(first["quarantine"]) == 1

    # simula il purge gia' avvenuto: i facts vittima non ci sono piu'
    con = sqlite3.connect(facts_db["db"])
    con.execute("DELETE FROM facts WHERE chunk_id LIKE ?",
                (f"doc_{VICTIM_HASH}_chunk_%",))
    con.commit()
    con.close()

    # un ricalcolo adesso troverebbe 0 contaminati: il piano su disco salva.
    assert purge.build_derived_plan([VICTIM_HASH])["quarantine"] == []
    resumed = purge.load_or_build_derived_plan([VICTIM_HASH], log=lambda m: None)
    assert len(resumed["quarantine"]) == 1
    assert resumed["quarantine"][0]["id"] == facts_db["d_hit"]


# ============================================================================
# (c) QUARANTENA
# ============================================================================
def test_quarantena_toglie_dal_retrieval_senza_cancellare(facts_db):
    plan = purge.build_derived_plan([VICTIM_HASH])
    quarantined, reset, pendenti = purge.apply_derived_plan(
        plan, log=lambda m: None)

    assert (quarantined, reset, pendenti) == (1, 1, 0)
    con = sqlite3.connect(facts_db["db"])

    # il fatto ESISTE ancora (quarantena, non cancellazione)
    row = con.execute("SELECT relation_type, derived_from FROM facts WHERE id=?",
                      (facts_db["d_hit"],)).fetchone()
    assert row is not None
    assert row[0] == purge.QUARANTINE_RELATION

    # ...ma la query di retrieval (relation_type='derived') non lo vede piu'
    served = con.execute(
        "SELECT id FROM facts WHERE relation_type='derived'").fetchall()
    assert [r[0] for r in served] == [facts_db["d_clean"]]

    # marker onesto al posto degli id morti
    marker = json.loads(row[1])
    assert marker["purged"] == purge.PURGE_DATE
    assert marker["docs"] == [VICTIM_HASH]
    assert marker["kept_refs"] == [facts_db["safe"]]

    # superseded_by resettato solo dove il superseder e' morto
    assert con.execute("SELECT superseded_by FROM facts WHERE id=?",
                       (facts_db["sup_victim"],)).fetchone()[0] is None
    assert con.execute("SELECT superseded_by FROM facts WHERE id=?",
                       (facts_db["sup_alive"],)).fetchone()[0] == facts_db["safe"]
    con.close()


def test_quarantena_idempotente(facts_db):
    plan = purge.build_derived_plan([VICTIM_HASH])
    purge.apply_derived_plan(plan, log=lambda m: None)
    purge.apply_derived_plan(plan, log=lambda m: None)   # resume dopo abort

    con = sqlite3.connect(facts_db["db"])
    n = con.execute("SELECT COUNT(*) FROM facts WHERE relation_type=?",
                    (purge.QUARANTINE_RELATION,)).fetchone()[0]
    marker = json.loads(con.execute(
        "SELECT derived_from FROM facts WHERE id=?",
        (facts_db["d_hit"],)).fetchone()[0])
    con.close()

    assert n == 1
    assert marker["kept_refs"] == [facts_db["safe"]], \
        "il secondo giro non deve riscrivere il marker su se stesso"


# ============================================================================
# CHECKPOINT
# ============================================================================
def test_checkpoint_sentinelle_di_fase(tmp_path, monkeypatch):
    monkeypatch.setattr(purge, "CHECKPOINT_PATH", str(tmp_path / "ck.done"))
    with open(purge.CHECKPOINT_PATH, "w") as fh:
        fh.write("/a/b.xlsx\tdeadbeef\n")
        fh.write(f"{purge.SENTINEL_SUMMARIES}\tdone\n")

    done = purge.load_checkpoint()
    assert done["/a/b.xlsx"] == "deadbeef"
    assert purge.SENTINEL_SUMMARIES in done
    assert purge.SENTINEL_DERIVED not in done


# ============================================================================
# AUDIT DI STATO — deve essere indifferente a CHI ha fatto il lavoro
# ============================================================================
def _fake_chroma(tmp_path, doc_hashes, extra_rows=0, summaries=()):
    """chroma.sqlite3 minimo: embeddings + embedding_metadata."""
    db = tmp_path / "chroma.sqlite3"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding_id TEXT)")
    con.execute("CREATE TABLE embedding_metadata (id INTEGER, key TEXT,"
                " string_value TEXT, int_value INT, float_value REAL)")
    n = 0
    for h in doc_hashes:
        for i in range(2):
            n += 1
            con.execute("INSERT INTO embeddings VALUES (?,?)",
                        (n, f"doc_{h}_chunk_{i:04d}"))
    for title, folder in summaries:
        n += 1
        con.execute("INSERT INTO embeddings VALUES (?,?)",
                    (n, f"doc_summary_{n}"))
        for k, v in (("title", title), ("folder", folder),
                     ("category", "doc_summary")):
            con.execute("INSERT INTO embedding_metadata VALUES (?,?,?,NULL,NULL)",
                        (n, k, v))
    for _ in range(extra_rows):
        n += 1
        con.execute("INSERT INTO embeddings VALUES (?,?)", (n, f"conv_{n}_chunk_0000"))
    con.commit()
    con.close()
    return str(db)


def test_audit_di_stato_verde_su_lavoro_diviso_fra_due_run(facts_db, tmp_path,
                                                           monkeypatch, capsys):
    """Lo stato finale e' pulito ma nessun singolo processo lo ha fatto tutto.

    Un audit a contatori direbbe "1 doc su 2" da entrambi i run. Quello di
    stato guarda i DB e vede il lavoro completo.
    """
    paths = ["/w/a.xlsx", "/w/b.xlsx"]
    in_reg = [(paths[0], VICTIM_HASH), (paths[1], OTHER_HASH)]

    # Chroma DOPO il purge: nessun chunk dei due doc, solo altro contenuto.
    monkeypatch.setattr(purge, "CHROMA_SQLITE",
                        _fake_chroma(tmp_path, [], extra_rows=7))
    monkeypatch.setattr(ing, "load_registry", lambda: {})   # entry rimosse

    # facts: il piano applicato per intero (da due run diversi, indistinguibile)
    plan = purge.build_derived_plan([VICTIM_HASH], kb_before=11,
                                    expected_deleted=4)
    purge.apply_derived_plan(plan, log=lambda m: None)

    failed = purge.audit_state(in_reg, plan, log=lambda m: None)
    assert failed == 0


def test_audit_di_stato_becca_residui_che_i_contatori_non_vedrebbero(
        facts_db, tmp_path, monkeypatch):
    """Un run "riuscito" che ha lasciato indietro chunk, summary e registry."""
    paths = ["/w/a.xlsx"]
    in_reg = [(paths[0], VICTIM_HASH)]
    monkeypatch.setattr(purge, "CHROMA_SQLITE", _fake_chroma(
        tmp_path, [VICTIM_HASH], extra_rows=3,
        summaries=[("a.xlsx", "W")]))
    monkeypatch.setattr(ing, "load_registry", lambda: {paths[0]: {"hash": "x"}})

    plan = purge.build_derived_plan([VICTIM_HASH], kb_before=99,
                                    expected_deleted=1)
    # quarantena NON applicata

    failed = purge.audit_state(in_reg, plan, log=lambda m: None)
    # chunk residui, summary residui, registry, kb, derivati, superseded
    assert failed == 6


# ============================================================================
# VALIDAZIONE DEL PIANO PERSISTITO
# ============================================================================
def _write_plan(path, **over):
    plan = {"schema_version": purge.PLAN_SCHEMA_VERSION,
            "purge_date": purge.PURGE_DATE, "hashes": [VICTIM_HASH],
            "kb_before": 10, "expected_deleted": 2,
            "victim_facts": 0, "quarantine": [], "superseded_reset": []}
    plan.update(over)
    with open(path, "w") as fh:
        json.dump(plan, fh)


def test_plan_con_perimetro_diverso_aborta(facts_db):
    _write_plan(purge.DERIVED_PLAN_PATH, hashes=[VICTIM_HASH, "cccccccccccc"])

    with pytest.raises(purge.PlanMismatch) as e:
        purge.load_or_build_derived_plan([VICTIM_HASH], log=lambda m: None)

    assert "perimetro diverso" in str(e.value)
    assert purge.DERIVED_PLAN_PATH in str(e.value), "deve dire QUALE file"
    assert "rm " in str(e.value), "deve dire come rimuoverlo consapevolmente"


def test_plan_con_schema_o_data_diversi_aborta(facts_db):
    _write_plan(purge.DERIVED_PLAN_PATH, schema_version=99)
    with pytest.raises(purge.PlanMismatch, match="schema_version"):
        purge.load_or_build_derived_plan([VICTIM_HASH], log=lambda m: None)

    _write_plan(purge.DERIVED_PLAN_PATH, purge_date="1999-01-01")
    with pytest.raises(purge.PlanMismatch, match="purge_date"):
        purge.load_or_build_derived_plan([VICTIM_HASH], log=lambda m: None)


def test_plan_coerente_viene_riusato(facts_db):
    _write_plan(purge.DERIVED_PLAN_PATH)
    plan = purge.load_or_build_derived_plan([VICTIM_HASH], log=lambda m: None)
    assert plan["hashes"] == [VICTIM_HASH]


# ============================================================================
# PERIMETRO STABILE FRA RESUME
# ============================================================================
def test_perimetro_include_i_gia_purgati_dal_checkpoint(monkeypatch):
    """Il purge cancella l'entry di registry: senza il checkpoint il perimetro
    si accorcerebbe a ogni run e la validazione del piano abortirebbe."""
    a, b = "/w/a.xlsx", "/w/b.xlsx"
    monkeypatch.setattr(ing, "load_denylist", lambda: {a, b})

    # stato iniziale: entrambi in registry
    per, off = purge.resolve_perimeter({a: {"hash": "h1"}, b: {"hash": "h2"}}, {})
    assert sorted(p for p, _ in per) == [a, b] and off == []

    # dopo aver purgato `a`: sparito dal registry, presente a checkpoint
    per, off = purge.resolve_perimeter({b: {"hash": "h2"}}, {a: "h1"})
    assert sorted(p for p, _ in per) == [a, b], "il perimetro resta di 2"
    assert off == [], "un gia'-purgato NON e' una copia mai indicizzata"

    # un path mai indicizzato e mai purgato resta fuori
    per, off = purge.resolve_perimeter({b: {"hash": "h2"}}, {})
    assert [p for p, _ in per] == [b] and off == [a]


# ============================================================================
# EXECUTE — checkpoint enforcement e uscita smoke
# ============================================================================
@pytest.fixture
def exec_env(facts_db, tmp_path, monkeypatch):
    """execute() con gate, lock e chromadb finti. Un solo doc in perimetro."""
    path = "/w/a.xlsx"
    monkeypatch.setattr(purge, "CHECKPOINT_PATH", str(tmp_path / "ck.done"))
    monkeypatch.setattr(purge, "CHROMA_SQLITE",
                        _fake_chroma(tmp_path, [VICTIM_HASH], extra_rows=5))
    monkeypatch.setattr(purge, "maintenance_state", lambda: (True, "test"))
    monkeypatch.setattr(purge, "_build_entity_index_running", lambda: (False, ""))
    monkeypatch.setattr(purge, "_entity_index_fingerprint", lambda: (True, 1))
    monkeypatch.setattr(purge, "_assert_entity_index_stable",
                        lambda *a, **k: None)
    monkeypatch.setattr(purge, "invalidate_chunk_sidecars",
                        lambda *a, **k: {"facts": 0})
    monkeypatch.setattr(ing, "acquire_single_instance_lock",
                        lambda *a, **k: open(os.devnull))
    monkeypatch.setattr(ing, "load_denylist", lambda: {path})
    monkeypatch.setattr(ing, "save_registry", lambda reg: None)
    monkeypatch.setattr(ing, "infer_folder", lambda p: "W")

    deleted: list = []

    class _Coll:
        def get(self, where=None, include=None, **kw):
            return {"ids": []}          # gia' pulito: la delete e' un no-op

        def delete(self, ids=None):
            deleted.extend(ids or [])

    fake = types.ModuleType("chromadb")
    fake.PersistentClient = lambda path: types.SimpleNamespace(
        get_collection=lambda name: _Coll())
    monkeypatch.setitem(sys.modules, "chromadb", fake)
    return types.SimpleNamespace(path=path, tmp=tmp_path, deleted=deleted)


def test_checkpoint_con_hash_diverso_aborta(exec_env, monkeypatch, capsys):
    """Mai skip silenzioso su hash cambiato in un'operazione distruttiva."""
    monkeypatch.setattr(ing, "load_registry",
                        lambda: {exec_env.path: {"hash": "n" * 64}})
    with open(purge.CHECKPOINT_PATH, "w") as fh:
        fh.write(f"{exec_env.path}\tvecchiohash12\n")

    rc = purge.execute(limit_docs=None)

    assert rc == 4
    err = capsys.readouterr().err
    assert "checkpoint incoerente" in err
    assert "vecchiohash12" in err and "nnnnnnnnnnnn" in err
    assert exec_env.deleted == [], "nessuna delete prima di aver capito"


def test_checkpoint_con_hash_uguale_salta_senza_abortire(exec_env, monkeypatch):
    monkeypatch.setattr(ing, "load_registry",
                        lambda: {exec_env.path: {"hash": "n" * 64}})
    with open(purge.CHECKPOINT_PATH, "w") as fh:
        fh.write(f"{exec_env.path}\t{'n' * 12}\n")

    rc = purge.execute(limit_docs=None)
    assert rc != 4, "stesso hash = gia' fatto, si prosegue"


def test_smoke_non_stampa_i_next_step_di_chiusura(exec_env, monkeypatch, capsys):
    """--limit-docs lascia la KB a meta': la sequenza di chiusura non va data."""
    monkeypatch.setattr(ing, "load_registry",
                        lambda: {exec_env.path: {"hash": "n" * 64}})

    purge.execute(limit_docs=1)
    out = capsys.readouterr().out

    assert "RUN PARZIALE" in out
    assert "NON lanciare il reconciler" in out
    assert "Rilanciare senza --limit-docs" in out
    # Si controllano le forme-COMANDO, non le parole: "niente kickstart" e'
    # l'istruzione giusta e deve poter comparire; e' il comando pronto da
    # incollare che non deve esserci.
    for vietato in ("[next]",
                    "reconcile_lexical_index.py --max-churn",
                    "launchctl kickstart",
                    "kill -USR2 <mcp_pid>"):
        assert vietato not in out, f"lo smoke non deve suggerire {vietato!r}"
    assert "niente kickstart" in out, "deve dire esplicitamente di NON farlo"


# ============================================================================
# SENTINELLE — sigillano una fase COMPLETA, mai una tentata
# ============================================================================
class _CollResiduo:
    """Collection che NON riesce a cancellare i summary: la delete e' un no-op.

    Simula il caso che conta: la fase gira, tenta, e lascia residui.
    """

    def __init__(self, residuo_ids):
        self.residuo = list(residuo_ids)
        self.delete_calls = 0

    def get(self, where=None, include=None, **kw):
        w = json.dumps(where or {})
        if "doc_summary" in w:
            return {"ids": list(self.residuo)}
        return {"ids": []}

    def delete(self, ids=None):
        self.delete_calls += 1          # "cancella" ma i residui restano


def test_sentinella_summary_non_scritta_se_restano_residui(exec_env, monkeypatch):
    """Sigillare una fase con residui la rende irreparabile a ogni resume."""
    coll = _CollResiduo(["doc_summary_1"])
    fake = types.ModuleType("chromadb")
    fake.PersistentClient = lambda path: types.SimpleNamespace(
        get_collection=lambda name: coll)
    monkeypatch.setitem(sys.modules, "chromadb", fake)
    monkeypatch.setattr(ing, "load_registry",
                        lambda: {exec_env.path: {"hash": "n" * 64}})

    rc = purge.execute(limit_docs=None)

    assert rc == 1, "residui = purge incompleto"
    assert coll.delete_calls > 0, "la fase deve aver PROVATO"
    done = purge.load_checkpoint()
    assert purge.SENTINEL_SUMMARIES not in done, \
        "con residui la sentinella non va scritta: il resume deve riprovare"


def test_sentinella_derived_non_scritta_se_un_fact_del_piano_e_sparito(
        exec_env, facts_db, monkeypatch):
    """UPDATE su id inesistente -> rowcount 0 -> fase incompleta, niente sigillo."""
    monkeypatch.setattr(ing, "load_registry",
                        lambda: {exec_env.path: {"hash": "n" * 64}})
    # il piano viene calcolato al primo run: lo pre-costruisco e poi cancello
    # il fatto che cita, come farebbe una delete andata oltre il previsto.
    plan = purge.build_derived_plan([VICTIM_HASH], kb_before=1,
                                    expected_deleted=0)
    assert len(plan["quarantine"]) == 1
    with open(purge.DERIVED_PLAN_PATH, "w") as fh:
        json.dump(plan, fh)
    con = sqlite3.connect(facts_db["db"])
    con.execute("DELETE FROM facts WHERE id=?", (facts_db["d_hit"],))
    con.commit(); con.close()

    purge.execute(limit_docs=None)

    assert purge.SENTINEL_DERIVED not in purge.load_checkpoint()


def test_audit_rosso_se_un_fact_del_piano_e_stato_cancellato(facts_db, tmp_path,
                                                             monkeypatch):
    """Il contratto e' quarantena, non delete: un id sparito deve fare rosso.

    Senza il controllo di ESISTENZA passerebbe: "nessuna riga in stato
    sbagliato" e' vero anche quando di righe non ce n'e' nessuna.
    """
    in_reg = [("/w/a.xlsx", VICTIM_HASH)]
    monkeypatch.setattr(purge, "CHROMA_SQLITE",
                        _fake_chroma(tmp_path, [], extra_rows=7))
    monkeypatch.setattr(ing, "load_registry", lambda: {})

    plan = purge.build_derived_plan([VICTIM_HASH], kb_before=11,
                                    expected_deleted=4)
    purge.apply_derived_plan(plan, log=lambda m: None)
    assert purge.audit_state(in_reg, plan, log=lambda m: None) == 0

    con = sqlite3.connect(facts_db["db"])
    con.execute("DELETE FROM facts WHERE id=?", (plan["quarantine"][0]["id"],))
    con.commit(); con.close()

    assert purge.audit_state(in_reg, plan, log=lambda m: None) == 1


# ============================================================================
# DRY-RUN RESUMABILE
# ============================================================================
def test_dry_run_gira_su_stato_post_resume_parziale(facts_db, tmp_path,
                                                    monkeypatch, capsys):
    """A purge interrotto il doc e' fuori registry ma nel checkpoint.

    E' lo stato in cui il dry-run serve di piu': deve girare, non esplodere.
    """
    purgato, rimasto = "/w/fatto.xlsx", "/w/damfare.xlsx"
    monkeypatch.setattr(purge, "CHECKPOINT_PATH", str(tmp_path / "ck.done"))
    monkeypatch.setattr(purge, "CHROMA_SQLITE",
                        _fake_chroma(tmp_path, [OTHER_HASH], extra_rows=4))
    monkeypatch.setattr(ing, "load_denylist", lambda: {purgato, rimasto})
    monkeypatch.setattr(ing, "load_registry",
                        lambda: {rimasto: {"hash": OTHER_HASH + "0" * 52,
                                           "chunks": 2}})
    with open(purge.CHECKPOINT_PATH, "w") as fh:
        fh.write(f"{purgato}\t{VICTIM_HASH}\n")

    rc = purge.dry_run(show=5)
    out = capsys.readouterr().out

    assert rc == 0
    assert "fatto.xlsx" in out and "damfare.xlsx" in out
    assert "gia' purgato" in out, "va detto perche' non e' piu' in registry"
    assert "2 indicizzati" in out, "il perimetro resta di 2, non si accorcia"


# ============================================================================
# SUPERSEDED — il piano interseca le vittime della delete facts
# ============================================================================
def test_fase_derived_completa_se_i_superseded_mancanti_erano_vittime(
        exec_env, facts_db, monkeypatch):
    """Un id del piano cancellato dalla fase sidecar non e' lavoro pendente.

    Riproduce il purge del 2026-07-21: 1.328 dei 1.353 superseded erano facts
    dei chunk purgati. Col criterio rowcount==len(piano) la fase restava
    eternamente "incompleta" e la sentinella non veniva mai scritta.
    """
    # l'hash di registry deve troncare a VICTIM_HASH, altrimenti il perimetro
    # non corrisponde al piano e validate_plan aborta (giustamente).
    monkeypatch.setattr(ing, "load_registry",
                        lambda: {exec_env.path: {"hash": VICTIM_HASH + "0" * 52}})
    plan = purge.build_derived_plan([VICTIM_HASH], kb_before=1,
                                    expected_deleted=0)
    assert plan["superseded_reset"] == [facts_db["sup_victim"]]
    with open(purge.DERIVED_PLAN_PATH, "w") as fh:
        json.dump(plan, fh)

    # la fase sidecar ha gia' cancellato quell'id (era un fact di chunk purgato)
    con = sqlite3.connect(facts_db["db"])
    con.execute("DELETE FROM facts WHERE id=?", (facts_db["sup_victim"],))
    con.commit(); con.close()

    purge.execute(limit_docs=None)

    assert purge.SENTINEL_DERIVED in purge.load_checkpoint(), \
        "niente da resettare != fase incompleta"
    # La tolleranza dell'audit sullo stesso caso e' verificata da
    # test_audit_resta_rosso_se_manca_un_derivato_della_quarantena, che parte
    # da uno stato post-purge pulito: qui la fixture exec_env lascia di
    # proposito chunk e registry sporchi, e l'audit direbbe rosso per quelli.


def test_audit_resta_rosso_se_manca_un_derivato_della_quarantena(
        facts_db, tmp_path, monkeypatch):
    """L'asimmetria non deve indebolire il lato quarantena.

    I derived hanno chunk_id 'derived_*': non possono essere vittime della
    delete per chunk doc, quindi uno sparito resta una perdita di dati.
    """
    in_reg = [("/w/a.xlsx", VICTIM_HASH)]
    monkeypatch.setattr(purge, "CHROMA_SQLITE",
                        _fake_chroma(tmp_path, [], extra_rows=7))
    monkeypatch.setattr(ing, "load_registry", lambda: {})
    plan = purge.build_derived_plan([VICTIM_HASH], kb_before=11,
                                    expected_deleted=4)
    purge.apply_derived_plan(plan, log=lambda m: None)

    # superseded sparito -> tollerato
    con = sqlite3.connect(facts_db["db"])
    con.execute("DELETE FROM facts WHERE id=?", (facts_db["sup_victim"],))
    con.commit(); con.close()
    assert purge.audit_state(in_reg, plan, log=lambda m: None) == 0

    # derivato sparito -> NON tollerato
    con = sqlite3.connect(facts_db["db"])
    con.execute("DELETE FROM facts WHERE id=?", (plan["quarantine"][0]["id"],))
    con.commit(); con.close()
    assert purge.audit_state(in_reg, plan, log=lambda m: None) == 1
