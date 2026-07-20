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
    quarantined, reset = purge.apply_derived_plan(plan, log=lambda m: None)

    assert (quarantined, reset) == (1, 1)
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
