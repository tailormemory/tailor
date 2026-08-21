"""Guardie di scripts/maintenance/realign_moved_docs.py.

Quattro invarianti, verificate sugli effetti e non sugli exit code:

  (a) le tre categorie (riallineabile / orfano / modificato) sono classificate
      correttamente su un registry sintetico;
  (b) il dry-run non scrive nulla — ne' in Chroma, ne' nel registry, ne'
      aprendo un PersistentClient;
  (c) un documento con piu' candidati su disco allo stesso hash viene SALTATO,
      non scelto a caso;
  (d) l'update tocca chunk e doc_summary insieme, perche' l'aggancio e'
      file_path e i due gruppi non condividono il conv_id.

Nessun test tocca db/: chroma.sqlite3 e' ricreato in tmp_path e chromadb non
viene mai istanziato davvero.
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "maintenance"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))

import ingest_docs as ing                        # noqa: E402
import realign_moved_docs as rmd                 # noqa: E402


# ============================================================================
# FIXTURE — un disco finto e una chroma.sqlite3 minima
# ============================================================================
def _fake_chroma(tmp_path, docs):
    """chroma.sqlite3 minima.

    `docs` = [(conv_id, file_path, n_chunk, n_summary)]. I summary hanno
    category='doc_summary' e un conv_id DIVERSO da quello dei chunk: e'
    esattamente la forma reale, ed e' il motivo per cui l'aggancio e'
    file_path.
    """
    db = tmp_path / "chroma.sqlite3"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY,"
                " embedding_id TEXT)")
    con.execute("CREATE TABLE embedding_metadata (id INTEGER, key TEXT,"
                " string_value TEXT, int_value INT, float_value REAL)")
    n = 0

    def row(eid, meta):
        nonlocal n
        n += 1
        con.execute("INSERT INTO embeddings VALUES (?,?)", (n, eid))
        for k, v in meta.items():
            con.execute("INSERT INTO embedding_metadata VALUES (?,?,?,NULL,NULL)",
                        (n, k, v))

    for conv_id, fp, n_chunk, n_sum in docs:
        for i in range(n_chunk):
            row(f"{conv_id}_chunk_{i:04d}",
                {"conv_id": conv_id, "file_path": fp, "category": "Cartella",
                 "title": os.path.basename(fp), "folder": "Vecchia",
                 "file_type": os.path.splitext(fp)[1]})
        for i in range(n_sum):
            row(f"doc_summary_{conv_id}_{i}",
                {"conv_id": "doc_summary_20260330", "file_path": fp,
                 "category": "doc_summary", "title": os.path.basename(fp),
                 "folder": "Vecchia", "file_type": os.path.splitext(fp)[1]})
    con.commit()
    con.close()
    return str(db)


HASH_SPOSTATO = "a" * 64
HASH_ORFANO = "b" * 64
HASH_VECCHIO = "c" * 64      # contenuto in KB del documento modificato


@pytest.fixture
def scena(tmp_path, monkeypatch):
    """Disco + registry + chroma che coprono tutte e tre le categorie."""
    # DUE watch folder come in produzione: il `file_path` dell'ingest e'
    # relativo al loro commonpath, quindi con una sola cartella la radice
    # sparirebbe dai path e il test non misurerebbe la forma reale.
    watch = tmp_path / "Personale" / "Wealth"
    altra = tmp_path / "Personale" / "Case"
    (watch / "Nuova").mkdir(parents=True)
    (watch / "Vecchia").mkdir(parents=True)
    altra.mkdir(parents=True)

    # RIALLINEABILE: il file e' sotto Nuova/, il registry lo crede in Vecchia/
    spostato = watch / "Nuova" / "atto.pdf"
    spostato.write_bytes(b"contenuto-spostato")
    # MODIFICATO: il path c'e', il contenuto no
    modificato = watch / "Vecchia" / "dettaglio.csv"
    modificato.write_bytes(b"contenuto-nuovo")

    monkeypatch.setattr(ing, "WATCH_FOLDERS", [str(watch), str(altra)])
    monkeypatch.setattr(ing, "DENYLIST", set())
    monkeypatch.setattr(rmd, "CHROMA_SQLITE", _fake_chroma(tmp_path, [
        ("doc_" + HASH_SPOSTATO[:12], "Wealth/Vecchia/atto.pdf", 3, 2),
        ("doc_" + HASH_ORFANO[:12], "Wealth/Vecchia/sparito.pdf", 4, 1),
        ("doc_" + HASH_VECCHIO[:12], "Wealth/Vecchia/dettaglio.csv", 5, 1),
    ]))

    h_spostato = ing.file_hash(str(spostato))
    registry = {
        str(watch / "Vecchia" / "atto.pdf"): {
            "hash": h_spostato, "filename": "atto.pdf", "chunks": 3},
        str(watch / "Vecchia" / "sparito.pdf"): {
            "hash": HASH_ORFANO, "filename": "sparito.pdf", "chunks": 4},
        str(modificato): {
            "hash": HASH_VECCHIO, "filename": "dettaglio.csv", "chunks": 5},
    }
    monkeypatch.setattr(ing, "load_registry", lambda: dict(registry))

    return {
        "watch": watch, "registry": registry,
        "spostato": str(spostato), "modificato": str(modificato),
        "vecchio_path": str(watch / "Vecchia" / "atto.pdf"),
        "h_spostato": h_spostato,
    }


def _plan(scena):
    _, by_hash = rmd.scan_disk(log=lambda *a, **k: None)
    scanned = {f["filepath"]: f for fs in by_hash.values() for f in fs}
    con = rmd._ro(rmd.CHROMA_SQLITE)
    try:
        return rmd.build_plan(scena["registry"], by_hash, scanned, con)
    finally:
        con.close()


# ============================================================================
# (a) LE TRE CATEGORIE
# ============================================================================
def test_tre_categorie_classificate_correttamente(scena):
    plan = _plan(scena)

    assert [r["old_path"] for r in plan["realign"]] == [scena["vecchio_path"]]
    assert plan["realign"][0]["new_path"] == scena["spostato"]

    assert [o["path"] for o in plan["orphans"]] == [
        str(scena["watch"] / "Vecchia" / "sparito.pdf")]

    assert [m["path"] for m in plan["modified"]] == [scena["modificato"]]
    mod = plan["modified"][0]
    # Il modificato va letto come DUE fatti: il vecchio obsoleto in KB e il
    # contenuto nuovo mai indicizzato. Entrambi devono comparire nel piano.
    assert mod["old_hash"] == HASH_VECCHIO
    assert mod["new_hash"] != HASH_VECCHIO
    assert mod["chunks"] == 5 and mod["summaries"] == 1
    assert mod["new_content_indexed"] is False

    assert plan["skipped"] == []
    assert plan["aligned"] == 0


def test_riallineabile_propone_i_campi_di_ogni_riga(scena):
    r = _plan(scena)["realign"][0]

    assert set(rmd.UPDATED_FIELDS) <= set(r["updates"])
    assert r["updates"]["file_path"] == "Wealth/Nuova/atto.pdf"
    assert r["updates"]["title"] == "atto.pdf"
    assert r["updates"]["file_type"] == ".pdf"
    assert r["updates"]["folder"] == ing.infer_folder(scena["spostato"])
    # `category` non sta fra i campi di ogni riga: e' chunk-only
    assert "category" not in r["updates"]
    assert r["chunk_updates"]["category"] == "Nuova"
    # 3 chunk + 2 summary: il conteggio del piano e' quello che l'update tocca
    assert (r["chunks"], r["summaries"]) == (3, 2)


# ============================================================================
# doc_type — si ricalcola dal nome, ma non si sovrascrive alla cieca
# ============================================================================
def test_doc_type_non_si_scrive_se_il_nome_non_cambia_verdetto(scena):
    """Spostamento puro: il nome e' identico, doc_type resta fuori dal piano.

    Sulla KB reale il 56% delle righe porta un doc_type che infer_doc_type
    non riproduce (arricchimento storico): riscriverlo qui sarebbe perdita.
    """
    r = _plan(scena)["realign"][0]

    assert rmd.DOC_TYPE_FIELD not in r["updates"]
    assert r["doc_type_old"] == r["doc_type_new"]


def test_doc_type_si_scrive_se_la_rinomina_cambia_verdetto():
    old, new, write = rmd.doc_type_update("scan_001.pdf", "Polizza Allianz.pdf")

    assert old == "documento"
    assert new == "polizza_assicurativa"
    assert write == "polizza_assicurativa"


def test_doc_type_invariato_conserva_il_valore_arricchito():
    """`business_plan` non e' in DOC_TYPE_PATTERNS: nessun ricalcolo lo
    rigenera, quindi il piano non deve nemmeno provarci."""
    old, new, write = rmd.doc_type_update("BP 2WATCH.xlsx", "BP 2WATCH v2.xlsx")

    assert old == new == "spreadsheet"
    assert write is None, "invariato = si conserva quello che c'e' in KB"


def test_path_allineato_non_finisce_in_nessuna_categoria(scena, monkeypatch):
    """Il caso normale — file al suo posto, hash invariato — non e' un caso."""
    fermo = scena["watch"] / "Vecchia" / "fermo.pdf"
    fermo.write_bytes(b"immobile")
    scena["registry"][str(fermo)] = {"hash": ing.file_hash(str(fermo)),
                                     "filename": "fermo.pdf"}

    plan = _plan(scena)

    assert plan["aligned"] == 1
    for cat in ("realign", "orphans", "modified", "skipped"):
        assert str(fermo) not in [
            d.get("path") or d.get("old_path") for d in plan[cat]]


# ============================================================================
# (b) IL DRY-RUN NON SCRIVE
# ============================================================================
def test_dry_run_non_scrive_nulla(scena, monkeypatch, capsys):
    """Ne' Chroma, ne' registry, ne' un PersistentClient aperto."""
    def _vietato(*a, **k):
        raise AssertionError("il dry-run non deve scrivere")

    monkeypatch.setattr(ing, "save_registry", _vietato)

    import chromadb
    monkeypatch.setattr(chromadb, "PersistentClient", _vietato)

    prima = open(rmd.CHROMA_SQLITE, "rb").read()
    mtime = os.path.getmtime(rmd.CHROMA_SQLITE)

    rc = rmd.dry_run(show=10)

    assert rc == 0
    assert open(rmd.CHROMA_SQLITE, "rb").read() == prima
    assert os.path.getmtime(rmd.CHROMA_SQLITE) == mtime

    out = capsys.readouterr().out
    assert "RIALLINEABILI         : 1" in out
    assert "ORFANI                : 1" in out
    assert "MODIFICATI            : 1" in out
    assert scena["spostato"] in out


# ============================================================================
# (c) PIU' CANDIDATI = SI SALTA
# ============================================================================
def test_piu_candidati_stesso_hash_viene_saltato(scena):
    """Quale sia il path canonico non lo decide uno script."""
    gemello = scena["watch"] / "Nuova" / "atto (copia).pdf"
    gemello.write_bytes(open(scena["spostato"], "rb").read())

    plan = _plan(scena)

    assert plan["realign"] == [], "con 2 candidati non si sceglie"
    saltati = [s for s in plan["skipped"]
               if s["old_path"] == scena["vecchio_path"]]
    assert len(saltati) == 1
    assert "2 candidati" in saltati[0]["reason"]
    assert set(saltati[0]["candidates"]) == {scena["spostato"], str(gemello)}


def test_candidato_gia_in_registry_non_e_uno_spostamento(scena):
    """Hash gia' registrato sotto il suo path = duplicato, non un move."""
    scena["registry"][scena["spostato"]] = {
        "hash": scena["h_spostato"], "filename": "atto.pdf"}

    plan = _plan(scena)

    assert plan["realign"] == []
    saltati = [s for s in plan["skipped"]
               if s["old_path"] == scena["vecchio_path"]]
    assert len(saltati) == 1
    assert "gia' in registry" in saltati[0]["reason"]


# ============================================================================
# (d) L'UPDATE TOCCA CHUNK E SUMMARY INSIEME
# ============================================================================
class _Coll:
    """Collection finta con get(where={'file_path': ...}) / update()."""

    def __init__(self, rows):
        self.rows = dict(rows)          # id -> metadata
        self.updated_ids: list[str] = []

    def get(self, where=None, include=None, **kw):
        fp = (where or {}).get("file_path")
        ids = [i for i, m in self.rows.items() if m.get("file_path") == fp]
        out = {"ids": ids}
        if include and "metadatas" in include:
            out["metadatas"] = [dict(self.rows[i]) for i in ids]
        return out

    def update(self, ids=None, metadatas=None):
        for i, m in zip(ids or [], metadatas or []):
            self.rows[i] = dict(m)
            self.updated_ids.append(i)


def test_update_tocca_chunk_e_summary_insieme():
    """L'aggancio e' file_path: i doc_summary hanno un altro conv_id e
    andrebbero persi da qualunque update per conv_id."""
    old_fp = "Wealth/Vecchia/atto.pdf"
    rows = {}
    for i in range(3):
        rows[f"doc_aaaaaaaaaaaa_chunk_{i:04d}"] = {
            "conv_id": "doc_aaaaaaaaaaaa", "file_path": old_fp,
            "title": "atto.pdf", "folder": "Vecchia", "file_type": ".pdf",
            "category": "Vecchia", "chunk_index": i}
    for i in range(2):
        rows[f"doc_summary_x_{i}"] = {
            "conv_id": "doc_summary_20260330", "file_path": old_fp,
            "category": "doc_summary", "title": "atto.pdf",
            "folder": "Vecchia", "file_type": ".pdf"}
    coll = _Coll(rows)

    r = {"old_file_path": old_fp,
         "updates": {"file_path": "Wealth/Nuova/rinominato.pdf",
                     "folder": "Nuova", "title": "rinominato.pdf",
                     "file_type": ".pdf"},
         "chunk_updates": {"category": "Nuova"}}

    n = rmd._apply_one(coll, r, log=lambda *a, **k: None)

    assert n == 5, "3 chunk + 2 summary"
    assert sorted(coll.updated_ids) == sorted(rows)
    assert not [m for m in coll.rows.values() if m["file_path"] == old_fp]
    for m in coll.rows.values():
        assert m["file_path"] == "Wealth/Nuova/rinominato.pdf"
        assert m["title"] == "rinominato.pdf"
        assert m["folder"] == "Nuova"
    # I metadati NON in perimetro sopravvivono: si riscrive il dict completo,
    # non si azzera quello che non si aggiorna.
    assert coll.rows["doc_aaaaaaaaaaaa_chunk_0002"]["chunk_index"] == 2
    assert coll.rows["doc_summary_x_0"]["category"] == "doc_summary"


# ============================================================================
# category — due semantiche sotto lo stesso nome, un solo update
# ============================================================================
def test_category_cambia_sui_chunk_e_resta_doc_summary_sui_summary():
    """Sui chunk `category` e' una cartella, sui summary e' il marcatore di
    tipo. Sovrascriverla sui summary li renderebbe irriconoscibili a
    purge_lowvalue_docs, garbage_collect e create_conv_summaries."""
    old_fp = "Wealth/Vecchia/atto.pdf"
    rows = {}
    for i in range(3):
        rows[f"doc_aaaaaaaaaaaa_chunk_{i:04d}"] = {
            "conv_id": "doc_aaaaaaaaaaaa", "file_path": old_fp,
            "category": "Vecchia", "title": "atto.pdf",
            "folder": "Vecchia", "file_type": ".pdf"}
    for i in range(2):
        rows[f"doc_summary_x_{i}"] = {
            "conv_id": "doc_summary_20260330", "file_path": old_fp,
            "category": "doc_summary", "title": "atto.pdf",
            "folder": "Vecchia", "file_type": ".pdf"}
    coll = _Coll(rows)

    n = rmd._apply_one(coll, {
        "old_file_path": old_fp,
        "updates": {"file_path": "Wealth/Nuova/atto.pdf", "folder": "Nuova",
                    "title": "atto.pdf", "file_type": ".pdf",
                    "doc_type": "atto_notarile"},
        "chunk_updates": {"category": "Nuova"}},
        log=lambda *a, **k: None)

    assert n == 5
    chunk = [m for k, m in coll.rows.items() if "_chunk_" in k]
    summary = [m for k, m in coll.rows.items() if k.startswith("doc_summary_")]
    assert len(chunk) == 3 and len(summary) == 2

    # chunk: category ricalcolata dal nuovo path
    assert {m["category"] for m in chunk} == {"Nuova"}
    # summary: marcatore intatto
    assert {m["category"] for m in summary} == {"doc_summary"}
    # doc_type invece va OVUNQUE, summary inclusi
    assert {m["doc_type"] for m in coll.rows.values()} == {"atto_notarile"}
    # e i campi di ogni riga sono cambiati su tutte e cinque
    assert {m["folder"] for m in coll.rows.values()} == {"Nuova"}
    assert {m["file_path"] for m in coll.rows.values()} == {"Wealth/Nuova/atto.pdf"}


def test_marcatore_summary_perso_solleva():
    """La guardia sul marcatore si misura, non si assume."""
    old_fp = "W/vecchio.pdf"

    class _Sbadata(_Coll):
        def update(self, ids=None, metadatas=None):
            # applica tutto ANCHE ai summary: il bug che il test previene
            for i, m in zip(ids or [], metadatas or []):
                self.rows[i] = {**m, "category": "Nuova"}
                self.updated_ids.append(i)

    coll = _Sbadata({
        "c0": {"file_path": old_fp, "category": "Vecchia", "title": "v",
               "folder": "W", "file_type": ".pdf"},
        "doc_summary_x_0": {"file_path": old_fp, "category": "doc_summary",
                            "title": "v", "folder": "W", "file_type": ".pdf"},
    })

    with pytest.raises(rmd.RealignInconsistent,
                       match="marcatore doc_summary perso"):
        rmd._apply_one(coll, {
            "old_file_path": old_fp,
            "updates": {"file_path": "W/nuovo.pdf", "folder": "W",
                        "title": "nuovo.pdf", "file_type": ".pdf"},
            "chunk_updates": {"category": "Nuova"}},
            log=lambda *a, **k: None)


def test_update_a_batch_copre_tutte_le_righe(monkeypatch):
    """Il batching non deve lasciare indietro la coda."""
    monkeypatch.setattr(rmd, "UPDATE_BATCH", 2)
    old_fp = "W/vecchio.pdf"
    rows = {f"c{i}": {"file_path": old_fp, "title": "vecchio.pdf",
                      "folder": "W", "file_type": ".pdf"} for i in range(7)}
    coll = _Coll(rows)

    n = rmd._apply_one(coll, {
        "old_file_path": old_fp,
        "updates": {"file_path": "W/nuovo.pdf", "folder": "W",
                    "title": "nuovo.pdf", "file_type": ".pdf"}},
        log=lambda *a, **k: None)

    assert n == 7
    assert len(coll.updated_ids) == 7


def test_update_fallito_a_meta_solleva_invece_di_proseguire():
    """Meta' documento al path nuovo e meta' al vecchio non e' uno stato."""
    old_fp = "W/vecchio.pdf"

    class _Rotta(_Coll):
        def update(self, ids=None, metadatas=None):
            pass                        # accetta e non fa niente

    coll = _Rotta({f"c{i}": {"file_path": old_fp, "title": "v",
                             "folder": "W", "file_type": ".pdf"}
                   for i in range(3)})

    with pytest.raises(rmd.RealignInconsistent,
                       match="ancora al vecchio file_path"):
        rmd._apply_one(coll, {
            "old_file_path": old_fp,
            "updates": {"file_path": "W/nuovo.pdf", "folder": "W",
                        "title": "nuovo.pdf", "file_type": ".pdf"}},
            log=lambda *a, **k: None)


# ============================================================================
# doc_type — il ripiego non sovrascrive mai una classificazione
# ============================================================================
def test_doc_type_da_nome_specifico_a_generico_non_scrive():
    """Il degrado che la regola doveva impedire e non impediva.

    'Contratto locazione.pdf' -> 'scan.pdf' da' ('contratto', 'documento'):
    la sola divergenza dei verdetti scriverebbe il ripiego sopra la
    classificazione. Il doc_type in KB viene dal contenuto, non dal titolo.
    """
    old, new, write = rmd.doc_type_update("Contratto locazione.pdf", "scan.pdf")

    assert (old, new) == ("contratto", "documento")
    assert write is None, "il ripiego non sovrascrive una classificazione"


def test_doc_type_ripiego_di_estensione_non_scrive():
    """Non riguarda solo 'documento': anche 'spreadsheet' e' un ripiego.

    'Contratto locazione.xlsx' -> 'scan.xlsx' da' ('contratto',
    'spreadsheet') — stessa perdita, travestita da tipo specifico.
    """
    old, new, write = rmd.doc_type_update("Contratto locazione.xlsx",
                                          "scan.xlsx")

    assert (old, new) == ("contratto", "spreadsheet")
    assert write is None


def test_doc_type_scrive_solo_se_il_nuovo_nome_classifica():
    """La condizione positiva resta: rinomina verso un nome che classifica."""
    assert rmd.name_classifies("Polizza Allianz.pdf")
    assert not rmd.name_classifies("scan.pdf")

    _, _, write = rmd.doc_type_update("scan_001.pdf", "Polizza Allianz.pdf")
    assert write == "polizza_assicurativa"


# ============================================================================
# EXECUTE — un documento spezzato ferma il run
# ============================================================================
@pytest.fixture
def exec_env(tmp_path, monkeypatch):
    """execute() con gate, lock e chromadb finti. DUE documenti riallineabili.

    Servono due: con uno solo non si puo' distinguere "si e' fermato" da
    "aveva finito".
    """
    watch = tmp_path / "Personale" / "Wealth"
    altra = tmp_path / "Personale" / "Case"
    (watch / "Nuova").mkdir(parents=True)
    (watch / "Vecchia").mkdir(parents=True)
    altra.mkdir(parents=True)

    primo = watch / "Nuova" / "aaa.pdf"
    primo.write_bytes(b"primo")
    secondo = watch / "Nuova" / "bbb.pdf"
    secondo.write_bytes(b"secondo")

    monkeypatch.setattr(ing, "WATCH_FOLDERS", [str(watch), str(altra)])
    monkeypatch.setattr(ing, "DENYLIST", set())

    h1 = ing.file_hash(str(primo))
    h2 = ing.file_hash(str(secondo))
    monkeypatch.setattr(rmd, "CHROMA_SQLITE", _fake_chroma(tmp_path, [
        ("doc_" + h1[:12], "Wealth/Vecchia/aaa.pdf", 3, 1),
        ("doc_" + h2[:12], "Wealth/Vecchia/bbb.pdf", 2, 1),
    ]))

    registry = {
        str(watch / "Vecchia" / "aaa.pdf"): {"hash": h1, "filename": "aaa.pdf"},
        str(watch / "Vecchia" / "bbb.pdf"): {"hash": h2, "filename": "bbb.pdf"},
    }
    monkeypatch.setattr(ing, "load_registry", lambda: dict(registry))

    saved: list[dict] = []
    monkeypatch.setattr(ing, "save_registry", lambda reg: saved.append(dict(reg)))
    monkeypatch.setattr(ing, "acquire_single_instance_lock",
                        lambda *a, **k: open(os.devnull))

    monkeypatch.setattr(rmd, "maintenance_state", lambda: (True, "test"))
    monkeypatch.setattr(rmd, "_build_entity_index_running", lambda: (False, ""))
    monkeypatch.setattr(rmd, "_entity_index_fingerprint", lambda: (True, 1))
    monkeypatch.setattr(rmd, "_assert_entity_index_stable", lambda *a, **k: None)
    monkeypatch.setattr(rmd, "LOGS_DIR", str(tmp_path / "logs"))

    import types
    fake = types.ModuleType("chromadb")
    fake.PersistentClient = lambda path: types.SimpleNamespace(
        get_collection=lambda name: object())
    monkeypatch.setitem(sys.modules, "chromadb", fake)

    return types.SimpleNamespace(
        saved=saved, registry=registry,
        vecchio1=str(watch / "Vecchia" / "aaa.pdf"),
        vecchio2=str(watch / "Vecchia" / "bbb.pdf"),
        nuovo1=str(primo), nuovo2=str(secondo))


def test_execute_si_ferma_su_documento_incoerente(exec_env, monkeypatch, capsys):
    """RealignInconsistent = documento SPEZZATO, non documento saltato.

    Proseguire ne accumulerebbe altri mentre il log dice che va tutto bene.
    """
    visti: list[str] = []

    def _boom(collection, r, log=print):
        visti.append(r["old_path"])
        raise rmd.RealignInconsistent("2 righe ancora al vecchio file_path")

    monkeypatch.setattr(rmd, "_apply_one", _boom)

    rc = rmd.execute(show=10)

    assert rc == 4, "exit dedicato, non 0 e non lo stesso di MaintenanceLost"
    assert len(visti) == 1, "il secondo documento non deve essere toccato"
    assert exec_env.saved == [], "nessun registry salvato"

    err = capsys.readouterr().err
    assert "INCOERENTE" in err
    assert "il run si ferma qui" in err
    # Deve dire QUALE documento e che il registry non e' stato aggiornato
    assert visti[0] in err
    assert "registry NON e' stato aggiornato" in err
    # E non deve piu' esistere il messaggio che rassicurava
    assert "documento SALTATO" not in err


def test_execute_prosegue_su_fallimento_prima_di_ogni_scrittura(
        exec_env, monkeypatch, capsys):
    """RealignPreflight: la KB e' intatta, saltare e' onesto."""
    visti: list[str] = []

    def _preflight(collection, r, log=print):
        visti.append(r["old_path"])
        raise rmd.RealignPreflight("nessuna riga con file_path X")

    monkeypatch.setattr(rmd, "_apply_one", _preflight)

    rc = rmd.execute(show=10)

    assert len(visti) == 2, "entrambi tentati: nessuno ha scritto"
    assert rc == 1, "run completo ma con documenti saltati"
    assert exec_env.saved == []
    out = capsys.readouterr().out
    assert "SALTATO (nessuna scrittura)" in out
    assert "SALTATI: 2 documenti" in out


# ============================================================================
# RESUME — KB gia' aggiornata, registry ancora al vecchio path
# ============================================================================
def test_resume_kb_aggiornata_registry_vecchio_si_chiude_senza_errore(
        tmp_path, monkeypatch):
    """La finestra di crash fra _apply_one e save_registry e' idempotente.

    Il registry si salva DOPO la KB di proposito, quindi questo e' lo stato di
    interruzione ATTESO. resolve_old_file_path recupera il file_path via
    conv_id e trova gia' quello nuovo: old_fp == new_fp. Prima finiva nella
    verifica sui residui e falliva citando il path NUOVO come "vecchio".
    """
    watch = tmp_path / "Personale" / "Wealth"
    altra = tmp_path / "Personale" / "Case"
    (watch / "Nuova").mkdir(parents=True)
    (watch / "Vecchia").mkdir(parents=True)
    altra.mkdir(parents=True)
    spostato = watch / "Nuova" / "atto.pdf"
    spostato.write_bytes(b"contenuto")

    monkeypatch.setattr(ing, "WATCH_FOLDERS", [str(watch), str(altra)])
    monkeypatch.setattr(ing, "DENYLIST", set())

    h = ing.file_hash(str(spostato))
    NEW_FP = "Wealth/Nuova/atto.pdf"
    # KB GIA' al nuovo file_path — l'update era andato a buon fine
    monkeypatch.setattr(rmd, "CHROMA_SQLITE",
                        _fake_chroma(tmp_path, [("doc_" + h[:12], NEW_FP, 3, 1)]))

    vecchio = str(watch / "Vecchia" / "atto.pdf")
    registry = {vecchio: {"hash": h, "filename": "atto.pdf"}}

    _, by_hash = rmd.scan_disk(log=lambda *a, **k: None)
    scanned = {f["filepath"]: f for fs in by_hash.values() for f in fs}
    con = rmd._ro(rmd.CHROMA_SQLITE)
    try:
        plan = rmd.build_plan(registry, by_hash, scanned, con)
    finally:
        con.close()

    r = plan["realign"][0]
    assert r["old_fp_source"] == "conv_id"
    assert r["old_file_path"] == r["updates"]["file_path"] == NEW_FP

    rows = {f"doc_{h[:12]}_chunk_{i:04d}": {
        "conv_id": f"doc_{h[:12]}", "file_path": NEW_FP, "category": "Nuova",
        "title": "atto.pdf", "folder": "Wealth", "file_type": ".pdf"}
        for i in range(3)}
    rows[f"doc_summary_{h[:12]}_0"] = {
        "conv_id": "doc_summary_20260330", "file_path": NEW_FP,
        "category": "doc_summary", "title": "atto.pdf",
        "folder": "Wealth", "file_type": ".pdf"}
    coll = _Coll(rows)

    righe = []
    n = rmd._apply_one(coll, r, log=righe.append)

    assert n == 4, "3 chunk + 1 summary, riapplicati"
    assert any("[resume]" in x for x in righe), "il caso va dichiarato nel log"
    # Idempotente: lo stato finale e' quello atteso, marcatore compreso
    assert {m["file_path"] for m in coll.rows.values()} == {NEW_FP}
    assert coll.rows[f"doc_summary_{h[:12]}_0"]["category"] == "doc_summary"
    assert {m["category"] for k, m in coll.rows.items()
            if "_chunk_" in k} == {"Nuova"}


def test_resume_e_ripetibile_quante_volte_serve(tmp_path, monkeypatch):
    """Idempotente vuol dire anche alla seconda e alla terza passata."""
    NEW_FP = "Wealth/Nuova/atto.pdf"
    rows = {f"c{i}": {"file_path": NEW_FP, "category": "Nuova",
                      "title": "atto.pdf", "folder": "Wealth",
                      "file_type": ".pdf"} for i in range(3)}
    coll = _Coll(rows)
    r = {"old_file_path": NEW_FP,
         "updates": {"file_path": NEW_FP, "folder": "Wealth",
                     "title": "atto.pdf", "file_type": ".pdf"},
         "chunk_updates": {"category": "Nuova"}}

    for _ in range(3):
        assert rmd._apply_one(coll, r, log=lambda *a, **k: None) == 3
    assert {m["file_path"] for m in coll.rows.values()} == {NEW_FP}
