"""Tests per scripts.maintenance.reconcile_lexical_index (P6 step 2).

Il reconciler legge la KB via SQL diretto (nessun PersistentClient, nessun
chromadb importato) → i test costruiscono un chroma.sqlite3 sintetico minimale
sotto tmp_path con lo schema reale (collections / segments / embeddings /
embedding_metadata) ed esercitano bootstrap, delete, update, guardiano e
sanitizzazione senza toccare la KB viva.

La logica set/hash è pura: build_plan / check_churn / compute_fingerprint si
testano su dict, senza I/O.
"""

from __future__ import annotations

import sqlite3

import pytest

from scripts.maintenance.reconcile_lexical_index import (
    DOCUMENT_KEY,
    FTS_DIVERGENT,
    ReconcilePlan,
    as_text,
    build_plan,
    check_churn,
    compute_fingerprint,
    is_bootstrap,
    open_index,
    read_index_state,
    reconcile,
)

COLLECTION_ID = "col-tailor-kb-v2"
SEGMENT_METADATA = "seg-metadata"
SEGMENT_OTHER = "seg-vector"


# ============================================================
# FIXTURES — chroma.sqlite3 sintetico
# ============================================================


def _make_chroma(path, chunks, collection_name="tailor_kb_v2"):
    """Costruisce un chroma.sqlite3 minimale.

    `chunks` = {chunk_id: {"document": ..., "title": ..., ...}}. Le chiavi
    assenti/None non producono riga in embedding_metadata (metadato assente,
    che è il caso reale di folder/email_from).
    """
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE collections (id TEXT PRIMARY KEY, name TEXT);
        CREATE TABLE segments (id TEXT PRIMARY KEY, collection TEXT, scope TEXT);
        CREATE TABLE embeddings (
            id INTEGER PRIMARY KEY, segment_id TEXT, embedding_id TEXT
        );
        CREATE TABLE embedding_metadata (
            id INTEGER, key TEXT, string_value TEXT,
            int_value INTEGER, float_value REAL, bool_value INTEGER
        );
        """
    )
    conn.execute("INSERT INTO collections VALUES (?, ?)", (COLLECTION_ID, collection_name))
    conn.execute(
        "INSERT INTO segments VALUES (?, ?, 'METADATA')", (SEGMENT_METADATA, COLLECTION_ID)
    )
    conn.execute("INSERT INTO segments VALUES (?, ?, 'VECTOR')", (SEGMENT_OTHER, COLLECTION_ID))
    _write_chunks(conn, chunks)
    conn.commit()
    conn.close()


def _write_chunks(conn, chunks, segment_id=SEGMENT_METADATA, start_rowid=1):
    rowid = start_rowid
    for chunk_id, fields in chunks.items():
        conn.execute(
            "INSERT INTO embeddings (id, segment_id, embedding_id) VALUES (?, ?, ?)",
            (rowid, segment_id, chunk_id),
        )
        for name, value in fields.items():
            if value is None:
                continue
            key = DOCUMENT_KEY if name == "document" else name
            conn.execute(
                "INSERT INTO embedding_metadata (id, key, string_value) VALUES (?, ?, ?)",
                (rowid, key, value),
            )
        rowid += 1
    return rowid


def _rewrite_kb(path, chunks):
    """Riscrive da zero il contenuto KB (simula ingest / delete reali)."""
    conn = sqlite3.connect(str(path))
    conn.execute("DELETE FROM embedding_metadata")
    conn.execute("DELETE FROM embeddings")
    _write_chunks(conn, chunks)
    conn.commit()
    conn.close()


def _chunk(n, document=None, **overrides):
    fields = {
        "document": document if document is not None else f"contenuto del chunk {n}",
        "title": f"documento {n}",
        "folder": "Condominio",
        "doc_type": "pdf",
        "email_from": f"mittente{n}@example.com",
        "source": "docs",
        "file_path": f"Case/Viale Ippocrate/Rate mutuo/rata_{n}.pdf",
    }
    fields.update(overrides)
    return fields


@pytest.fixture
def kb(tmp_path):
    path = tmp_path / "chroma.sqlite3"
    _make_chroma(path, {f"c{n}": _chunk(n) for n in range(1, 11)})
    return path


@pytest.fixture
def index_path(tmp_path):
    return tmp_path / "lexical_index.sqlite3"


def _run(kb, index_path, max_churn=0.20, dry_run=False):
    return reconcile(str(kb), str(index_path), max_churn, dry_run=dry_run)


def _fts_ids(index_path, match):
    conn = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT chunk_id FROM lexical_fts WHERE lexical_fts MATCH ?", (match,))
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()


# ============================================================
# LOGICA PURA
# ============================================================


class TestPureLogic:
    def test_build_plan_su_indice_vuoto_e_tutto_insert(self):
        plan = build_plan({"a": "fp1", "b": "fp2"}, {})
        assert sorted(plan.inserted) == ["a", "b"]
        assert plan.deleted == [] and plan.updated == [] and plan.unchanged == []

    def test_build_plan_classifica_le_tre_divergenze(self):
        kb = {"stay": "fp", "changed": "fp-new", "new": "fp"}
        index = {"stay": "fp", "changed": "fp-old", "gone": "fp"}
        plan = build_plan(kb, index)
        assert plan.inserted == ["new"]
        assert plan.updated == ["changed"]
        assert plan.deleted == ["gone"]
        assert plan.unchanged == ["stay"]
        assert plan.churn == 3  # unchanged non è churn

    def test_fingerprint_stabile_e_sensibile_a_ogni_campo(self):
        base = _chunk(1)
        assert compute_fingerprint(base) == compute_fingerprint(dict(base))
        for field in (
            "document",
            "title",
            "folder",
            "doc_type",
            "email_from",
            "source",
            "file_path",
        ):
            mutated = dict(base)
            mutated[field] = "diverso"
            assert compute_fingerprint(mutated) != compute_fingerprint(base), field

    def test_fingerprint_non_collide_su_shift_di_confine(self):
        # Concatenazione nuda: "ab"+"" == "a"+"b". Length-prefix la evita.
        a = compute_fingerprint({"title": "ab", "folder": ""})
        b = compute_fingerprint({"title": "a", "folder": "b"})
        assert a != b

    def test_fingerprint_tratta_assente_e_vuoto_come_equivalenti(self):
        assert compute_fingerprint({"title": "x"}) == compute_fingerprint(
            {"title": "x", "folder": None, "email_from": ""}
        )

    def test_as_text_non_usa_bool_implicito(self):
        # Il caveat tipi: un oggetto il cui __bool__ solleva (ndarray-like)
        # deve passare da as_text senza esplodere.
        class ArrayLike:
            def __bool__(self):
                raise ValueError("truth value of an array is ambiguous")

            def __str__(self):
                return "array-like"

        assert as_text(ArrayLike()) == "array-like"
        assert as_text(None) == ""
        assert as_text(0) == "0"  # uno 0 legittimo NON è "assente"
        assert as_text(b"byte") == "byte"

    def test_check_churn_bypassa_il_guardiano_in_bootstrap(self):
        plan = build_plan({f"c{n}": "fp" for n in range(100)}, {})
        ok, message = check_churn(plan, {}, 0.20)
        assert ok is True
        assert "bootstrap" in message

    def test_check_churn_passa_sotto_soglia(self):
        kb = {f"c{n}": "fp" for n in range(100)}
        index = dict(kb)
        index["c0"] = "fp-old"  # 1/100 = 1%
        plan = build_plan(kb, index)
        ok, _ = check_churn(plan, index, 0.20)
        assert ok is True

    def test_check_churn_scatta_sopra_soglia_in_run_non_bootstrap(self):
        kb = {f"c{n}": "fp" for n in range(100)}
        index = {f"c{n}": ("fp" if n >= 30 else "fp-old") for n in range(100)}
        plan = build_plan(kb, index)  # 30 update = 30%
        ok, message = check_churn(plan, index, 0.20)
        assert ok is False
        assert "30.0%" in message and "NON procedo" in message

    def test_check_churn_vede_il_mass_delete(self):
        # KB svuotata: la base è l'indice, non la KB → niente div/0, niente 0%.
        index = {f"c{n}": "fp" for n in range(100)}
        plan = build_plan({}, index)
        ok, message = check_churn(plan, index, 0.20)
        assert ok is False
        assert "100.0%" in message

    def test_is_bootstrap_solo_su_indice_vuoto(self):
        assert is_bootstrap({}) is True
        assert is_bootstrap({"a": "fp"}) is False

    def test_plan_counts_report(self):
        plan = ReconcilePlan(inserted=["a"], deleted=["b", "c"], updated=["d"], unchanged=["e"])
        assert plan.counts() == {"inserted": 1, "deleted": 2, "updated": 1, "unchanged": 1}


# ============================================================
# RECONCILE END-TO-END
# ============================================================


class TestBootstrap:
    def test_indice_vuoto_e_tutto_insert(self, kb, index_path):
        report = _run(kb, index_path)
        assert report["bootstrap"] is True
        assert report["inserted"] == 10
        assert report["deleted"] == 0 and report["updated"] == 0 and report["unchanged"] == 0
        assert report["applied"] is True
        assert len(read_index_state(open_index(str(index_path)))) == 10

    def test_secondo_run_e_tutto_unchanged(self, kb, index_path):
        _run(kb, index_path)
        report = _run(kb, index_path)
        assert report["bootstrap"] is False
        assert report["unchanged"] == 10
        assert report["churn"] == 0

    def test_dry_run_non_scrive(self, kb, index_path):
        report = _run(kb, index_path, dry_run=True)
        assert report["inserted"] == 10
        assert report["applied"] is False
        assert read_index_state(open_index(str(index_path))) == {}


class TestDelete:
    def test_id_sparito_da_kb_viene_cancellato(self, kb, index_path):
        _run(kb, index_path)
        _rewrite_kb(kb, {f"c{n}": _chunk(n) for n in range(1, 11) if n != 4})

        report = _run(kb, index_path)

        assert report["deleted"] == 1
        assert report["unchanged"] == 9
        assert report["inserted"] == 0 and report["updated"] == 0
        index_conn = open_index(str(index_path))
        assert "c4" not in read_index_state(index_conn)
        # Sparisce anche dalla FTS, non solo dal sidecar.
        assert _fts_ids(index_path, "documento") == [f"c{n}" for n in range(1, 11) if n != 4]


class TestUpdate:
    def test_fingerprint_cambiata_produce_update_non_duplicato(self, kb, index_path):
        _run(kb, index_path)
        chunks = {f"c{n}": _chunk(n) for n in range(1, 11)}
        chunks["c3"]["document"] = "contenuto riscritto da un writer qualunque"
        _rewrite_kb(kb, chunks)

        report = _run(kb, index_path)

        assert report["updated"] == 1
        assert report["unchanged"] == 9
        assert report["inserted"] == 0 and report["deleted"] == 0

        conn = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM lexical_fts WHERE chunk_id = 'c3'")
            assert cursor.fetchone()[0] == 1  # una riga, non due
            cursor.execute("SELECT COUNT(*) FROM lexical_fts")
            assert cursor.fetchone()[0] == 10
            cursor.execute("SELECT document FROM lexical_fts WHERE chunk_id = 'c3'")
            assert cursor.fetchone()[0] == "contenuto riscritto da un writer qualunque"
        finally:
            conn.close()
        # Il contenuto vecchio non è più cercabile.
        assert _fts_ids(index_path, "riscritto") == ["c3"]
        assert "c3" not in _fts_ids(index_path, '"contenuto del chunk"')

    def test_update_su_solo_metadato_viene_catturato(self, kb, index_path):
        # extract_entities muta metadati, non corpo: la fingerprint lo vede.
        _run(kb, index_path)
        chunks = {f"c{n}": _chunk(n) for n in range(1, 11)}
        chunks["c5"]["folder"] = "Ninfa"
        _rewrite_kb(kb, chunks)

        report = _run(kb, index_path)

        assert report["updated"] == 1
        assert _fts_ids(index_path, "folder:Ninfa") == ["c5"]

    def test_update_di_source_aggiorna_il_sidecar(self, kb, index_path):
        _run(kb, index_path)
        chunks = {f"c{n}": _chunk(n) for n in range(1, 11)}
        chunks["c6"]["source"] = "gmail"
        _rewrite_kb(kb, chunks)

        assert _run(kb, index_path)["updated"] == 1

        conn = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT source FROM lexical_meta WHERE chunk_id = 'c6'")
            assert cursor.fetchone()[0] == "gmail"
        finally:
            conn.close()


class TestFilePathGerarchia:
    """`file_path` porta la gerarchia di cartelle — il caso d'uso di P6.

    unicode61 tratta `/` e `_` come separatori (entrambi verificati sui path
    reali del corpus, forma `Divorzio/Avv. Telesca/.../Atto_Marche_103.pdf`),
    quindi ogni segmento del path diventa un token: "cerca in Case, poi
    restringi a Marche" è un MATCH, non un LIKE.
    """

    # Forme prese dal corpus reale: slash come separatore di ramo, underscore
    # dentro il basename, punto prima dell'estensione.
    TREE = {
        "mutuo": "Case/Viale Ippocrate/Rate mutuo/estratto_2026.pdf",
        "marche_atto": "Divorzio/Avv. Telesca/Asset Immobiliari/Atto_Marche_103.pdf",
        "marche_perizia": "Divorzio/Avv. Telesca/Asset Immobiliari/Perizia_Marche.pdf",
        "altrove": "Companies/Red Pill Ventures/Bilanci/2026.xlsx",
    }

    @pytest.fixture
    def tree_index(self, tmp_path, index_path):
        path = tmp_path / "chroma.sqlite3"
        chunks = {
            chunk_id: _chunk(i, document="corpo che NON nomina la cartella", file_path=fp)
            for i, (chunk_id, fp) in enumerate(self.TREE.items())
        }
        _make_chroma(path, chunks)
        _run(path, index_path)
        return index_path

    def test_lo_slash_spezza_in_token(self, tree_index):
        # "Ippocrate" sta solo dentro il path, mai nel corpo.
        assert _fts_ids(tree_index, "Ippocrate") == ["mutuo"]

    @pytest.mark.parametrize(
        "term,expected",
        [
            ("Ippocrate", ["mutuo"]),  # segmento intermedio
            ("Case", ["mutuo"]),  # radice del ramo
            ("estratto", ["mutuo"]),  # basename, spezzato dall'underscore
            ("2026", ["mutuo", "altrove"]),  # token numerico, due rami
            ("Telesca", ["marche_atto", "marche_perizia"]),  # ramo condiviso
            ("xlsx", ["altrove"]),  # estensione
        ],
    )
    def test_ogni_segmento_del_path_e_un_token(self, tree_index, term, expected):
        assert sorted(_fts_ids(tree_index, term)) == sorted(expected)

    def test_ricerca_gerarchica_restringe_al_sotto_ramo(self, tree_index):
        """"Marche" pesca SOLO i chunk sotto Marche, non tutto il corpus."""
        assert sorted(_fts_ids(tree_index, "Marche")) == ["marche_atto", "marche_perizia"]
        assert "altrove" not in _fts_ids(tree_index, "Marche")
        assert "mutuo" not in _fts_ids(tree_index, "Marche")

    def test_underscore_spezza_come_lo_slash(self, tree_index):
        # `Atto_Marche_103.pdf` -> atto, marche, 103, pdf. Verificato sui path
        # reali: senza questo, "Marche" non troverebbe Atto_Marche_103.pdf.
        assert _fts_ids(tree_index, "Atto") == ["marche_atto"]
        assert _fts_ids(tree_index, "103") == ["marche_atto"]

    def test_cerca_in_case_poi_restringi(self, tree_index):
        # Il caso d'uso del design: ramo largo, poi intersezione.
        assert sorted(_fts_ids(tree_index, "Divorzio")) == ["marche_atto", "marche_perizia"]
        assert _fts_ids(tree_index, "Divorzio AND Perizia") == ["marche_perizia"]

    def test_filtro_per_colonna_sul_path(self, tree_index):
        assert _fts_ids(tree_index, "file_path:Ippocrate") == ["mutuo"]
        # Il corpo non nomina la cartella: senza file_path indicizzato,
        # "Ippocrate" non avrebbe alcun hit. È il buco che P6 chiude.
        assert _fts_ids(tree_index, "document:Ippocrate") == []

    def test_fingerprint_sensibile_a_file_path(self, tmp_path, index_path):
        """Un file spostato di cartella (stesso corpo) -> update, non unchanged."""
        path = tmp_path / "chroma.sqlite3"
        _make_chroma(path, {f"c{n}": _chunk(n) for n in range(1, 11)})
        _run(path, index_path)

        chunks = {f"c{n}": _chunk(n) for n in range(1, 11)}
        chunks["c2"]["file_path"] = "Case/Viale Ippocrate/Archivio/rata_2.pdf"
        _rewrite_kb(path, chunks)

        report = _run(path, index_path)

        assert report["updated"] == 1
        assert report["unchanged"] == 9
        assert _fts_ids(index_path, "Archivio") == ["c2"]

    def test_file_path_assente_non_rompe(self, tmp_path, index_path):
        # 95.512/161.744 chunk hanno file_path: i restanti (email, conv) no.
        path = tmp_path / "chroma.sqlite3"
        _make_chroma(path, {"senza": _chunk(1, file_path=None), "con": _chunk(2)})

        report = _run(path, index_path)

        assert report["inserted"] == 2
        conn = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT file_path FROM lexical_fts WHERE chunk_id = 'senza'")
            assert cursor.fetchone()[0] == ""
        finally:
            conn.close()
        assert _fts_ids(index_path, "Ippocrate") == ["con"]


class TestFtsSidecarDivergenza:
    """Codex #2 — la FTS è la seconda fonte di verità, non un assunto.

    Prima il reconciler confrontava KB <-> `lexical_meta` e basta: una FTS che
    perdeva o duplicava righe passava per `unchanged, churn 0%` mentre il
    retrieval era sbagliato. Un guardiano che mente è peggio di nessuno.
    """

    def _corrompi(self, index_path, sql, *params):
        conn = sqlite3.connect(str(index_path))
        conn.execute(sql, params)
        conn.commit()
        conn.close()

    def test_riga_fts_persa_viene_ricostruita_non_ignorata(self, kb, index_path):
        _run(kb, index_path)
        # Sidecar coerente, FTS bucata: è il caso che prima diceva "unchanged".
        self._corrompi(index_path, "DELETE FROM lexical_fts WHERE chunk_id = 'c7'")
        assert _fts_ids(index_path, "documento") == [f"c{n}" for n in range(1, 11) if n != 7]

        report = _run(kb, index_path)

        assert report["updated"] == 1  # NON unchanged
        assert report["unchanged"] == 9
        assert report["fts_divergent"] == 1
        # Ricostruita davvero: torna cercabile. `sorted` perché la riga
        # riscritta prende un rowid nuovo e riemerge in coda: l'ordine dei
        # risultati FTS non è un contratto, l'insieme sì.
        assert sorted(_fts_ids(index_path, "documento")) == sorted(
            f"c{n}" for n in range(1, 11)
        )

    def test_riga_fts_duplicata_viene_deduplicata(self, kb, index_path):
        _run(kb, index_path)
        self._corrompi(
            index_path,
            """
            INSERT INTO lexical_fts (chunk_id, document, title, folder, doc_type,
                                     email_from, file_path)
            SELECT chunk_id, document, title, folder, doc_type, email_from, file_path
            FROM lexical_fts WHERE chunk_id = 'c2'
            """,
        )
        assert _fts_ids(index_path, "documento").count("c2") == 2

        report = _run(kb, index_path)

        assert report["updated"] == 1
        assert report["fts_divergent"] == 1
        assert _fts_ids(index_path, "documento").count("c2") == 1

    def test_orfano_fts_ancora_in_kb_viene_ricostruito(self, kb, index_path):
        # In FTS ma non nel sidecar, e l'id esiste in KB -> update (rebuild).
        _run(kb, index_path)
        self._corrompi(index_path, "DELETE FROM lexical_meta WHERE chunk_id = 'c8'")

        report = _run(kb, index_path)

        assert report["updated"] == 1
        assert report["fts_divergent"] == 1
        assert "c8" in read_index_state(open_index(str(index_path)))
        assert _fts_ids(index_path, "documento").count("c8") == 1

    def test_orfano_fts_sparito_da_kb_viene_cancellato(self, kb, index_path):
        # In FTS, non nel sidecar, non in KB -> delete, senza codice dedicato.
        _run(kb, index_path)
        self._corrompi(index_path, "DELETE FROM lexical_meta WHERE chunk_id = 'c9'")
        _rewrite_kb(kb, {f"c{n}": _chunk(n) for n in range(1, 11) if n != 9})

        report = _run(kb, index_path)

        assert report["deleted"] == 1
        assert "c9" not in _fts_ids(index_path, "documento")

    def test_divergenza_di_massa_fa_scattare_il_guardiano(self, kb, index_path):
        # La riparazione non è un bypass: 5/10 righe FTS perse = 50% churn.
        _run(kb, index_path)
        self._corrompi(index_path, "DELETE FROM lexical_fts WHERE chunk_id IN ('c1','c2','c3','c4','c5')")

        with pytest.raises(RuntimeError, match="50.0%"):
            _run(kb, index_path)

    def test_read_index_state_marca_il_sentinel(self, kb, index_path):
        _run(kb, index_path)
        self._corrompi(index_path, "DELETE FROM lexical_fts WHERE chunk_id = 'c1'")

        state = read_index_state(open_index(str(index_path)))

        assert state["c1"] == FTS_DIVERGENT
        assert state["c2"] != FTS_DIVERGENT
        # Il sentinel non è un sha256: non può collidere con una fingerprint vera.
        assert not all(ch in "0123456789abcdef" for ch in FTS_DIVERGENT)

    def test_indice_sano_non_marca_nulla(self, kb, index_path):
        _run(kb, index_path)
        state = read_index_state(open_index(str(index_path)))
        assert FTS_DIVERGENT not in state.values()


class TestDryRunReadOnly:
    """Codex #3 — un comando che dichiara di non scrivere non lascia tracce."""

    def test_dry_run_su_path_inesistente_non_crea_il_file(self, kb, tmp_path):
        target = tmp_path / "mai_creato.sqlite3"
        assert not target.exists()

        report = reconcile(str(kb), str(target), 0.20, dry_run=True)

        assert not target.exists()  # né file, né schema, né -wal
        assert list(tmp_path.glob("mai_creato*")) == []
        # Il piano è comunque quello giusto: bootstrap, tutti insert.
        assert report["bootstrap"] is True
        assert report["inserted"] == 10
        assert report["applied"] is False

    def test_dry_run_su_indice_esistente_non_lo_muta(self, kb, index_path):
        _run(kb, index_path)
        _rewrite_kb(kb, {f"c{n}": _chunk(n) for n in range(1, 13)})  # +2 nuovi
        before = index_path.read_bytes()

        report = _run(kb, index_path, dry_run=True)

        assert report["inserted"] == 2
        assert report["applied"] is False
        assert index_path.read_bytes() == before

    def test_dry_run_apre_in_sola_lettura(self, kb, index_path, monkeypatch):
        """Non è solo convenzione: sqlite rifiuta la scrittura (mode=ro)."""
        _run(kb, index_path)
        from scripts.maintenance import reconcile_lexical_index as mod

        catturata = {}
        originale = mod.apply_plan

        def spia(conn, *a, **kw):
            catturata["conn"] = conn
            return originale(conn, *a, **kw)

        monkeypatch.setattr(mod, "apply_plan", spia)
        _run(kb, index_path, dry_run=True)
        assert "conn" not in catturata  # apply_plan non è nemmeno chiamata

        conn = mod.open_index_for_plan(str(index_path), dry_run=True)
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("DELETE FROM lexical_meta")
        conn.close()

    def test_dry_run_via_cli_non_crea_il_file(self, kb, tmp_path, capsys):
        from scripts.maintenance.reconcile_lexical_index import main

        target = tmp_path / "cli_mai_creato.sqlite3"
        code = main(["--chroma-db", str(kb), "--index-db", str(target), "--dry-run"])

        assert code == 0
        assert not target.exists()
        assert "DRY-RUN" in capsys.readouterr().out


class TestSegmentDatabase:
    """Codex #1 — `collections` ha UNIQUE (name, database_id): il nome da solo
    è ambiguo. Qualificare per database, e mai scegliere a caso.
    """

    def _con_databases(self, path, collections, segments):
        """Chroma con la tabella `databases` (schema corrente)."""
        conn = sqlite3.connect(str(path))
        conn.executescript(
            """
            CREATE TABLE databases (id TEXT PRIMARY KEY, name TEXT, tenant_id TEXT);
            CREATE TABLE collections (id TEXT PRIMARY KEY, name TEXT, database_id TEXT);
            CREATE TABLE segments (id TEXT PRIMARY KEY, collection TEXT, scope TEXT);
            CREATE TABLE embeddings (id INTEGER PRIMARY KEY, segment_id TEXT, embedding_id TEXT);
            CREATE TABLE embedding_metadata (
                id INTEGER, key TEXT, string_value TEXT,
                int_value INTEGER, float_value REAL, bool_value INTEGER
            );
            """
        )
        conn.execute("INSERT INTO databases VALUES ('db-def', 'default_database', 't')")
        conn.execute("INSERT INTO databases VALUES ('db-other', 'altro_database', 't')")
        conn.executemany("INSERT INTO collections VALUES (?, ?, ?)", collections)
        conn.executemany("INSERT INTO segments VALUES (?, ?, 'METADATA')", segments)
        return conn

    def test_prende_il_segmento_del_database_default(self, tmp_path, index_path):
        path = tmp_path / "chroma.sqlite3"
        conn = self._con_databases(
            path,
            [("col-def", "tailor_kb_v2", "db-def"), ("col-other", "tailor_kb_v2", "db-other")],
            [("seg-def", "col-def"), ("seg-other", "col-other")],
        )
        _write_chunks(conn, {"giusto": _chunk(1)}, segment_id="seg-def")
        _write_chunks(conn, {"sbagliato": _chunk(2)}, segment_id="seg-other", start_rowid=100)
        conn.commit()
        conn.close()

        report = reconcile(str(path), str(index_path), 0.20)

        # Senza qualifica di database prenderebbe uno dei due a caso.
        assert report["segment_id"] == "seg-def"
        assert list(read_index_state(open_index(str(index_path)))) == ["giusto"]

    def test_collection_ambigua_aborta_invece_di_scegliere(self, tmp_path, index_path):
        # Due segmenti METADATA per la stessa collection nello stesso database.
        path = tmp_path / "chroma.sqlite3"
        conn = self._con_databases(
            path,
            [("col-def", "tailor_kb_v2", "db-def")],
            [("seg-a", "col-def"), ("seg-b", "col-def")],
        )
        conn.commit()
        conn.close()

        with pytest.raises(RuntimeError, match="ambiguo"):
            reconcile(str(path), str(index_path), 0.20)

    def test_fallback_su_schema_legacy_senza_databases(self, tmp_path, index_path):
        # `_make_chroma` non ha la tabella `databases`: il ramo di fallback deve
        # funzionare, non esplodere su OperationalError.
        path = tmp_path / "chroma.sqlite3"
        _make_chroma(path, {"c1": _chunk(1)})

        report = reconcile(str(path), str(index_path), 0.20)

        assert report["segment_id"] == SEGMENT_METADATA
        assert report["inserted"] == 1

    def test_fallback_legacy_aborta_su_ambiguita(self, tmp_path, index_path):
        path = tmp_path / "chroma.sqlite3"
        _make_chroma(path, {"c1": _chunk(1)})
        conn = sqlite3.connect(str(path))
        conn.execute("INSERT INTO collections VALUES ('col-2', 'tailor_kb_v2')")
        conn.execute("INSERT INTO segments VALUES ('seg-2', 'col-2', 'METADATA')")
        conn.commit()
        conn.close()

        with pytest.raises(RuntimeError, match="ambiguo"):
            reconcile(str(path), str(index_path), 0.20)


class TestGuardiano:
    def test_soglia_scatta_e_non_applica(self, kb, index_path):
        _run(kb, index_path)
        before = read_index_state(open_index(str(index_path)))

        chunks = {f"c{n}": _chunk(n) for n in range(1, 11)}
        for n in (1, 2, 3):  # 3/10 = 30% > 20%
            chunks[f"c{n}"]["document"] = "riscritto in massa"
        _rewrite_kb(kb, chunks)

        with pytest.raises(RuntimeError, match="30.0%"):
            _run(kb, index_path)

        # Fail-loud = niente apply silenzioso: l'indice è intatto.
        assert read_index_state(open_index(str(index_path))) == before

    def test_soglia_non_scatta_in_bootstrap(self, kb, index_path):
        # 100% di churn su indice vuoto è il bootstrap atteso, non un sintomo.
        report = _run(kb, index_path, max_churn=0.01)
        assert report["applied"] is True
        assert report["inserted"] == 10

    def test_soglia_alzata_lascia_passare(self, kb, index_path):
        _run(kb, index_path)
        chunks = {f"c{n}": _chunk(n) for n in range(1, 11)}
        for n in (1, 2, 3):
            chunks[f"c{n}"]["document"] = "riscritto in massa"
        _rewrite_kb(kb, chunks)

        report = _run(kb, index_path, max_churn=0.50)
        assert report["updated"] == 3
        assert report["applied"] is True

    def test_exit_code_non_zero_quando_scatta(self, kb, index_path, capsys):
        from scripts.maintenance.reconcile_lexical_index import main

        _run(kb, index_path)
        chunks = {f"c{n}": _chunk(n) for n in range(1, 11)}
        for n in (1, 2, 3):
            chunks[f"c{n}"]["document"] = "riscritto in massa"
        _rewrite_kb(kb, chunks)

        code = main(["--chroma-db", str(kb), "--index-db", str(index_path)])

        assert code == 1
        assert "RECONCILE ABORTITO" in capsys.readouterr().err


class TestSegmentFilter:
    def test_legge_solo_il_segmento_metadata_della_collection(self, tmp_path, index_path):
        # I6: un chunk in un altro segmento non deve contaminare l'indice.
        path = tmp_path / "chroma.sqlite3"
        _make_chroma(path, {"c1": _chunk(1)})
        conn = sqlite3.connect(str(path))
        _write_chunks(conn, {"intruso": _chunk(99)}, segment_id=SEGMENT_OTHER, start_rowid=500)
        conn.commit()
        conn.close()

        report = _run(path, index_path)

        assert report["inserted"] == 1
        assert report["segment_id"] == SEGMENT_METADATA
        assert list(read_index_state(open_index(str(index_path)))) == ["c1"]

    def test_collection_mancante_e_fail_loud(self, tmp_path, index_path):
        path = tmp_path / "chroma.sqlite3"
        _make_chroma(path, {"c1": _chunk(1)}, collection_name="qualche_altra_collection")
        with pytest.raises(RuntimeError, match="non trovati"):
            _run(path, index_path)


class TestReadOnlyKB:
    def test_la_kb_non_viene_modificata(self, kb, index_path):
        before = kb.read_bytes()
        _run(kb, index_path)
        assert kb.read_bytes() == before


class TestSanitizzazione:
    """Il parser FTS5 non deve vedere il contenuto come sintassi (bind params)."""

    HOSTILE = {
        "at_e_path": 'mail di gianluca@example.com in /Users/jarvis/tailor "preventivo"',
        "operatori": "AND OR NOT NEAR* (foo) -bar column:value",
        "virgolette": 'ha detto "il "vero" preventivo" e poi \'basta\'',
        "punteggiatura": "rif. n.2026/07: art. 1-bis, c.d. «usufrutto»",
        "accenti": "perché è così, città, Ninfa",
    }

    @pytest.fixture
    def hostile_index(self, tmp_path, index_path):
        path = tmp_path / "chroma.sqlite3"
        chunks = {
            chunk_id: _chunk(i, document=doc, title=doc, email_from="a@b.it")
            for i, (chunk_id, doc) in enumerate(self.HOSTILE.items())
        }
        _make_chroma(path, chunks)
        _run(path, index_path)
        return index_path

    def test_indicizza_senza_errori_fts5(self, hostile_index):
        conn = sqlite3.connect(f"file:{hostile_index}?mode=ro", uri=True)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM lexical_fts")
            assert cursor.fetchone()[0] == len(self.HOSTILE)
        finally:
            conn.close()

    @pytest.mark.parametrize(
        "term,expected",
        [
            ("gianluca", "at_e_path"),
            ("tailor", "at_e_path"),
            ("preventivo", "at_e_path"),
            ("usufrutto", "punteggiatura"),
            ("bis", "punteggiatura"),
        ],
    )
    def test_i_termini_si_ritrovano(self, hostile_index, term, expected):
        assert expected in _fts_ids(hostile_index, term)

    def test_remove_diacritics_trova_senza_accenti(self, hostile_index):
        # unicode61 remove_diacritics: "perche" trova "perché" (query IT reale).
        assert _fts_ids(hostile_index, "perche") == ["accenti"]
        assert _fts_ids(hostile_index, "citta") == ["accenti"]

    def test_tokenizer_ha_confini_di_parola_veri(self, hostile_index):
        # Non è trigram: "usufrutt" (sottostringa) NON matcha; il prefix sì.
        assert _fts_ids(hostile_index, "usufrutt") == []
        assert _fts_ids(hostile_index, "usufrutt*") == ["punteggiatura"]

    def test_il_contenuto_ostile_non_diventa_sintassi(self, hostile_index):
        # Il chunk "operatori" contiene AND/OR/NOT/NEAR: sono stati indicizzati
        # come termini, non interpretati.
        assert _fts_ids(hostile_index, "near") == ["operatori"]

    def test_ricerca_per_colonna_sul_metadato(self, hostile_index):
        assert sorted(_fts_ids(hostile_index, 'email_from:"b.it"')) == sorted(self.HOSTILE)
        assert _fts_ids(hostile_index, "folder:Condominio") == list(self.HOSTILE)

    def test_il_termine_di_query_non_quotato_esplode(self):
        """Conferma il buco §9.6 — NON è nello scope di questo step.

        Il contenuto ostile si indicizza (bind params). La *query* no: un
        `email_from:b.it` nudo è `sqlite3.OperationalError: fts5: syntax error
        near "."`. È esattamente il caso "mail di Gianluca" che ha motivato P6.
        Questo test è il promemoria che il ramo lessicale DEVE sanitizzare la
        query prima del MATCH; qui documenta il confine, non lo risolve.
        """
        with pytest.raises(sqlite3.OperationalError, match="syntax error"):
            conn = sqlite3.connect(":memory:")
            conn.execute("CREATE VIRTUAL TABLE t USING fts5(email_from)")
            conn.execute("SELECT * FROM t WHERE t MATCH ?", ("email_from:b.it",)).fetchall()


class TestSchemaSidecar:
    def test_schema_idempotente(self, index_path):
        open_index(str(index_path)).close()
        open_index(str(index_path)).close()  # non solleva

    def test_source_e_una_colonna_non_fts_interrogabile(self, kb, index_path):
        """I7: il filtro pre-query si fa sul sidecar, senza toccare Chroma."""
        _run(kb, index_path)
        conn = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT m.chunk_id
                FROM lexical_fts f
                JOIN lexical_meta m ON m.chunk_id = f.chunk_id
                WHERE lexical_fts MATCH 'contenuto' AND m.source = 'docs'
                """
            )
            assert len(cursor.fetchall()) == 10
            cursor.execute(
                """
                SELECT m.chunk_id
                FROM lexical_fts f
                JOIN lexical_meta m ON m.chunk_id = f.chunk_id
                WHERE lexical_fts MATCH 'contenuto' AND m.source = 'gmail'
                """
            )
            assert cursor.fetchall() == []
        finally:
            conn.close()

    def test_source_non_e_indicizzato_in_fts(self, kb, index_path):
        # source sta SOLO nel sidecar: cercarlo come colonna FTS è un errore.
        _run(kb, index_path)
        with pytest.raises(sqlite3.OperationalError):
            _fts_ids(index_path, "source:docs")
