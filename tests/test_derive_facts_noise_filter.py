"""Tests for the pre-derivation noise filter in scripts.enrichment.derive_facts.

Two levels:

  1. Unit — classify_noise() puts known extraction-noise keys into the right
     blocklist rule, and leaves real entities (including single-word and
     mono-name companies) untouched. Conservative by design: only zero-FP
     categories; "single word, no digits" is NEVER filtered.

  2. Integration — a nightly run skips the noise entities a MONTE: they are
     never processed AND never get a watermark, so a future loosening of the
     blocklist lets them re-enter derivation on their own. The filter must not
     burn the state of the entities it excludes.

No real HTTP / LLM: derive_for_entity and BackendManager are patched.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.enrichment import derive_facts as df  # noqa: E402


# ── 1. unit: classify_noise ───────────────────────────────────

@pytest.mark.parametrize("raw, expected_rule", [
    # (a) chiavi puramente numeriche (id, fatture, riferimenti pagamento)
    ("8861", "numeric"),
    ("222693765", "numeric"),
    ("000 703910", "numeric"),
    # (b) token-rumore tecnici
    ("null", "noise_token"),
    ("NULL", "noise_token"),
    ("", "noise_token"),
    ("n/a", "noise_token"),
    ("undefined", "noise_token"),
    # (c) periodi temporali "MESE ANNO" (IT + EN, in qualunque ordine)
    ("dicembre 2016", "temporal"),
    ("gennaio 2021", "temporal"),
    ("2014 luglio", "temporal"),
    ("january 2011", "temporal"),
    # (d) frammenti UI noti
    ("Search bar", "ui_fragment"),
    ("Option 1", "ui_fragment"),
    ("Notifiche Push", "ui_fragment"),
])
def test_noise_is_excluded(raw, expected_rule):
    assert df.classify_noise(df.normalize_entity(raw)) == expected_rule


@pytest.mark.parametrize("raw", [
    # persone
    "Gianluca Manni",
    "Nhu-An",
    # aziende multi-parola
    "Archidomus SRL",
    "Telecom Italia",
    # aziende / entità mono-nome (la categoria "singola parola" NON va filtrata)
    "AerLingus",
    "CONAI",
    "Google",
    "telecomando",
    # luoghi
    "Roma",
    # parola-mese ma SENZA anno → non è un periodo, deve passare
    "Maggio Editore",
    # sigle/ticker/brand corti (<=2 char): NON sono junk — la regola `short` è
    # stata rimossa proprio perché li scartava. Devono passare il filtro.
    # (questa è la copertura che mancava e che avrebbe colto il bug Codex.)
    "BP",   # ticker / azienda
    "EU",   # entità geopolitica
    "3M",   # azienda (norm: "3m", non puramente numerica → non `numeric`)
    "A1",   # brand / sigla
    "GE",   # azienda
])
def test_real_entities_pass(raw):
    assert df.classify_noise(df.normalize_entity(raw)) is None


def test_short_rule_removed():
    """Regressione esplicita: la regola `short` (<=2 char) non esiste più.
    Catturava sigle reali (BP/EU/3M/A1) → falso zero-FP, rimossa."""
    assert "short" not in {df.classify_noise(df.normalize_entity(x))
                           for x in ("bp", "eu", "3m", "a1", "ge", "x")}


# ── 2. integration: filtered entities are skipped AND not watermarked ──

class _FakeMgr:
    def __init__(self, *a, **k):
        self.backends = [{"name": "anthropic", "model": "m"}]


def _build_db(path, entities):
    """entities: list of (name, created_at, n_facts)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    db = sqlite3.connect(path)
    db.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chunk_id TEXT, fact TEXT, category TEXT, entity_tags TEXT,
            event_date TEXT, confidence REAL, created_at TEXT,
            relation_type TEXT, derived_from TEXT, superseded_by TEXT DEFAULT ''
        )
    """)
    for name, created_at, n_facts in entities:
        for i in range(n_facts):
            db.execute(
                "INSERT INTO facts (chunk_id, fact, category, entity_tags, event_date, "
                "confidence, created_at, relation_type, derived_from, superseded_by) "
                "VALUES (?,?,?,?,?,?,?,?,?,'')",
                (f"c_{name}_{i}", f"{name} fact {i}", "cat", json.dumps([name]),
                 "", 0.9, created_at, "extracted", ""),
            )
    db.commit()
    db.close()


def test_filtered_entities_are_skipped_and_not_watermarked(monkeypatch, tmp_path):
    db_path = str(tmp_path / "facts.sqlite3")
    ckpt = str(tmp_path / "checkpoints" / "derives_checkpoint.json")
    monkeypatch.setattr(df, "FACTS_DB", db_path)
    monkeypatch.setattr(df, "CHECKPOINT_FILE", ckpt)
    monkeypatch.setattr(df, "BackendManager", _FakeMgr)
    monkeypatch.setattr(df, "RATE_LIMIT_DELAY", 0)

    calls = []
    monkeypatch.setattr(df, "derive_for_entity",
                        lambda norm, data, existing, manager: calls.append(norm) or [])

    # mix: 3 reali (mono-nome + sigla corta) + 3 rumore, tutte con >=5 fatti
    _build_db(db_path, [
        ("Gianluca Manni", "2026-06-10T00:01:00", 5),   # reale, passa
        ("AerLingus", "2026-06-10T00:02:00", 5),         # reale mono-nome, passa
        ("BP", "2026-06-10T00:03:00", 5),                # sigla <=2 char, passa (no `short`)
        ("8861", "2026-06-10T00:04:00", 5),              # numeric, escluso
        ("null", "2026-06-10T00:05:00", 5),              # noise_token, escluso
        ("dicembre 2016", "2026-06-10T00:06:00", 5),     # temporal, escluso
    ])

    monkeypatch.setattr(sys, "argv", ["derive_facts.py", "--nightly"])
    df.main()

    processed = set(calls)
    # reali (inclusa la sigla corta BP) processate; nessun junk tocca il LLM
    assert processed == {"gianluca manni", "aerlingus", "bp"}

    cp = json.load(open(ckpt))
    wm = set(cp["derived_watermarks"])
    # CRITICO: le escluse NON hanno watermark → potranno rientrare se il filtro
    # un domani si restringe. Solo le reali processate sono marcate.
    assert wm == {"gianluca manni", "aerlingus", "bp"}
    for noise in ("8861", "null", "2016 dicembre"):
        assert noise not in wm
