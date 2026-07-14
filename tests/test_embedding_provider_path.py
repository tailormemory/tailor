"""Regressione (Codex HIGH): il provider `embedding` con backend openai/google
fa, dentro `_get_api_key()`, `from lib.api_keys import resolve_api_key`.

Quell'import richiede che **scripts/** sia su sys.path (per il package `lib`),
oltre a **scripts/lib/** (per `from config import`). I tool di manutenzione
(repair_hnsw_index, rebuild_db, rebuild_db_v2) devono mettere ENTRAMBI su path
prima di usare il provider. Con Ollama il ramo non viene mai preso, quindi il
bug era mascherato; al primo cambio provider esplodeva con ModuleNotFoundError.

Questo test forza provider=openai e verifica che l'import NON crashi.
"""

import importlib
import os
import sys

import pytest


def test_openai_provider_branch_imports_lib_api_keys(monkeypatch):
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts = os.path.join(base, "scripts")
    scripts_lib = os.path.join(base, "scripts", "lib")

    # Mirror del setup dei tool di manutenzione: scripts/ E scripts/lib/ su path.
    saved = list(sys.path)
    for p in (scripts, scripts_lib):
        if p not in sys.path:
            sys.path.insert(0, p)
    try:
        embedding = importlib.import_module("embedding")  # `from config import` -> scripts/lib
        # Forza il ramo cloud senza toccare la config reale.
        monkeypatch.setattr(embedding, "_provider", "openai", raising=False)
        try:
            key = embedding._get_api_key()   # esegue `from lib.api_keys import ...`
        except ModuleNotFoundError as e:
            pytest.fail(f"provider path rotto (lib.api_keys non importabile): {e}")
        except Exception:
            # Qualsiasi altro errore (chiave mancante, ecc.) e' oltre lo scopo:
            # l'import di lib.api_keys e' gia' andato a buon fine.
            pass
        else:
            assert isinstance(key, str)
    finally:
        sys.path[:] = saved
