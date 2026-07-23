"""Tests for backend_manager.gemini_thinking_config + il contratto
"dict vuoto ⇒ chiave omessa" lato call site.

Il branch è stretto di proposito (docs Google: disattivare il thinking
è documentato solo per la famiglia 2.5-flash), quindi ogni ramo ha un
test: un allargamento accidentale a Pro o alla serie 2.0 rimanderebbe
un parametro non supportato.

Nessun HTTP reale: il call site usa requests_mock.
"""

from __future__ import annotations

import os
import sys

import pytest
import requests_mock as rm_module  # noqa: F401 — needed for the requests_mock fixture

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib.backend_manager import gemini_thinking_config  # noqa: E402
from scripts.enrichment import derive_facts as df  # noqa: E402


# ============================================================
# 1. helper: un caso per ramo
# ============================================================


@pytest.mark.parametrize("model", ["gemini-2.5-flash", "gemini-2.5-flash-lite"])
def test_25_flash_family_gets_thinking_budget_zero(model):
    assert gemini_thinking_config(model) == {"thinkingBudget": 0}


@pytest.mark.parametrize("model", ["gemini-flash-latest", "gemini-flash-lite-latest"])
def test_latest_aliases_get_thinking_level_minimal(model):
    # Gli alias risolvono a model 3.x, che rifiutano thinkingBudget con 400.
    assert gemini_thinking_config(model) == {"thinkingLevel": "minimal"}


@pytest.mark.parametrize("model", ["gemini-2.5-pro", "gemini-2.0-flash"])
def test_other_gemini_2x_omits_thinking_config(model):
    # 2.5 Pro: thinking non disattivabile. 2.0: parametro non documentato.
    assert gemini_thinking_config(model) == {}


def test_pro_latest_alias_is_not_treated_as_2x():
    # gemini-pro-latest risolve a 3.x: deve prendere il ramo thinkingLevel,
    # non essere confuso col ramo Pro 2.5.
    assert gemini_thinking_config("gemini-pro-latest") == {"thinkingLevel": "minimal"}


# ============================================================
# 2. call site: la chiave sparisce dal payload quando l'helper è vuoto
# ============================================================


def _mock_google(requests_mock, model):
    requests_mock.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
        json={"candidates": [{"content": {"parts": [{"text": "ok"}]}}]},
    )


def test_call_site_omits_thinking_config_when_helper_empty(requests_mock):
    _mock_google(requests_mock, "gemini-2.5-pro")

    assert df._call_google("prompt", "gemini-2.5-pro", "k", "system") == "ok"

    gen_cfg = requests_mock.request_history[-1].json()["generationConfig"]
    assert "thinkingConfig" not in gen_cfg
    assert gen_cfg["maxOutputTokens"] == 800  # il resto del config resta intatto


def test_call_site_sends_thinking_config_when_helper_non_empty(requests_mock):
    _mock_google(requests_mock, "gemini-flash-latest")

    assert df._call_google("prompt", "gemini-flash-latest", "k", "system") == "ok"

    gen_cfg = requests_mock.request_history[-1].json()["generationConfig"]
    assert gen_cfg["thinkingConfig"] == {"thinkingLevel": "minimal"}
