"""Supporto .doc (Word 97-2003) nell'ingest documenti.

Tre invarianti:

  (a) il dispatcher extract_text() instrada .doc su extract_doc() e chiama
      textutil con una LISTA di argomenti (mai shell, mai stringa composta):
      i path reali contengono spazi, apostrofi e accenti;
  (b) ogni modo di fallire — file inesistente, .doc corrotto, output vuoto,
      timeout, textutil assente — ritorna [] senza sollevare, cioe' lo stesso
      contratto di extract_docx/extract_csv: il chiamante marca
      "no_text_extracted" e il batch prosegue;
  (c) .doc e' in supported_extensions, altrimenti scan_folders lo salta prima
      ancora di arrivare all'estrattore.

Nessun test invoca textutil davvero, tranne quello marcato macOS-only sul
round-trip reale.
"""
from __future__ import annotations

import os
import subprocess
import sys

import pytest
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))

import ingest_docs as ing  # noqa: E402


class _Proc:
    def __init__(self, returncode=0, stdout=b"", stderr=b""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _patch_run(monkeypatch, result):
    """Sostituisce subprocess.run e registra gli argomenti ricevuti."""
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr(subprocess, "run", fake_run)
    return calls


# ── (a) dispatcher + forma della chiamata ────────────────────────────────

def test_dispatcher_routes_doc_to_extract_doc(monkeypatch, tmp_path):
    seen = []
    monkeypatch.setattr(ing, "extract_doc", lambda p: seen.append(p) or [{"text": "x", "metadata": {}}])

    path = str(tmp_path / "Contratto Henkel.doc")
    out = ing.extract_text(path)

    assert seen == [path]
    assert out == [{"text": "x", "metadata": {}}]


def test_dispatcher_case_insensitive(monkeypatch, tmp_path):
    seen = []
    monkeypatch.setattr(ing, "extract_doc", lambda p: seen.append(p) or [])
    ing.extract_text(str(tmp_path / "ATTO.DOC"))
    assert len(seen) == 1


def test_docx_still_routed_to_extract_docx(monkeypatch, tmp_path):
    """Il ramo .doc non deve rubare i .docx: l'ordine elif conta."""
    seen = []
    monkeypatch.setattr(ing, "extract_docx", lambda p: seen.append(("docx", p)) or [])
    monkeypatch.setattr(ing, "extract_doc", lambda p: seen.append(("doc", p)) or [])
    ing.extract_text(str(tmp_path / "f.docx"))
    assert [k for k, _ in seen] == ["docx"]


def test_textutil_called_with_arg_list_never_shell(monkeypatch):
    calls = _patch_run(monkeypatch, _Proc(stdout="contenuto".encode("utf-8")))

    path = "/Users/x/OneDrive/Contratti/L'atto d'assemblea — città.doc"
    ing.extract_doc(path)

    cmd, kwargs = calls[0]
    assert cmd == [ing.TEXTUTIL_BIN, "-convert", "txt", "-stdout", path]
    assert isinstance(cmd, list)                    # mai stringa == mai shell
    assert kwargs.get("shell", False) is False
    assert kwargs["timeout"] == ing.DOC_EXTRACT_TIMEOUT


def test_extract_doc_returns_section_with_text(monkeypatch):
    _patch_run(monkeypatch, _Proc(stdout="  Contratto Henkel 2019  ".encode("utf-8")))

    out = ing.extract_doc("/tmp/x.doc")

    assert len(out) == 1
    assert out[0]["text"] == "Contratto Henkel 2019"
    assert out[0]["metadata"]["extractor"] == "textutil"
    assert out[0]["metadata"]["char_count"] == len("Contratto Henkel 2019")


def test_decode_is_lenient_on_invalid_utf8(monkeypatch):
    """errors='replace': un byte sporco non deve far fallire l'estrazione."""
    _patch_run(monkeypatch, _Proc(stdout=b"citt\xe0 di Roma"))

    out = ing.extract_doc("/tmp/x.doc")

    assert len(out) == 1
    assert "di Roma" in out[0]["text"]


# ── (b) ogni fallimento -> [] senza sollevare ────────────────────────────

@pytest.mark.parametrize("result, needle", [
    (_Proc(returncode=1, stderr=b"textutil: error"), "rc=1"),
    (_Proc(returncode=0, stdout=b"   \n  "), "vuoto"),
    (subprocess.TimeoutExpired(cmd="textutil", timeout=20), "timeout"),
    (FileNotFoundError(2, "No such file or directory"), "non trovato"),
    (OSError("boom"), "boom"),
])
def test_failures_return_empty_and_report(monkeypatch, capsys, result, needle):
    _patch_run(monkeypatch, result)

    out = ing.extract_doc("/tmp/rotto.doc")

    assert out == []                                 # stesso contratto di extract_docx
    err = capsys.readouterr().out
    assert "ERRORE DOC" in err and needle in err


def test_missing_textutil_names_the_binary(monkeypatch, capsys):
    """Non-macOS: fallimento leggibile, non 121 file silenziosamente 'vuoti'."""
    _patch_run(monkeypatch, FileNotFoundError(2, "No such file or directory"))

    ing.extract_doc("/tmp/x.doc")

    err = capsys.readouterr().out
    assert ing.TEXTUTIL_BIN in err
    assert "macOS" in err


def test_nonexistent_doc_does_not_raise(tmp_path):
    """textutil vero su un path che non esiste: rc != 0, nessuna eccezione."""
    out = ing.extract_doc(str(tmp_path / "non-esiste.doc"))
    assert out == []


def test_corrupt_doc_does_not_raise(tmp_path):
    """textutil vero su bytes spazzatura con estensione .doc."""
    p = tmp_path / "corrotto.doc"
    p.write_bytes(os.urandom(4096))
    out = ing.extract_doc(str(p))
    assert isinstance(out, list)                     # [] o sezione, mai eccezione


def test_batch_survives_a_broken_doc(monkeypatch, tmp_path):
    """Il file rotto sta in mezzo: gli altri due arrivano comunque a sezione."""
    def fake_run(cmd, **kwargs):
        if "rotto" in cmd[-1]:
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=20)
        return _Proc(stdout=b"ok")

    monkeypatch.setattr(subprocess, "run", fake_run)

    batch = ["/tmp/a.doc", "/tmp/rotto.doc", "/tmp/b.doc"]
    results = [ing.extract_text(p) for p in batch]

    assert [len(r) for r in results] == [1, 0, 1]


# ── (c) config ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("name", ["tailor.yaml", "tailor.yaml.example"])
def test_doc_in_supported_extensions(name):
    path = os.path.join(BASE_DIR, "config", name)
    if not os.path.exists(path):
        pytest.skip(f"{name} assente")
    cfg = yaml.safe_load(open(path, encoding="utf-8"))
    exts = cfg["ingest"]["supported_extensions"]
    assert ".doc" in exts
    assert ".docx" in exts                            # non sostituito
    assert ".txt" not in exts and ".rtf" not in exts   # decisione esplicita


def test_module_supported_extensions_includes_doc():
    assert ".doc" in ing.SUPPORTED_EXTENSIONS


# ── round-trip reale (solo macOS) ────────────────────────────────────────

@pytest.mark.skipif(not os.path.exists("/usr/bin/textutil"), reason="macOS only")
def test_real_textutil_roundtrip(tmp_path):
    """Genera un .doc vero con textutil e rileggilo: path con spazi e accenti."""
    src = tmp_path / "sorgente.txt"
    src.write_text("Contratto d'affitto — città di Roma\nSeconda riga.", encoding="utf-8")

    target_dir = tmp_path / "cartella con spazi"
    target_dir.mkdir()
    target = target_dir / "L'atto — città.doc"

    subprocess.run(
        ["/usr/bin/textutil", "-convert", "doc", "-output", str(target), str(src)],
        check=True, capture_output=True, timeout=20,
    )

    out = ing.extract_text(str(target))

    assert len(out) == 1
    assert "città di Roma" in out[0]["text"]
    assert "Seconda riga." in out[0]["text"]
