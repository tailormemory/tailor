"""Regression tests for the run-sync fail-loud fixes in triage_gmail.py.

DIFETTO 1: an email that errors was written to the sync progress file and then
treated as "already processed" on --resume, so it was never retried — silently
dropped from the triage. The fix: only SUCCESSFUL records (useful in {true,
false}, no error) count as done; errored ones are retried.

DIFETTO 2: run_sync() returned None (exit 0) even with unresolved errors. The
fix: return a non-zero code and print an explicit summary when residual errors
remain.

Both tests mock the provider call (_call_one_sync) — no real API is hit.
"""

from __future__ import annotations

import json
import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))
sys.path.insert(0, os.path.join(ROOT, "scripts", "gmail"))

import triage_gmail as tg  # noqa: E402


def _wire(tmp_path, monkeypatch, emails, fake_call):
    """Redirect all module-global paths to tmp and inject the fake provider."""
    monkeypatch.setattr(tg, "load_emails", lambda: [dict(e) for e in emails])
    monkeypatch.setattr(tg, "SYNC_PROGRESS_FILE", str(tmp_path / "sync_progress.jsonl"))
    monkeypatch.setattr(tg, "OUTPUT_FILE", str(tmp_path / "triage_out.jsonl"))
    monkeypatch.setattr(tg, "LOG_DIR", str(tmp_path))  # keep real logs/ clean
    monkeypatch.setattr(tg, "_call_one_sync", fake_call)


_EMAILS = [
    {"id": "e1", "from": "a@x.it", "subject": "s1", "snippet": "hello"},
    {"id": "e2", "from": "b@x.it", "subject": "s2", "snippet": "world"},
]


def test_errored_email_is_retried_on_resume(tmp_path, monkeypatch):
    """DIFETTO 1: e1 errors on the first run; on --resume it MUST be retried.
    Fails on the old code (e1 skipped because present in progress)."""
    calls = []

    def fake_call(email):
        cid = email["id"]
        calls.append(cid)
        # e1 fails on its first attempt, succeeds afterwards; e2 always ok.
        if cid == "e1" and calls.count("e1") == 1:
            return cid, {"useful": None, "error": "timeout"}
        return cid, {"useful": True}

    _wire(tmp_path, monkeypatch, _EMAILS, fake_call)

    # Fresh run: e1 errors, e2 ok.
    tg.run_sync(resume=False)
    n_after_first = len(calls)
    assert "e1" in calls and "e2" in calls

    # Resume: e1 (errored) must be reconsidered; e2 (ok) must be skipped.
    tg.run_sync(resume=True)
    retried = calls[n_after_first:]
    assert "e1" in retried, "e1 was NOT retried on resume (DIFETTO 1 present)"
    assert "e2" not in retried, "e2 (already ok) should not be retried"


def test_run_sync_returns_nonzero_on_residual_error(tmp_path, monkeypatch):
    """DIFETTO 2: residual errors => non-zero exit. Fails on the old code
    (returned None). Uses exact `== 1` so None != 1 makes the old code red."""

    def fake_call(email):
        cid = email["id"]
        if cid == "e1":
            return cid, {"useful": None, "error": "rate_limit"}
        return cid, {"useful": True}

    _wire(tmp_path, monkeypatch, _EMAILS, fake_call)

    rc = tg.run_sync(resume=False)
    assert rc == 1, f"expected exit code 1 on residual error, got {rc!r}"

    # And the errored record is still traced in the output file.
    out = tmp_path / "triage_out.jsonl"
    rows = [json.loads(l) for l in out.read_text().splitlines() if l.strip()]
    e1 = next(r for r in rows if r["id"] == "e1")
    assert e1.get("useful") is None and e1.get("triage_error") == "rate_limit"


def test_run_sync_returns_zero_on_full_success(tmp_path, monkeypatch):
    """Full success => exit 0 (guards against over-eager fail-loud)."""

    def fake_call(email):
        return email["id"], {"useful": True}

    _wire(tmp_path, monkeypatch, _EMAILS, fake_call)
    rc = tg.run_sync(resume=False)
    assert rc == 0, f"expected exit 0 on full success, got {rc!r}"


def test_fresh_run_full_success_exits_zero(tmp_path, monkeypatch):
    """The BACKFILL case: a truly FRESH run (resume=False, no pre-existing
    progress file) that fully succeeds must exit 0.

    Regression guard against the concern that `residual` reads a `progress`
    dict left empty on a fresh run. It is not empty: the run loop fills it
    in-memory (progress[custom_id] = result), the same source `merge` uses.
    This test asserts the fresh precondition EXPLICITLY (progress absent
    before the call) so the coverage is unambiguous."""

    def fake_call(email):
        return email["id"], {"useful": True}

    _wire(tmp_path, monkeypatch, _EMAILS, fake_call)

    # Fresh precondition: no progress file exists yet, and this is NOT a resume.
    assert not os.path.exists(tg.SYNC_PROGRESS_FILE), "precondition: progress must be absent"

    rc = tg.run_sync(resume=False)

    assert rc == 0, f"fresh full-success backfill must exit 0, got {rc!r}"
    # No row should carry a triage_error, and none should be unclassified.
    rows = [json.loads(l) for l in (tmp_path / "triage_out.jsonl").read_text().splitlines() if l.strip()]
    assert len(rows) == len(_EMAILS)
    assert all(r.get("useful") is not None and "triage_error" not in r for r in rows)


def test_load_sync_progress_raises_on_corrupt_line(tmp_path, monkeypatch):
    """C2: a malformed JSONL line in the progress journal must be DETECTED and
    reported with file path + line number, never silently skipped. Fails on the
    old code (silently continues and returns a dict)."""
    p = tmp_path / "sync_progress.jsonl"
    p.write_text(
        json.dumps({"custom_id": "e1", "useful": True}) + "\n"
        + "{ this is not valid json\n"                      # line 2: corrupt
        + json.dumps({"custom_id": "e2", "useful": False}) + "\n"
    )
    monkeypatch.setattr(tg, "SYNC_PROGRESS_FILE", str(p))

    with pytest.raises(ValueError) as ei:
        tg._load_sync_progress()

    msg = str(ei.value)
    assert str(p) in msg, "error must report the progress file path"
    assert "riga 2" in msg, "error must report the corrupt line number"


def test_run_sync_fails_on_duplicate_export_ids(tmp_path, monkeypatch):
    """C3: duplicate custom_id in the export must fail explicitly BEFORE any
    work. Fails on the old code (no dedup guard -> provider called, exit 0)."""
    emails = [
        {"id": "e1", "from": "a@x.it", "subject": "s1", "snippet": "hello"},
        {"id": "e1", "from": "a2@x.it", "subject": "s1-bis", "snippet": "other"},
        {"id": "e2", "from": "b@x.it", "subject": "s2", "snippet": "world"},
    ]
    called = []

    def fake_call(email):
        called.append(email["id"])
        return email["id"], {"useful": True}

    _wire(tmp_path, monkeypatch, emails, fake_call)

    rc = tg.run_sync(resume=False)

    assert rc == 1, f"expected exit 1 on duplicate export ids, got {rc!r}"
    assert called == [], "provider must NOT run when the export has duplicate ids"


def test_run_sync_reports_missing_id_not_duplicate(tmp_path, monkeypatch, capsys):
    """C3 split: two emails with id="" must be reported as MISSING-ID, not
    collapsed onto the "" key and mislabeled as duplicates. Fails on the old
    code (Counter(get("id","")) -> {"": 2} -> 'duplicati')."""
    emails = [
        {"id": "", "from": "a@x.it", "subject": "s1", "snippet": "hello"},
        {"id": "", "from": "b@x.it", "subject": "s2", "snippet": "world"},
        {"id": "e3", "from": "c@x.it", "subject": "s3", "snippet": "ok"},
    ]
    called = []

    def fake_call(email):
        called.append(email.get("id"))
        return email.get("id"), {"useful": True}

    _wire(tmp_path, monkeypatch, emails, fake_call)

    rc = tg.run_sync(resume=False)

    assert rc == 1, f"expected exit 1 on missing id, got {rc!r}"
    assert called == [], "provider must NOT run when the export has emails without id"
    out = capsys.readouterr().out
    assert "senza id" in out, "must report the MISSING-ID cause"
    assert "duplicati" not in out, "must NOT mislabel missing ids as duplicates"
