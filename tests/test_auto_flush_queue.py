"""Tests for scripts.services.auto_flush_queue.

Coverage del gate puro (evaluate_gate) e dell'estrazione JSON multi-oggetto
(_extract_json_objects), che è il punto fragile: il tool flush stampa DUE
oggetti JSON concatenati su stdout (blocco audit + blocco flush) e
l'orchestratore deve prendere quello giusto. Niente side effect: non tocca
ChromaDB, non manda Telegram, non muta processi.
"""

from __future__ import annotations

import json
import os
import sys
import time

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts", "services"))

import auto_flush_queue as afq  # noqa: E402


# ─────────────────────────── evaluate_gate ───────────────────────────

def _audit(queue=0, orphans=0, ghosts=0, unknown=0):
    return {
        "queue_total": queue,
        "sql_only_orphans": [{"embedding_id": f"o{i}"} for i in range(orphans)],
        "hnsw_only_ghosts": [{"embedding_id": f"g{i}"} for i in range(ghosts)],
        "unknown_count": unknown,
    }


def test_gate_act_clean_backlog_above_min():
    action, _ = afq.evaluate_gate(_audit(queue=363), qmin=300)
    assert action == "act"


def test_gate_act_exactly_at_min():
    action, _ = afq.evaluate_gate(_audit(queue=300), qmin=300)
    assert action == "act"


def test_gate_skip_below_min():
    action, reason = afq.evaluate_gate(_audit(queue=299), qmin=300)
    assert action == "skip"
    assert "299" in reason


def test_gate_skip_empty_queue():
    action, _ = afq.evaluate_gate(_audit(queue=0), qmin=300)
    assert action == "skip"


def test_gate_escalate_on_orphans_even_above_min():
    # orphans > 0 NON è #6975-puro: mai auto-flush, anche con queue alta.
    action, reason = afq.evaluate_gate(_audit(queue=900, orphans=5), qmin=300)
    assert action == "escalate"
    assert "orphans=5" in reason


def test_gate_escalate_on_ghosts():
    action, _ = afq.evaluate_gate(_audit(queue=400, ghosts=2), qmin=300)
    assert action == "escalate"


def test_gate_escalate_on_unknown():
    action, _ = afq.evaluate_gate(_audit(queue=400, unknown=1), qmin=300)
    assert action == "escalate"


def test_gate_escalate_takes_priority_over_below_min():
    # orphans>0 ma queue sotto soglia → comunque escalation (danno reale possibile).
    action, _ = afq.evaluate_gate(_audit(queue=10, orphans=3), qmin=300)
    assert action == "escalate"


def test_gate_handles_missing_keys():
    action, _ = afq.evaluate_gate({}, qmin=300)
    assert action == "skip"


# ───────────────────────── _extract_json_objects ─────────────────────────

def test_extract_single_object():
    objs = afq._extract_json_objects('{"queue_total": 363}')
    assert len(objs) == 1
    assert objs[0]["queue_total"] == 363


def test_extract_two_concatenated_objects():
    # È ESATTAMENTE l'output di --flush-queue-backlog --json: audit poi flush.
    audit = json.dumps({"queue_total": 363, "sql_only_orphans": []}, indent=2)
    flush = json.dumps({"success": True, "post_queue_total": 0}, indent=2)
    objs = afq._extract_json_objects(audit + "\n" + flush + "\n")
    assert len(objs) == 2
    assert objs[0]["queue_total"] == 363       # primo = audit
    assert objs[-1]["post_queue_total"] == 0   # ultimo = flush


def test_extract_empty_string():
    assert afq._extract_json_objects("") == []


def test_extract_ignores_trailing_garbage():
    objs = afq._extract_json_objects('{"a": 1}\nnot-json-here')
    assert len(objs) == 1
    assert objs[0]["a"] == 1


def test_extract_with_leading_whitespace():
    objs = afq._extract_json_objects('\n\n  {"a": 1}\n  {"b": 2}\n')
    assert [o.get("a") or o.get("b") for o in objs] == [1, 2]


# ───────────────────────── _classify_flush (skipped bool vs int) ─────────────

def test_classify_flush_gate_skip_bool_true():
    # Ramo gate-skip del tool: {"skipped": True} (bool) → NO-OP, non un flush reale.
    fj = {"flush_queue_backlog": {"skipped": True, "reason": "below_..."}}
    assert afq._classify_flush(fj) == ("gate_skip", None)


def test_classify_flush_real_flush_skipped_zero():
    # Risultato di un flush reale: skipped è un int → "flushed".
    fj = {"flush_queue_backlog": {"processed": 900, "skipped": 0, "errors": 0}}
    assert afq._classify_flush(fj) == ("flushed", 0)


def test_classify_flush_real_flush_partial():
    fj = {"flush_queue_backlog": {"processed": 850, "skipped": 50, "errors": 0}}
    assert afq._classify_flush(fj) == ("flushed", 50)


def test_classify_flush_false_is_real_flush_not_gate_skip():
    fj = {"flush_queue_backlog": {"processed": 0, "skipped": False, "errors": 0}}
    assert afq._classify_flush(fj) == ("flushed", False)


def test_classify_flush_none():
    assert afq._classify_flush(None) == ("none", None)
    assert afq._classify_flush({}) == ("none", None)


# ───────────────────────── verify_backup ─────────────────────────────────────

def test_verify_backup_accepts_fresh_valid_gzip(monkeypatch, tmp_path):
    backup = tmp_path / "chroma_fresh.sqlite3.gz"
    backup.write_bytes(b"placeholder")
    monkeypatch.setattr(afq, "BACKUPS_DIR", str(tmp_path))
    monkeypatch.setattr(afq.subprocess, "run", lambda cmd, **kw: _FakeProc(returncode=0))
    assert afq.verify_backup() is True


def test_verify_backup_rejects_old_file(monkeypatch, tmp_path):
    backup = tmp_path / "chroma_old.sqlite3.gz"
    backup.write_bytes(b"placeholder")
    old = time.time() - afq.BACKUP_FRESH_MAX_S - 10
    os.utime(backup, (old, old))
    monkeypatch.setattr(afq, "BACKUPS_DIR", str(tmp_path))
    assert afq.verify_backup() is False


# ───────────────────────── restart_mcp ───────────────────────────────────────

class _FakeProc:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_restart_mcp_grant_missing(monkeypatch):
    # Anche se unload fallisce, il recovery tenta comunque load.
    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        return _FakeProc(returncode=1, stderr="sudo: a password is required")

    monkeypatch.setattr(afq.subprocess, "run", fake_run)
    monkeypatch.setattr(afq.time, "sleep", lambda *_: None)
    assert afq.restart_mcp(old_pid=1000) == "unload_failed"
    assert [c[3] for c in calls] == ["unload", "load"]


def test_restart_mcp_healthy_new_pid(monkeypatch):
    monkeypatch.setattr(afq.subprocess, "run", lambda cmd, **kw: _FakeProc(returncode=0))
    monkeypatch.setattr(afq.time, "sleep", lambda *_: None)
    monkeypatch.setattr(afq, "find_mcp_pid", lambda: 2222)
    monkeypatch.setattr(afq.requests, "get", lambda *a, **kw: _FakeProc(returncode=0))
    monkeypatch.setattr(_FakeProc, "status_code", 200, raising=False)
    assert afq.restart_mcp(old_pid=1000) == "healthy"


def test_restart_mcp_load_failed_after_unload_ok_is_mcp_down(monkeypatch):
    def fake_run(cmd, **kw):
        return _FakeProc(returncode=0 if cmd[3] == "unload" else 1)

    monkeypatch.setattr(afq.subprocess, "run", fake_run)
    monkeypatch.setattr(afq.time, "sleep", lambda *_: None)
    assert afq.restart_mcp(old_pid=1000) == "MCP-DOWN"


def test_restart_mcp_http_health_timeout(monkeypatch):
    monkeypatch.setattr(afq.subprocess, "run", lambda cmd, **kw: _FakeProc(returncode=0))
    monkeypatch.setattr(afq.time, "sleep", lambda *_: None)
    ticks = iter([0, 0, 31])
    monkeypatch.setattr(afq.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(afq, "find_mcp_pid", lambda: 2222)
    monkeypatch.setattr(afq.requests, "get", lambda *a, **kw: _FakeProc())
    assert afq.restart_mcp(old_pid=1000) == "health_timeout"


def test_find_mcp_pid_multiple_aborts(monkeypatch):
    monkeypatch.setattr(afq.subprocess, "run",
                        lambda cmd, **kw: _FakeProc(returncode=0, stdout="1000\n2000\n"))
    assert afq.find_mcp_pid() is None


# ───────────────────────── run_procedure (orchestrazione) ────────────────────

def _stub_procedure(monkeypatch, tmp_path, *, flush=None, flush_raises=False,
                    wait_lock=True, restart="healthy"):
    """Stubba tutte le dipendenze esterne di run_procedure e ritorna un dict di
    contatori delle chiamate per le asserzioni."""
    calls = {"exit_maintenance": 0, "restart": 0, "flush": 0, "signal_on": 0,
             "telegram": []}

    monkeypatch.setattr(afq, "MAINTENANCE_LOCK", str(tmp_path / "noexist.lock"))
    monkeypatch.setattr(afq, "queue_min", lambda: 300)
    monkeypatch.setattr(afq, "send_telegram", lambda m: calls["telegram"].append(m) or True)
    monkeypatch.setattr(afq, "find_mcp_pid", lambda: 1000)
    monkeypatch.setattr(afq, "run_backup", lambda: True)
    monkeypatch.setattr(afq, "verify_backup", lambda: True)

    def signal_on(pid):
        calls["signal_on"] += 1
        return True
    monkeypatch.setattr(afq, "signal_maintenance_on", signal_on)
    monkeypatch.setattr(afq, "_wait_lock", lambda present, timeout_s: wait_lock)

    def run_flush():
        calls["flush"] += 1
        if flush_raises:
            raise RuntimeError("boom in flush")
        return flush
    monkeypatch.setattr(afq, "run_flush", run_flush)

    def exit_m(pid):
        calls["exit_maintenance"] += 1
        return True
    monkeypatch.setattr(afq, "exit_maintenance", exit_m)

    def restart_m(old_pid):
        calls["restart"] += 1
        return restart
    monkeypatch.setattr(afq, "restart_mcp", restart_m)
    return calls


def _audit_act():
    return _audit(queue=350)


def test_run_procedure_success_full(monkeypatch, tmp_path):
    flush = {"flush_queue_backlog": {"processed": 900, "skipped": 0, "errors": 0},
             "post_queue_total": 0, "pre": {"drift": 800}, "post": {"drift": 5},
             "vector_seq_delta": 1100, "success": True}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(0, flush), restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 0
    assert calls["exit_maintenance"] == 1
    assert calls["restart"] == 1
    assert any("SUCCESS" in m for m in calls["telegram"])


def test_run_procedure_success_flush_but_restart_failed_is_partial(monkeypatch, tmp_path):
    flush = {"flush_queue_backlog": {"processed": 900, "skipped": 0, "errors": 0},
             "post_queue_total": 0, "pre": {"drift": 800}, "post": {"drift": 5},
             "success": True}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(0, flush), restart="health_timeout")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 1                                  # PARTIAL → return 1
    assert calls["exit_maintenance"] == 1
    assert calls["restart"] == 1
    assert any("PARTIAL" in m for m in calls["telegram"])


def test_run_procedure_flush_exception_still_cleans_and_restarts(monkeypatch, tmp_path):
    calls = _stub_procedure(monkeypatch, tmp_path, flush_raises=True, restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 1                                  # flush fallito → FAIL
    assert calls["flush"] == 1
    assert calls["exit_maintenance"] == 1           # cleanup garantito
    assert calls["restart"] == 1                    # restart comunque chiamato
    assert any("FAIL" in m for m in calls["telegram"])


def test_run_procedure_lock_timeout_after_usr1_still_exits_and_restarts(monkeypatch, tmp_path):
    # USR1 inviato ma maintenance.lock non comparso → flush NON eseguito,
    # ma exit_maintenance (USR2) e restart devono comunque partire.
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(0, None),
                            wait_lock=False, restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 1                                  # maintenance non confermata → FAIL
    assert calls["signal_on"] == 1
    assert calls["flush"] == 0                       # flush MAI eseguito
    assert calls["exit_maintenance"] == 1           # USR2 garantito (issue #1)
    assert calls["restart"] == 1


def test_run_procedure_noop_when_tool_gate_skips(monkeypatch, tmp_path):
    # La queue si drena tra gate e flush: il tool ritorna skipped=True (bool).
    flush = {"flush_queue_backlog": {"skipped": True, "reason": "below_..."},
             "post_queue_total": 0}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(0, flush), restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 0
    assert any("NO-OP" in m for m in calls["telegram"])


def test_run_procedure_noop_restart_failed_is_partial(monkeypatch, tmp_path):
    flush = {"flush_queue_backlog": {"skipped": True, "reason": "below_..."}}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(0, flush), restart="MCP-DOWN")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 1
    assert any("PARTIAL / FAIL-RESTART" in m for m in calls["telegram"])
    assert any("MCP-DOWN" in m for m in calls["telegram"])
    assert not any("— NO-OP" in m for m in calls["telegram"])


def test_run_procedure_rc0_malformed_json_never_success(monkeypatch, tmp_path):
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(0, {"unexpected": True}),
                            restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 1
    assert any("FAIL" in m for m in calls["telegram"])
    assert not any("SUCCESS" in m for m in calls["telegram"])


def test_run_procedure_false_skipped_not_noop(monkeypatch, tmp_path):
    flush = {"flush_queue_backlog": {"processed": 1, "skipped": False, "errors": 0},
             "success": True, "post_queue_total": 0}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(0, flush), restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 0
    assert any("SUCCESS" in m for m in calls["telegram"])
    assert not any("NO-OP" in m for m in calls["telegram"])


def test_exit_maintenance_never_removes_lock_manually(monkeypatch, tmp_path):
    lock = tmp_path / "maintenance.lock"
    lock.write_text("other-session")
    monkeypatch.setattr(afq, "MAINTENANCE_LOCK", str(lock))
    monkeypatch.setattr(afq.os, "kill", lambda *a: None)
    monkeypatch.setattr(afq, "_wait_lock", lambda **kw: False)
    assert afq.exit_maintenance(1000) is False
    assert lock.read_text() == "other-session"


def test_run_procedure_aborts_on_race_lock_present(monkeypatch, tmp_path):
    # maintenance.lock già presente al re-check → abort, niente segnali/flush/restart.
    lock = tmp_path / "maintenance.lock"
    lock.write_text("9999")
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(0, None))
    monkeypatch.setattr(afq, "MAINTENANCE_LOCK", str(lock))   # override: esiste
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 0
    assert calls["signal_on"] == 0
    assert calls["flush"] == 0
    assert calls["exit_maintenance"] == 0
    assert calls["restart"] == 0
    assert any("ABORT" in m for m in calls["telegram"])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
