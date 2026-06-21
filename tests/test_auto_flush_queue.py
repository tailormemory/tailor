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


@pytest.fixture(autouse=True)
def _isolate_log_path(monkeypatch, tmp_path):
    """log() scrive in afq.LOG_PATH (default = logs/auto_flush.log di produzione).
    Solo _drive_main_escalation patcha afq.log a no-op; tutti gli altri test che
    arrivano a log() inquinerebbero il log reale. Reindirizza LOG_PATH a tmp_path
    per ogni test."""
    monkeypatch.setattr(afq, "LOG_PATH", str(tmp_path / "auto_flush.log"))


# ─────────────────────────── evaluate_gate ───────────────────────────

def _audit(queue=0, orphans=0, ghosts=0, unknown=0, benign_ghosts=0,
           anomalous_ghost_count="auto", blocking_unknown_count="auto",
           with_benign_key=True, ghosts_list="auto", queue_total="auto",
           collection_pending="absent"):
    """ghosts/benign_ghosts costruiscono la lista; gli override 'raw'
    (ghosts_list, queue_total, *_count) servono ai test di incoerenza schema.

    collection_pending: 'absent' (default) → campo collection_queue_pending_count
    NON iniettato (deploy misto / tool vecchio → fallback queue_total). Un valore
    esplicito (int o malformato) inietta il campo → metrica primaria del gate."""
    gl = [{"embedding_id": f"g{i}", **({"benign": False} if with_benign_key else {})}
          for i in range(ghosts)]
    gl += [{"embedding_id": f"b{i}", "benign": True} for i in range(benign_ghosts)]
    a = {
        "sql_only_orphans": [{"embedding_id": f"o{i}"} for i in range(orphans)],
        "hnsw_only_ghosts": gl if ghosts_list == "auto" else ghosts_list,
        "unknown_count": unknown,
    }
    a["queue_total"] = queue if queue_total == "auto" else queue_total
    if collection_pending != "absent":
        a["collection_queue_pending_count"] = collection_pending
    if anomalous_ghost_count != "auto":
        a["anomalous_ghost_count"] = anomalous_ghost_count
    if blocking_unknown_count != "auto":
        a["blocking_unknown_count"] = blocking_unknown_count
    return a


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


def test_gate_empty_dict_is_schema_invalid_escalate():
    # audit vuoto/malformato (hnsw_only_ghosts assente) → conservativo, non silenzioso.
    # run_audit() ritorna None su fallimento (e main() aborta prima); {} è degenere.
    action, reason = afq.evaluate_gate({}, qmin=300)
    assert action == "escalate"
    assert "schema_invalid" in reason


# ─────────────────── _ghost_counts: JSON incoerente (round 3) ───────────────────

def test_ghost_counts_contradiction_agg_vs_list():
    # lista ha 1 ghost benign:False ma anomalous_ghost_count:0 → invalid
    gc = afq._ghost_counts(_audit(ghosts=1, anomalous_ghost_count=0, blocking_unknown_count=0))
    assert gc.schema_invalid is True


def test_ghost_counts_agg_exceeds_list():
    gc = afq._ghost_counts(_audit(ghosts=2, anomalous_ghost_count=5))
    assert gc.schema_invalid is True


def test_ghost_counts_negative_anomalous():
    gc = afq._ghost_counts(_audit(ghosts=2, anomalous_ghost_count=-1))
    assert gc.schema_invalid is True


def test_ghost_counts_non_numeric_anomalous_no_crash():
    gc = afq._ghost_counts(_audit(ghosts=2, anomalous_ghost_count="x"))
    assert gc.schema_invalid is True           # niente eccezione


def test_ghost_counts_negative_blocking_unknown():
    gc = afq._ghost_counts(_audit(ghosts=0, anomalous_ghost_count=0, blocking_unknown_count=-3))
    assert gc.schema_invalid is True


def test_ghost_counts_list_absent():
    gc = afq._ghost_counts(_audit(ghosts_list=None))   # hnsw_only_ghosts non-lista
    assert gc.schema_invalid is True


def test_ghost_counts_mixed_per_ghost_flags():
    gl = [{"embedding_id": "g0", "benign": False}, {"embedding_id": "g1"}]  # uno senza flag
    gc = afq._ghost_counts(_audit(ghosts_list=gl))
    assert gc.schema_invalid is True


def test_ghost_counts_queue_non_numeric_escalates():
    action, reason = afq.evaluate_gate(
        _audit(ghosts=0, anomalous_ghost_count=0, blocking_unknown_count=0, queue_total="NaN"),
        qmin=300)
    assert action == "escalate" and "schema_invalid" in reason


def test_ghost_counts_legacy_distinct_from_invalid():
    # ghost senza flag e senza aggregati → legacy (conservativo) ma NON invalid
    gc = afq._ghost_counts(_audit(ghosts=2, with_benign_key=False))
    assert gc.schema_invalid is False and gc.legacy is True and gc.anomalous == 2


def test_ghost_counts_partial_json_blocking_defaults_conservative():
    # anomalous presente, blocking assente, unknown_count=2 → bu=2 (non 0), valido
    gc = afq._ghost_counts(_audit(ghosts=0, benign_ghosts=1, anomalous_ghost_count=0, unknown=2))
    assert gc.schema_invalid is False and gc.blocking_unknown == 2


def test_gate_schema_invalid_forces_escalate_over_skip():
    # anche con queue sotto soglia, schema_invalid → escalate (non skip)
    action, _ = afq.evaluate_gate(_audit(ghosts_list="not-a-list", queue_total=0), qmin=300)
    assert action == "escalate"


# ─────────────────── gate: benigni vs anomali (round 2/3) ───────────────────

def test_gate_act_only_benign_ghosts():
    # caso #6975 reale: solo ghost benigni, niente anomali → flush ammesso.
    action, _ = afq.evaluate_gate(
        _audit(queue=400, benign_ghosts=5, anomalous_ghost_count=0, blocking_unknown_count=0),
        qmin=300)
    assert action == "act"


def test_gate_escalate_mixed_benign_anomalous():
    action, reason = afq.evaluate_gate(_audit(queue=400, ghosts=1, benign_ghosts=3), qmin=300)
    assert action == "escalate"
    assert "anomalous_ghosts=1" in reason and "benign_ghosts=3" in reason


def test_gate_skip_benign_below_min():
    action, _ = afq.evaluate_gate(
        _audit(queue=100, benign_ghosts=5, anomalous_ghost_count=0, blocking_unknown_count=0),
        qmin=300)
    assert action == "skip"


# ─────────────────── gate: ramo drift (close drift-blind gap) ───────────────────

def _audit_drift(drift=None, drift_warning=None, **kw):
    """_audit() + iniezione opzionale dei campi drift/drift_warning (additivi)."""
    a = _audit(**kw)
    if drift is not None:
        a["drift"] = drift
    if drift_warning is not None:
        a["drift_warning"] = drift_warning
    return a


def test_gate_act_on_drift_with_queue_below_min():
    # drift >= warning, queue sotto soglia, indice pulito → act (ramo drift).
    action, reason = afq.evaluate_gate(
        _audit_drift(drift=850, drift_warning=800, queue=10,
                     anomalous_ghost_count=0, blocking_unknown_count=0),
        qmin=300)
    assert action == "act"
    assert "drift=850" in reason and "queue_total=10" in reason


def test_gate_skip_drift_below_warning_queue_below_min():
    # drift < warning E queue < min → skip.
    action, reason = afq.evaluate_gate(
        _audit_drift(drift=500, drift_warning=800, queue=100,
                     anomalous_ghost_count=0, blocking_unknown_count=0),
        qmin=300)
    assert action == "skip"
    assert "drift=500" in reason and "queue_total=100" in reason


def test_gate_drift_missing_falls_back_to_queue_only_no_escalate():
    # Campi drift assenti (tool vecchio): gate degrada a queue-only, NON escalate.
    action_skip, reason = afq.evaluate_gate(_audit(queue=100), qmin=300)
    assert action_skip == "skip"
    assert "n/d" in reason                                  # nota campo-assente
    action_act, _ = afq.evaluate_gate(_audit(queue=400), qmin=300)
    assert action_act == "act"                              # queue alta → act comunque


def test_gate_act_on_drift_with_empty_queue():
    # drift >= warning con queue == 0 → act. Il rifiuto queue-empty lo gestisce
    # do_flush (il tool), NON il gate.
    action, _ = afq.evaluate_gate(
        _audit_drift(drift=900, drift_warning=800, queue=0,
                     anomalous_ghost_count=0, blocking_unknown_count=0),
        qmin=300)
    assert action == "act"


def test_gate_drift_fields_do_not_bypass_schema_invalid_guard():
    # Anche con drift >= warning, schema_invalid resta intatto → escalate.
    action, reason = afq.evaluate_gate(
        _audit_drift(drift=900, drift_warning=800, ghosts_list="not-a-list"),
        qmin=300)
    assert action == "escalate"
    assert "schema_invalid" in reason


def test_gate_drift_fields_do_not_bypass_orphan_escalate():
    # drift >= warning ma orphans > 0 → escalate (drift anomalo ha priorità).
    action, _ = afq.evaluate_gate(
        _audit_drift(drift=900, drift_warning=800, queue=10, orphans=2),
        qmin=300)
    assert action == "escalate"


# ──────── gate: metrica collection_queue_pending_count (#6975, switch) ────────

def test_gate_act_on_collection_pending_queue_total_irrelevant():
    # collection_pending >= min con queue_total basso → act sul backlog azionabile.
    action, reason = afq.evaluate_gate(
        _audit(queue_total=5, collection_pending=363,
               anomalous_ghost_count=0, blocking_unknown_count=0),
        qmin=300)
    assert action == "act"
    assert "collection_pending=363>=min=300" in reason
    assert "queue_total=5" in reason                   # globale riportato (osservabilità)


def test_gate_skip_high_global_queue_but_collection_pending_below_min():
    # backlog globale alto (altro topic) ma collection_pending < min → NO act.
    action, reason = afq.evaluate_gate(
        _audit(queue_total=5000, collection_pending=50,
               anomalous_ghost_count=0, blocking_unknown_count=0),
        qmin=300)
    assert action == "skip"
    assert "collection_pending=50" in reason and "queue_total=5000" in reason


def test_gate_act_on_drift_safety_net_with_collection_pending_below_min():
    # drift >= warning, collection_pending < min → act via drift (rete di sicurezza).
    action, reason = afq.evaluate_gate(
        _audit_drift(drift=850, drift_warning=800, collection_pending=10, queue_total=5000,
                     anomalous_ghost_count=0, blocking_unknown_count=0),
        qmin=300)
    assert action == "act"
    assert "drift=850>=warning=800" in reason


def test_gate_collection_pending_absent_falls_back_to_queue_total():
    # campo assente (tool vecchio / deploy misto) → fallback queue_total, NO escalate.
    action_act, reason_act = afq.evaluate_gate(_audit(queue_total=400), qmin=300)
    assert action_act == "act"
    assert "fallback queue_total=400" in reason_act
    action_skip, _ = afq.evaluate_gate(_audit(queue_total=100), qmin=300)
    assert action_skip == "skip"


def test_gate_collection_pending_malformed_escalates_schema_invalid():
    # campo PRESENTE ma malformato/negativo → schema_invalid escalate (≠ fallback).
    action_neg, reason_neg = afq.evaluate_gate(
        _audit(queue_total=400, collection_pending=-5,
               anomalous_ghost_count=0, blocking_unknown_count=0),
        qmin=300)
    assert action_neg == "escalate" and "schema_invalid" in reason_neg
    action_nan, _ = afq.evaluate_gate(
        _audit(queue_total=400, collection_pending="NaN",
               anomalous_ghost_count=0, blocking_unknown_count=0),
        qmin=300)
    assert action_nan == "escalate"


def _drive_main_escalation(monkeypatch, audit_obj, sent):
    monkeypatch.setattr(afq.sys, "argv", ["auto_flush_queue.py"])
    monkeypatch.setattr(afq, "log", lambda *a, **k: None)
    monkeypatch.setattr(afq, "send_telegram", lambda t: sent.append(t) or True)
    monkeypatch.setattr(afq, "run_audit", lambda: audit_obj)
    monkeypatch.setattr(afq, "queue_min", lambda: 300)
    monkeypatch.setattr(afq, "_acquire_run_lock", lambda: object())
    monkeypatch.setattr(afq.os.path, "exists", lambda p: False)
    return afq.main()


def test_telegram_escalation_mixed_state_exact_counts(monkeypatch):
    sent = []
    rc = _drive_main_escalation(monkeypatch, _audit(queue=400, ghosts=2, benign_ghosts=3), sent)
    assert rc == 0
    assert any("3 ghost benigni in attesa DIETRO 2 anomali" in s for s in sent)


def test_telegram_escalation_schema_invalid_message(monkeypatch):
    sent = []
    rc = _drive_main_escalation(monkeypatch, _audit(ghosts_list="bad", queue_total=400), sent)
    assert rc == 0
    assert any("schema invalid" in s.lower() for s in sent)


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
    backup = tmp_path / "tailor_db_full_20260609_080000.tar.gz"
    backup.write_bytes(b"placeholder")
    monkeypatch.setattr(afq, "BACKUPS_DIR", str(tmp_path))
    monkeypatch.setattr(afq.subprocess, "run", lambda cmd, **kw: _FakeProc(returncode=0))
    assert afq.verify_backup() is True


def test_verify_backup_rejects_old_file(monkeypatch, tmp_path):
    backup = tmp_path / "tailor_db_full_20260608_080000.tar.gz"
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
    health_calls = []

    def fake_get(*a, **kw):
        health_calls.append(a[0] if a else kw.get("url"))
        return _FakeProc(returncode=0)

    monkeypatch.setattr(afq.requests, "get", fake_get)
    monkeypatch.setattr(_FakeProc, "status_code", 200, raising=False)
    assert afq.restart_mcp(old_pid=1000) == "healthy"
    # il probe deve colpire esattamente l'endpoint auth-check (no-ChromaDB, 200 su localhost)
    assert health_calls == ["http://127.0.0.1:8787/api/auth/check"]


def test_restart_mcp_load_failed_after_unload_ok_is_mcp_down(monkeypatch):
    def fake_run(cmd, **kw):
        return _FakeProc(returncode=0 if cmd[3] == "unload" else 1)

    monkeypatch.setattr(afq.subprocess, "run", fake_run)
    monkeypatch.setattr(afq.time, "sleep", lambda *_: None)
    assert afq.restart_mcp(old_pid=1000) == "MCP-DOWN"


def test_restart_mcp_http_health_timeout(monkeypatch):
    monkeypatch.setattr(afq.subprocess, "run", lambda cmd, **kw: _FakeProc(returncode=0))
    monkeypatch.setattr(afq.time, "sleep", lambda *_: None)
    ticks = iter([0, 0, 91])
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


# ──────────── run_procedure: rc=4 refuse strutturati → escalation gestita ────────────

def test_run_procedure_rc4_queue_empty_is_managed_escalation(monkeypatch, tmp_path):
    flush = {"error": "queue_empty", "pre": {"drift": 850}}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(4, flush), restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 0                                          # gestito (alert inviato), non sano
    assert any("DRIFT NON FLUSHABILE" in m for m in calls["telegram"])
    assert not any("FAIL" in m for m in calls["telegram"])  # NON il ramo FAIL generico


def test_run_procedure_rc4_collection_queue_empty_is_managed_escalation(monkeypatch, tmp_path):
    # Nuovo error code post-switch: drift ≥ soglia ma collection_pending=0 → escalation
    # gestita con messaggio collection-scoped (NON il vecchio "embeddings_queue è vuota").
    flush = {"error": "collection_queue_empty", "collection_queue_pending_count": 0,
             "queue_total": 5000, "pre": {"drift": 850}}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(4, flush), restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 0
    msg = next(m for m in calls["telegram"] if "DRIFT NON FLUSHABILE" in m)
    assert "collection_queue_pending=0" in msg              # messaggio collection-scoped
    assert "embeddings_queue" not in msg.lower()            # NON il vecchio testo queue-globale
    assert not any("— FAIL" in m for m in calls["telegram"])


@pytest.mark.parametrize("err", ["queue_empty", "collection_queue_empty"])
def test_run_procedure_rc4_both_empty_errors_same_classification(monkeypatch, tmp_path, err):
    # Rolling deploy: il daemon nuovo riconosce ENTRAMBI gli error code (tool vecchio
    # emette ancora "queue_empty", nuovo "collection_queue_empty") → stessa escalation.
    flush = {"error": err, "pre": {"drift": 850}}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(4, flush), restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 0
    assert any("DRIFT NON FLUSHABILE" in m for m in calls["telegram"])
    assert not any("— FAIL" in m for m in calls["telegram"])


def test_run_procedure_rc4_ambiguous_topic_is_managed_escalation(monkeypatch, tmp_path):
    flush = {"error": "ambiguous_collection_topic"}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(4, flush), restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 0
    assert any("topic WAL ambiguo" in m for m in calls["telegram"])
    assert not any("FAIL" in m for m in calls["telegram"])


def test_run_procedure_rc4_anomalous_drift_reports_three_counts(monkeypatch, tmp_path):
    flush = {"error": "anomalous_drift", "sql_only_count": 7,
             "anomalous_ghost_count": 3, "blocking_unknown_count": 2}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(4, flush), restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 0
    msg = next(m for m in calls["telegram"] if "DRIFT ANOMALO" in m)
    assert "sql_only_orphans=`7`" in msg
    assert "anomalous_ghosts=`3`" in msg
    assert "blocking_unknown=`2`" in msg


def test_run_procedure_rc4_unknown_error_is_generic_fail(monkeypatch, tmp_path):
    # error fuori allowlist (no_recent_backup) → FAIL generico, NON escalation.
    flush = {"error": "no_recent_backup"}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(4, flush), restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 1
    assert any("— FAIL" in m for m in calls["telegram"])
    assert not any("ESCALATION" in m for m in calls["telegram"])


def test_run_procedure_rc4_missing_error_is_generic_fail(monkeypatch, tmp_path):
    # rc=4 senza campo error → mai escalation (coppia non verificata) → FAIL.
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(4, {"post_queue_total": 10}),
                            restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 1
    assert any("— FAIL" in m for m in calls["telegram"])
    assert not any("ESCALATION" in m for m in calls["telegram"])


def test_run_procedure_rc4_allowlisted_but_restart_failed_is_partial(monkeypatch, tmp_path):
    # Refuse gestito ma restart fallito → il restart fallito prevale → return 1.
    flush = {"error": "queue_empty", "pre": {"drift": 850}}
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(4, flush), restart="MCP-DOWN")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 1
    assert any("PARTIAL / FAIL-RESTART" in m for m in calls["telegram"])
    assert any("MCP-DOWN" in m for m in calls["telegram"])


def test_run_procedure_rc1_still_generic_fail(monkeypatch, tmp_path):
    calls = _stub_procedure(monkeypatch, tmp_path, flush=(1, None), restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 1
    assert any("— FAIL" in m for m in calls["telegram"])
    assert not any("ESCALATION" in m for m in calls["telegram"])


def test_run_procedure_rc5_still_generic_fail(monkeypatch, tmp_path):
    # rc=5 (criteria-non-met) NON è un refuse allowlistato → FAIL come prima.
    calls = _stub_procedure(monkeypatch, tmp_path,
                            flush=(5, {"error": "below_drift_and_queue_thresholds"}),
                            restart="healthy")
    rc = afq.run_procedure(_audit_act(), dry_run=False)
    assert rc == 1
    assert any("— FAIL" in m for m in calls["telegram"])
    assert not any("ESCALATION" in m for m in calls["telegram"])


# ─────────────────────────── write_audit_cache ───────────────────────────
# La cache è consumata da queue_depth_monitor per il segnale STUCK senza re-audit.

def test_write_audit_cache_persists_fields(tmp_path):
    path = tmp_path / "gate_audit_cache.json"
    audit = {"collection_queue_pending_count": 420, "queue_total": 1300, "extra": "ignored"}
    assert afq.write_audit_cache(audit, path=str(path)) is True
    data = json.loads(path.read_text())
    assert data["collection_queue_pending_count"] == 420
    assert data["queue_total"] == 1300
    assert isinstance(data["ts_unix"], int)
    assert data["ts_unix"] <= int(time.time())
    assert "ts" in data
    assert "extra" not in data  # solo i campi che servono al monitor


def test_write_audit_cache_missing_keys_become_null(tmp_path):
    path = tmp_path / "c.json"
    assert afq.write_audit_cache({}, path=str(path)) is True
    data = json.loads(path.read_text())
    assert data["collection_queue_pending_count"] is None
    assert data["queue_total"] is None


def test_write_audit_cache_atomic_overwrite(tmp_path):
    path = tmp_path / "c.json"
    afq.write_audit_cache({"collection_queue_pending_count": 1, "queue_total": 1}, path=str(path))
    afq.write_audit_cache({"collection_queue_pending_count": 2, "queue_total": 2}, path=str(path))
    data = json.loads(path.read_text())
    assert data["collection_queue_pending_count"] == 2
    assert not (tmp_path / "c.json.tmp").exists()  # tmp rimosso da os.replace


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
