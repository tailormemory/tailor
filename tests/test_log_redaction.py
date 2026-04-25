"""Regression test for _RedactTokenFilter — ensures the MCP token (passed
as `?token=<hex>` query param) is never written verbatim to uvicorn access
logs.

Imports the filter class directly from mcp_server.py rather than spinning
up the FastMCP app, so the test is fast and side-effect free.
"""

from __future__ import annotations

import logging
import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)


@pytest.fixture(scope="module")
def filter_class():
    """Pull _RedactTokenFilter from mcp_server without triggering server startup.

    mcp_server.py guards its uvicorn launch behind `if __name__ == '__main__':`,
    so a plain import is safe.
    """
    import mcp_server  # noqa: F401
    return mcp_server._RedactTokenFilter


def _make_record(client_addr, method, full_path, http_version, status_code):
    """Build a fake uvicorn.access LogRecord matching uvicorn's args tuple."""
    return logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg='%s - "%s %s HTTP/%s" %d',
        args=(client_addr, method, full_path, http_version, status_code),
        exc_info=None,
    )


def test_redacts_lowercase_token(filter_class):
    f = filter_class()
    r = _make_record("1.2.3.4:0", "GET", "/mcp?token=secret123", "1.1", 200)
    assert f.filter(r) is True
    assert r.args[2] == "/mcp?token=REDACTED"


def test_redacts_long_hex_token(filter_class):
    """Real token is 64 hex chars — make sure we don't truncate the redaction."""
    long_token = "f8a692cfdcc7d21bcadb5dc0b58e0ec91458191ca6abdf26367369a8f955eff9"
    f = filter_class()
    r = _make_record("1.2.3.4:0", "GET", f"/mcp?token={long_token}", "1.1", 200)
    f.filter(r)
    assert long_token not in r.args[2]
    assert r.args[2] == "/mcp?token=REDACTED"


def test_preserves_other_query_params(filter_class):
    f = filter_class()
    r = _make_record("1.2.3.4:0", "POST", "/mcp?token=abc&foo=bar", "1.1", 202)
    f.filter(r)
    assert r.args[2] == "/mcp?token=REDACTED&foo=bar"


def test_redacts_when_token_is_not_first_param(filter_class):
    f = filter_class()
    r = _make_record("1.2.3.4:0", "GET", "/mcp?foo=bar&token=xyz", "1.1", 200)
    f.filter(r)
    assert r.args[2] == "/mcp?foo=bar&token=REDACTED"


def test_case_insensitive_match(filter_class):
    f = filter_class()
    for variant in ("TOKEN=upper", "Token=mixed", "tOkEn=weird"):
        r = _make_record("1.2.3.4:0", "GET", f"/mcp?{variant}", "1.1", 200)
        f.filter(r)
        # The original value must be gone, replaced by REDACTED
        assert "REDACTED" in r.args[2], f"Failed for variant {variant!r}: {r.args[2]!r}"


def test_passthrough_when_no_token(filter_class):
    f = filter_class()
    r = _make_record("1.2.3.4:0", "GET", "/api/health", "1.1", 200)
    f.filter(r)
    assert r.args[2] == "/api/health"


def test_safe_on_empty_args(filter_class):
    """Non-uvicorn.access records (e.g. uvicorn errors) have different args; must not crash."""
    f = filter_class()
    r = logging.LogRecord("uvicorn.error", logging.INFO, "", 0, "boom", (), None)
    assert f.filter(r) is True


def test_safe_on_non_string_full_path(filter_class):
    f = filter_class()
    r = _make_record("1.2.3.4:0", "GET", None, "1.1", 200)
    assert f.filter(r) is True
    assert r.args[2] is None


def test_filter_returns_true_always(filter_class):
    """Filter must never drop a record — only mutate it."""
    f = filter_class()
    cases = [
        ("/mcp?token=x", "1.1"),
        ("/api/health", "1.1"),
        ("/", "1.0"),
    ]
    for path, ver in cases:
        r = _make_record("1.2.3.4:0", "GET", path, ver, 200)
        assert f.filter(r) is True


def test_filter_survives_uvicorn_dictconfig():
    """End-to-end: uvicorn calls dictConfig(LOGGING_CONFIG_with_filter) during
    serve(). The filter must remain wired to uvicorn.access after that.

    This caught a regression where addFilter() at module import was wiped out
    by uvicorn's own dictConfig call. Fix: build log_config with the filter
    declaratively and pass via uvicorn.Config(log_config=...).
    """
    import io
    import logging
    import logging.config
    from copy import deepcopy

    import mcp_server
    from uvicorn.config import LOGGING_CONFIG

    # In production mcp_server.py runs as __main__ and references
    # "__main__._RedactTokenFilter". In tests pytest is __main__, so we
    # reference the class via its actual module path. Both forms exercise
    # the same dictConfig resolution path.
    log_cfg = deepcopy(LOGGING_CONFIG)
    log_cfg.setdefault("filters", {})["redact_token"] = {
        "()": "mcp_server._RedactTokenFilter",
    }
    log_cfg["loggers"]["uvicorn.access"]["filters"] = ["redact_token"]

    logging.config.dictConfig(log_cfg)

    acc = logging.getLogger("uvicorn.access")
    assert any(
        f.__class__.__name__ == "_RedactTokenFilter" for f in acc.filters
    ), "filter not installed on uvicorn.access after dictConfig"

    # Capture the formatted output
    buf = io.StringIO()
    for h in acc.handlers:
        if hasattr(h, "stream"):
            h.stream = buf

    acc.info(
        '%s - "%s %s HTTP/%s" %d',
        "1.2.3.4:0", "GET", "/mcp?token=THIS_MUST_NOT_LEAK", "1.1", 200,
    )
    out = buf.getvalue()
    assert "THIS_MUST_NOT_LEAK" not in out, f"raw token leaked into log: {out!r}"
    assert "token=REDACTED" in out, f"redaction not applied: {out!r}"
