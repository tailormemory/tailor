"""
TAILOR — Serve source document binaries referenced by KB chunks.

Exposes a single helper, handle_kb_document_request, that resolves the
`path` query parameter (relative to ingest.document_root) to an absolute
path, verifies it stays within the root, and returns the file bytes.

Auth is enforced upstream by mcp_server.BearerAuthMiddleware — this
module trusts the caller is already authenticated.
"""

from __future__ import annotations

import mimetypes
import os
from urllib.parse import quote, unquote

from starlette.responses import Response


def _resolve_download_base_url(request) -> tuple[str, str]:
    """Resolve the public base URL for constructing absolute download links.

    Returns (base_url, source) where source ∈ {"config", "auto"}. `base_url`
    has no trailing slash.

    Precedence:
      1. server.public_base_url from tailor.yaml if set and non-empty.
      2. Auto-detect from the incoming request's Host / X-Forwarded-Proto
         headers. Falls back to http://localhost:8787 when no request is
         available (e.g. a direct tool call outside an HTTP context).

    Smaller LLMs hallucinate absolute hostnames when they see relative URLs
    in tool payloads; this helper is what makes the emitted download_url
    absolute so they have something real to parrot back.
    """
    # 1. Explicit config wins — set via the dashboard when the reverse proxy
    # doesn't forward Host / X-Forwarded-Proto the way we'd auto-detect.
    try:
        from scripts.lib.config import get as _cfg_get
        configured = _cfg_get("server", "public_base_url")
    except Exception:
        configured = None
    if configured:
        s = str(configured).strip()
        if s:
            return (s.rstrip("/"), "config")

    # 2. Auto-detect from request headers.
    host = "localhost:8787"
    proto = "http"
    if request is not None:
        try:
            headers = request.headers
            host = headers.get("host") or host
            proto = headers.get("x-forwarded-proto") or proto
        except Exception:
            pass
    return (f"{proto}://{host}", "auto")


def format_document_fileref(meta: dict, base_url: str = "") -> str:
    """Render the file_path / file_type / folder / download_url lines for
    document-sourced KB search results.

    Returns '' when meta['source'] != 'document' so the four fields are
    absent (not null, not present-with-empty-value) in the rendered output
    for non-document results. The download_url is only emitted when a
    file_path is present; the path component is URL-encoded with safe=''.

    When `base_url` is non-empty, the download_url is emitted as an
    absolute URL ("{base_url}/api/kb/document?path=..."). When empty, a
    relative URL is emitted for backward compatibility with tests and
    callers that don't have a request in scope.
    """
    if (meta or {}).get("source") != "document":
        return ""
    file_path = meta.get("file_path", "") or ""
    file_type = meta.get("file_type", "") or ""
    folder = meta.get("folder", "") or ""
    if file_path:
        prefix = base_url.rstrip("/") if base_url else ""
        download_url = f"{prefix}/api/kb/document?path={quote(file_path, safe='')}"
    else:
        download_url = ""
    return (
        f"file_path: {file_path}\n"
        f"file_type: {file_type}\n"
        f"folder: {folder}\n"
        f"download_url: {download_url}\n"
    )


_EXTRA_MIMES = {
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".pdf": "application/pdf",
}


def _json_response(data: dict, status_code: int = 200, cors_headers: dict | None = None) -> Response:
    import json
    body = json.dumps(data, ensure_ascii=False)
    headers = dict(cors_headers or {})
    return Response(
        content=body, status_code=status_code,
        media_type="application/json", headers=headers,
    )


def _guess_mime(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext in _EXTRA_MIMES:
        return _EXTRA_MIMES[ext]
    guess, _ = mimetypes.guess_type(path)
    return guess or "application/octet-stream"


class DocPathError(Exception):
    """Raised by validate_doc_path when rel_path fails validation.

    `reason` is one of: "empty", "absolute", "is_root", "outside_root".
    Callers map reason → HTTP status / error string.
    """
    def __init__(self, reason: str, message: str = ""):
        self.reason = reason
        super().__init__(message or f"Invalid path: {reason}")


def validate_doc_path(rel_path: str, document_root: str) -> str:
    """Validate rel_path is a safe relative path under document_root.

    Returns the resolved absolute path (post-realpath) on success.
    Raises DocPathError(reason=...) on failure.

    Does NOT check existence or file type — caller handles those, since
    they require I/O and may want different policies (e.g. follow vs
    reject symlinked targets, read text vs return bytes).
    """
    if not rel_path:
        raise DocPathError("empty")
    if os.path.isabs(rel_path):
        raise DocPathError("absolute")
    real_root = os.path.realpath(document_root)
    abs_path = os.path.realpath(os.path.join(document_root, rel_path))
    if abs_path == real_root:
        raise DocPathError("is_root")
    if not abs_path.startswith(real_root + os.sep):
        raise DocPathError("outside_root")
    return abs_path


def handle_kb_document_request(
    query_string: str,
    document_root: str | None,
    cors_headers: dict | None = None,
) -> Response:
    """Resolve ?path=<rel>, validate, return the file bytes.

    - 503 if document_root is falsy.
    - 400 if path is missing or empty.
    - 403 if path is absolute, or escapes the root via '..'/symlinks.
    - 404 if the resolved target doesn't exist or isn't a regular file.
    - 200 with inline Content-Disposition on success.
    """
    cors = dict(cors_headers or {})

    if not document_root:
        return _json_response(
            {"error": "ingest.document_root not configured"},
            status_code=503, cors_headers=cors,
        )

    params: dict[str, str] = {}
    for part in (query_string or "").split("&"):
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        params[k] = unquote(v)
    rel_path = params.get("path", "")

    try:
        abs_path = validate_doc_path(rel_path, document_root)
    except DocPathError as e:
        if e.reason == "empty":
            return _json_response({"error": "Missing query param: path"}, 400, cors)
        if e.reason == "absolute":
            return _json_response({"error": "Absolute paths are not allowed"}, 403, cors)
        # "is_root" or "outside_root" — same opaque "Forbidden" as before
        return _json_response({"error": "Forbidden"}, 403, cors)

    if not os.path.exists(abs_path):
        return _json_response({"error": "File not found"}, 404, cors)
    if not os.path.isfile(abs_path):
        return _json_response({"error": "Not a regular file"}, 404, cors)

    try:
        with open(abs_path, "rb") as f:
            data = f.read()
    except OSError as e:
        return _json_response({"error": f"Read error: {e}"}, 500, cors)

    mime = _guess_mime(abs_path)
    basename = os.path.basename(abs_path)
    headers = {
        "Content-Disposition": f'inline; filename="{basename}"',
        "Content-Length": str(len(data)),
        "Cache-Control": "private, max-age=0",
    }
    headers.update(cors)
    return Response(content=data, status_code=200, media_type=mime, headers=headers)
