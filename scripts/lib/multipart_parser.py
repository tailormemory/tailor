"""Multipart form-data parser for TAILOR MCP server."""


def parse_multipart(body: bytes, content_type: str) -> dict:
    """Parse multipart/form-data body.

    Returns:
        {"fields": {name: str_value}, "files": {name: (filename, bytes_data)}}
    """
    result = {"fields": {}, "files": {}}

    # Extract boundary
    boundary = ""
    for p in content_type.split(";"):
        p = p.strip()
        if p.startswith("boundary="):
            boundary = p[9:].strip('"')
    if not boundary:
        return result

    delim = b"--" + boundary.encode()

    # Split body by delimiter
    parts = body.split(delim)

    for part in parts:
        if not part or part.strip(b"\r\n- ") == b"":
            continue

        # Find header/body separator (double CRLF)
        idx = part.find(b"\r\n\r\n")
        sep_len = 4
        if idx == -1:
            idx = part.find(b"\n\n")
            sep_len = 2
        if idx == -1:
            continue

        headers_raw = part[:idx].decode("utf-8", errors="replace")
        pbody = part[idx + sep_len:]

        # Strip trailing CRLF
        if pbody.endswith(b"\r\n"):
            pbody = pbody[:-2]
        elif pbody.endswith(b"\n"):
            pbody = pbody[:-1]

        # Parse Content-Disposition header
        name = ""
        filename = ""
        for line in headers_raw.replace("\r\n", "\n").split("\n"):
            if "Content-Disposition" not in line:
                continue
            if 'name="' in line:
                name = line.split('name="')[1].split('"')[0]
            if 'filename="' in line:
                filename = line.split('filename="')[1].split('"')[0]

        if not name:
            continue

        if filename:
            result["files"][name] = (filename, pbody)
        else:
            result["fields"][name] = pbody.decode("utf-8", errors="replace").strip()

    return result
