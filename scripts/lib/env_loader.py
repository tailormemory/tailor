"""Load /etc/tailor/env into os.environ; pre-set keys win via setdefault."""
import os
import sys

ENV_FILE = "/etc/tailor/env"


def load_env(path: str = ENV_FILE) -> None:
    try:
        with open(path) as f:
            text = f.read()
    except OSError as e:
        print(f"FATAL: cannot read {path}: {e}", file=sys.stderr)
        sys.exit(1)
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
            v = v[1:-1]
        os.environ.setdefault(k, v)
