#!/usr/bin/env python3
"""
TAILOR — Pre-launch audit script
Run before every git push to catch issues.

Usage:
    python3 scripts/maintenance/audit_opensource.py
"""

import os
import sys
import ast
import re
import json
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(BASE_DIR)

PASS = "✅"
WARN = "⚠️"
FAIL = "❌"
errors = []
warnings = []

def check(ok, msg, level="error"):
    if ok:
        print(f"  {PASS} {msg}")
    elif level == "warn":
        print(f"  {WARN} {msg}")
        warnings.append(msg)
    else:
        print(f"  {FAIL} {msg}")
        errors.append(msg)

def get_active_py_files():
    """All .py files that would be committed (not in .gitignore dirs)."""
    excluded = {"archive", "__pycache__", ".venv", "models", "data", "db", "backups", "logs", "credentials"}
    result = []
    for root, dirs, files in os.walk("."):
        dirs[:] = [d for d in dirs if d not in excluded and not d.startswith(".")]
        for f in files:
            if f.endswith(".py") and f != "audit_opensource.py":
                result.append(os.path.join(root, f))
    return result

def get_active_sh_files():
    excluded = {"archive", ".venv", "models", "data", "db", "backups", "logs"}
    result = []
    for root, dirs, files in os.walk("."):
        dirs[:] = [d for d in dirs if d not in excluded and not d.startswith(".")]
        for f in files:
            if f.endswith(".sh"):
                result.append(os.path.join(root, f))
    return result

# ═══════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("TAILOR — Pre-Launch Audit")
print("=" * 60)

# ── 1. Critical files ──
print("\n📁 Critical files:")
critical_files = {
    "README.md": "Project documentation",
    "LICENSE": "Apache 2.0 license",
    ".gitignore": "Git exclusions",
    "setup.sh": "Installation script",
    "mcp_server.py": "MCP server",
    "config/tailor.yaml.example": "Config example",
    "dashboard/index.html": "Dashboard",
    "dashboard/logo.svg": "Logo",
}
for path, desc in critical_files.items():
    check(os.path.exists(path), f"{path} ({desc})")

# ── 2. Dashboard assets ──
print("\n🎨 Dashboard assets:")
check(os.path.isdir("dashboard/vendor"), "dashboard/vendor/ exists")
for f in ["react.min.js", "react-dom.min.js", "tailwind.js"]:
    check(os.path.exists(f"dashboard/vendor/{f}"), f"vendor/{f}")
check(os.path.isdir("dashboard/icons"), "dashboard/icons/ exists")
for f in ["chatgpt.ico", "claude.ico", "gemini.png"]:
    check(os.path.exists(f"dashboard/icons/{f}"), f"icons/{f}")

# ── 3. No external CDN in dashboard (except fonts) ──
print("\n🌐 Dashboard CDN dependencies:")
with open("dashboard/index.html") as f:
    html = f.read()
cdns = re.findall(r'src="(https?://[^"]+)"', html)
for cdn in cdns:
    is_font = "fonts.googleapis" in cdn or "fonts.gstatic" in cdn
    check(is_font, f"External: {cdn[:80]}", level="warn" if not is_font else "error")
check('type="text/babel"' not in html, "No Babel text/babel script tag")
check("babel-standalone" not in html and "babel.min.js" not in html, "Babel standalone not loaded")

# ── 4. Hardcoded personal references ──
print("\n🔒 Personal references in code:")
PERSONAL = [
    r"[Ee]miliano",
    r"[Cc]arlucci",
    r"emicarlu",
    r"jarvis\.carlucci",
    r"carlucci\.io",
]
py_files = get_active_py_files()
sh_files = get_active_sh_files()
code_files = py_files + sh_files

for pattern in PERSONAL:
    found = []
    for fpath in code_files:
        try:
            with open(fpath) as f:
                for i, line in enumerate(f, 1):
                    if re.search(pattern, line) and not line.strip().startswith("#"):
                        found.append(f"{fpath}:{i}")
        except Exception:
            pass
    check(len(found) == 0, f"Pattern '{pattern}' not in code ({len(found)} hits)")
    for hit in found[:3]:
        print(f"      → {hit}")

# ── 5. JARVIS references ──
print("\n🏷️  JARVIS→TAILOR migration:")
jarvis_hits = []
JARVIS_ALLOWLIST = [
    "JARVIS_API_KEY",  # backward compat env var
    "Jarvis\")  # fallback",  # persona fallback
    "# ",  # comments
]
for fpath in py_files:
    try:
        with open(fpath) as f:
            for i, line in enumerate(f, 1):
                if re.search(r"[Jj][Aa][Rr][Vv][Ii][Ss]", line):
                    stripped = line.strip()
                    # Skip comments
                    if stripped.startswith("#"):
                        continue
                    # Skip allowlisted patterns
                    if any(a in line for a in ["JARVIS_API_KEY", 'Jarvis")', "# jarvis", "# Jarvis"]):
                        continue
                    jarvis_hits.append((fpath, i, stripped[:100]))
    except Exception:
        pass
check(len(jarvis_hits) == 0, f"No active JARVIS references ({len(jarvis_hits)} found)", level="warn")
for fpath, line, text in jarvis_hits[:5]:
    print(f"      → {fpath}:{line}: {text}")

# ── 6. Ollama hardcoding ──
print("\n🔌 Ollama URL hardcoding:")
bad_ollama = []
for fpath in py_files:
    try:
        with open(fpath) as f:
            for i, line in enumerate(f, 1):
                if "localhost:11434" in line:
                    stripped = line.strip()
                    if stripped.startswith("#"):
                        continue
                    # OK patterns: config fallback, comment, default value, docstring
                    if 'or "http://localhost:11434"' in line or "cfg(" in line or "os.environ" in line:
                        continue
                    if '.get("base_url"' in line or '.get("OLLAMA_URL"' in line:
                        continue
                    # Docstring/comment with example URL
                    if 'endpoint:' in stripped and '#' in stripped:
                        continue
                    if "# " in stripped and "localhost:11434" in stripped.split("# ")[1]:
                        continue
                    bad_ollama.append((fpath, i, stripped[:100]))
    except Exception:
        pass
check(len(bad_ollama) == 0, f"No raw localhost:11434 (all config-wrapped) ({len(bad_ollama)} found)")
for fpath, line, text in bad_ollama[:5]:
    print(f"      → {fpath}:{line}: {text}")

# ── 7. Python syntax ──
print("\n🐍 Python syntax validation:")
syntax_errors = []
for fpath in py_files:
    try:
        with open(fpath) as f:
            source = f.read()
        ast.parse(source)
    except SyntaxError as e:
        syntax_errors.append(f"{fpath}:{e.lineno}: {e.msg}")
check(len(syntax_errors) == 0, f"All {len(py_files)} Python files parse OK ({len(syntax_errors)} errors)")
for err in syntax_errors:
    print(f"      → {err}")

# ── 8. .gitignore coverage ──
print("\n📋 .gitignore coverage:")
with open(".gitignore") as f:
    gitignore = f.read()
must_exclude = ["db/", "data/", "credentials/", "config/tailor.yaml", "logs/", "models/", "__pycache__/", ".venv/"]
for pattern in must_exclude:
    check(pattern in gitignore, f".gitignore excludes {pattern}")

# ── 9. No secrets in committable files ──
print("\n🔑 Secret scanning:")
SECRET_PATTERNS = [
    (r"sk-[a-zA-Z0-9]{20,}", "OpenAI API key"),
    (r"sk-ant-[a-zA-Z0-9]{20,}", "Anthropic API key"),
    (r"AIza[a-zA-Z0-9_-]{35}", "Google API key"),
    (r"ghp_[a-zA-Z0-9]{36}", "GitHub token"),
    (r"bot[0-9]{8,}:[a-zA-Z0-9_-]{35}", "Telegram bot token"),
]
all_committable = code_files + ["dashboard/index.html", "README.md", "setup.sh"]
secret_hits = []
for fpath in all_committable:
    if not os.path.exists(fpath):
        continue
    try:
        with open(fpath) as f:
            content = f.read()
        for pattern, name in SECRET_PATTERNS:
            if re.search(pattern, content):
                secret_hits.append(f"{fpath}: {name}")
    except Exception:
        pass
check(len(secret_hits) == 0, f"No secrets in committable files ({len(secret_hits)} found)")
for hit in secret_hits:
    print(f"      → {hit}")

# ── 10. Config example completeness ──
print("\n⚙️  Config example:")
if os.path.exists("config/tailor.yaml.example"):
    with open("config/tailor.yaml.example") as f:
        example = f.read()
    key_sections = ["llm:", "embedding:", "telegram:", "enrichment:", "user:", "kb:"]
    for section in key_sections:
        check(section in example, f"Example config has '{section}' section")

# ── 11. README content checks ──
print("\n📖 README content:")
if os.path.exists("README.md"):
    with open("README.md") as f:
        readme = f.read()
    check("tailormemory.ai" in readme, "Links to tailormemory.ai")
    check("Apache" in readme, "Mentions Apache license")
    check("setup.sh" in readme, "Mentions setup.sh")
    check("MCP" in readme, "Mentions MCP")
    check("JARVIS" not in readme and "Jarvis" not in readme, "No JARVIS references in README", level="warn")

# ═══════════════════════════════════════════════════════════
print("\n" + "=" * 60)
if errors:
    print(f"🚨 {len(errors)} ERROR(S) — fix before push:")
    for e in errors:
        print(f"   {FAIL} {e}")
elif warnings:
    print(f"⚠️  0 errors, {len(warnings)} warning(s) — review before push:")
    for w in warnings:
        print(f"   {WARN} {w}")
else:
    print("🎉 ALL CLEAR — ready to push!")
print("=" * 60 + "\n")

sys.exit(1 if errors else 0)
