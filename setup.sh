#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
# TAILOR — Setup Script
# ═══════════════════════════════════════════════════════════════
# Creates venv, installs dependencies, downloads models,
# and prepares TAILOR for first run.
#
# Usage:
#   chmod +x setup.sh && ./setup.sh
#
# Prerequisites:
#   - Python 3.11+ installed
#   - Ollama installed (https://ollama.com)
# ═══════════════════════════════════════════════════════════════

set -e

TAILOR_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$TAILOR_DIR"

VENV_DIR="$TAILOR_DIR/.venv"
MIN_PYTHON="3.11"
RERANKER_DIR="$TAILOR_DIR/models/reranker"
ONNX_DIR="$RERANKER_DIR/onnx"

# ── Colors ────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}[TAILOR]${NC} $1"; }
ok()    { echo -e "${GREEN}[  OK ]${NC} $1"; }
warn()  { echo -e "${YELLOW}[ WARN]${NC} $1"; }
fail()  { echo -e "${RED}[FAIL ]${NC} $1"; exit 1; }

echo ""
echo -e "${CYAN}╔═══════════════════════════════════════╗${NC}"
echo -e "${CYAN}║        T[AI]LOR — Setup                ║${NC}"
echo -e "${CYAN}║  Self-hosted AI memory                 ║${NC}"
echo -e "${CYAN}║  that never forgets                    ║${NC}"
echo -e "${CYAN}╚═══════════════════════════════════════╝${NC}"
echo ""

# ── Step 1: Python check ─────────────────────────────────────
info "Checking Python..."

PYTHON_CMD=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        version=$("$cmd" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null)
        major=$("$cmd" -c "import sys; print(sys.version_info.major)" 2>/dev/null)
        minor=$("$cmd" -c "import sys; print(sys.version_info.minor)" 2>/dev/null)
        if [ "$major" -ge 3 ] && [ "$minor" -ge 11 ]; then
            PYTHON_CMD="$cmd"
            break
        fi
    fi
done

if [ -z "$PYTHON_CMD" ]; then
    fail "Python 3.11+ required. Found: ${version:-none}. Install from https://python.org"
fi
ok "Python $version ($PYTHON_CMD)"

# ── Step 2: Virtual environment ───────────────────────────────
info "Setting up virtual environment..."

if [ -d "$VENV_DIR" ]; then
    ok "Virtual environment exists at .venv/"
else
    "$PYTHON_CMD" -m venv "$VENV_DIR"
    ok "Created virtual environment at .venv/"
fi

# Activate
source "$VENV_DIR/bin/activate"

# Upgrade pip
pip install --quiet --upgrade pip

# ── Step 3: Install dependencies ──────────────────────────────
info "Installing Python dependencies..."

pip install --quiet -r requirements.txt
ok "All dependencies installed"

# ── Step 4: Create directories ────────────────────────────────
info "Creating directory structure..."

for dir in db data logs backups credentials models/reranker/onnx; do
    mkdir -p "$TAILOR_DIR/$dir"
done
ok "Directories created"

# ── Step 5: Ollama check + embedding model ────────────────────
info "Checking Ollama..."

if command -v ollama &>/dev/null; then
    # Check if Ollama is running
    if curl -s http://localhost:11434/api/tags &>/dev/null; then
        ok "Ollama is running"

        # Pull embedding model if not present
        if ollama list 2>/dev/null | grep -q "nomic-embed-text"; then
            ok "Embedding model (nomic-embed-text) already pulled"
        else
            info "Pulling embedding model (nomic-embed-text)... this may take a minute"
            ollama pull nomic-embed-text
            ok "Embedding model ready"
        fi

        # Pull intent classifier if not present
        if ollama list 2>/dev/null | grep -q "qwen2.5:7b"; then
            ok "Classifier model (qwen2.5:7b) already pulled"
        else
            info "Pulling classifier model (qwen2.5:7b)... this may take a few minutes"
            ollama pull qwen2.5:7b
            ok "Classifier model ready"
        fi
    else
        warn "Ollama installed but not running. Start it with: ollama serve"
    fi
else
    warn "Ollama not found. Install from https://ollama.com"
    warn "TAILOR needs Ollama for local embeddings (nomic-embed-text)."
    warn "Alternatively, configure a cloud embedding provider in tailor.yaml."
fi

# ── Step 6: Download reranker model ──────────────────────────
info "Checking reranker model..."

# Check if any ONNX model already exists
EXISTING_ONNX=$(ls "$ONNX_DIR"/*.onnx 2>/dev/null | head -1)

if [ -n "$EXISTING_ONNX" ]; then
    ok "Reranker model already present ($(basename "$EXISTING_ONNX"))"
else
    ONNX_FILE="model.onnx"
    ONNX_URL="https://huggingface.co/cross-encoder/ms-marco-MiniLM-L-6-v2/resolve/main/onnx/model.onnx"
    info "Downloading reranker model (ms-marco-MiniLM-L-6-v2, ~90MB)..."
    curl -L --progress-bar -o "$ONNX_DIR/$ONNX_FILE" "$ONNX_URL"
    if [ -f "$ONNX_DIR/$ONNX_FILE" ] && [ $(stat -f%z "$ONNX_DIR/$ONNX_FILE" 2>/dev/null || stat -c%s "$ONNX_DIR/$ONNX_FILE" 2>/dev/null) -gt 1000000 ]; then
        ok "Reranker model downloaded"
    else
        warn "Reranker download may have failed. Check models/reranker/onnx/"
        rm -f "$ONNX_DIR/$ONNX_FILE"
    fi
fi

# Download tokenizer files if missing
TOKENIZER_BASE="https://huggingface.co/cross-encoder/ms-marco-MiniLM-L-6-v2/resolve/main"
for tfile in tokenizer.json vocab.txt special_tokens_map.json tokenizer_config.json; do
    if [ ! -f "$RERANKER_DIR/$tfile" ]; then
        info "Downloading $tfile..."
        curl -sL -o "$RERANKER_DIR/$tfile" "$TOKENIZER_BASE/$tfile"
    fi
done
ok "Reranker tokenizer files ready"

# ── Step 7: Config file ──────────────────────────────────────
info "Checking configuration..."

if [ -f "config/tailor.yaml" ]; then
    ok "Config file exists (config/tailor.yaml)"
else
    if [ -f "config/tailor.yaml.example" ]; then
        cp config/tailor.yaml.example config/tailor.yaml
        ok "Config created from template — edit config/tailor.yaml with your settings"
    else
        warn "No config template found. The dashboard wizard will create one on first run."
    fi
fi

# ── Step 8: MCP server path in config ─────────────────────────
info "Updating paths in config..."

# If config exists, update the home path to current directory
if [ -f "config/tailor.yaml" ]; then
    "$VENV_DIR/bin/python3" -c "
import yaml
with open('config/tailor.yaml') as f:
    cfg = yaml.safe_load(f)
needs_update = False
if cfg.get('paths', {}).get('home', '') in ['', '/path/to/your/tailor']:
    cfg.setdefault('paths', {})['home'] = '$TAILOR_DIR'
    needs_update = True
if needs_update:
    with open('config/tailor.yaml', 'w') as f:
        yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
    print('Updated paths.home to $TAILOR_DIR')
else:
    print('Paths already configured')
" 2>/dev/null || true
fi

# ── Done ──────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo -e "${GREEN}  TAILOR setup complete!${NC}"
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo ""
echo "  Next steps:"
echo ""
echo "  1. Start the MCP server:"
echo "     .venv/bin/python3 mcp_server.py"
echo ""
echo "  2. Open the dashboard:"
echo "     http://localhost:8787/dashboard"
echo ""
echo "  3. Complete the Setup Wizard in the dashboard"
echo "     to configure your LLM provider, embedding, and data sources."
echo ""
echo "  4. Import your first data via the dashboard KB tab"
echo "     or run the nightly pipeline manually:"
echo "     ./sync_and_ingest.sh"
echo ""
echo "  5. Set up the nightly pipeline (optional):"
echo "     Add to your crontab (crontab -e):"
echo ""
echo "     TAILOR_HOME="
echo "     0 3 * * * cd  && ./sync_and_ingest.sh"
echo ""
echo "  For background service setup (auto-start on boot),"
echo "  see install/README.md"
echo ""
