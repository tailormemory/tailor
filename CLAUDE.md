# CLAUDE.md — convention TAILOR per AI agent

Questo file detta come gli AI agent (Claude Code, Cursor, qualsiasi copilot) devono operare in questo repo. Lingua di lavoro: italiano. Tono: asciutto, pari a pari, no padding.

---

## 1. Secrets handling

I secrets vivono in **`/etc/tailor/env`** (`640 root:staff`). I LaunchDaemon TAILOR (`com.tailor.mcp`, `com.tailor.telegram`, ecc.) li iniettano al boot tramite il wrapper sh `scripts/services/run_*.sh` che fa `source`.

**Mai mettere secrets sotto `~/tailor/`** — qualunque file della repo è leggibile dall'agente in sessione e finisce nei log del provider AI. Mai sotto `/tmp/` (multi-user, non encrypted).

**Mai shell redirect per scrivere secrets**:

- ❌ `echo "TELEGRAM_BOT_TOKEN=..." > path`
- ❌ `cat <<EOF > path … EOF`
- ❌ `printf 'TOKEN=%s\n' "$val" > /etc/tailor/env`

Residuano nella history; se il quoting è sbagliato lo zsh può creare file con nomi imprevisti — incident 4 maggio 2026: `echo TOKEN=… > "il file"` interpretato come 4 file separati `#`, `il`, `mostra`, `nuovo` con il token in chiaro.

**Pattern corretti** per editare `/etc/tailor/env` (operatore-side, non agente):

```sh
sudo -e /etc/tailor/env                       # editor di sistema, atomico
sudo tee /etc/tailor/env >/dev/null <<'EOF'   # rewrite atomico via heredoc
TELEGRAM_BOT_TOKEN=…
TAILOR_API_KEY=…
EOF
sudo chmod 640 /etc/tailor/env
sudo chown root:staff /etc/tailor/env
```

**Naming convention** per consentire pattern-grep di secret-detection: suffix `_TOKEN`, `_KEY`, `_SECRET`, `_PASSWORD`, `_BEARER`. Niente varianti (`_pwd`, `_apikey`, `_pw`).

**Mai usare il tool `Read` su file plaintext-secret**: il contenuto va dritto nella transcript dell'agente = leak vector. Per ispezionare struttura senza leakare contenuto: Python `plistlib` / `json.load` / `yaml.safe_load` stampando solo le chiavi (non i valori).

---

## 2. Working tree hygiene

I file **untracked** sono leggibili dall'AI agent quanto i tracked. `.gitignore` non li nasconde da `find` / `ls` / `Read` — copre solo `git add`.

**Scan manuale per pattern-secret in untracked files** prima di milestone o push:

```sh
cd ~/tailor
git ls-files --others --exclude-standard \
  | xargs -I{} grep -lE '(TOKEN=|API_KEY=|BEARER |sk-(ant|proj)-|AIzaSy|[0-9]{9,10}:[A-Za-z0-9_-]{30,})' {} 2>/dev/null
```

Se ritorna path: ispezionare con cautela (non `cat`, usa Python che filtra), rimuovere o spostare fuori dal working tree, rotare i secrets esposti.

Secret scanner notturno automatizzato è in piano (post-incident 4 maggio 2026, TBD). Fino ad allora: scan manuale.

---

## 3. Authority boundaries

L'agente AI **non** esegue queste operazioni — sono di Emiliano:

- **`sudo`** in qualunque forma. Mai `sudo launchctl …`, `sudo tee`, `sudo chmod`, `sudo systemctl`. Se serve sudo, l'agente riporta il comando esatto e si ferma.
- **`git push`**. Commit locali OK. Push remote richiede OK esplicito e lo lancia Emiliano.
- **Modifiche a file root-owned**: `/etc/tailor/env`, plist sotto `/Library/LaunchDaemons/com.tailor.*.plist`, `/etc/sudoers.d/`. Read metadata (`ls -la`) OK; modify NO.
- **`launchctl bootstrap`/`bootout`/`kickstart`** verso domain `system/*`. Diagnose via `launchctl list | grep tailor` OK; pilotare service NO.
- **Telegram messaging** verso il canale produzione senza OK esplicito.

Se un task richiede uno di questi, fermati e proponi: *"il prossimo step è `<comando>`, lo lancio io o lo lanci tu?"*

---

## 4. Backup pre-edit pattern

Prima di edit a file critici in produzione, salva una copia datata in `.backups/` (root-level) o in `.backups/` adiacente al file (es. `scripts/lib/.backups/`).

**Naming**:

```sh
cp <path> <basedir>/.backups/$(basename <path>).pre-<task>-$(date +%Y%m%d_%H%M%S)
```

Esempio:

```sh
mkdir -p ~/tailor/.backups
cp ~/tailor/mcp_server.py ~/tailor/.backups/mcp_server.py.pre-fase2-$(date +%Y%m%d_%H%M%S)
```

**Backup obbligatorio**:

- `mcp_server.py` (server core, ~3000 righe, blast radius alto)
- `scripts/lib/*.py` (`tool_executor`, `llm_client`, `kb_document_api`, `config`, `token_auth`, ecc. — tutti consumati da mcp_server)
- `scripts/services/telegram_bot.py`, `heartbeat.py`, `reminder_checker.py`
- `config/tailor.yaml` (gitignored, configurazione runtime)
- `dashboard/index.html` (single-file frontend, ~5000 righe)

**Backup non necessario** per file tracked se l'edit è puramente reversibile via `git checkout HEAD -- <file>`.

I `.backups/` sono gitignored. Convention forensica per backup di artifact ops (db, chroma, ecc.) è in [docs/conventions.md](docs/conventions.md) col formato `<file>.backup.<reason>_<timestamp>` — applicabile a backup operativi, separata dal pattern code-edit qui sopra.

---

## 5. Validation post-edit

**Sintassi minima** dopo edit a file Python / YAML / JSON:

```sh
python3 -c "import ast; ast.parse(open('<path>').read()); print('syntax OK')"
python3 -c "import yaml; yaml.safe_load(open('<path>')); print('YAML OK')"
python3 -c "import json; json.load(open('<path>')); print('JSON OK')"
```

**Test funzionali** dopo edit alla logica:

```sh
.venv/bin/python3 -m pytest tests/test_<area>.py -v
```

Sempre `.venv/bin/python3`, **non** il `python3` di sistema — manca `requests`, `chromadb`, `openpyxl`, ecc. Se l'env di sistema fallisce su un import, quasi sicuramente serve il venv.

**AST-based sanity check** preferito a import diretto quando il modulo fa side effect all'import (`llm_client.py` importa `requests` al top, `mcp_server.py` istanzia chromadb / tokenizer): meno fragile.

**Smoke test E2E** dopo edit a flow esposti all'utente (chat dashboard, MCP tool, reminder pipeline): chiamare la funzione via `scripts.lib.tool_executor.execute_tool(...)` con input realistici e verificare ritorno + side effect.

---

## 6. Edit flow per round

Pattern per ogni round di edit non-triviale:

1. **Propose diff** — descrivere edit in testo, prima di applicarlo
2. **Wait** — aspettare OK esplicito
3. **Apply** — `Edit` tool, mai overwrite ciecamente
4. **Validate** — syntax check + test mirato (§5)
5. **Show diff** — `git diff <file>` per conferma
6. **Commit** — message descrittivo, scope chirurgico
7. **STOP** — non chainare il prossimo round senza OK

Per task multi-step (Fase 1, Fase 2, Roadmap), questo flow si applica per ogni step.

---

## 7. Restart daemons

Edit a backend/runtime richiedono restart (operatore-side, sezione 3):

| Modifica a | Componente da restartare | Comando |
|---|---|---|
| `mcp_server.py` o `scripts/lib/*.py` (importati da MCP) | MCP server | `sudo launchctl kickstart system/com.tailor.mcp` (graceful, **senza `-k`**) |
| `scripts/services/telegram_bot.py` | Telegram bot | `sudo launchctl kickstart -k system/com.tailor.telegram` |
| `dashboard/index.html` o `dashboard/*.jsx` | nessuno | hard-refresh in browser (Cmd+Shift+R) |
| `config/tailor.yaml` (campi runtime) | dipende dal campo, spesso MCP | come sopra |
| `/etc/tailor/env` | TUTTI i daemon che lo source | restart per service |

L'agente riporta il comando, non lo esegue.

**MCP — restart graceful di default (no `-k`)**: `-k` manda SIGKILL e salta lo shutdown pulito (`System.stop()`/atexit), che è l'unico momento in cui chromadb persiste l'indice HNSW e drena `embeddings_queue` (bug chromadb #6975). Un `kickstart -k` lascia il backlog non drenato e può lasciare l'HNSW su disco indietro rispetto a SQL. Usare il restart **graceful** (senza `-k`). `-k` resta accettabile solo per daemon che non scrivono Chroma (es. `com.tailor.telegram`), o subito dopo un `--flush-queue-backlog` che ha già persistito su disco. Procedura completa drift/queue: [docs/runbooks/RUNBOOK_drift_alert.md](docs/runbooks/RUNBOOK_drift_alert.md).

---

## 8. Commit & changelog

- **Scope**: 1 commit = 1 cambio logico. No bundling unrelated edits.
- **Message format**: conventional commits — `feat(area):`, `fix(area):`, `refactor:`, `docs(area):`. Em-dash (`—`) come separatore in titoli `[x.y.z]` del CHANGELOG (vedi entry recenti).
- **Trailer**: `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>` alla fine del messaggio (policy harness).
- **Niente push automatico**. `git push origin main` lo lancia Emiliano dopo review.
- **Niente file `docs/releases/vX.Y.Z.md`**: il canonico è `CHANGELOG.md` + GitHub Release page. Mai duplicare.
- **`[Unreleased]` in CHANGELOG**: contiene solo cambi non ancora versionati. Al release, promuovere in `[X.Y.Z]` e ripristinare il template commento.
