# OAuth `/mcp` — Scoping Dossier (task in coda)

> **Stato**: BACKLOG, scoped, non iniziato. Priorità Alta come feature di prodotto, NON urgente.
> **Scoping deciso**: sessione 2026-05-18. Prossimo deliverable = design doc, NON codice.
> **Perché questo doc esiste**: nella sessione di scoping, l'assistente è partito senza
> alcun contesto su Caveat 3 / OAuth e ha dovuto ricostruirlo via ricerca conversazioni.
> Questo dossier evita che succeda di nuovo. Allegarlo alla sessione che riprende il task.

---

## 0. AGGIORNAMENTO SESSIONE 2026-05-19 — Incognite chiuse, direzione decisa

> Le 3 incognite tecniche di §3 sono state risolte in questa sessione via lettura spec MCP corrente + discovery codice verbatim. Task RESTA IN BACKLOG (non urgente — deploy single-user, `?token=` funziona, Caveat 3 chiuso). Quando il task riprende: NON rifare l'istruttoria. Partire dal design doc sull'opzione C qui sotto.

### Spec MCP corrente verificata: 2025-11-25
Profilo OAuth richiesto dal connector Claude.ai (Web/Mobile), confermato da doc ufficiale Anthropic + sorgente SDK installato:
- OAuth 2.1, Authorization Code + PKCE obbligatorio, code_challenge_method=S256
- RFC 8707 Resource Indicators (audience validation) — obbligatorio
- RFC 9728 Protected Resource Metadata per discovery (no più endpoint fallback fissi)
- DCR (Dynamic Client Registration) DEPRECATO dalla 2025-11-25 — NON serve implementare `/register`. Pre-registrazione client_id/secret via "Advanced settings" dell'UI custom connector Claude.ai = la nostra strada (single-owner).
- Callback URL fisso: `https://claude.ai/api/mcp/auth_callback` (futuro: `https://claude.com/api/mcp/auth_callback` — allowlistare entrambi)
- Claude richiede token expiry + refresh → il provider deve emettere refresh token

### Incognita 1 — CHIUSA
Profilo sopra. Decisivo: l'SDK installato è `mcp 1.27.0` (SDK ufficiale Anthropic, NON `fastmcp` standalone). Espone l'INTERA macchina OAuth nativa in `mcp/server/auth/`: handler `/authorize` `/token` `/register` `/revoke`, metadata `.well-known`, `BearerAuthBackend`, `ClientAuthenticator`. Verificato nel sorgente: `authorize.py` ha `code_challenge_method: Literal["S256"]` hardcoded; `provider.py`/`token.py` hanno `resource` (RFC 8707) ovunque; `OAuthToken` ha `refresh_token`. Conforme alla spec by construction.
→ NON scrivere handler OAuth a mano. `FastMCP.__init__` accetta `auth_server_provider: OAuthAuthorizationServerProvider`, `token_verifier`, `auth: AuthSettings`. La decisione §4 "nativo vs custom" → NATIVO.

### Incognita 2 — CHIUSA (superficie molto più piccola del previsto)
Censimento client verbatim. UN SOLO client usa `?token=` su `/mcp`:
- Connector Claude.ai → `?token=` nell'URL — UNICO interessato
- Chrome extension → `Authorization: Bearer` su `/api/ingest-live` e `/api/dashboard/stats`. NON passa da `/mcp`. Non interessato.
- Telegram bot → `import mcp_server` IN-PROCESS, chiama `mcp.kb_update_session()` diretto. ZERO HTTP. Non interessato.
- chat-dashboard → cookie `tailor_session` (`credentials: "include"`). Il residuo `API_TOKEN` da query string è parsato ma MAI applicato alle fetch (codice morto). Non interessato.
→ Strategia coesistenza = UNA dimensione: connector Claude.ai `?token=` legacy ↔ connector Claude.ai OAuth. Nessuna migrazione multi-client. `?token=` legacy accettato in parallelo a OAuth, deprecazione single-track.

### Incognita 3 — RIDOTTA A UN BIVIO NETTO (da decidere nel design doc)
Nodo reale identificato nel codice: `BearerAuthMiddleware.__call__` (mcp_server.py:1179-1358) intercetta OGNI richiesta HTTP PRIMA dell'app FastMCP. `mcp.streamable_http_app()` è wrappato dal middleware custom in `run()` (mcp_server.py:3356-3358). Per path non-`/api/` e non-dashboard l'esito è binario: token valido → pass, altrimenti 401 `WWW-Authenticate: Bearer`. `PUBLIC_PATHS = {"/health", "/", "/dashboard"}` — nessun path OAuth.
→ Conseguenza: `GET /.well-known/oauth-authorization-server` (deve essere pubblico per il discovery OAuth) verrebbe bloccato 401 dal middleware custom PRIMA di raggiungere l'handler metadata dell'SDK. Il discovery fallisce in partenza. Il middleware custom e l'auth SDK competono sullo stesso scope ASGI. Va conciliato — è la scelta strutturale del design doc.

### Direzione architetturale decisa: OPZIONE C (sub-app OAuth delegata)
Tre opzioni valutate:
- A — Auth SDK nativa, middleware custom smontato/ridotto. Più pulita in astratto ma failure mode peggiore: smontare il middleware tocca `/api/*`, dashboard, cookie, rate-limiter, Caveat 3 — tutto in produzione. SCARTATA.
- B — Middleware esteso a mano con handler OAuth. Troppo codice nostro su dominio OAuth (sbagliare = Claude.ai non si connette). SCARTATA.
- C — SCELTA. Sub-app OAuth (Starlette con handler SDK) delegata dal middleware, stesso pattern della sub-app chat GIÀ in produzione e collaudato (`_get_chat_app()`, mcp_server.py:173-180; dispatch a 1310-1328). I path OAuth aggiunti a un gate pubblico nel middleware PRIMA del 401. `/api/*`, Caveat 3, dashboard intatti. Additiva, rollback semplice, handler SDK conformi. Decisione presa da Emiliano questa sessione.

### Buco residuo da chiudere PRIMA dell'implementazione (non prima del design)
Comportamento di `mcp.streamable_http_app()` quando `FastMCP` riceve `auth=`: monta gli endpoint OAuth internamente? Se montiamo noi una sub-app OAuth separata SENZA passare `auth=`, c'è collisione di path con quanto monta l'SDK? Micro-verifica CC mirata (read-only, ~5 min) = primo checkpoint del design doc, non un'altra discovery ampia.

### Provider da costruire (necessario in TUTTE le opzioni)
`OAuthAuthorizationServerProvider` — lo storage di client/authorization code/access token/refresh token. È l'unico componente OAuth che scriviamo noi (l'SDK fornisce handler e validazione, non la persistenza). Single-owner: un client OAuth, un set di credenziali. Storage semplice (es. SQLite o file come `dashboard_sessions.json` già fa per le sessioni cookie).

---

## 1. CORREZIONE CRITICA — "Caveat 3" ≠ "OAuth /mcp"

Il continuation 2026-05-16 conteneva la riga sbagliata:
> "OAuth provider per `/mcp` (v1.3.0) — Chiude asimmetria Caveat 3"

**È falso. Due cose distinte:**

### Caveat 3 — GIÀ CHIUSO (8 maggio 2026, verificato in produzione 10 maggio)
Caveat 3 = l'asimmetria di autenticazione tra `/api/*` e `/mcp`. Risolto e live.
Stato verificato con sanity test in produzione (sessione 2026-05-10):

| Test | Endpoint | Auth | Atteso | Ottenuto |
|---|---|---|---|---|
| T1 | `/api/*` | Bearer header valido | 200 | 200 ✅ |
| T2 | `/api/*` | `?token=` query | 401 | 401 ✅ |
| T3 | `/mcp` | `?token=` query valido | 200 | 406* ✅ |
| T4 | `/mcp` | `?token=` query invalido | 401 | 401 ✅ |

\*406 = post-auth, content-negotiation (curl non manda `Accept: text/event-stream`).
Significa: auth passata, l'asimmetria regge.

**L'asimmetria è VOLUTA e va PRESERVATA**: `?token=` nell'URL è BLOCCATO su `/api/*`
(più sicuro) ma ANCORA ACCETTATO su `/mcp` — perché il connector Claude Web/Mobile
NON supporta header custom, può autenticare SOLO via `?token=` nell'URL. Rimuoverlo
da `/mcp` romperebbe l'accesso da Claude.ai. → Caveat 3 = "non togliere `?token=`
da `/mcp` anche se lo togli da `/api/*`; l'asimmetria è intenzionale". CHIUSO.

→ **OAuth NON deve "chiudere Caveat 3". Caveat 3 non è un problema aperto.**

### OAuth /mcp — task reale, indipendente
Origine: roadmap di marzo 2026, "Fase 13 — OAuth 2.1 per connector Web/Mobile".
Citazione testuale di allora:
> "Attualmente il connector Web/Mobile usa `?token=<key>` nell'URL. Funziona ma il
> token è visibile nelle impostazioni Claude. OAuth 2.1 lo eliminerebbe. Richiede
> un auth server minimale (`/authorize` + `/token`) sul Mac Mini. **Priorità: bassa
> — il setup attuale è sicuro per single-user.**"

---

## 2. SCOPE BLOCCATO (deciso 2026-05-18)

**OAuth /mcp = FEATURE DI PRODOTTO OSS, single-owner per deploy.**

Ogni self-hoster installa TAILOR sulla SUA macchina, con la SUA KB, ed è l'UNICO
owner di quel deploy. OAuth sostituisce il `token=<key>` in chiaro nell'URL con un
flow OAuth2 standard, così il connector Claude.ai si autentica senza esporre un
secret nelle settings dell'account Claude.

### IN SCOPE
- Authorization server minimale embedded in `mcp_server.py` (`/authorize`, `/token`,
  probabilmente `/.well-known/oauth-authorization-server` per discovery).
- OAuth2 flow che il connector MCP di Claude.ai sa parlare nativamente.
- Coesistenza/sostituzione del meccanismo `?token=` esistente (transizione, non cut-over).
- Configurabilità per il self-hoster non-esperto OAuth (abilitare/configurare senza
  conoscere OAuth internamente).

### OUT OF SCOPE (esplicito — non scivolarci)
- Multi-utente, ruoli, permessi.
- Isolamento dati KB / multi-tenant (un utente non vede dati di un altro).
- IdP esterni (login con Google/GitHub).
- Single owner = un client OAuth, un set di credenziali. Fine.

> Nota: il multi-tenant sulla istanza personale di Emiliano (dare la sua KB a un
> team) è stato esplicitamente ESCLUSO. Se mai servisse, è un task fase-2 separato
> e MOLTO più grande (tocca metadata chunk, pipeline, retrieval — non solo auth).

---

## 3. TRE INCOGNITE TECNICHE DA CHIUDERE NEL DESIGN DOC (prima di qualsiasi codice)

> ⚠️ AGGIORNAMENTO 2026-05-19: tutte e 3 le incognite di questa sezione sono CHIUSE. Vedi §0 per gli esiti. Questa sezione resta come traccia del ragionamento originale di scoping — NON è più lavoro aperto.

### Incognita 1 — Quale OAuth flow parla il connector MCP di Claude.ai?
L'MCP spec ha una sezione auth SPECIFICA (OAuth 2.1: authorization code + PKCE,
discovery via `.well-known`). NON è "OAuth generico". Va verificato il profilo
ESATTO che il client MCP di Claude.ai si aspetta, contro la spec MCP CORRENTE
(la spec evolve — non fidarsi di conoscenza pregressa, leggere la versione attuale).
**Sbagliare il flow = Claude.ai non si connette. Punto.**
Fonte da consultare: spec ufficiale MCP, sezione Authorization. Verificare data/versione.

### Incognita 2 — Coesistenza con `?token=` durante la transizione
`?token=` su `/mcp` è IN PRODUZIONE e funzionante (Caveat 3, vedi §1). Client che
ne dipendono OGGI, da censire prima di toccare l'auth:
- Connector Claude.ai (Web/Mobile) — usa `?token=` nell'URL
- Chrome extension — verificare come autentica
- Telegram bot — gira IN-PROCESS (importa mcp_server), verificare se tocca auth HTTP
- chat-dashboard — gira come thread del processo MCP
OAuth NON deve rompere questi mentre li migra. Serve strategia di COESISTENZA
(es. OAuth nuovo + `?token=` legacy accettato in parallelo per un periodo), non un
cut-over secco. Il design doc deve specificare la strategia di transizione.

### Incognita 3 — Topologia nel processo single-threaded asyncio
`mcp_server.py` è single-threaded asyncio (FastMCP, porta 8787). Caveat noto:
HTTP self-call a localhost:8787 → DEADLOCK (MCP single-threaded). Un auth server
aggiunge endpoint (`/authorize`, `/token`, `/.well-known/...`) nello STESSO processo.
La topologia (gli endpoint OAuth devono fare lookup/redirect — non devono auto-chiamarsi
via HTTP) va DISEGNATA, non improvvisata. Il design doc deve mostrare il flow
senza self-HTTP-call.

---

## 4. STATO ATTUALE DELL'AUTH SU `/mcp` (da sessioni passate — verificare ancora valido)

Da sessione 2026-04-09 (potrebbe essere evoluto — RI-VERIFICARE in discovery codice):
- `/api/search`, `/api/fetch`: solo localhost senza auth; da remoto serve token.
- `/api/dashboard/*`: localhost senza auth; remoto serve cookie/token.
- `/mcp`: richiede Bearer token OPPURE query `?token=`, sempre. Se `TAILOR_API_KEY`
  (allora `JARVIS_API_KEY`) è vuoto → bypass totale auth.
- Auth implementata come middleware ASGI (`BearerAuthMiddleware`) che wrappa la
  Starlette app generata da `FastMCP.streamable_http_app`.
- FastMCP HA un meccanismo auth interno nativo ma richiede full OAuth setup — nelle
  sessioni passate fu scartato a favore del middleware API-key custom (single-user).
  → Per OAuth come feature di prodotto, RIVALUTARE il meccanismo nativo FastMCP:
    potrebbe ora essere la strada giusta (allora scartato perché single-user; ora
    è feature di prodotto). Verificare cosa offre la versione FastMCP installata.
- Secret: `TAILOR_API_KEY` in `/etc/tailor/env` (640 root:staff). Vedi SECRETS.md.

---

## 5. PROSSIMI PASSI (quando il task viene ripreso)

1. **NON scrivere codice subito.** Il deliverable è un DESIGN DOC.
2. ~~Passo 1 — Lettura spec MCP auth corrente~~ ✅ FATTO 2026-05-19 (vedi §0).
3. ~~Passo 2 — Discovery codice~~ ✅ FATTO 2026-05-19 (vedi §0, Incognita 2/3).
4. **Passo 3 — Design doc** (assistant + Emiliano): impostato su OPZIONE C (§0). Primo checkpoint interno = micro-verifica `streamable_http_app()` + `auth=` (§0, "Buco residuo"). Poi: design del provider, della sub-app OAuth, dell'aggancio al middleware, della coesistenza `?token=` legacy, strategia rollback.
5. **Passo 4 — Solo dopo design approvato**: prompt CC implementazione, step-by-step, endpoint PRODUZIONE → massima cautela, deploy verificato browser (Emiliano), rollback pronto.

## 6. CAUTELE SPECIFICHE DI QUESTO TASK
- `/mcp` è la SOLA via d'accesso alla KB da Claude.ai. Romperlo = perdere l'accesso
  a TAILOR da Claude. Ogni modifica auth richiede strategia di rollback e verifica
  in produzione via browser (Emiliano), non curl localhost.
- Endpoint di produzione attivo → deploy col protocollo bootout→verify→bootstrap
  su ENTRAMBI i daemon (MCP + Telegram in-process), come da caveat operativi.
- È il task più grande del backlog. Merita una sessione dedicata, non coda di sessione.
- Verificare SEMPRE la spec MCP corrente, non assumere da conoscenza pregressa
  (la spec auth MCP è evoluta più volte).

---

*Dossier generato 2026-05-18. Contesto Caveat 3 ricostruito da: chat 2026-03-10
(implementazione API-key auth), 2026-04-09 (modello auth endpoint), 2026-05-10
(verifica Caveat 3 fix in produzione), 2026-03-21 (roadmap Fase 13 OAuth originale).*
