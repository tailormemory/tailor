/*
 * TAILOR — native chat tab for the dashboard.
 *
 * Plain ES2017 using React.createElement (no Babel in browser — matches the
 * rest of dashboard/index.html). Exposed globals when loaded:
 *   window.TailorChat.Tab({ theme })  — React component for the "Chat" tab.
 *
 * Dependencies expected to already be on window:
 *   React, ReactDOM, marked, DOMPurify.
 */

(function () {
  "use strict";

  var React = window.React;
  var h = React.createElement;
  var useState = React.useState;
  var useEffect = React.useEffect;
  var useRef = React.useRef;
  var useCallback = React.useCallback;

  // ── utilities ────────────────────────────────────────────────

  function cl() {
    var out = [];
    for (var i = 0; i < arguments.length; i++) if (arguments[i]) out.push(arguments[i]);
    return out.join(" ");
  }

  function relTime(iso) {
    if (!iso) return "";
    var t = Date.parse(iso);
    if (isNaN(t)) return "";
    var s = Math.max(0, Math.floor((Date.now() - t) / 1000));
    if (s < 60) return s + "s ago";
    var m = Math.floor(s / 60);
    if (m < 60) return m + "m ago";
    var h = Math.floor(m / 60);
    if (h < 24) return h + "h ago";
    var d = Math.floor(h / 24);
    if (d < 30) return d + "d ago";
    return new Date(t).toLocaleDateString();
  }

  function renderMarkdown(text) {
    var marked = window.marked;
    var purify = window.DOMPurify;
    if (!text) return "";
    if (!marked || !purify) {
      var esc = String(text).replace(/[&<>]/g, function (c) {
        return c === "&" ? "&amp;" : c === "<" ? "&lt;" : "&gt;";
      });
      return "<p>" + esc.replace(/\n\n+/g, "</p><p>").replace(/\n/g, "<br/>") + "</p>";
    }
    marked.setOptions({ breaks: true, gfm: true });
    return purify.sanitize(marked.parse(text));
  }

  // ── SSE reader ───────────────────────────────────────────────

  function readSSE(response, onEvent, signal) {
    if (!response.body || !response.body.getReader) {
      return Promise.reject(new Error("ReadableStream not supported"));
    }
    var reader = response.body.getReader();
    var decoder = new TextDecoder();
    var buffer = "";
    function pump() {
      return reader.read().then(function (res) {
        if (res.done) return;
        buffer += decoder.decode(res.value, { stream: true });
        var parts = buffer.split(/\r?\n\r?\n/);
        buffer = parts.pop() || "";
        for (var i = 0; i < parts.length; i++) {
          var block = parts[i];
          if (!block || block.charAt(0) === ":") continue;
          var lines = block.split(/\r?\n/);
          var eventName = "message";
          var dataLines = [];
          for (var j = 0; j < lines.length; j++) {
            var line = lines[j];
            if (line.indexOf("event:") === 0) eventName = line.slice(6).trim();
            else if (line.indexOf("data:") === 0) dataLines.push(line.slice(5).replace(/^ /, ""));
          }
          var dataStr = dataLines.join("\n");
          var data = null;
          if (dataStr) { try { data = JSON.parse(dataStr); } catch (_e) { data = dataStr; } }
          try { onEvent(eventName, data); } catch (_e) { /* swallow handler errors */ }
        }
        if (signal && signal.aborted) return;
        return pump();
      });
    }
    return pump();
  }

  // ── API ──────────────────────────────────────────────────────

  var API_BASE = window.location.origin;

  function api(path, opts) {
    opts = opts || {};
    opts.credentials = "include";
    opts.headers = opts.headers || {};
    return fetch(API_BASE + path, opts);
  }

  // ── UI components ────────────────────────────────────────────

  function SessionItem(props) {
    var t = props.theme, s = props.session, active = props.active;
    var activeCls = active
      ? "bg-teal-500/10 border-teal-500/30 text-teal-200"
      : cl("border-transparent hover:bg-white/[0.03]", t.textLabel);
    return h("div", {
      onClick: props.onSelect,
      className: cl(
        "group flex items-center gap-2 px-2 py-2 rounded-lg border cursor-pointer text-sm min-h-[44px]",
        activeCls
      ),
    },
      h("div", { className: "flex-1 min-w-0 px-1" },
        h("div", { className: "truncate" }, s.title || "New chat"),
        h("div", { className: cl("text-xs mt-0.5 font-mono", t.textFaint) }, relTime(s.updated_at))
      ),
      // Delete button: hover-revealed on desktop, always visible on touch (touch devices can't hover).
      h("button", {
        onClick: function (e) { e.stopPropagation(); props.onDelete(s.id); },
        title: "Delete session",
        "aria-label": "Delete session",
        className: cl(
          "shrink-0 w-9 h-9 rounded-lg flex items-center justify-center text-sm",
          "opacity-100 md:opacity-0 md:group-hover:opacity-100 transition",
          t.textFaint, "hover:text-red-400 hover:bg-white/[0.04]"
        ),
      }, "\u2715")
    );
  }

  function ToolPill(props) {
    var t = props.theme, call = props.call, done = props.done, duration = props.duration;
    var base = "inline-flex items-center gap-1.5 text-xs font-mono px-2 py-0.5 rounded border mr-2";
    if (done) {
      return h("span", { className: cl(base, "border-zinc-700/50", t.textFaint) },
        "\u2713 ", call.tool, duration != null ? " (" + duration + "ms)" : ""
      );
    }
    return h("span", { className: cl(base, "border-teal-600/40 text-teal-300 bg-teal-900/10") },
      h("span", { className: "animate-pulse" }, "\u27F3"), " ", call.tool,
      call.input && call.input.query ? ": \"" + String(call.input.query).slice(0, 40) + "\"" : ""
    );
  }

  function MessageRow(props) {
    var t = props.theme, m = props.message, streaming = props.streaming;
    var isUser = m.role === "user";

    if (isUser) {
      return h("div", { className: "flex justify-end my-3" },
        h("div", {
          className: cl(
            "max-w-[70%] rounded-2xl px-4 py-2.5 text-sm whitespace-pre-wrap break-words",
            "bg-teal-600/25 text-zinc-100 border border-teal-600/30"
          ),
        }, m.content)
      );
    }

    var tools = m.tool_calls || [];
    var results = m.tool_results || [];
    var durationByTool = {};
    for (var i = 0; i < results.length; i++) {
      durationByTool[results[i].tool] = results[i].duration_ms;
    }
    var labelCls = cl("text-xs uppercase tracking-widest font-mono mb-1", t.textFaint);
    var contentHtml = renderMarkdown(m.content || "");

    return h("div", { className: "my-4" },
      h("div", { className: labelCls }, "TAILOR", streaming ? h("span", { className: "ml-2 animate-pulse text-teal-400" }, "\u25CF") : null),
      tools.length > 0 ? h("div", { className: "mb-1.5 flex flex-wrap gap-y-1" },
        tools.map(function (call, k) {
          var done = durationByTool.hasOwnProperty(call.tool) || !streaming;
          return h(ToolPill, {
            key: k,
            theme: t,
            call: call,
            done: done,
            duration: durationByTool[call.tool],
          });
        })
      ) : null,
      h("div", {
        className: cl("chat-prose text-sm leading-relaxed", t.text),
        dangerouslySetInnerHTML: { __html: contentHtml || (streaming ? "" : "<p class=\"italic opacity-60\">(empty response)</p>") },
      })
    );
  }

  function Composer(props) {
    var t = props.theme;
    var taRef = useRef(null);

    function autoresize() {
      var el = taRef.current;
      if (!el) return;
      el.style.height = "auto";
      var max = 6 * 24; // ~6 rows
      el.style.height = Math.min(el.scrollHeight, max) + "px";
    }
    useEffect(autoresize, [props.value]);

    function onKey(e) {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        if (!props.busy && props.value.trim()) props.onSend();
      }
    }

    return h("div", { className: cl("chat-composer border rounded-2xl p-2 flex items-center gap-2", t.border, t.input) },
      h("textarea", {
        ref: taRef,
        value: props.value,
        onChange: function (e) { props.onChange(e.target.value); },
        onKeyDown: onKey,
        rows: 1,
        disabled: props.busy,
        placeholder: "Ask TAILOR...",
        // On mobile we enforce 16px font via scoped CSS (.chat-composer textarea
        // @ max-width:767px) so iOS Safari/Chrome don't auto-zoom on focus.
        className: cl(
          "flex-1 resize-none bg-transparent outline-none border-0 px-2 py-1.5 text-base md:text-sm leading-relaxed self-center",
          t.text, "placeholder-zinc-500"
        ),
        style: { minHeight: "36px", maxHeight: "144px" },
      }),
      props.busy
        ? h("button", {
            onClick: props.onStop,
            className: "min-h-[44px] min-w-[44px] px-4 rounded-xl text-sm font-medium bg-red-600/80 hover:bg-red-600 text-white transition",
          }, "Stop")
        : h("button", {
            onClick: props.onSend,
            disabled: !props.value.trim(),
            className: cl(
              "min-h-[44px] min-w-[44px] px-4 rounded-xl text-sm font-semibold transition",
              props.value.trim()
                ? "bg-teal-600 hover:bg-teal-500 text-white"
                : "bg-zinc-800 text-zinc-500 cursor-not-allowed"
            ),
          }, "Send")
    );
  }

  // Hamburger SVG — inlined to avoid adding an icon dep.
  function HamburgerIcon() {
    return h("svg", {
      width: 20, height: 20, viewBox: "0 0 20 20",
      fill: "none", stroke: "currentColor", strokeWidth: 2, strokeLinecap: "round",
      "aria-hidden": "true",
    },
      h("line", { x1: 3, y1: 6, x2: 17, y2: 6 }),
      h("line", { x1: 3, y1: 10, x2: 17, y2: 10 }),
      h("line", { x1: 3, y1: 14, x2: 17, y2: 14 })
    );
  }

  // Native <select> for provider/model. Native is the right default for
  // mobile UX — iOS/Android render it as an OS picker which is far more
  // usable than a custom dropdown on a touch device. Styled to match the
  // dashboard's input fields. The value is encoded as "provider|model".
  function ProviderSelector(props) {
    var t = props.theme;
    var options = props.providers || [];
    var sel = props.value || {};
    var cur = sel.provider && sel.model ? sel.provider + "|" + sel.model : "";
    var labelFor = props.labelFor || function (p) { return p.label || (p.provider + "/" + p.model); };
    return h("label", { className: "flex items-center gap-2 text-xs" },
      h("span", { className: cl("uppercase tracking-widest font-mono", t.textFaint) }, "Model"),
      h("select", {
        value: cur,
        onChange: function (e) {
          var v = e.target.value.split("|");
          if (v.length === 2) props.onChange({ provider: v[0], model: v[1] });
        },
        disabled: props.disabled,
        className: cl(
          "px-2 py-1.5 rounded-lg border text-sm min-h-[36px] max-w-[240px] truncate",
          t.input
        ),
      },
        options.map(function (p) {
          var key = p.provider + "|" + p.model;
          return h("option", { key: key, value: key }, labelFor(p));
        })
      )
    );
  }

  // One-click "promote current selection to the default llm.* brain".
  // Status is owned by the parent: null = idle, "loading" = POST in
  // flight, "ok" = success flash (cleared after 2s), {error: str} = show
  // the message inline. No modal, no spinner — matches the rest of the
  // dashboard's inline-feedback pattern.
  function SetDefaultButton(props) {
    var t = props.theme;
    var status = props.status;
    var loading = status === "loading";
    var okFlash = status === "ok";
    var errMsg = (status && typeof status === "object" && status.error) ? status.error : null;
    return h("span", { className: "inline-flex items-center gap-2" },
      h("button", {
        onClick: props.onClick,
        disabled: loading,
        className: cl(
          "px-3 py-1.5 rounded-lg text-xs font-medium border transition",
          loading ? "opacity-50 cursor-wait" : "hover:border-teal-500/50 hover:text-teal-300 hover:bg-teal-500/[0.06]",
          t.border, t.textLabel
        ),
      }, "Set as default"),
      okFlash ? h("span", {
        className: cl("text-xs", "text-teal-400"),
      }, "✓ Default updated") : null,
      errMsg ? h("span", {
        className: "text-xs text-red-400 font-mono truncate max-w-[200px]",
        title: errMsg,
      }, errMsg) : null
    );
  }

  function EmptyState(props) {
    var t = props.theme;
    var prompts = props.suggestPrompts || [];
    return h("div", { className: "flex flex-col items-center justify-center text-center py-16 px-6" },
      h("div", { className: cl("text-2xl font-semibold mb-2", t.text) }, "Start a conversation with TAILOR"),
      h("div", { className: cl("text-sm mb-6", t.textMuted) },
        "Your memory, your model, your machine."
      ),
      h("div", { className: "flex flex-col gap-2 w-full max-w-md" },
        prompts.map(function (p, i) {
          return h("button", {
            key: i,
            onClick: function () { props.onSuggest(p); },
            className: cl(
              "text-left text-sm px-4 py-3 rounded-xl border transition",
              t.border, t.textLabel,
              "hover:border-teal-500/40 hover:text-teal-300 hover:bg-teal-500/[0.04]"
            ),
          }, p);
        })
      )
    );
  }

  // ── Tab ──────────────────────────────────────────────────────

  function Tab(props) {
    var t = props.theme;
    var _s = useState([]); var sessions = _s[0], setSessions = _s[1];
    var _p = useState([]); var prompts = _p[0], setPrompts = _p[1];
    var _en = useState(true); var enabled = _en[0], setEnabled = _en[1];
    var _a = useState(null); var activeId = _a[0], setActiveId = _a[1];
    var _m = useState([]); var messages = _m[0], setMessages = _m[1];
    var _i = useState(""); var input = _i[0], setInput = _i[1];
    var _b = useState(false); var busy = _b[0], setBusy = _b[1];
    var _e = useState(null); var error = _e[0], setError = _e[1];
    var abortRef = useRef(null);
    var scrollRef = useRef(null);
    // True when activeId was just assigned from an in-flight stream.
    // Suppresses the next session-detail fetch so the optimistic
    // [user, assistant-streaming] state isn't clobbered by the server's
    // partial snapshot (which only has the user message persisted so far).
    var skipNextLoadRef = useRef(false);
    // Mobile drawer state. On desktop the sidebar is always visible
    // (via md: utilities) and this flag is effectively ignored.
    var _so = useState(false); var sidebarOpen = _so[0], setSidebarOpen = _so[1];
    // Per-session provider/model selection — populated from /api/chat/providers.
    // `selected` starts at the server-reported default and the user can override
    // it via the dropdown shown in the "New chat" empty state. `defaultSel`
    // is retained so `newChat()` can reset back to the server default regardless
    // of any previously-loaded session's pin.
    var _av = useState([]); var providers = _av[0], setProviders = _av[1];
    var _def = useState(null); var defaultSel = _def[0], setDefaultSel = _def[1];
    var _sel = useState(null); var selected = _sel[0], setSelected = _sel[1];
    // `defaultProvider` tracks what llm.* currently points at. Drives the
    // "(default)" suffix in the dropdown and the visibility of the "Set as
    // default" button. Distinct from `defaultSel`: the latter has a fallback
    // to avail[0] when llm.* is missing, which we don't want for labeling.
    var _dp = useState(null); var defaultProvider = _dp[0], setDefaultProvider = _dp[1];
    // Transient status for the "Set as default" button. null | "loading" |
    // "ok" | {error: string}. Cleared after 2s on success.
    var _du = useState(null); var defaultUpdate = _du[0], setDefaultUpdate = _du[1];

    function openSidebar() { setSidebarOpen(true); }
    function closeSidebar() { setSidebarOpen(false); }

    // ESC closes the mobile drawer; also lock body scroll while it's open
    // so iOS rubber-band doesn't reveal the page underneath.
    useEffect(function () {
      function onKey(e) { if (e.key === "Escape") closeSidebar(); }
      if (sidebarOpen) {
        document.addEventListener("keydown", onKey);
        var prev = document.body.style.overflow;
        document.body.style.overflow = "hidden";
        return function () {
          document.removeEventListener("keydown", onKey);
          document.body.style.overflow = prev;
        };
      }
    }, [sidebarOpen]);

    var refreshSessions = useCallback(function () {
      return api("/api/chat/sessions").then(function (r) { return r.json(); }).then(function (body) {
        if (!body) return;
        if (body.enabled === false) setEnabled(false);
        setSessions(body.sessions || []);
        setPrompts(body.suggest_prompts || []);
      });
    }, []);

    useEffect(function () { refreshSessions(); }, [refreshSessions]);

    // Fetch available providers once. Falls back silently on error — the
    // dropdown just won't render, and the API will use the default brain.
    useEffect(function () {
      api("/api/chat/providers").then(function (r) { return r.json(); }).then(function (body) {
        if (!body) return;
        var avail = Array.isArray(body.available) ? body.available : [];
        setProviders(avail);
        var strictDefault = null;
        if (body.default && body.default.provider && body.default.model) {
          strictDefault = { provider: body.default.provider, model: body.default.model };
        }
        setDefaultProvider(strictDefault);
        var def = strictDefault;
        if (!def && avail.length > 0) {
          def = { provider: avail[0].provider, model: avail[0].model };
        }
        if (def) {
          setDefaultSel(def);
          setSelected(def);
        }
      }).catch(function () { /* dropdown simply won't render */ });
    }, []);

    // Map (provider, model) → label from the fetched list; falls back to
    // "<provider>/<model>" when the pair isn't in available_providers (e.g.
    // a session pinned on a provider that was later removed from config).
    // Appends " (default)" when the pair matches the currently-active llm.*
    // — drives the dropdown suffix and the pinned-provider badge on mobile.
    function labelFor(provider, model) {
      if (!provider || !model) return "";
      var base = null;
      for (var i = 0; i < providers.length; i++) {
        var p = providers[i];
        if (p.provider === provider && p.model === model) { base = p.label; break; }
      }
      if (!base) base = provider + "/" + model;
      if (defaultProvider && defaultProvider.provider === provider && defaultProvider.model === model) {
        // Don't append "(default)" if the configured label already contains it
        // (e.g. label: "Claude Haiku 4.5 (default)"). Case-insensitive check to
        // catch common variations like "(Default)" or "(DEFAULT)".
        if (base.toLowerCase().indexOf("(default)") !== -1) return base;
        return base + " (default)";
      }
      return base;
    }

    useEffect(function () {
      if (!activeId) { setMessages([]); return; }
      if (skipNextLoadRef.current) { skipNextLoadRef.current = false; return; }
      api("/api/chat/sessions/" + encodeURIComponent(activeId)).then(function (r) { return r.json(); }).then(function (body) {
        if (!body) return;
        if (body.messages) setMessages(body.messages);
        // If the loaded session has a pinned provider, surface it as the
        // currently-selected pair. This also drives the "via <label>" badge
        // in the mobile top bar.
        if (body.session && body.session.provider && body.session.model) {
          setSelected({ provider: body.session.provider, model: body.session.model });
        }
      });
    }, [activeId]);

    useEffect(function () {
      var el = scrollRef.current;
      if (el) el.scrollTop = el.scrollHeight;
    }, [messages]);

    function newChat() {
      setActiveId(null);
      setMessages([]);
      setInput("");
      setError(null);
      // Reset the dropdown to the server's default so a new chat doesn't
      // silently inherit the previously-loaded session's pinned provider.
      if (defaultSel) setSelected(defaultSel);
    }

    function deleteSession(sid) {
      if (!confirm("Delete this chat?")) return;
      api("/api/chat/sessions/" + encodeURIComponent(sid), { method: "DELETE" }).then(function () {
        if (activeId === sid) newChat();
        refreshSessions();
      });
    }

    function send(text) {
      var payload = (text != null ? text : input).trim();
      if (!payload || busy) return;
      setError(null);
      setInput("");
      setBusy(true);

      var userMsg = { role: "user", content: payload, created_at: new Date().toISOString() };
      var streamMsg = { role: "assistant", content: "", tool_calls: [], tool_results: [], streaming: true };
      setMessages(function (prev) { return prev.concat([userMsg, streamMsg]); });

      var controller = new AbortController();
      abortRef.current = controller;

      // Include provider/model ONLY on new sessions. For existing sessions
      // the backend ignores them anyway (DB row is the source of truth),
      // but sending them would be semantically misleading.
      var reqBody = { session_id: activeId, message: payload };
      if (activeId == null && selected && selected.provider && selected.model) {
        reqBody.provider = selected.provider;
        reqBody.model = selected.model;
      }

      api("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(reqBody),
        signal: controller.signal,
      }).then(function (resp) {
        if (!resp.ok) {
          return resp.text().then(function (tx) { throw new Error("HTTP " + resp.status + ": " + tx); });
        }
        return readSSE(resp, function (eventName, data) {
          if (eventName === "session") {
            if (data && data.session_id && !activeId) {
              // Adopt the server-assigned id without triggering the
              // session-detail fetch — our local state is already correct.
              skipNextLoadRef.current = true;
              setActiveId(data.session_id);
            }
          } else if (eventName === "token") {
            var delta = (data && data.delta) || "";
            setMessages(function (prev) {
              var out = prev.slice();
              var last = Object.assign({}, out[out.length - 1]);
              last.content = (last.content || "") + delta;
              out[out.length - 1] = last;
              return out;
            });
          } else if (eventName === "tool_start") {
            setMessages(function (prev) {
              var out = prev.slice();
              var last = Object.assign({}, out[out.length - 1]);
              last.tool_calls = (last.tool_calls || []).concat([{ tool: data.tool, input: data.input || {} }]);
              out[out.length - 1] = last;
              return out;
            });
          } else if (eventName === "tool_end") {
            setMessages(function (prev) {
              var out = prev.slice();
              var last = Object.assign({}, out[out.length - 1]);
              last.tool_results = (last.tool_results || []).concat([{ tool: data.tool, duration_ms: data.duration_ms }]);
              out[out.length - 1] = last;
              return out;
            });
          } else if (eventName === "error") {
            setError((data && data.error) || "unknown error");
          }
        }, controller.signal);
      }).then(function () {
        setMessages(function (prev) {
          var out = prev.slice();
          var last = Object.assign({}, out[out.length - 1]);
          delete last.streaming;
          out[out.length - 1] = last;
          return out;
        });
        refreshSessions();
      }).catch(function (err) {
        if (err && err.name === "AbortError") {
          // Stopped by user — partial message already persisted server-side on disconnect.
          refreshSessions();
        } else {
          setError(String((err && err.message) || err));
        }
      }).then(function () {
        setBusy(false);
        abortRef.current = null;
      });
    }

    function stop() {
      if (abortRef.current) abortRef.current.abort();
    }

    // Promote the currently-selected (provider, model) to the default llm.*.
    // Fire-and-wait: POST, update local state on success, flash an inline
    // confirmation for 2s. On failure, surface the error via the same
    // inline mechanism (no banner, no modal).
    function promoteToDefault() {
      if (!selected || !selected.provider || !selected.model) return;
      setDefaultUpdate("loading");
      api("/api/chat/providers/default", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider: selected.provider, model: selected.model }),
      }).then(function (r) { return r.json().then(function (body) { return { ok: r.ok, body: body }; }); })
        .then(function (res) {
          if (res.ok && res.body && res.body.ok && res.body.default) {
            setDefaultProvider({
              provider: res.body.default.provider,
              model: res.body.default.model,
            });
            setDefaultSel({
              provider: res.body.default.provider,
              model: res.body.default.model,
            });
            setDefaultUpdate("ok");
            setTimeout(function () { setDefaultUpdate(null); }, 2000);
          } else {
            var msg = (res.body && res.body.error) || "update failed";
            setDefaultUpdate({ error: msg });
          }
        })
        .catch(function (err) {
          setDefaultUpdate({ error: String((err && err.message) || err) });
        });
    }

    // Is `selected` different from the current default? Controls visibility
    // of the "Set as default" button — hiding it when the user is already
    // on the default avoids a no-op click.
    function selectionIsDefault() {
      if (!selected || !defaultProvider) return false;
      return selected.provider === defaultProvider.provider
        && selected.model === defaultProvider.model;
    }

    if (!enabled) {
      return h("div", { className: cl("rounded-2xl border p-8 text-center", t.border, t.tile) },
        h("div", { className: cl("text-lg font-semibold mb-1", t.text) }, "Chat interface disabled"),
        h("div", { className: cl("text-sm", t.textMuted) }, "Enable it in ", h("code", { className: "font-mono" }, "config/tailor.yaml"), " → ", h("code", { className: "font-mono" }, "chat_interface.enabled"), ".")
      );
    }

    var activeSession = null;
    for (var si = 0; si < sessions.length; si++) {
      if (sessions[si].id === activeId) { activeSession = sessions[si]; break; }
    }
    var mobileTitle = activeSession ? (activeSession.title || "New chat") : "New chat";
    // Label for the desktop "via" badge and the active-session case on
    // mobile. Priority:
    //   1. Session has an explicit pinned provider/model → use that
    //   2. Otherwise, fall back to the current llm.* default
    //   3. If neither (defaults not loaded yet) → null, badge hidden
    // A session's provider is immutable after creation — the badge is
    // informational only.
    var pinnedLabel = null;
    if (activeSession && activeSession.provider && activeSession.model) {
      pinnedLabel = labelFor(activeSession.provider, activeSession.model);
    } else if (activeSession && defaultProvider) {
      pinnedLabel = labelFor(defaultProvider.provider, defaultProvider.model);
    }
    // Extends pinnedLabel to the pre-send "new chat" state on mobile by
    // falling back to whichever pair is currently selected in the dropdown.
    // Keeps the top-bar badge populated in every state so the mobile user
    // always sees which model will answer, even when the dropdown isn't
    // rendered (single-provider setups) or is easy to miss.
    var mobileModelLabel = pinnedLabel;
    if (!mobileModelLabel && selected && selected.provider && selected.model) {
      mobileModelLabel = labelFor(selected.provider, selected.model);
    }
    // Show the provider selector only on a fresh "new chat" — before a session
    // is created/selected. After the first send `activeId` is set and the
    // selection is locked in the DB.
    var showSelector = activeId == null && providers.length > 1;
    // Labeler for the dropdown options. Same rules as labelFor() used
    // elsewhere; passed into ProviderSelector so option text also reflects
    // the "(default)" suffix on the matching pair.
    var optionLabelFor = function (p) { return labelFor(p.provider, p.model); };
    var showSetDefault = showSelector && !!selected && !!defaultProvider && !selectionIsDefault();

    return h("div", {
      className: cl("chat-shell relative rounded-2xl border overflow-hidden flex", t.border),
    },
      // Mobile-only backdrop behind the drawer. Renders when the drawer is
      // open; tap to close. Hidden on md+ where the sidebar is always in flow.
      sidebarOpen ? h("div", {
        className: "fixed inset-0 z-30 bg-black/60 md:hidden",
        onClick: closeSidebar,
        "aria-hidden": "true",
      }) : null,

      // Sidebar.
      // Mobile: fixed drawer, slides in from left.
      // Desktop: static column in the flex row, always visible.
      h("aside", {
        className: cl(
          "flex flex-col border-r",
          t.border, t.tile,
          "fixed inset-y-0 left-0 z-40 w-72 shadow-2xl transform transition-transform duration-200 ease-out",
          sidebarOpen ? "translate-x-0" : "-translate-x-full",
          "md:static md:inset-auto md:w-60 md:flex-shrink-0 md:translate-x-0 md:shadow-none md:transition-none"
        ),
        "aria-label": "Chat sessions",
      },
        h("div", { className: "p-3 flex items-center gap-2" },
          h("button", {
            onClick: closeSidebar,
            className: "md:hidden w-11 h-11 rounded-xl flex items-center justify-center text-zinc-300 hover:bg-white/[0.05]",
            "aria-label": "Close sessions",
          }, "\u2715"),
          h("button", {
            onClick: function () { newChat(); closeSidebar(); },
            className: "flex-1 min-h-[44px] rounded-xl text-sm font-semibold bg-teal-600 hover:bg-teal-500 text-white transition",
          }, "+ New chat")
        ),
        h("div", { className: "flex-1 overflow-y-auto px-2 pb-3 space-y-1" },
          sessions.length === 0
            ? h("div", { className: cl("text-xs text-center py-4", t.textFaint) }, "No sessions yet")
            : sessions.map(function (s) {
                return h(SessionItem, {
                  key: s.id,
                  theme: t,
                  session: s,
                  active: s.id === activeId,
                  onSelect: function () { setActiveId(s.id); closeSidebar(); },
                  onDelete: deleteSession,
                });
              })
        )
      ),

      // Main pane (messages + composer).
      h("div", { className: "flex-1 flex flex-col min-w-0 min-h-0" },
        // Mobile top bar: hamburger + current session title + pinned-provider
        // badge (if any). Desktop hides the whole bar.
        h("div", {
          className: cl("md:hidden flex items-center gap-2 px-2 py-2 border-b", t.border),
        },
          h("button", {
            onClick: openSidebar,
            className: "w-11 h-11 rounded-xl flex items-center justify-center text-zinc-300 hover:bg-white/[0.05]",
            "aria-label": "Open sessions",
          }, h(HamburgerIcon)),
          h("div", { className: cl("flex-1 min-w-0 truncate text-sm font-medium", t.text) }, mobileTitle),
          mobileModelLabel ? h("span", {
            className: cl("shrink-0 text-xs font-mono px-2 py-1 rounded-md border", t.textFaint, "border-zinc-700/50"),
            title: "Model: " + mobileModelLabel,
          }, mobileModelLabel) : null
        ),

        // Chat header strip. Shown when there's something to show — either the
        // provider selector (new chat) or the pinned-provider badge (loaded
        // session). Otherwise collapses to nothing so the conversation owns
        // the full vertical space.
        (showSelector || pinnedLabel) ? h("div", {
          className: cl("hidden md:flex items-center justify-end gap-3 px-5 py-2 border-b", t.border),
        },
          showSelector ? h(ProviderSelector, {
            theme: t,
            providers: providers,
            value: selected,
            onChange: setSelected,
            disabled: busy,
            labelFor: optionLabelFor,
          }) : null,
          showSetDefault ? h(SetDefaultButton, {
            theme: t,
            status: defaultUpdate,
            onClick: promoteToDefault,
          }) : null,
          (!showSelector && pinnedLabel) ? h("span", {
            className: cl("text-xs font-mono", t.textFaint),
          }, "via ", h("span", { className: t.text }, pinnedLabel)) : null
        ) : null,

        // Mobile variant of the provider selector — stacked above the scroll
        // area so the dropdown has room on a narrow viewport.
        showSelector ? h("div", {
          className: cl("md:hidden flex items-center justify-between gap-2 px-3 py-2 border-b", t.border),
        },
          h(ProviderSelector, {
            theme: t,
            providers: providers,
            value: selected,
            onChange: setSelected,
            disabled: busy,
            labelFor: optionLabelFor,
          }),
          showSetDefault ? h(SetDefaultButton, {
            theme: t,
            status: defaultUpdate,
            onClick: promoteToDefault,
          }) : null
        ) : null,

        h("div", { ref: scrollRef, className: "flex-1 overflow-y-auto px-4 md:px-5 py-4 min-h-0" },
          messages.length === 0
            ? h(EmptyState, { theme: t, suggestPrompts: prompts, onSuggest: function (p) { send(p); } })
            : messages.map(function (m, i) {
                return h(MessageRow, { key: i, theme: t, message: m, streaming: !!m.streaming });
              })
        ),
        error ? h("div", { className: "mx-4 md:mx-5 mb-2 text-xs px-3 py-2 rounded-lg border border-red-800/50 bg-red-900/20 text-red-300 font-mono" }, "error: " + error) : null,
        // chat-composer-wrap carries the safe-area-inset-bottom padding so the
        // composer clears the iOS home indicator on notched phones.
        h("div", { className: "chat-composer-wrap px-3 md:px-4 pt-2 flex-shrink-0" },
          h(Composer, {
            theme: t,
            value: input,
            onChange: setInput,
            onSend: function () { send(); },
            onStop: stop,
            busy: busy,
          })
        )
      )
    );
  }

  window.TailorChat = { Tab: Tab, renderMarkdown: renderMarkdown };
})();
