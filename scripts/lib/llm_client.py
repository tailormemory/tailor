"""
TAILOR — LLM-agnostic client.

Tier 1 (tool use): LLM decides what to search. Supports multi-step tool calls.
Tier 2 (prompt-based): Bot pre-fetches context, passes it in the prompt.

Supported providers: anthropic, openai, google, ollama
"""

import os, sys, json, re, time, requests
from lib.config import get

# ═══════════════════════════════════════════════════════════════
# Streaming event protocol (shared across providers).
#
# Each event is a dict with a "type" key. Consumers (e.g. the native
# chat endpoint in lib.chat_api) translate these into SSE frames.
#
#   {"type": "token",      "delta": str}
#   {"type": "tool_start", "tool": str, "input": dict}
#   {"type": "tool_end",   "tool": str, "duration_ms": int}
#   {"type": "done",       "tokens": int | None}
#   {"type": "error",      "error": str}
#
# Providers that cannot stream (OpenAI/Google/Ollama today) inherit the
# base LLMClient.stream_chat_with_tools(), which runs the blocking
# chat_with_tools() and word-chunks the final string into token events.
# AnthropicClient overrides with real SSE from the Messages API.
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# Tool Definitions (exposed to Tier 1 LLMs)
# ═══════════════════════════════════════════════════════════════

TOOLS = [
    {
        "name": "kb_hybrid_search",
        "description": "Search the Knowledge Base. Combines semantic + entity search. Use this first to find relevant context before answering.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "n_results": {"type": "integer", "description": "Number of results (default 5)", "default": 5},
                "source_filter": {"type": "string", "description": "Filter by source: document, email, claude, chatgpt, or empty for all", "default": ""},
            },
            "required": ["query"]
        }
    },
    {
        "name": "kb_search_docs",
        "description": "Search documents in the KB by folder or doc_type.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "folder": {"type": "string", "description": "Folder filter", "default": ""},
                "doc_type": {"type": "string", "description": "Document type filter", "default": ""},
                "n_results": {"type": "integer", "default": 5},
            },
            "required": ["query"]
        }
    },
    {
        "name": "get_user_profile",
        "description": "Get the full user profile (identity, preferences, projects, recent activity). Call this when you need context about the user.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "kb_add",
        "description": "Save a new fact or note to the Knowledge Base. Use when the user shares important information worth remembering.",
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "The fact or note to save"},
                "title": {"type": "string", "description": "Short title (max 80 chars)"},
                "category": {"type": "string", "description": "Category: decision/preference/fact/project/person/financial/health/technical/personal", "default": "general"},
            },
            "required": ["content", "title"]
        }
    },
    {
        "name": "system_status",
        "description": "Get system operational status: KB stats, fact extraction progress (coverage %, nights remaining), pipeline last run, services health. Use this when the user asks about system state, pipeline progress, or operational metrics.",
        "input_schema": {"type": "object", "properties": {}}
    },
]

# ═══════════════════════════════════════════════════════════════
# MCP Proxy — executes tool calls against localhost MCP server
# ═══════════════════════════════════════════════════════════════

def _mcp_call(tool_name: str, args: dict) -> str:
    """Execute a tool locally (direct Python call, no HTTP)."""
    from lib.tool_executor import execute_tool
    return execute_tool(tool_name, args)


_WORD_CHUNK_RE = re.compile(r"\S+\s*|\s+")

def _word_chunks(text: str):
    """Split a final response into word-plus-trailing-whitespace chunks.
    Used to fake streaming for providers without native SSE support.
    Word-based rather than char-based so the UX reads as natural typing."""
    if not text:
        return
    for m in _WORD_CHUNK_RE.finditer(text):
        chunk = m.group(0)
        if chunk:
            yield chunk


# ═══════════════════════════════════════════════════════════════
# Base class
# ═══════════════════════════════════════════════════════════════

class LLMClient:
    """Abstract LLM client interface."""
    
    def __init__(self, cfg: dict):
        self.provider = cfg.get("provider", "anthropic")
        self.model = cfg.get("model", "")
        self.api_key = cfg.get("api_key", "")
        self.max_tokens = cfg.get("max_tokens", 1000)
        self.temperature = cfg.get("temperature", 0.3)
        self.tool_use = cfg.get("tool_use", True)
    
    def chat(self, system: str, messages: list[dict], max_tokens: int = 0) -> str:
        """Simple chat without tools. Returns text response."""
        raise NotImplementedError
    
    def chat_with_tools(self, system: str, messages: list[dict], tools: list = None) -> str:
        """Chat with tool use. Handles tool call loop internally. Returns final text."""
        raise NotImplementedError

    def stream_chat_with_tools(self, system: str, messages: list[dict], tools: list = None):
        """Default: run blocking chat_with_tools and word-chunk the final text.
        Providers that support native streaming override this."""
        try:
            text = self.chat_with_tools(system, messages, tools=tools)
        except Exception as e:
            yield {"type": "error", "error": str(e)}
            return
        for chunk in _word_chunks(text):
            yield {"type": "token", "delta": chunk}
        yield {"type": "done", "tokens": None}


# ═══════════════════════════════════════════════════════════════
# Anthropic (Claude)
# ═══════════════════════════════════════════════════════════════

class AnthropicClient(LLMClient):
    
    def _call_api(self, system: str, messages: list[dict], tools: list = None, max_tokens: int = 0):
        payload = {
            "model": self.model,
            "max_tokens": max_tokens or self.max_tokens,
            "system": system,
            "messages": messages,
            "temperature": self.temperature,
        }
        if tools:
            payload["tools"] = tools
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
            timeout=60,
        )
        return resp.json()
    
    def chat(self, system: str, messages: list[dict], max_tokens: int = 0) -> str:
        data = self._call_api(system, messages, max_tokens=max_tokens)
        if "content" in data and data["content"]:
            return data["content"][0].get("text", "").strip()
        return ""
    
    def chat_with_tools(self, system: str, messages: list[dict], tools: list = None) -> str:
        tools = tools or TOOLS
        anthropic_tools = [{"name": t["name"], "description": t["description"], "input_schema": t["input_schema"]} for t in tools]
        msgs = list(messages)
        
        # Tool use loop (max 5 iterations to prevent infinite loops)
        for _ in range(5):
            data = self._call_api(system, msgs, tools=anthropic_tools)
            if "error" in data:
                return f"API error: {data['error'].get('message', str(data['error']))}"
            
            content = data.get("content", [])
            stop_reason = data.get("stop_reason", "")
            
            if stop_reason != "tool_use":
                # Final response — extract text
                texts = [b.get("text", "") for b in content if b.get("type") == "text"]
                return "\n".join(texts).strip()
            
            # Handle tool calls
            msgs.append({"role": "assistant", "content": content})
            tool_results = []
            for block in content:
                if block.get("type") == "tool_use":
                    result = _mcp_call(block["name"], block.get("input", {}))
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block["id"],
                        "content": result,
                    })
            msgs.append({"role": "user", "content": tool_results})

        return "Max tool iterations reached."

    def stream_chat_with_tools(self, system: str, messages: list[dict], tools: list = None):
        """Real SSE streaming from Anthropic Messages API, with tool loop."""
        tools = tools or TOOLS
        anthropic_tools = [
            {"name": t["name"], "description": t["description"], "input_schema": t["input_schema"]}
            for t in tools
        ]
        msgs = list(messages)
        total_output_tokens = 0

        for _ in range(5):
            payload = {
                "model": self.model,
                "max_tokens": self.max_tokens,
                "system": system,
                "messages": msgs,
                "temperature": self.temperature,
                "tools": anthropic_tools,
                "stream": True,
            }
            try:
                resp = requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": self.api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json=payload,
                    stream=True,
                    timeout=120,
                )
            except Exception as e:
                yield {"type": "error", "error": f"anthropic request failed: {e}"}
                return

            if resp.status_code != 200:
                try:
                    err = resp.json().get("error", {}).get("message") or resp.text[:500]
                except Exception:
                    err = resp.text[:500]
                yield {"type": "error", "error": f"anthropic {resp.status_code}: {err}"}
                return

            # Per-turn accumulators. Anthropic streams content blocks by index;
            # a single turn can contain one text block and/or several tool_use blocks.
            blocks: dict[int, dict] = {}
            assistant_content: list[dict] = []
            stop_reason = ""

            for event_name, data in _iter_anthropic_sse(resp):
                if event_name == "content_block_start":
                    idx = data.get("index", 0)
                    cb = data.get("content_block", {}) or {}
                    btype = cb.get("type")
                    if btype == "text":
                        blocks[idx] = {"type": "text", "text": ""}
                    elif btype == "tool_use":
                        # cb.input may already be populated (non-streamed inline input)
                        # or may be {} if the input arrives via input_json_delta events.
                        # Capture both cases.
                        inline_input = cb.get("input")
                        blocks[idx] = {
                            "type": "tool_use",
                            "id": cb.get("id", ""),
                            "name": cb.get("name", ""),
                            "input_raw": "",
                            "inline_input": inline_input if isinstance(inline_input, dict) else None,
                            "started_at": time.monotonic(),
                            "start_emitted": False,
                        }

                elif event_name == "content_block_delta":
                    idx = data.get("index", 0)
                    delta = data.get("delta", {}) or {}
                    dtype = delta.get("type")
                    blk = blocks.get(idx)
                    if blk is None:
                        continue
                    if dtype == "text_delta":
                        chunk = delta.get("text", "")
                        if chunk:
                            blk["text"] += chunk
                            yield {"type": "token", "delta": chunk}
                    elif dtype == "input_json_delta":
                        blk["input_raw"] += delta.get("partial_json", "")
                        if not blk.get("start_emitted"):
                            # Emit tool_start as soon as we see any input arguments,
                            # even if the input JSON is not yet complete — the UI
                            # just needs to know a tool call has begun.
                            blk["start_emitted"] = True
                            yield {
                                "type": "tool_start",
                                "tool": blk["name"],
                                "input": _try_json(blk["input_raw"]),
                            }

                elif event_name == "content_block_stop":
                    idx = data.get("index", 0)
                    blk = blocks.get(idx)
                    if blk and blk["type"] == "tool_use" and not blk.get("start_emitted"):
                        yield {
                            "type": "tool_start",
                            "tool": blk["name"],
                            "input": _try_json(blk.get("input_raw", "") or "{}"),
                        }
                        blk["start_emitted"] = True

                elif event_name == "message_delta":
                    delta = data.get("delta", {}) or {}
                    if delta.get("stop_reason"):
                        stop_reason = delta["stop_reason"]
                    usage = data.get("usage", {}) or {}
                    if usage.get("output_tokens"):
                        total_output_tokens = usage["output_tokens"]

                elif event_name == "message_stop":
                    break

                elif event_name == "error":
                    err = data.get("error", {}).get("message") or str(data)
                    yield {"type": "error", "error": f"anthropic stream error: {err}"}
                    return

            # Reconstruct the assistant content blocks in index order for the loop-back.
            for idx in sorted(blocks):
                blk = blocks[idx]
                if blk["type"] == "text":
                    if blk["text"]:
                        assistant_content.append({"type": "text", "text": blk["text"]})
                elif blk["type"] == "tool_use":
                    # Prefer streamed input (input_json_delta accumulation) when
                    # it parses to a non-empty dict; otherwise fall back to any
                    # inline input captured in content_block_start.
                    parsed_input = _try_json(blk.get("input_raw", "") or "{}")
                    if not isinstance(parsed_input, dict):
                        parsed_input = {}
                    if not parsed_input and isinstance(blk.get("inline_input"), dict):
                        parsed_input = blk["inline_input"]
                    assistant_content.append({
                        "type": "tool_use",
                        "id": blk["id"],
                        "name": blk["name"],
                        "input": parsed_input,
                    })

            if stop_reason != "tool_use":
                yield {"type": "done", "tokens": total_output_tokens or None}
                return

            # Run tool calls, feed results back, continue the loop.
            msgs.append({"role": "assistant", "content": assistant_content})
            tool_results = []
            for blk in assistant_content:
                if blk.get("type") != "tool_use":
                    continue
                tool_input = blk.get("input") or {}
                if not isinstance(tool_input, dict):
                    tool_input = {}
                started = time.monotonic()
                result = _mcp_call(blk["name"], tool_input)
                duration_ms = int((time.monotonic() - started) * 1000)
                yield {"type": "tool_end", "tool": blk["name"], "duration_ms": duration_ms}
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": blk["id"],
                    "content": result,
                })
            # Insert a visible separator between the pre-tool text (e.g. "Cerco.")
            # and the post-tool answer, so adjacent content blocks from different
            # turns don't render as "Knowledge Base.Problema tecnico..." with no
            # space. Also nudge the model to continue on a fresh paragraph.
            yield {"type": "token", "delta": "\n\n"}
            msgs.append({"role": "user", "content": tool_results})

        yield {"type": "error", "error": "max tool iterations reached"}


def _try_json(raw: str):
    try:
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def _iter_anthropic_sse(resp):
    """Yield (event_name, data_dict) tuples from an Anthropic streaming response."""
    event_name = ""
    for raw in resp.iter_lines(decode_unicode=True):
        if raw is None:
            continue
        line = raw.rstrip("\r")
        if not line:
            event_name = ""
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event_name = line[len("event:"):].strip()
        elif line.startswith("data:"):
            payload = line[len("data:"):].strip()
            try:
                data = json.loads(payload) if payload else {}
            except Exception:
                data = {}
            yield event_name, data


# ═══════════════════════════════════════════════════════════════
# OpenAI (GPT-4o, GPT-4o-mini)
# ═══════════════════════════════════════════════════════════════

class OpenAIClient(LLMClient):
    
    def _call_api(self, system: str, messages: list[dict], tools: list = None, max_tokens: int = 0):
        oai_msgs = [{"role": "system", "content": system}] + messages
        payload = {
            "model": self.model,
            "messages": oai_msgs,
            "max_tokens": max_tokens or self.max_tokens,
            "temperature": self.temperature,
        }
        if tools:
            payload["tools"] = [{"type": "function", "function": {"name": t["name"], "description": t["description"], "parameters": t["input_schema"]}} for t in tools]
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json=payload, timeout=60,
        )
        return resp.json()
    
    def chat(self, system: str, messages: list[dict], max_tokens: int = 0) -> str:
        data = self._call_api(system, messages, max_tokens=max_tokens)
        try:
            return data["choices"][0]["message"]["content"].strip()
        except (KeyError, IndexError):
            return ""
    
    def chat_with_tools(self, system: str, messages: list[dict], tools: list = None) -> str:
        tools = tools or TOOLS
        oai_msgs = [{"role": "system", "content": system}] + list(messages)
        
        for _ in range(5):
            payload = {
                "model": self.model,
                "messages": oai_msgs,
                "max_tokens": self.max_tokens,
                "temperature": self.temperature,
                "tools": [{"type": "function", "function": {"name": t["name"], "description": t["description"], "parameters": t["input_schema"]}} for t in tools],
            }
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json=payload, timeout=60,
            )
            data = resp.json()
            msg = data.get("choices", [{}])[0].get("message", {})
            
            if not msg.get("tool_calls"):
                return msg.get("content", "").strip()
            
            oai_msgs.append(msg)
            for tc in msg["tool_calls"]:
                args = json.loads(tc["function"]["arguments"])
                result = _mcp_call(tc["function"]["name"], args)
                oai_msgs.append({"role": "tool", "tool_call_id": tc["id"], "content": result})
        
        return "Max tool iterations reached."


# ═══════════════════════════════════════════════════════════════
# Google (Gemini)
# ═══════════════════════════════════════════════════════════════

class GoogleClient(LLMClient):
    
    def _call_api(self, system: str, messages: list[dict], tools: list = None, max_tokens: int = 0):
        # Convert messages to Gemini format
        contents = []
        for m in messages:
            role = "user" if m["role"] == "user" else "model"
            if isinstance(m["content"], str):
                contents.append({"role": role, "parts": [{"text": m["content"]}]})
            elif isinstance(m["content"], list):
                # Tool results
                parts = []
                for item in m["content"]:
                    if item.get("type") == "tool_result":
                        parts.append({"functionResponse": {"name": item.get("tool_use_id", ""), "response": {"result": item.get("content", "")}}})
                    elif item.get("type") == "tool_use":
                        parts.append({"functionCall": {"name": item["name"], "args": item.get("input", {})}})
                    elif item.get("text"):
                        parts.append({"text": item["text"]})
                if parts:
                    contents.append({"role": role, "parts": parts})
        
        payload = {
            "contents": contents,
            "systemInstruction": {"parts": [{"text": system}]},
            "generationConfig": {
                "maxOutputTokens": max_tokens or self.max_tokens,
                "temperature": self.temperature,
            },
        }
        if tools:
            payload["tools"] = [{"functionDeclarations": [{"name": t["name"], "description": t["description"], "parameters": t["input_schema"]} for t in tools]}]
        
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}"
        resp = requests.post(url, json=payload, timeout=60)
        return resp.json()
    
    def chat(self, system: str, messages: list[dict], max_tokens: int = 0) -> str:
        data = self._call_api(system, messages, max_tokens=max_tokens)
        try:
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        except (KeyError, IndexError):
            return ""
    
    def chat_with_tools(self, system: str, messages: list[dict], tools: list = None) -> str:
        tools = tools or TOOLS
        msgs = list(messages)
        
        for _ in range(5):
            data = self._call_api(system, msgs, tools=tools)
            try:
                parts = data["candidates"][0]["content"]["parts"]
            except (KeyError, IndexError):
                return f"Gemini error: {json.dumps(data)[:500]}"
            
            # Check for function calls
            fn_calls = [p for p in parts if "functionCall" in p]
            if not fn_calls:
                texts = [p.get("text", "") for p in parts if "text" in p]
                return "\n".join(texts).strip()
            
            # Add assistant response
            msgs.append({"role": "assistant", "content": parts})
            # Execute and return results
            result_parts = []
            for fc in fn_calls:
                name = fc["functionCall"]["name"]
                args = fc["functionCall"].get("args", {})
                result = _mcp_call(name, args)
                result_parts.append({"functionResponse": {"name": name, "response": {"result": result}}})
            msgs.append({"role": "user", "content": result_parts})
        
        return "Max tool iterations reached."


# ═══════════════════════════════════════════════════════════════
# Ollama (Tier 2 — no tool use, prompt-based)
# ═══════════════════════════════════════════════════════════════

class OllamaClient(LLMClient):
    
    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self.base_url = cfg.get("base_url", "http://localhost:11434")
        self.tool_use = False  # Always Tier 2
        # Keep model hot by default (avoids cold-start latency on intermittent use)
        self.keep_alive = cfg.get("keep_alive", -1)
    
    def chat(self, system: str, messages: list[dict], max_tokens: int = 0) -> str:
        # Build Ollama raw prompt
        prompt = f"<|im_start|>system\n{system}<|im_end|>\n"
        for m in messages:
            role = m["role"]
            content = m["content"] if isinstance(m["content"], str) else str(m["content"])
            prompt += f"<|im_start|>{role}\n{content}<|im_end|>\n"
        prompt += "<|im_start|>assistant\n"
        
        try:
            resp = requests.post(
                f"{self.base_url}/api/generate",
                json={"model": self.model, "prompt": prompt, "stream": False, "raw": True,
                      "think": False,
                      "keep_alive": self.keep_alive,
                      "options": {"temperature": self.temperature, "num_predict": max_tokens or self.max_tokens}},
                timeout=60,
            )
            return resp.json().get("response", "").strip()
        except Exception as e:
            return f"Ollama error: {e}"
    
    def chat_with_tools(self, system: str, messages: list[dict], tools: list = None) -> str:
        # Tier 2: no tool use, just chat
        return self.chat(system, messages)


# ═══════════════════════════════════════════════════════════════
# Factory
# ═══════════════════════════════════════════════════════════════

_PROVIDERS = {
    "anthropic": AnthropicClient,
    "openai": OpenAIClient,
    "google": GoogleClient,
    "ollama": OllamaClient,
}

_brain = None
_classifier = None

def get_brain() -> LLMClient:
    """Get the main LLM client (brain) — singleton."""
    global _brain
    if _brain is None:
        cfg = get("llm")
        cls = _PROVIDERS.get(cfg["provider"])
        if not cls:
            raise ValueError(f"Unknown LLM provider: {cfg['provider']}. Supported: {list(_PROVIDERS.keys())}")
        _brain = cls(cfg)
    return _brain


# Per-provider env-var names for API keys. Same precedence as the existing
# config-file paths: if llm.api_key is left as ${ANTHROPIC_API_KEY} etc. the
# env var wins. `build_brain` uses these directly to avoid depending on the
# default llm config when the caller explicitly picks a different provider.
_PROVIDER_API_KEY_ENV = {
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "google": "GOOGLE_API_KEY",
    "ollama": None,  # no key — local endpoint
}


def build_brain(provider: str, model: str) -> LLMClient:
    """Build an LLM client ad-hoc for a specific provider/model.

    Unlike `get_brain()`, this does NOT cache — each call returns a fresh
    instance. Used by the chat API to honour a session's pinned
    provider/model without mutating the process-wide singleton.

    Behaviour:
      * `provider` must be a key of `_PROVIDERS`; otherwise ValueError.
      * `model` is passed through to the provider constructor as-is.
      * For cloud providers (anthropic, openai, google), the API key is
        read from the corresponding env var. If missing, raise ValueError
        with a clear message so the caller can surface a 400 rather than
        letting an opaque 500 escape at first HTTP call.
      * For ollama, the `base_url` and `keep_alive` settings are inherited
        from the default llm config if present (otherwise the defaults
        baked into `OllamaClient.__init__` apply).
      * `max_tokens` / `temperature` are inherited from the default llm
        config so per-session brains match the cost/behaviour envelope of
        the default brain. This also lets `chat_interface.system_prompt`
        reasoning stay consistent across providers.

    The returned client is short-lived (build per request). That's fine
    for cloud providers (stateless HTTP) and for ollama (requests
    wrapper); no hidden resources to tear down.
    """
    cls = _PROVIDERS.get(provider)
    if not cls:
        raise ValueError(
            f"Unknown LLM provider: {provider!r}. Supported: {sorted(_PROVIDERS.keys())}"
        )
    if not model or not isinstance(model, str):
        raise ValueError("build_brain() requires a non-empty model string")

    # Inherit knobs from the default llm config so per-session clients
    # have sensible defaults without forcing the caller to repeat them.
    default_cfg = get("llm") or {}
    cfg: dict = {
        "provider": provider,
        "model": model,
        "max_tokens": default_cfg.get("max_tokens", 1000),
        "temperature": default_cfg.get("temperature", 0.3),
        "tool_use": default_cfg.get("tool_use", True),
    }

    env_name = _PROVIDER_API_KEY_ENV.get(provider)
    if env_name:
        key = os.environ.get(env_name, "")
        if not key:
            raise ValueError(
                f"Missing API key: set {env_name} to use provider {provider!r}."
            )
        cfg["api_key"] = key
    if provider == "ollama":
        # Preserve user-configured ollama endpoint / keep_alive if any.
        if default_cfg.get("provider") == "ollama":
            for k in ("base_url", "keep_alive"):
                if k in default_cfg:
                    cfg[k] = default_cfg[k]

    return cls(cfg)

def get_classifier() -> LLMClient:
    """Get the classifier LLM client — singleton."""
    global _classifier
    if _classifier is None:
        cfg = get("classifier")
        cls = _PROVIDERS.get(cfg["provider"])
        if not cls:
            raise ValueError(f"Unknown classifier provider: {cfg['provider']}")
        _classifier = cls(cfg)
    return _classifier
