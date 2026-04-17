"""
TAILOR — LLM-agnostic client.

Tier 1 (tool use): LLM decides what to search. Supports multi-step tool calls.
Tier 2 (prompt-based): Bot pre-fetches context, passes it in the prompt.

Supported providers: anthropic, openai, google, ollama
"""

import os, json, requests
from lib.config import get

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
