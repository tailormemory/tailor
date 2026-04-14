/**
 * TAILOR Chrome Extension — Content Script
 * Observes the DOM of AI chat platforms and captures conversations.
 * Supports: claude.ai, chatgpt.com, gemini.google.com
 */

(function () {
  "use strict";

  const hostname = window.location.hostname;
  let platform = null;
  if (hostname.includes("claude.ai")) platform = "claude";
  else if (hostname.includes("chatgpt.com") || hostname.includes("chat.openai.com")) platform = "chatgpt";
  else if (hostname.includes("gemini.google.com")) platform = "gemini";
  if (!platform) return;

  console.log(`[TAILOR] Content script loaded on ${platform}`);

  let lastTextHash = null;
  let flushTimer = null;
  const FLUSH_INTERVAL = 30000;

  function simpleHash(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) - hash + str.charCodeAt(i)) | 0;
    }
    return hash;
  }

  function getConversationId() {
    const path = window.location.pathname;
    if (platform === "claude") {
      const m = path.match(/\/chat\/([a-f0-9-]+)/);
      if (m) return "claude:" + m[1];
    }
    if (platform === "chatgpt") {
      const m = path.match(/\/c\/([a-zA-Z0-9_-]+)/);
      if (m) return "chatgpt:" + m[1];
    }
    if (platform === "gemini") {
      const m = path.match(/\/app\/([a-f0-9]+)/);
      if (m) return "gemini:" + m[1];
    }
    return "";
  }

  function extractMessages() {
    const messages = [];

    if (platform === "claude") {
      // Strategy 1: data-testid selectors (most reliable)
      const userEls = document.querySelectorAll("[data-testid='user-message']");
      if (userEls.length > 0) {
        // Walk up to find the conversation container — each turn is a sibling group
        // User messages have data-testid='user-message', assistant messages are adjacent containers
        const chatContainer = userEls[0].closest("[class*='container'], main, [role='main']")?.parentElement || document.querySelector("main") || document.body;
        // Collect all turn-level containers: find the common ancestor depth
        const turnParent = userEls[0].closest("[data-testid='user-message']")?.parentElement?.parentElement?.parentElement;
        if (turnParent?.parentElement) {
          const turnContainer = turnParent.parentElement;
          Array.from(turnContainer.children).forEach((child) => {
            const userMsg = child.querySelector("[data-testid='user-message']");
            const text = child.innerText?.trim();
            if (!text || text.length < 2) return;
            if (userMsg) {
              messages.push({ role: "user", content: userMsg.innerText?.trim() });
            } else {
              // Assistant turn — grab the text content, skip UI chrome
              const prose = child.querySelector("div[class*='grid-cols'], div[class*='prose'], div[class*='markdown']");
              const content = prose ? prose.innerText?.trim() : text;
              if (content && content.length > 5) messages.push({ role: "assistant", content });
            }
          });
        }
      }
      // Strategy 2: class-based fallback
      if (messages.length === 0) {
        document.querySelectorAll("[class*='font-claude-message'], [class*='font-user-message'], [class*='human-turn'], [class*='ai-turn']").forEach((el) => {
          const isUser = el.className.includes("user") || el.className.includes("human");
          const text = el.innerText?.trim();
          if (text && text.length > 1) messages.push({ role: isUser ? "user" : "assistant", content: text });
        });
      }
      // Strategy 3: last resort — any prose blocks in main
      if (messages.length === 0) {
        const main = document.querySelector("main");
        if (main) {
          main.querySelectorAll("div[class*='prose'], div[class*='whitespace-pre']").forEach((el) => {
            const text = el.innerText?.trim();
            if (text && text.length > 10) messages.push({ role: "unknown", content: text });
          });
        }
      }
    }

    if (platform === "chatgpt") {
      document.querySelectorAll("div[data-message-id]").forEach((el) => {
        const role = el.getAttribute("data-message-author-role");
        const text = el.innerText?.trim();
        if (text && text.length > 1 && (role === "user" || role === "assistant")) messages.push({ role, content: text });
      });
    }

    if (platform === "gemini") {
      document.querySelectorAll(".query-content, user-query, .response-content, model-response, message-content").forEach((el) => {
        const isUser = el.matches(".query-content, user-query") || el.className?.includes("user") || el.tagName?.toLowerCase() === "user-query";
        const text = el.innerText?.trim();
        if (text && text.length > 1) messages.push({ role: isUser ? "user" : "assistant", content: text });
      });
    }

    return messages;
  }

  function getConversationTitle() {
    let title = document.title || "";
    title = title.replace(/ \| Claude$/i, "").replace(/ \| ChatGPT$/i, "").replace(/ - Gemini$/i, "")
      .replace(/^ChatGPT$/i, "").replace(/^Claude$/i, "").replace(/^Gemini$/i, "").trim();
    return title || "Untitled conversation";
  }

  function flush() {
    const allMessages = extractMessages();
    if (allMessages.length === 0) return;

    // Check if conversation actually changed
    const fullText = allMessages.map(m => m.content).join("|");
    const textHash = simpleHash(fullText);
    if (textHash === lastTextHash) return;
    lastTextHash = textHash;

    const title = getConversationTitle();
    const conversationId = getConversationId();
    console.log(`[TAILOR] Flushing full conversation (${allMessages.length} msgs, ${platform}): "${title}"`);
    chrome.runtime.sendMessage({
      type: "tailor_ingest",
      source: platform,
      title: title,
      messages: allMessages,
      conversation_id: conversationId,
    }, (response) => {
      if (response?.ok) {
        console.log(`[TAILOR] Ingested ${response.chunks || 0} chunks${response.updated ? " (updated)" : ""}`);
        updateBadge("ok");
      } else {
        console.warn(`[TAILOR] Ingest failed: ${response?.error || "unknown"}`);
        updateBadge("error");
      }
    });
  }

  let badge = null;
  function createBadge() {
    badge = document.createElement("div");
    badge.id = "tailor-capture-badge";
    badge.style.cssText = "position:fixed;bottom:16px;right:16px;z-index:99999;background:#0d9488;color:white;font-size:11px;font-weight:600;padding:4px 10px;border-radius:12px;font-family:system-ui,sans-serif;opacity:0.7;pointer-events:none;transition:opacity 0.3s;";
    badge.textContent = "TAILOR \u25CF";
    document.body.appendChild(badge);
  }

  function updateBadge(status) {
    if (!badge) return;
    if (status === "ok") {
      badge.style.background = "#0d9488";
      badge.textContent = "TAILOR \u25CF";
      badge.style.opacity = "0.7";
      setTimeout(() => { if (badge) badge.style.opacity = "0.3"; }, 3000);
    } else if (status === "error") {
      badge.style.background = "#ef4444";
      badge.textContent = "TAILOR \u2715";
      badge.style.opacity = "0.9";
    } else if (status === "disabled") {
      badge.style.background = "#6b7280";
      badge.textContent = "TAILOR \u25CB";
      badge.style.opacity = "0.4";
    }
  }

  function startObserver() {
    const observer = new MutationObserver(() => {
      if (flushTimer) clearTimeout(flushTimer);
      flushTimer = setTimeout(flush, 5000);
    });
    const target = document.querySelector("main") || document.body;
    observer.observe(target, { childList: true, subtree: true, characterData: true });
    console.log(`[TAILOR] DOM observer started on ${platform}`);
  }

  async function init() {
    chrome.runtime.sendMessage({ type: "tailor_status" }, (status) => {
      if (!status?.enabled || !status?.configured) {
        console.log("[TAILOR] Extension disabled or not configured");
        createBadge();
        updateBadge("disabled");
        return;
      }
      console.log(`[TAILOR] Active on ${platform}, sending to ${status.url}`);
      createBadge();
      updateBadge("ok");
      startObserver();
      setInterval(flush, FLUSH_INTERVAL);
      window.addEventListener("beforeunload", flush);
    });
  }

  if (document.readyState === "complete") init();
  else window.addEventListener("load", init);
})();
