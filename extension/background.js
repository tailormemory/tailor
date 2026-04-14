/**
 * TAILOR Chrome Extension — Background Service Worker
 * Receives messages from content scripts and sends them to TAILOR API.
 */

async function sendToTailor(messages, source, conversationTitle, conversationId) {
  const config = await chrome.storage.sync.get(["tailorUrl", "tailorToken", "enabled"]);
  if (!config.enabled || !config.tailorUrl) return { ok: false, error: "Not configured" };

  const url = config.tailorUrl.replace(/\/+$/, "") + "/api/ingest-live";
  const headers = { "Content-Type": "application/json" };
  if (config.tailorToken) {
    headers["Authorization"] = "Bearer " + config.tailorToken;
  }

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify({
        source,
        title: conversationTitle || "Untitled conversation",
        messages,
        conversation_id: conversationId || "",
        timestamp: new Date().toISOString(),
      }),
    });
    const data = await resp.json();
    if (resp.ok) {
      return { ok: true, chunks: data.chunks || 0, updated: data.updated || false };
    }
    return { ok: false, error: data.error || resp.statusText };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.type === "tailor_ingest") {
    sendToTailor(msg.messages, msg.source, msg.title, msg.conversation_id).then(sendResponse);
    return true;
  }
  if (msg.type === "tailor_status") {
    chrome.storage.sync.get(["tailorUrl", "tailorToken", "enabled"], (config) => {
      sendResponse({
        configured: !!config.tailorUrl,
        enabled: !!config.enabled,
        url: config.tailorUrl || "",
      });
    });
    return true;
  }
});
