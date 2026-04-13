const urlInput = document.getElementById("url");
const tokenInput = document.getElementById("token");
const enabledInput = document.getElementById("enabled");
const saveBtn = document.getElementById("save");
const statusEl = document.getElementById("status");

chrome.storage.sync.get(["tailorUrl", "tailorToken", "enabled"], (config) => {
  urlInput.value = config.tailorUrl || "";
  tokenInput.value = config.tailorToken || "";
  enabledInput.checked = !!config.enabled;
});

function setStatus(text, type) {
  statusEl.textContent = text;
  statusEl.className = "status " + type;
}

saveBtn.addEventListener("click", async () => {
  const url = urlInput.value.trim().replace(/\/+$/, "");
  const token = tokenInput.value.trim();
  const enabled = enabledInput.checked;

  if (!url) { setStatus("Please enter your TAILOR server URL", "error"); return; }

  chrome.storage.sync.set({ tailorUrl: url, tailorToken: token, enabled }, () => {
    setStatus("Saved. Testing connection...", "info");
  });

  try {
    const headers = { "Content-Type": "application/json" };
    if (token) headers["Authorization"] = "Bearer " + token;
    const resp = await fetch(url + "/api/dashboard/stats", { headers });
    if (resp.ok) {
      const data = await resp.json();
      const chunks = data.chunks?.total || 0;
      setStatus("Connected. KB has " + chunks.toLocaleString() + " chunks.", "ok");
    } else if (resp.status === 401 || resp.status === 403) {
      setStatus("Invalid token", "error");
    } else {
      setStatus("Server error: " + resp.status, "error");
    }
  } catch (e) {
    setStatus(e.message.includes("Failed to fetch") ? "Cannot reach server" : "Error: " + e.message, "error");
  }
});
