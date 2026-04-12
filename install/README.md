# TAILOR — Service Installation Guide

## macOS (LaunchDaemon)

The MCP server should run as a system service so it starts automatically on boot.

### Quick setup

```bash
# 1. Copy and edit the template
sudo cp install/com.tailor.mcp.plist.example /Library/LaunchDaemons/com.tailor.mcp.plist
sudo nano /Library/LaunchDaemons/com.tailor.mcp.plist
# → Update all paths and API keys

# 2. Load the service
sudo launchctl load /Library/LaunchDaemons/com.tailor.mcp.plist

# 3. Verify
curl http://localhost:8787/api/search?q=test
```

### Managing the service

```bash
# Stop
sudo launchctl unload /Library/LaunchDaemons/com.tailor.mcp.plist

# Restart (always unload first)
sudo launchctl unload /Library/LaunchDaemons/com.tailor.mcp.plist
sleep 2
sudo launchctl load /Library/LaunchDaemons/com.tailor.mcp.plist

# View logs
tail -f logs/mcp_stderr.log
```

### Optional: Telegram Bot

Copy and adapt the same pattern for `com.tailor.telegram.plist`, replacing
`mcp_server.py` with `scripts/services/telegram_bot.py`.

### Optional: Heartbeat + Reminders via cron

```bash
crontab -e
# Add:
*/5 * * * * /path/to/tailor/.venv/bin/python3 /path/to/tailor/scripts/services/heartbeat.py
* * * * *   /path/to/tailor/.venv/bin/python3 /path/to/tailor/scripts/services/reminder_checker.py
```

### Optional: Nightly pipeline

```bash
crontab -e
# Add:
30 2 * * * /path/to/tailor/scripts/maintenance/backup_db.sh
0  3 * * * /path/to/tailor/sync_and_ingest.sh
0  4 * * * /path/to/tailor/sync_email.sh
```

---

## Linux (systemd)

### Quick setup

```bash
# 1. Copy and edit the template
sudo cp install/tailor-mcp.service.example /etc/systemd/system/tailor-mcp.service
sudo nano /etc/systemd/system/tailor-mcp.service
# → Update all paths, user, and API keys

# 2. Enable and start
sudo systemctl daemon-reload
sudo systemctl enable tailor-mcp
sudo systemctl start tailor-mcp

# 3. Verify
sudo systemctl status tailor-mcp
curl http://localhost:8787/api/search?q=test
```

### Managing the service

```bash
sudo systemctl stop tailor-mcp
sudo systemctl restart tailor-mcp
journalctl -u tailor-mcp -f      # or: tail -f logs/mcp_stderr.log
```

### Optional: Nightly pipeline via cron

```bash
crontab -e
# Add:
30 2 * * * /path/to/tailor/scripts/maintenance/backup_db.sh
0  3 * * * /path/to/tailor/sync_and_ingest.sh
0  4 * * * /path/to/tailor/sync_email.sh
*/5 * * * * /path/to/tailor/.venv/bin/python3 /path/to/tailor/scripts/services/heartbeat.py
* * * * *   /path/to/tailor/.venv/bin/python3 /path/to/tailor/scripts/services/reminder_checker.py
```

---

## Important notes

- **FileVault (macOS)**: If you have FileVault enabled, never `sudo reboot` remotely — 
  the machine will hang at the unlock screen. Always reboot physically.
- **API keys**: Store them in the service config (plist/systemd), not in shell profiles.
  The MCP server reads them from environment variables at startup.
- **Logs**: All services log to the `logs/` directory. The heartbeat script
  automatically restarts the MCP server if it crashes.
