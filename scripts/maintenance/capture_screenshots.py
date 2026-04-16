#!/usr/bin/env python3
"""
Capture TAILOR dashboard screenshots for README.
Run from terminal (not via MCP):
    .venv/bin/python3 scripts/maintenance/capture_screenshots.py
"""
import asyncio
import os
import sys
from playwright.async_api import async_playwright

TOKEN = os.environ.get("TAILOR_API_KEY")
if not TOKEN:
    # try to read from plist
    import subprocess
    try:
        out = subprocess.check_output(
            ["grep", "-A1", "TAILOR_API_KEY", "/Library/LaunchDaemons/com.tailor.mcp.plist"],
            text=True,
        )
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("<string>") and line.endswith("</string>"):
                TOKEN = line.replace("<string>", "").replace("</string>", "")
                break
    except Exception:
        pass

if not TOKEN:
    print("ERROR: set TAILOR_API_KEY env var")
    sys.exit(1)

BASE = "http://localhost:8787"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "docs", "images")
os.makedirs(OUT, exist_ok=True)


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            device_scale_factor=2,
        )
        page = await context.new_page()

        # Login via API to set cookie
        print("→ Login...")
        resp = await context.request.post(
            f"{BASE}/api/auth/login",
            data={"token": TOKEN},
        )
        print(f"  status: {resp.status}")

        # Main dashboard
        print("→ Loading dashboard...")
        await page.goto(f"{BASE}/dashboard", wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(4000)

        # List all visible buttons/tabs for debugging
        print("→ Discovering navigation...")
        tabs = await page.evaluate("""
            () => {
                const els = document.querySelectorAll('button, a, [role="tab"]');
                return Array.from(els).slice(0, 80).map(e => ({
                    text: (e.innerText || '').trim().slice(0, 40),
                    tag: e.tagName,
                    role: e.getAttribute('role') || '',
                })).filter(x => x.text.length > 0 && x.text.length < 40);
            }
        """)
        for t in tabs[:30]:
            print(f"  [{t['tag']}] {t['text']}")

        # Screenshot 1: Overview (full page)
        print("→ Capturing overview...")
        await page.screenshot(path=os.path.join(OUT, "dashboard-overview.png"), full_page=True)

        # Try clicking common section names and capture
        sections = ["KB Health", "Model Advisor", "Config", "Setup Wizard", "Pipeline", "Upload", "Services"]
        for section in sections:
            try:
                btn = page.get_by_text(section, exact=False).first
                if await btn.count() > 0:
                    print(f"→ Capturing {section}...")
                    await btn.click(timeout=3000)
                    await page.wait_for_timeout(1500)
                    fn = section.lower().replace(" ", "-")
                    await page.screenshot(
                        path=os.path.join(OUT, f"dashboard-{fn}.png"),
                        full_page=True,
                    )
            except Exception as e:
                print(f"  skip {section}: {e}")

        await browser.close()
        print(f"\n✓ Screenshots saved to {OUT}")


asyncio.run(main())
