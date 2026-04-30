"""
debug_options.py
1. Prints ALL 64 clerk dropdown option values so we can map the missing ones
2. Follows links on HCPA Maps-Data page one level deeper to find the bulk DBF
Run: python scraper/debug_options.py
"""
import asyncio
import re
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CLERK_URL          = "https://publicaccess.hillsclerk.com/oripublicaccess/"
HCPA_MAPS_DATA_URL = "https://hcpafl.org/Downloads/Maps-Data"

async def main():
    Path("data").mkdir(exist_ok=True)
    lines = []
    def log(s=""):
        print(s)
        lines.append(s)

    # ── 1. All clerk dropdown options ─────────────────────────────────────────
    log("=" * 60)
    log("ALL CLERK DROPDOWN OPTIONS")
    log("=" * 60)
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True, args=["--no-sandbox","--disable-dev-shm-usage"]
        )
        page = await browser.new_page()
        await page.goto(CLERK_URL, wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(2_000)

        options = await page.evaluate("""
            () => {
                const sel = document.getElementById('OBKey__1285_1');
                if (!sel) return [];
                return [...sel.options].map(o => ({
                    value: o.value,
                    text:  o.text.trim()
                }));
            }
        """)
        for o in options:
            log(f"  value={o['value']!r}")

        await browser.close()

    # ── 2. HCPA Maps-Data — follow every link one level deep ─────────────────
    log("")
    log("=" * 60)
    log("HCPA MAPS-DATA — DEEP LINK SCAN")
    log("=" * 60)
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(HCPA_MAPS_DATA_URL, headers=headers, timeout=20, verify=False)
        soup = BeautifulSoup(r.text, "lxml")
        all_links = [
            (a.get_text(" ", strip=True), a["href"])
            for a in soup.find_all("a", href=True)
        ]
        log(f"\nAll links on Maps-Data page ({len(all_links)} total):")
        for text, href in all_links:
            full = href if href.startswith("http") else "https://hcpafl.org" + href
            log(f"  [{text[:60]}]  {full}")

        # Follow every hcpafl.org link one level deeper and look for file links
        log("\n── Following each link for file downloads ──")
        visited = set()
        for text, href in all_links:
            full = href if href.startswith("http") else "https://hcpafl.org" + href
            if full in visited or "hcpafl.org" not in full:
                continue
            visited.add(full)
            try:
                r2 = requests.get(full, headers=headers, timeout=15, verify=False)
                if r2.status_code != 200:
                    continue
                soup2 = BeautifulSoup(r2.text, "lxml")
                for a2 in soup2.find_all("a", href=True):
                    h2 = a2["href"]
                    t2 = a2.get_text(" ", strip=True)
                    if re.search(r"\.(zip|dbf|csv|xlsx?)\b|LinkClick|NAL|parcel|bulk", h2, re.I) or \
                       re.search(r"NAL|parcel|bulk|download|DBF|name.?address", t2, re.I):
                        full2 = h2 if h2.startswith("http") else "https://hcpafl.org" + h2
                        log(f"  FOUND on {full}:")
                        log(f"    [{t2[:80]}]  {full2}")
                        # Test if it's a real downloadable file
                        try:
                            head = requests.head(full2, headers=headers, timeout=10,
                                                 verify=False, allow_redirects=True)
                            cl = head.headers.get("Content-Length","?")
                            ct = head.headers.get("Content-Type","?")
                            log(f"    → status={head.status_code} size={cl} type={ct[:50]}")
                        except Exception as e:
                            log(f"    → HEAD error: {e}")
            except Exception as e:
                log(f"  Error fetching {full}: {e}")

    except Exception as e:
        log(f"Error: {e}")

    out = "\n".join(lines)
    Path("data/debug_options.txt").write_text(out, encoding="utf-8")
    log("\nSaved → data/debug_options.txt")

if __name__ == "__main__":
    asyncio.run(main())
