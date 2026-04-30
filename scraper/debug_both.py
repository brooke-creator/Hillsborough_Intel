"""
debug_both.py
Probes:
  1. Hillsborough Clerk portal — finds the real dropdown selector + option format
  2. HCPA property appraiser — finds the bulk DBF/ZIP download URL

Run locally or as a one-off GitHub Actions step:
  python scraper/debug_both.py
Output saved to data/debug_output.txt and data/clerk_debug.html
"""
import asyncio
import re
import sys
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CLERK_URL = "https://publicaccess.hillsclerk.com/oripublicaccess/"
HCPA_BULK_CANDIDATES = [
    # Common patterns for HCPA bulk data
    "https://gis.hcpafl.org/propertysearch/data/NAL_OWNER.zip",
    "https://gis.hcpafl.org/propertysearch/data/NAL.zip",
    "https://gis.hcpafl.org/propertysearch/data/NAL_ALL.zip",
    "https://gis.hcpafl.org/propertysearch/data/CAMA.zip",
    "https://hcpafl.org/data/NAL_OWNER.zip",
    "https://hcpafl.org/data/NAL.zip",
    "https://www.hcpafl.org/LinkClick.aspx?fileticket=NAL",
]
HCPA_PAGES = [
    "https://hcpafl.org/About-HCPA/open-data",
    "https://www.hcpafl.org/About-HCPA/open-data",
    "https://gis.hcpafl.org/propertysearch/",
    "https://hcpafl.org/downloads",
    "https://www.hcpafl.org/downloads",
]

lines = []
def log(s=""):
    print(s)
    lines.append(s)


async def probe_clerk(page):
    log("=" * 60)
    log("CLERK PORTAL PROBE")
    log("=" * 60)
    log(f"Loading {CLERK_URL} ...")

    await page.goto(CLERK_URL, wait_until="networkidle", timeout=60_000)
    await page.wait_for_timeout(4_000)

    # ── All <select> elements ─────────────────────────────────────────────────
    selects = await page.evaluate("""
        () => {
            const out = [];
            document.querySelectorAll('select').forEach(sel => {
                const opts = [];
                for (const o of sel.options) {
                    opts.push({ value: o.value, text: o.text.trim() });
                }
                out.push({
                    id:        sel.id,
                    name:      sel.name,
                    className: sel.className,
                    optCount:  sel.options.length,
                    allOpts:   opts,
                });
            });
            return out;
        }
    """)

    log(f"\nFound {len(selects)} <select> elements:")
    for s in selects:
        log(f"\n  <select> id={s['id']!r} name={s['name']!r} class={s['className']!r} ({s['optCount']} options)")
        for o in s['allOpts'][:8]:
            log(f"    value={o['value']!r}  text={o['text']!r}")
        if s['optCount'] > 8:
            log(f"    ... {s['optCount'] - 8} more options")
            # Print last 3 too
            for o in s['allOpts'][-3:]:
                log(f"    value={o['value']!r}  text={o['text']!r}")

    # ── Search for LP anywhere in any option ──────────────────────────────────
    log("\n── Options containing LP / LIS PENDENS ──")
    lp = await page.evaluate("""
        () => {
            const matches = [];
            document.querySelectorAll('select option').forEach(o => {
                const t = o.text.toUpperCase();
                if (t.includes('LP') || t.includes('LIS') || t.includes('PENDENS')
                    || t.includes('FORECLOS') || t.includes('LIEN') || t.includes('JUDG')) {
                    const sel = o.closest('select');
                    matches.push({
                        selectId:    sel ? sel.id   : '',
                        selectName:  sel ? sel.name : '',
                        selectClass: sel ? sel.className : '',
                        value: o.value,
                        text:  o.text.trim(),
                    });
                }
            });
            return matches;
        }
    """)
    if lp:
        for m in lp:
            log(f"  SELECT id={m['selectId']!r} name={m['selectName']!r} class={m['selectClass']!r}")
            log(f"    option value={m['value']!r}  text={m['text']!r}")
    else:
        log("  NONE — dropdown may be Chosen.js/Select2 (check HTML file)")

    # ── Chosen.js / Select2 ───────────────────────────────────────────────────
    log("\n── Chosen / Select2 containers ──")
    chosen = await page.evaluate("""
        () => {
            const out = [];
            document.querySelectorAll(
                '.chosen-container, .select2-container, [class*="chosen"], [class*="select2"]'
            ).forEach(el => {
                out.push({ class: el.className, id: el.id, snippet: el.outerHTML.slice(0,300) });
            });
            return out;
        }
    """)
    if chosen:
        for c in chosen[:5]:
            log(f"  class={c['class']!r} id={c['id']!r}")
            log(f"  {c['snippet']}\n")
    else:
        log("  None found")

    # ── All input fields ──────────────────────────────────────────────────────
    log("\n── Input / button elements ──")
    inputs = await page.evaluate("""
        () => document.querySelectorAll('input, button').length > 0
            ? [...document.querySelectorAll('input, button')].map(el => ({
                tag:   el.tagName,
                id:    el.id,
                name:  el.name,
                type:  el.type,
                cls:   el.className,
                val:   el.value,
                ph:    el.placeholder,
              }))
            : []
    """)
    for inp in inputs:
        log(f"  <{inp['tag'].lower()}> id={inp['id']!r} name={inp['name']!r} "
            f"type={inp['type']!r} class={inp['cls']!r} value={inp['val']!r} "
            f"placeholder={inp['ph']!r}")

    # ── Page title + URL ──────────────────────────────────────────────────────
    title = await page.title()
    url   = page.url
    log(f"\nPage title: {title!r}")
    log(f"Final URL:  {url!r}")

    # ── Save full HTML ────────────────────────────────────────────────────────
    html = await page.content()
    Path("data").mkdir(exist_ok=True)
    Path("data/clerk_debug.html").write_text(html, encoding="utf-8")
    log(f"Full HTML saved → data/clerk_debug.html  ({len(html):,} bytes)")


def probe_hcpa():
    log("\n" + "=" * 60)
    log("HCPA BULK DATA PROBE")
    log("=" * 60)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    # ── Try known direct URLs ─────────────────────────────────────────────────
    log("\n── Testing direct bulk file URLs ──")
    for url in HCPA_BULK_CANDIDATES:
        try:
            r = requests.head(url, headers=headers, timeout=10, verify=False,
                              allow_redirects=True)
            ct = r.headers.get("Content-Type", "")
            cl = r.headers.get("Content-Length", "unknown")
            log(f"  {r.status_code}  {cl:>12} bytes  {ct[:40]}  {url}")
            if r.status_code == 200 and ("zip" in ct or "octet" in ct or "download" in ct):
                log(f"  *** FOUND: {url} ***")
        except Exception as e:
            log(f"  ERR  {url}  ({e})")

    # ── Scrape HCPA open-data pages for download links ────────────────────────
    log("\n── Scraping HCPA pages for download links ──")
    for page_url in HCPA_PAGES:
        try:
            r = requests.get(page_url, headers=headers, timeout=15, verify=False)
            if r.status_code != 200:
                log(f"  {r.status_code}  {page_url}")
                continue
            soup = BeautifulSoup(r.text, "lxml")
            links = soup.find_all("a", href=True)
            data_links = [
                a["href"] for a in links
                if re.search(
                    r"\.(zip|dbf|csv|txt|xlsx?)\b|NAL|parcel|bulk|download|export",
                    a["href"], re.I
                )
            ]
            if data_links:
                log(f"\n  PAGE: {page_url}")
                for lnk in data_links[:20]:
                    full = lnk if lnk.startswith("http") else "https://hcpafl.org" + lnk
                    log(f"    {full}")
            else:
                log(f"  {r.status_code}  (no data links)  {page_url}")
        except Exception as e:
            log(f"  ERR  {page_url}  ({e})")


async def main():
    Path("data").mkdir(exist_ok=True)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/New_York",
        )
        page = await ctx.new_page()

        await probe_clerk(page)
        await browser.close()

    probe_hcpa()

    # Save full output
    out = "\n".join(lines)
    Path("data/debug_output.txt").write_text(out, encoding="utf-8")
    log("\nAll output saved → data/debug_output.txt")


if __name__ == "__main__":
    asyncio.run(main())
