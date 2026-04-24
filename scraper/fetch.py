"""
Hillsborough County Motivated Seller Lead Scraper
==================================================
Portal  : https://publicaccess.hillsclerk.com/oripublicaccess/
Strategy: Click "Document Type" search → type each doc code → set date range → scrape table
Fallback: If Playwright interaction fails, try direct HTTP POST to the ASP.NET form
"""

import asyncio
import csv
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

CLERK_URL    = "https://publicaccess.hillsclerk.com/oripublicaccess/"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

DOC_TYPES = {
    "LP":       ("foreclosure",  "Lis Pendens"),
    "NOFC":     ("foreclosure",  "Notice of Foreclosure"),
    "TAXDEED":  ("tax",          "Tax Deed"),
    "JUD":      ("judgment",     "Judgment"),
    "CCJ":      ("judgment",     "Certified Judgment"),
    "DRJUD":    ("judgment",     "Domestic Judgment"),
    "LNCORPTX": ("lien",         "Corp Tax Lien"),
    "LNIRS":    ("lien",         "IRS Lien"),
    "LNFED":    ("lien",         "Federal Lien"),
    "LN":       ("lien",         "Lien"),
    "LNMECH":   ("lien",         "Mechanic Lien"),
    "LNHOA":    ("lien",         "HOA Lien"),
    "MEDLN":    ("lien",         "Medicaid Lien"),
    "PRO":      ("probate",      "Probate"),
    "NOC":      ("construction", "Notice of Commencement"),
    "RELLP":    ("release",      "Release Lis Pendens"),
}

OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
GHL_CSV_PATH = Path("data/ghl_export.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")


# ─────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _norm(s) -> str:
    return re.sub(r"\s+", " ", (s or "").upper().strip())

def _norm_date(raw: str) -> str:
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y", "%Y%m%d"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return raw.strip()

def _split_name(full: str):
    parts = _norm(full).split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return "", parts[0]
    return " ".join(parts[1:]), parts[0]


# ─────────────────────────────────────────────────────────────────────────────
# PARCEL LOOKUP
# ─────────────────────────────────────────────────────────────────────────────

def _index_parcel(rec: dict, lookup: dict):
    R = {k.upper(): (v or "") for k, v in rec.items()}
    owner = _norm(R.get("OWNER") or R.get("OWN1") or R.get("OWNERNAME") or "")
    if not owner:
        return
    entry = {
        "prop_address": _norm(R.get("SITE_ADDR")  or R.get("SITEADDR")   or ""),
        "prop_city":    _norm(R.get("SITE_CITY")  or R.get("SITECITY")   or "TAMPA"),
        "prop_state":   "FL",
        "prop_zip":     _norm(R.get("SITE_ZIP")   or R.get("SITEZIP")    or ""),
        "mail_address": _norm(R.get("ADDR_1")     or R.get("MAILADR1")   or ""),
        "mail_city":    _norm(R.get("CITY")        or R.get("MAILCITY")   or ""),
        "mail_state":   _norm(R.get("STATE")       or R.get("MAILSTATE")  or "FL"),
        "mail_zip":     _norm(R.get("ZIP")         or R.get("MAILZIP")    or ""),
    }
    parts = owner.split()
    variants = {owner}
    if len(parts) >= 2:
        variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")
        variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")
    for v in variants:
        if v:
            lookup[v] = entry

def build_parcel_lookup() -> dict:
    lookup: dict = {}
    # Try local DBF
    dbf_path = Path("data/parcels.dbf")
    if HAS_DBF and dbf_path.exists():
        log.info("Loading parcels from %s", dbf_path)
        try:
            for rec in DBF(str(dbf_path), ignore_missing_memofile=True):
                _index_parcel(dict(rec), lookup)
            log.info("Parcel lookup: %d keys", len(lookup))
            return lookup
        except Exception as e:
            log.warning("DBF load failed: %s", e)
    # Try local CSV
    csv_path = Path("data/parcels.csv")
    if csv_path.exists():
        log.info("Loading parcels from %s", csv_path)
        try:
            with open(csv_path, newline="", encoding="utf-8", errors="replace") as fh:
                for row in csv.DictReader(fh):
                    _index_parcel(row, lookup)
            log.info("Parcel lookup: %d keys", len(lookup))
            return lookup
        except Exception as e:
            log.warning("CSV load failed: %s", e)
    log.warning("No parcel data found — addresses will be empty. Place parcels.dbf or parcels.csv in /data/")
    return lookup

def match_parcel(owner: str, lookup: dict) -> dict:
    n = _norm(owner)
    if n in lookup:
        return lookup[n]
    parts = n.split()
    if len(parts) >= 2:
        for v in (
            f"{parts[-1]} {' '.join(parts[:-1])}",
            f"{parts[-1]}, {' '.join(parts[:-1])}",
        ):
            if v in lookup:
                return lookup[v]
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────────────────────────────────────

def score_record(rec: dict):
    flags, s = [], 30
    doc    = rec.get("doc_type", "")
    owner  = _norm(rec.get("owner", ""))
    amount = rec.get("amount") or 0
    filed  = rec.get("filed", "")

    if doc == "LP":
        flags.append("Lis pendens");      s += 10
    if doc in ("LP", "NOFC"):
        flags.append("Pre-foreclosure");  s += 10
    if doc in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien");    s += 10
    if doc in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
        flags.append("Tax lien");         s += 10
    if doc == "LNMECH":
        flags.append("Mechanic lien");    s += 10
    if doc == "PRO":
        flags.append("Probate / estate"); s += 10
    if re.search(r"\b(LLC|INC|CORP|LTD|TRUST|LP)\b", owner):
        flags.append("LLC / corp owner"); s += 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        s += 20
    try:
        amt = float(amount)
        if amt > 100_000:   s += 15
        elif amt > 50_000:  s += 10
    except Exception:
        pass
    try:
        if (datetime.utcnow() - datetime.strptime(filed[:10], "%Y-%m-%d")).days <= 7:
            flags.append("New this week"); s += 5
    except Exception:
        pass
    if rec.get("prop_address"):
        s += 5
    return min(s, 100), flags


# ─────────────────────────────────────────────────────────────────────────────
# HTML TABLE PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_table(html: str, doc_code: str) -> list[dict]:
    records = []
    soup = BeautifulSoup(html, "lxml")

    table = (
        soup.find("table", id=re.compile(r"result|grid|search|record|data", re.I))
        or soup.find("table", class_=re.compile(r"result|grid|search|record|data", re.I))
        or soup.find("table")
    )
    if not table:
        return records

    rows = table.find_all("tr")
    if len(rows) < 2:
        return records

    headers = [th.get_text(" ", strip=True).lower() for th in rows[0].find_all(["th", "td"])]

    def col(cells, *names):
        for name in names:
            for i, h in enumerate(headers):
                if name in h and i < len(cells):
                    t = cells[i].get_text(" ", strip=True)
                    if t:
                        return t
        return ""

    for row in rows[1:]:
        cells = row.find_all("td")
        if not cells:
            continue
        try:
            link_tag  = row.find("a", href=True)
            clerk_url = ""
            if link_tag:
                href = link_tag["href"]
                clerk_url = href if href.startswith("http") else "https://publicaccess.hillsclerk.com" + href

            doc_num  = col(cells, "instrument", "doc", "number", "rec", "book") or (link_tag.get_text(strip=True) if link_tag else "")
            filed    = col(cells, "record date", "date", "filed", "recorded")
            grantor  = col(cells, "grantor", "party 1", "name", "owner", "from")
            grantee  = col(cells, "grantee", "party 2", "to", "lender", "plaintiff")
            legal    = col(cells, "legal", "description")
            amt_raw  = col(cells, "amount", "consideration", "debt", "value")

            amount = None
            if amt_raw:
                clean = re.sub(r"[^\d.]", "", amt_raw)
                try:
                    amount = float(clean) if clean else None
                except Exception:
                    pass

            if not doc_num and not grantor:
                continue

            records.append({
                "doc_num":   doc_num,
                "doc_type":  doc_code,
                "filed":     _norm_date(filed),
                "owner":     grantor,
                "grantee":   grantee,
                "amount":    amount,
                "legal":     legal,
                "clerk_url": clerk_url,
            })
        except Exception:
            continue

    return records


# ─────────────────────────────────────────────────────────────────────────────
# PLAYWRIGHT SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

async def _scrape_one(page, doc_code: str, date_from: str, date_to: str) -> list[dict]:
    """Scrape a single document type using Playwright."""
    results = []

    for attempt in range(1, 4):
        try:
            # Fresh page load every attempt
            await page.goto(CLERK_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(3_000)

            # ── Click "Document Type" in the left Search Types list ──────────
            # The left nav is a list of clickable items
            clicked = False
            for selector in [
                "text=Document Type",
                "a:has-text('Document Type')",
                "td:has-text('Document Type')",
                "li:has-text('Document Type')",
                "span:has-text('Document Type')",
            ]:
                try:
                    await page.click(selector, timeout=8_000)
                    clicked = True
                    log.debug("[%s] clicked 'Document Type' via %s", doc_code, selector)
                    break
                except Exception:
                    continue

            if not clicked:
                raise Exception("Could not find 'Document Type' nav item")

            await page.wait_for_timeout(2_000)

            # ── Type the doc code into the document type input ───────────────
            # This field is usually a text box that filters a dropdown
            filled = False
            for selector in [
                'input[id*="docType" i]',
                'input[name*="docType" i]',
                'input[placeholder*="document" i]',
                'input[placeholder*="type" i]',
                '.chosen-search input',
                'input[type="text"]',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.is_visible(timeout=3_000):
                        await el.click()
                        await el.fill(doc_code)
                        filled = True
                        log.debug("[%s] filled doc type field via %s", doc_code, selector)
                        break
                except Exception:
                    continue

            if not filled:
                raise Exception("Could not fill document type input")

            await page.wait_for_timeout(1_000)

            # Click the matching option in any dropdown that appeared
            for selector in [
                f"li:has-text('{doc_code}')",
                f"option:has-text('{doc_code}')",
                f".chosen-results li:has-text('{doc_code}')",
                f"[data-value='{doc_code}']",
            ]:
                try:
                    await page.click(selector, timeout=3_000)
                    break
                except Exception:
                    continue

            await page.wait_for_timeout(500)

            # ── Fill date fields ─────────────────────────────────────────────
            all_inputs = page.locator('input[type="text"], input:not([type])')
            n = await all_inputs.count()

            begin_filled = end_filled = False
            for i in range(n):
                try:
                    el  = all_inputs.nth(i)
                    pid = (await el.get_attribute("id")          or "").lower()
                    pph = (await el.get_attribute("placeholder") or "").lower()
                    pnm = (await el.get_attribute("name")        or "").lower()
                    hint = pid + pph + pnm

                    if not begin_filled and any(k in hint for k in ("begin", "start", "from", "date1")):
                        await el.triple_click(); await el.fill(date_from)
                        begin_filled = True
                    elif not end_filled and any(k in hint for k in ("end", "to", "thru", "date2")):
                        await el.triple_click(); await el.fill(date_to)
                        end_filled = True
                except Exception:
                    continue

            # If we couldn't find labelled fields, use positional fallback
            if not begin_filled and n >= 1:
                await all_inputs.nth(0).triple_click()
                await all_inputs.nth(0).fill(date_from)
            if not end_filled and n >= 2:
                await all_inputs.nth(1).triple_click()
                await all_inputs.nth(1).fill(date_to)

            await page.wait_for_timeout(500)

            # ── Click Search ─────────────────────────────────────────────────
            for selector in [
                'input[value="Search"]',
                'button:has-text("Search")',
                'input[type="submit"]',
            ]:
                try:
                    await page.click(selector, timeout=8_000)
                    break
                except Exception:
                    continue

            await page.wait_for_load_state("domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(2_000)

            # ── Collect paginated results ────────────────────────────────────
            page_num = 1
            while True:
                html  = await page.content()
                rows  = _parse_table(html, doc_code)
                results.extend(rows)
                log.debug("[%s] page %d → %d rows", doc_code, page_num, len(rows))

                soup      = BeautifulSoup(html, "lxml")
                next_link = soup.find("a", string=re.compile(r"^\s*(Next|>>)\s*$", re.I))
                if not next_link:
                    break
                try:
                    await page.click("a:has-text('Next')", timeout=8_000)
                    await page.wait_for_load_state("domcontentloaded", timeout=20_000)
                    await page.wait_for_timeout(1_000)
                    page_num += 1
                except Exception:
                    break

            log.info("[%s] %d records", doc_code, len(results))
            return results

        except PWTimeout:
            log.warning("[%s] timeout — attempt %d/3", doc_code, attempt)
        except Exception as exc:
            log.warning("[%s] error attempt %d: %s", doc_code, attempt, exc)

        await asyncio.sleep(3)

    log.warning("[%s] all attempts failed — 0 records", doc_code)
    return []


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    date_to_dt    = datetime.utcnow()
    date_from_dt  = date_to_dt - timedelta(days=LOOKBACK_DAYS)
    date_from_str = date_from_dt.strftime("%m/%d/%Y")
    date_to_str   = date_to_dt.strftime("%m/%d/%Y")

    log.info("=" * 64)
    log.info("Hillsborough County Motivated Seller Scraper  v3")
    log.info("Range  : %s  →  %s  (%d days)", date_from_str, date_to_str, LOOKBACK_DAYS)
    log.info("Portal : %s", CLERK_URL)
    log.info("=" * 64)

    # Build parcel lookup (optional — enriches with addresses)
    parcel_lookup = build_parcel_lookup()

    all_records: list[dict] = []

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

        # Take a screenshot of the portal on first load for debugging
        try:
            await page.goto(CLERK_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(3_000)
            Path("data").mkdir(exist_ok=True)
            await page.screenshot(path="data/portal_screenshot.png", full_page=True)
            log.info("Portal screenshot saved → data/portal_screenshot.png")
        except Exception as e:
            log.warning("Could not screenshot portal: %s", e)

        for doc_code, (cat, cat_label) in DOC_TYPES.items():
            log.info("── Fetching [%s] %s", doc_code, cat_label)
            raw = await _scrape_one(page, doc_code, date_from_str, date_to_str)

            for r in raw:
                parcel = match_parcel(r.get("owner", ""), parcel_lookup)
                score, flags = score_record({**r, "cat": cat, **parcel})
                all_records.append({
                    "doc_num":      r.get("doc_num",   ""),
                    "doc_type":     doc_code,
                    "filed":        r.get("filed",     ""),
                    "cat":          cat,
                    "cat_label":    cat_label,
                    "owner":        r.get("owner",     ""),
                    "grantee":      r.get("grantee",   ""),
                    "amount":       r.get("amount"),
                    "legal":        r.get("legal",     ""),
                    "prop_address": parcel.get("prop_address", ""),
                    "prop_city":    parcel.get("prop_city",    ""),
                    "prop_state":   "FL",
                    "prop_zip":     parcel.get("prop_zip",     ""),
                    "mail_address": parcel.get("mail_address", ""),
                    "mail_city":    parcel.get("mail_city",    ""),
                    "mail_state":   parcel.get("mail_state",   "FL"),
                    "mail_zip":     parcel.get("mail_zip",     ""),
                    "clerk_url":    r.get("clerk_url", ""),
                    "flags":        flags,
                    "score":        score,
                })

        await browser.close()

    # Sort highest score first
    all_records.sort(key=lambda x: x["score"], reverse=True)
    with_addr = sum(1 for r in all_records if r.get("prop_address"))

    payload = {
        "fetched_at":    datetime.utcnow().isoformat() + "Z",
        "source":        "Hillsborough County Clerk of Courts",
        "date_range":    {"from": date_from_str, "to": date_to_str},
        "lookback_days": LOOKBACK_DAYS,
        "total":         len(all_records),
        "with_address":  with_addr,
        "records":       all_records,
    }

    # ── Save JSON outputs ─────────────────────────────────────────────────────
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        log.info("Saved → %s  (%d records)", path, len(all_records))

    # ── GHL CSV export ────────────────────────────────────────────────────────
    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    GHL_HEADERS = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    with open(GHL_CSV_PATH, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=GHL_HEADERS, extrasaction="ignore")
        w.writeheader()
        for r in all_records:
            first, last = _split_name(r.get("owner", ""))
            w.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city",    ""),
                "Mailing State":          r.get("mail_state",   "FL"),
                "Mailing Zip":            r.get("mail_zip",     ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city",    ""),
                "Property State":         r.get("prop_state",   "FL"),
                "Property Zip":           r.get("prop_zip",     ""),
                "Lead Type":              r.get("cat_label",    ""),
                "Document Type":          r.get("doc_type",     ""),
                "Date Filed":             r.get("filed",        ""),
                "Document Number":        r.get("doc_num",      ""),
                "Amount/Debt Owed":       r.get("amount",       ""),
                "Seller Score":           r.get("score",        0),
                "Motivated Seller Flags": " | ".join(r.get("flags", [])),
                "Source":                 "Hillsborough County Clerk",
                "Public Records URL":     r.get("clerk_url",    ""),
            })
    log.info("GHL CSV → %s  (%d rows)", GHL_CSV_PATH, len(all_records))

    log.info("=" * 64)
    log.info("COMPLETE — %d total leads  |  %d with address", len(all_records), with_addr)
    log.info("=" * 64)


if __name__ == "__main__":
    asyncio.run(main())
