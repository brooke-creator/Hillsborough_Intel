"""
Hillsborough County Motivated Seller Lead Scraper v29
Fix: Show grantee (homeowner) as owner for LP/foreclosure records
Fix: Use Playwright to fetch parcel pages (JS rendered)
"""

import asyncio
import base64
import csv
import json
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import urllib3
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

CLERK_URL  = "https://publicaccess.hillsclerk.com/oripublicaccess/"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
ENRICH_MIN_SCORE = 70

ALGOLIA_APP_ID  = "0LWZO52LS2"
ALGOLIA_API_KEY = "c0745578b56854a1b90ed57b63fbf0ba"
ALGOLIA_URL     = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/*/queries"
COUNTY_TAXES    = "https://county-taxes.net/hillsborough/property-tax"

TARGET_TYPES = {
    "LP","NOFC","TAXDEED","JUD","CCJ","DRJUD",
    "LNCORPTX","LNIRS","LNFED","LN","LNMECH",
    "LNHOA","MEDLN","PRO","NOC","RELLP"
}

DOC_TYPE_MAP = {
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

# For these doc types grantee = homeowner, grantor = institution filing claim
GRANTEE_IS_OWNER = {"LP", "NOFC", "TAXDEED", "LNHOA", "LNMECH", "LNCORPTX", "LNIRS", "LNFED", "MEDLN"}

OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV_PATH = Path("data/ghl_export.csv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("scraper")


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
    if not parts: return "", ""
    if len(parts) == 1: return "", parts[0]
    return " ".join(parts[1:]), parts[0]

def _is_institution(name: str) -> bool:
    n = _norm(name)
    keywords = ["BANK", "MORTGAGE", "LOAN", "LENDING", "FINANCIAL",
                "ASSOCIATION", "HOA", "LLC", "INC", "CORP", "SERVICES",
                "NATIONAL", "FEDERAL", "PENNYMAC", "NEWREZ", "ROCKET",
                "SHELLPOINT", "CARRINGTON", "LAKEVIEW", "REGIONS", "TRUIST",
                "WELLS FARGO", "MIDFIRST", "FLAGSTAR", "PLANET HOME",
                "CROSSCOUNTRY", "NATIONSTAR", "HABITAT", "CLICK N",
                "GUARDIAN", "TRUSTEE", "AS TRUSTEE", "SUCCESSOR",
                "FREDDIE MAC", "FANNIE MAE", "DEUTSCHE", "JPMORGAN",
                "CITIMORTGAGE", "OCWEN", "PHH", "SPS ", "BSI "]
    return any(k in n for k in keywords)

def _get_display_owner(rec: dict) -> str:
    """Get the person to display as owner — homeowner not the filing institution."""
    doc_type = rec.get("doc_type","")
    owner    = rec.get("owner","")
    grantee  = rec.get("grantee","")
    if doc_type in GRANTEE_IS_OWNER and grantee and not _is_institution(grantee):
        return grantee
    if _is_institution(owner) and grantee and not _is_institution(grantee):
        return grantee
    return owner

def score_record(rec: dict):
    flags, s = [], 30
    doc   = rec.get("doc_type", "")
    owner = _norm(rec.get("owner", ""))
    amount = rec.get("amount") or 0
    filed  = rec.get("filed", "")
    if doc == "LP":                                   flags.append("Lis pendens");      s += 10
    if doc in ("LP","NOFC"):                          flags.append("Pre-foreclosure");  s += 10
    if doc in ("JUD","CCJ","DRJUD"):                  flags.append("Judgment lien");    s += 10
    if doc in ("LNCORPTX","LNIRS","LNFED","TAXDEED"): flags.append("Tax lien");         s += 10
    if doc == "LNMECH":                               flags.append("Mechanic lien");    s += 10
    if doc == "PRO":                                  flags.append("Probate / estate"); s += 10
    if re.search(r"\b(LLC|INC|CORP|LTD|TRUST|LP)\b", owner): flags.append("LLC / corp owner"); s += 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags: s += 20
    try:
        amt = float(amount)
        if amt > 100_000: s += 15
        elif amt > 50_000: s += 10
    except Exception: pass
    try:
        if (datetime.utcnow() - datetime.strptime(filed[:10], "%Y-%m-%d")).days <= 7:
            flags.append("New this week"); s += 5
    except Exception: pass
    if rec.get("prop_address"): s += 5
    return min(s, 100), flags

def _parse_doc_type(raw: str) -> str:
    m = re.match(r"\(([A-Z]+)\)", raw.strip())
    return m.group(1) if m else raw.strip().upper()

def _parse_html(html: str) -> list[dict]:
    records = []
    soup = BeautifulSoup(html, "lxml")
    all_tables = soup.find_all("table")
    for table in all_tables:
        rows = table.find_all("tr")
        if len(rows) < 2: continue
        headers = [th.get_text(" ", strip=True).upper() for th in rows[0].find_all(["th","td"])]
        has_name = any("NAME" in h for h in headers)
        has_doc  = any("DOC" in h or "TYPE" in h for h in headers)
        if not has_name and not has_doc: continue
        def col(cells, *names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h and i < len(cells):
                        t = cells[i].get_text(" ", strip=True)
                        if t: return t
            return ""
        table_records = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells: continue
            try:
                link_tag  = row.find("a", href=True)
                clerk_url = ""
                if link_tag:
                    href = link_tag["href"]
                    clerk_url = href if href.startswith("http") else "https://publicaccess.hillsclerk.com" + href
                doc_type_raw = col(cells, "DOC TYPE","TYPE","DOCUMENT TYPE","DOCTYPE")
                doc_code     = _parse_doc_type(doc_type_raw)
                doc_num  = col(cells, "INSTRUMENT #","INST #","INST","INSTRUMENT","DOC #") or (link_tag.get_text(strip=True) if link_tag else "")
                filed    = col(cells, "RECORDING DATE","RECORD DATE","DATE RECORDED","DATE","FILED")
                grantor  = col(cells, "NAME","GRANTOR","PARTY 1","OWNER")
                grantee  = col(cells, "CROSS-PARTY NAME","CROSS PARTY","CROSS-PARTY","GRANTEE","PARTY 2")
                legal    = col(cells, "LEGAL DESCRIPTION","LEGAL","DESCRIPTION")
                if not grantor and not doc_num: continue
                table_records.append({
                    "doc_num": doc_num, "doc_type": doc_code,
                    "filed": _norm_date(filed), "owner": grantor,
                    "grantee": grantee, "amount": None,
                    "legal": legal, "clerk_url": clerk_url,
                })
            except Exception: continue
        if table_records:
            records.extend(table_records)
            break
    return records


# ─────────────────────────────────────────────────────────────────────────────
# ADDRESS ENRICHMENT
# ─────────────────────────────────────────────────────────────────────────────

def make_parcel_url(public_url: str) -> str:
    try:
        m = re.search(r"parcel=([a-f0-9\-]+)", public_url)
        if not m: return ""
        parcel_uuid = m.group(1)
        raw = f"hillsborough:real_estate:parents:{parcel_uuid}"
        encoded = base64.b64encode(raw.encode()).decode()
        return f"{COUNTY_TAXES}/{encoded}"
    except Exception:
        return ""

def algolia_search(query: str) -> list[dict]:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Content-Type": "application/json",
            "X-Algolia-Application-Id": ALGOLIA_APP_ID,
            "X-Algolia-API-Key": ALGOLIA_API_KEY,
            "Referer": "https://county-taxes.net/",
            "Origin": "https://county-taxes.net",
        }
        payload = {
            "requests": [{
                "indexName": "fl-hillsborough.property_tax",
                "query": query,
                "params": "hitsPerPage=5&page=0"
            }]
        }
        r = requests.post(ALGOLIA_URL, headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            return r.json().get("results", [{}])[0].get("hits", [])
    except Exception as e:
        log.debug("Algolia error: %s", e)
    return []

def get_public_url_from_hit(hit: dict) -> str:
    custom = hit.get("custom_parameters", {})
    if custom.get("public_url"):
        return custom["public_url"]
    for group in hit.get("child_groups", []):
        for child in group.get("children", []):
            cp = child.get("custom_parameters", {})
            if cp.get("public_url"):
                return cp["public_url"]
    return ""

async def fetch_situs_playwright(page, parcel_url: str) -> dict:
    """Use Playwright to fetch JS-rendered parcel page and extract situs."""
    empty = {
        "prop_address":"","prop_city":"","prop_state":"FL","prop_zip":"",
        "mail_address":"","mail_city":"","mail_state":"FL","mail_zip":""
    }
    try:
        await page.goto(parcel_url, wait_until="networkidle", timeout=30_000)
        await page.wait_for_timeout(2_000)

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text("\n", strip=True)
        lines = [l.strip() for l in text.split("\n") if l.strip()]

        situs_addr = situs_city = situs_zip = ""

        for i, line in enumerate(lines):
            if line.strip() in ("Situs:", "Situs", "Site Address:", "Property Address:"):
                if i + 1 < len(lines):
                    situs_addr = lines[i+1].strip()
                if i + 2 < len(lines):
                    m = re.match(r"([A-Z\s]+)\s+(\d{5}(?:-\d{4})?)", lines[i+2].upper())
                    if m:
                        situs_city = m.group(1).strip()
                        situs_zip  = m.group(2).strip()
                    else:
                        situs_city = lines[i+2].strip()
                break

        # Also check for "Owner:" and "Situs:" pattern
        full_text = " ".join(lines)
        if not situs_addr:
            m = re.search(r"Situs:\s*([0-9][^\n]+?)(?:\s+(?:Tampa|Brandon|Riverview|Plant City|Lutz|Wesley Chapel|Valrico|Wimauma|Sun City|Apollo Beach|Ruskin|Gibsonton|Lithia|Odessa|Citrus Park|Town N Country))", full_text, re.I)
            if m:
                situs_addr = m.group(1).strip()

        if not situs_addr:
            log.debug("No situs in page. Lines: %s", lines[:15])
            return empty

        log.info("✓ Situs: %s, %s %s", situs_addr, situs_city, situs_zip)
        return {
            "prop_address": situs_addr.upper(),
            "prop_city":    (situs_city or "TAMPA").upper(),
            "prop_state":   "FL",
            "prop_zip":     situs_zip,
            "mail_address": situs_addr.upper(),
            "mail_city":    (situs_city or "TAMPA").upper(),
            "mail_state":   "FL",
            "mail_zip":     situs_zip,
        }
    except Exception as e:
        log.debug("Playwright parcel fetch error: %s", e)
        return empty


async def enrich_address(page, rec: dict) -> dict:
    """Look up address: Algolia → parcel URL → Playwright scrape."""
    empty = {
        "prop_address":"","prop_city":"","prop_state":"FL","prop_zip":"",
        "mail_address":"","mail_city":"","mail_state":"FL","mail_zip":""
    }

    lookup_name = _get_display_owner(rec)
    if not lookup_name: return empty

    lookup_norm = _norm(lookup_name)
    parts = lookup_norm.split()
    if not parts: return empty

    queries = [" ".join(parts[:3]), " ".join(parts[:2]), parts[0]]

    for query in queries:
        hits = algolia_search(query)
        if not hits: continue

        best_hit = None
        for hit in hits:
            if any(p in _norm(json.dumps(hit)) for p in parts[:2]):
                best_hit = hit
                break
        if not best_hit:
            best_hit = hits[0]

        public_url = get_public_url_from_hit(best_hit)
        if not public_url: continue

        parcel_url = make_parcel_url(public_url)
        if not parcel_url: continue

        addr = await fetch_situs_playwright(page, parcel_url)
        if addr.get("prop_address"):
            return addr

    return empty


# ─────────────────────────────────────────────────────────────────────────────
# CLERK SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_one_doc_type(page, doc_code: str, date_from: str, date_to: str) -> list[dict]:
    results = []
    for attempt in range(1, 4):
        try:
            await page.goto(CLERK_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(3_000)

            await page.click('#ORI-Document\\ Type', timeout=10_000)
            await page.wait_for_timeout(2_000)

            selected = await page.evaluate(f"""
                () => {{
                    const sel = document.querySelector('select.doc-type, select.for-chosen, select[class*="doc-type"], select[id*="OBKey"]');
                    if (!sel) return 'no select found';
                    let found = null;
                    for (const opt of sel.options) {{
                        if (opt.text.includes('({doc_code})')) {{ found = opt; break; }}
                    }}
                    if (!found) return 'option not found for {doc_code}';
                    sel.value = found.value;
                    sel.dispatchEvent(new Event('change', {{bubbles: true}}));
                    if (window.jQuery) {{
                        window.jQuery(sel).val(found.value).trigger('change').trigger('chosen:updated');
                    }}
                    return 'selected: ' + found.text;
                }}
            """)
            log.info("[%s] %s", doc_code, selected)
            await page.wait_for_timeout(1_000)
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)

            await page.evaluate(f"""
                () => {{
                    const begins = document.querySelectorAll('input.record-begin, input[class*="record-begin"]');
                    const ends   = document.querySelectorAll('input.record-end, input[class*="record-end"]');
                    if (begins.length > 0) {{
                        begins[0].value = '{date_from}';
                        begins[0].dispatchEvent(new Event('change', {{bubbles: true}}));
                    }}
                    if (ends.length > 0) {{
                        ends[0].value = '{date_to}';
                        ends[0].dispatchEvent(new Event('change', {{bubbles: true}}));
                    }}
                }}
            """)
            await page.wait_for_timeout(500)
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(300)

            await page.evaluate("""
                () => {
                    const btns = document.querySelectorAll('input[value="Search"], button');
                    for (const btn of btns) {
                        if (btn.value === 'Search' || btn.textContent.trim() === 'Search') {
                            btn.click(); return;
                        }
                    }
                }
            """)
            log.info("[%s] clicked Search", doc_code)

            await page.wait_for_load_state("domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(4_000)

            page_num = 1
            while True:
                html  = await page.content()
                rows  = _parse_html(html)
                results.extend(rows)
                log.info("[%s] page %d: %d rows (total: %d)", doc_code, page_num, len(rows), len(results))
                soup      = BeautifulSoup(html, "lxml")
                next_link = soup.find("a", string=re.compile(r"^\s*(Next|>>)\s*$", re.I))
                if not next_link: break
                try:
                    await page.click("a:has-text('Next')", timeout=8_000)
                    await page.wait_for_load_state("domcontentloaded", timeout=20_000)
                    await page.wait_for_timeout(1_000)
                    page_num += 1
                except Exception: break

            log.info("[%s] DONE: %d records", doc_code, len(results))
            return results

        except PWTimeout:
            log.warning("[%s] timeout attempt %d/3", doc_code, attempt)
        except Exception as exc:
            log.warning("[%s] error attempt %d: %s", doc_code, attempt, exc)
        await asyncio.sleep(3)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    date_to_dt    = datetime.utcnow()
    date_from_dt  = date_to_dt - timedelta(days=LOOKBACK_DAYS)
    date_from_str = date_from_dt.strftime("%m/%d/%Y")
    date_to_str   = date_to_dt.strftime("%m/%d/%Y")

    log.info("=" * 64)
    log.info("Hillsborough County Motivated Seller Scraper  v29")
    log.info("Range  : %s  →  %s  (%d days)", date_from_str, date_to_str, LOOKBACK_DAYS)
    log.info("=" * 64)

    all_records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York",
            accept_downloads=True,
        )
        clerk_page = await ctx.new_page()
        Path("data").mkdir(exist_ok=True)

        # ── Step 1: Scrape clerk ──────────────────────────────────────────────
        for doc_code, (cat, cat_label) in DOC_TYPE_MAP.items():
            log.info("── Fetching [%s] %s", doc_code, cat_label)
            raw = await scrape_one_doc_type(clerk_page, doc_code, date_from_str, date_to_str)
            for r in raw:
                doc_type = r.get("doc_type","").upper()
                if doc_type not in TARGET_TYPES: continue

                # ── KEY FIX: display_owner = homeowner not institution ────────
                grantor  = r.get("owner","")
                grantee  = r.get("grantee","")
                display_owner = grantee if (
                    doc_type in GRANTEE_IS_OWNER and
                    grantee and
                    not _is_institution(grantee)
                ) else grantor
                if _is_institution(grantor) and grantee and not _is_institution(grantee):
                    display_owner = grantee

                score, flags = score_record({**r, "doc_type": doc_type, "cat": cat, "owner": display_owner})
                all_records.append({
                    "doc_num":    r.get("doc_num",""),
                    "doc_type":   doc_type,
                    "filed":      r.get("filed",""),
                    "cat":        cat,
                    "cat_label":  cat_label,
                    "owner":      display_owner,      # homeowner
                    "filer":      grantor,            # institution that filed
                    "grantee":    grantee,
                    "amount":     r.get("amount"),
                    "legal":      r.get("legal",""),
                    "prop_address": "", "prop_city": "", "prop_state": "FL", "prop_zip": "",
                    "mail_address": "", "mail_city": "", "mail_state": "FL", "mail_zip": "",
                    "clerk_url":  r.get("clerk_url",""),
                    "flags":      flags,
                    "score":      score,
                })

        # ── Step 2: Enrich with Playwright ────────────────────────────────────
        to_enrich = [r for r in all_records if r["score"] >= ENRICH_MIN_SCORE and r.get("owner")]
        log.info("Enriching %d high-score leads via Playwright...", len(to_enrich))

        parcel_page = await ctx.new_page()

        # Test first 3
        log.info("--- Testing enrichment ---")
        for rec in to_enrich[:3]:
            log.info("Lookup: [%s] owner=%s", rec["doc_type"], rec["owner"][:50])
            addr = await enrich_address(parcel_page, rec)
            log.info("Result: %s", addr)

        enriched = 0
        for i, rec in enumerate(to_enrich):
            try:
                addr = await enrich_address(parcel_page, rec)
                if addr.get("prop_address") or addr.get("mail_address"):
                    rec.update(addr)
                    score, flags = score_record(rec)
                    rec["score"] = score
                    rec["flags"] = flags
                    enriched += 1
                    log.info("✓ [%d] %s → %s", i+1, rec["owner"][:40], addr["prop_address"])
                if (i + 1) % 10 == 0:
                    log.info("Progress: %d/%d, %d got addresses", i+1, len(to_enrich), enriched)
                await asyncio.sleep(0.5)
            except Exception as e:
                log.debug("Enrich error: %s", e)

        log.info("Enrichment complete: %d/%d got addresses", enriched, len(to_enrich))
        await browser.close()

    all_records.sort(key=lambda x: x["score"], reverse=True)
    with_addr = sum(1 for r in all_records if r.get("prop_address"))

    payload = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "Hillsborough County Clerk of Courts",
        "date_range": {"from": date_from_str, "to": date_to_str},
        "lookback_days": LOOKBACK_DAYS,
        "total": len(all_records),
        "with_address": with_addr,
        "records": all_records,
    }

    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        log.info("Saved → %s  (%d records, %d with address)", path, len(all_records), with_addr)

    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    GHL_HEADERS = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip","Property Address","Property City","Property State","Property Zip","Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed","Seller Score","Motivated Seller Flags","Filer","Source","Public Records URL"]
    with open(GHL_CSV_PATH, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=GHL_HEADERS, extrasaction="ignore")
        w.writeheader()
        for r in all_records:
            first, last = _split_name(r.get("owner",""))
            w.writerow({
                "First Name": first, "Last Name": last,
                "Mailing Address": r.get("mail_address",""),
                "Mailing City": r.get("mail_city",""),
                "Mailing State": r.get("mail_state","FL"),
                "Mailing Zip": r.get("mail_zip",""),
                "Property Address": r.get("prop_address",""),
                "Property City": r.get("prop_city",""),
                "Property State": r.get("prop_state","FL"),
                "Property Zip": r.get("prop_zip",""),
                "Lead Type": r.get("cat_label",""),
                "Document Type": r.get("doc_type",""),
                "Date Filed": r.get("filed",""),
                "Document Number": r.get("doc_num",""),
                "Amount/Debt Owed": r.get("amount",""),
                "Seller Score": r.get("score",0),
                "Motivated Seller Flags": " | ".join(r.get("flags",[])),
                "Filer": r.get("filer",""),
                "Source": "Hillsborough County Clerk",
                "Public Records URL": r.get("clerk_url",""),
            })

    log.info("DONE — %d total leads  |  %d with address", len(all_records), with_addr)

if __name__ == "__main__":
    asyncio.run(main())
