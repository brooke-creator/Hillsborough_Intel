"""
Hillsborough County Motivated Seller Lead Scraper v30
Fixes:
  1. owner field = homeowner (grantee) not the filing institution (grantor)
  2. Address enrichment: HCPA ArcGIS REST API (no Algolia, no fragile Playwright parcel pages)
  3. clerk_url: fixed malformed javascript: hrefs
"""

import asyncio
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

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
CLERK_URL     = "https://publicaccess.hillsclerk.com/oripublicaccess/"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

# HCPA ArcGIS REST — public, no auth required
HCPA_API_URL = (
    "https://gis.hcpafl.org/arcgis/rest/services/HC/HCPAView/MapServer/0/query"
)

OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV_PATH = Path("data/ghl_export.csv")

TARGET_TYPES = {
    "LP","NOFC","TAXDEED","JUD","CCJ","DRJUD",
    "LNCORPTX","LNIRS","LNFED","LN","LNMECH",
    "LNHOA","MEDLN","PRO","NOC","RELLP",
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

# For these doc types: institution files AGAINST the homeowner
# → grantee = homeowner, grantor = institution
GRANTEE_IS_OWNER = {
    "LP","NOFC","TAXDEED","LNHOA","LNMECH",
    "LNCORPTX","LNIRS","LNFED","MEDLN","LN","CCJ","DRJUD","JUD",
}

INSTITUTION_KEYWORDS = [
    "BANK","MORTGAGE","LOAN","LENDING","FINANCIAL","ASSOCIATION","HOA",
    "LLC","INC","CORP","LTD","SERVICES","NATIONAL","FEDERAL",
    "PENNYMAC","NEWREZ","ROCKET","SHELLPOINT","CARRINGTON","LAKEVIEW",
    "REGIONS","TRUIST","WELLS FARGO","MIDFIRST","FLAGSTAR","PLANET HOME",
    "CROSSCOUNTRY","NATIONSTAR","HABITAT","TRUSTEE","AS TRUSTEE","SUCCESSOR",
    "FREDDIE MAC","FANNIE MAE","DEUTSCHE","JPMORGAN","CITIMORTGAGE",
    "OCWEN","PHH","SPS ","BSI ","ARK ","CENLAR","LOANCARE","SELENE",
    "RUSHMORE","SERVIS","SPECIALIZED","ROUNDPOINT","FREEDOM MORTGAGE",
    "MR. COOPER","BROCK & SCOTT","ROBERTSON ANSCHUTZ","DIAZ ANSELMO",
    "FLORIDA DEFAULT","LAW OFFICES","ATTORNEYS","PLLC","PA ",
    "INTERNAL REVENUE","DEPARTMENT OF","STATE OF","COUNTY OF","UNITED STATES",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").upper().strip())

def _norm_date(raw: str) -> str:
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y", "%Y%m%d"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return raw.strip()

def _split_name(full: str):
    """Return (first, last) from a full name string."""
    n = _norm(full)
    if not n:
        return "", ""
    # "LAST, FIRST" format
    if "," in n:
        parts = n.split(",", 1)
        return parts[1].strip(), parts[0].strip()
    parts = n.split()
    if len(parts) == 1:
        return "", parts[0]
    # Clerk stores as "LASTNAME FIRSTNAME" — first token = last name
    return " ".join(parts[1:]), parts[0]

def _is_institution(name: str) -> bool:
    n = _norm(name)
    if not n:
        return False
    return any(k in n for k in INSTITUTION_KEYWORDS)

def _resolve_owner(doc_type: str, grantor: str, grantee: str) -> str:
    """
    Return the property owner (homeowner / motivated seller).

    For LP, NOFC, liens, judgments: the institution files AGAINST the owner,
    so grantee = owner. Grantor = institution.
    For NOC, PRO, RELLP: grantor = owner (they filed it themselves).
    Always skip parties that look like institutions.
    """
    g1 = _norm(grantor)
    g2 = _norm(grantee)

    if doc_type in GRANTEE_IS_OWNER:
        # Prefer grantee if it's a real person
        if g2 and not _is_institution(g2):
            return g2
        # Grantee is an institution too — try grantor
        if g1 and not _is_institution(g1):
            return g1
        # Both institutions — take grantee as fallback
        return g2 or g1

    # NOC / PRO / RELLP — grantor filed it themselves
    if g1 and not _is_institution(g1):
        return g1
    if g2 and not _is_institution(g2):
        return g2
    return g1

def _fix_clerk_url(href: str) -> str:
    """
    Sanitise clerk hrefs.
    Bad:  "https://publicaccess.hillsclerk.comjavascript:;"
    Good: "https://publicaccess.hillsclerk.com/oripublicaccess/..."
    """
    if not href:
        return ""
    href = re.sub(r"javascript:.*$", "", href, flags=re.IGNORECASE).strip()
    if not href:
        return ""
    if href.startswith("http"):
        # Fix missing slash after .com
        href = re.sub(
            r"(https://publicaccess\.hillsclerk\.com)(?!/)",
            r"\1/oripublicaccess/",
            href,
        )
        return href
    if href.startswith("/"):
        return "https://publicaccess.hillsclerk.com" + href
    return "https://publicaccess.hillsclerk.com/oripublicaccess/" + href

def _parse_doc_type(raw: str) -> str:
    m = re.match(r"\(([A-Z0-9]+)\)", raw.strip())
    return m.group(1) if m else raw.strip().upper()


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────

def score_record(rec: dict):
    flags, s = [], 30
    doc    = rec.get("doc_type", "")
    owner  = _norm(rec.get("owner", ""))
    amount = rec.get("amount") or 0
    filed  = rec.get("filed", "")

    if doc == "LP":
        flags.append("Lis pendens"); s += 10
    if doc in ("LP", "NOFC"):
        flags.append("Pre-foreclosure"); s += 10
    if doc in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien"); s += 10
    if doc in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
        flags.append("Tax lien"); s += 10
    if doc == "LNMECH":
        flags.append("Mechanic lien"); s += 10
    if doc == "PRO":
        flags.append("Probate / estate"); s += 10

    # Only flag LLC/corp if the OWNER (homeowner) is an LLC — not the bank
    if re.search(r"\b(LLC|INC|CORP|LTD|TRUST|LP)\b", owner):
        flags.append("LLC / corp owner"); s += 10

    # Combo bonus
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        s += 20

    try:
        amt = float(amount)
        if amt > 100_000: s += 15
        elif amt > 50_000: s += 10
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
# HTML parser for clerk results pages
# ─────────────────────────────────────────────────────────────────────────────

def _parse_html(html: str) -> list[dict]:
    records = []
    soup = BeautifulSoup(html, "lxml")

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        headers = [
            th.get_text(" ", strip=True).upper()
            for th in rows[0].find_all(["th", "td"])
        ]
        if not any("DOC" in h or "TYPE" in h or "NAME" in h for h in headers):
            continue

        def col(cells, *names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h and i < len(cells):
                        t = cells[i].get_text(" ", strip=True)
                        if t:
                            return t
            return ""

        table_records = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            try:
                # Build clerk URL — skip javascript: links
                clerk_url = ""
                for a in row.find_all("a", href=True):
                    href = a["href"].strip()
                    if href and "javascript" not in href.lower():
                        clerk_url = _fix_clerk_url(href)
                        break

                doc_num_raw = col(
                    cells,
                    "INSTRUMENT #", "INST #", "INST", "INSTRUMENT", "DOC #",
                )
                # If no real URL found, build one from the instrument number
                if not clerk_url and doc_num_raw:
                    num = re.sub(r"\D", "", doc_num_raw)
                    if len(num) >= 8:
                        clerk_url = (
                            "https://publicaccess.hillsclerk.com/oripublicaccess/"
                            f"search.aspx?SearchType=OR&DocNumber={num}"
                        )

                doc_type_raw = col(
                    cells,
                    "DOC TYPE", "TYPE", "DOCUMENT TYPE", "DOCTYPE",
                )
                doc_code = _parse_doc_type(doc_type_raw)
                doc_num  = doc_num_raw or (
                    row.find("a").get_text(strip=True) if row.find("a") else ""
                )
                filed   = col(
                    cells,
                    "RECORDING DATE", "RECORD DATE", "DATE RECORDED", "DATE", "FILED",
                )
                grantor = col(cells, "GRANTOR", "NAME", "PARTY 1", "OWNER")
                grantee = col(
                    cells,
                    "CROSS-PARTY NAME", "CROSS PARTY", "CROSS-PARTY", "GRANTEE", "PARTY 2",
                )
                legal   = col(cells, "LEGAL DESCRIPTION", "LEGAL", "DESCRIPTION")

                amount_raw = col(cells, "AMOUNT", "CONSIDERATION", "DEBT")
                amount = None
                if amount_raw:
                    cleaned = re.sub(r"[^\d.]", "", amount_raw)
                    try:
                        amount = float(cleaned) if cleaned else None
                    except Exception:
                        amount = None

                if not grantor and not doc_num:
                    continue

                table_records.append({
                    "doc_num":   doc_num,
                    "doc_type":  doc_code,
                    "filed":     _norm_date(filed),
                    "grantor":   grantor,
                    "grantee":   grantee,
                    "amount":    amount,
                    "legal":     legal,
                    "clerk_url": clerk_url,
                })
            except Exception:
                continue

        if table_records:
            records.extend(table_records)
            break

    return records


# ─────────────────────────────────────────────────────────────────────────────
# Address enrichment — HCPA ArcGIS REST API
# ─────────────────────────────────────────────────────────────────────────────

def _hcpa_query(where: str) -> list[dict]:
    """Query HCPA ArcGIS REST — public endpoint, no auth."""
    params = {
        "where":              where,
        "outFields":          (
            "OWNER,OWN1,SITE_ADDR,SITEADDR,SITE_CITY,SITE_ZIP,"
            "ADDR_1,MAILADR1,CITY,MAILCITY,STATE,ZIP,MAILZIP"
        ),
        "returnGeometry":     "false",
        "f":                  "json",
        "resultRecordCount":  5,
    }
    try:
        r = requests.get(HCPA_API_URL, params=params, timeout=15, verify=False)
        if r.status_code == 200:
            data = r.json()
            return [f["attributes"] for f in data.get("features", [])]
    except Exception as e:
        log.debug("HCPA query error [%s]: %s", where[:60], e)
    return []

def _build_addr(attrs: dict) -> dict:
    site_addr  = _norm(attrs.get("SITE_ADDR") or attrs.get("SITEADDR") or "")
    site_city  = _norm(attrs.get("SITE_CITY") or "TAMPA")
    site_zip   = str(attrs.get("SITE_ZIP") or "").strip()[:5]
    mail_addr  = _norm(attrs.get("ADDR_1") or attrs.get("MAILADR1") or site_addr)
    mail_city  = _norm(attrs.get("CITY") or attrs.get("MAILCITY") or site_city)
    mail_state = _norm(attrs.get("STATE") or "FL")
    mail_zip   = str(attrs.get("ZIP") or attrs.get("MAILZIP") or site_zip).strip()[:5]
    return {
        "prop_address": site_addr,
        "prop_city":    site_city,
        "prop_state":   "FL",
        "prop_zip":     site_zip,
        "mail_address": mail_addr,
        "mail_city":    mail_city,
        "mail_state":   mail_state,
        "mail_zip":     mail_zip,
    }

def enrich_address_hcpa(owner_name: str) -> dict:
    """
    Search HCPA for a property address by owner name.
    Tries multiple name variants (FIRST LAST, LAST FIRST, LAST, FIRST).
    """
    empty = {
        "prop_address": "", "prop_city": "", "prop_state": "FL", "prop_zip": "",
        "mail_address": "", "mail_city": "", "mail_state": "FL", "mail_zip": "",
    }

    n = _norm(owner_name)
    if not n or _is_institution(n):
        return empty

    parts = n.replace(",", "").split()
    if not parts:
        return empty

    # Build name variants to try
    variants: list[str] = []
    if len(parts) >= 2:
        variants.append(n)                              # JOHN SMITH
        variants.append(f"{parts[-1]} {parts[0]}")     # SMITH JOHN
        variants.append(f"{parts[-1]}, {parts[0]}")    # SMITH, JOHN
        variants.append(parts[-1])                     # SMITH (broad)
    else:
        variants.append(parts[0])

    for variant in variants:
        safe = variant.replace("'", "''")
        for field in ("OWNER", "OWN1"):
            where = f"UPPER({field}) LIKE UPPER('%{safe}%')"
            results = _hcpa_query(where)
            if results:
                addr = _build_addr(results[0])
                if addr["prop_address"]:
                    log.info("  ✓ HCPA [%s] '%s' → %s, %s %s",
                             field, variant[:40],
                             addr["prop_address"], addr["prop_city"], addr["prop_zip"])
                    return addr

    log.debug("  ✗ No HCPA match: %s", n[:50])
    return empty


# ─────────────────────────────────────────────────────────────────────────────
# Clerk scraper (Playwright)
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_one_doc_type(
    page, doc_code: str, date_from: str, date_to: str
) -> list[dict]:
    results = []
    for attempt in range(1, 4):
        try:
            await page.goto(CLERK_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(3_000)

            selected = await page.evaluate(f"""
                () => {{
                    const sel = document.querySelector(
                        'select.doc-type, select.for-chosen, '
                        + 'select[class*="doc-type"], select[id*="OBKey"]'
                    );
                    if (!sel) return 'no select found';
                    let found = null;
                    for (const opt of sel.options) {{
                        if (opt.text.includes('({doc_code})')) {{ found = opt; break; }}
                    }}
                    if (!found) return 'option not found for {doc_code}';
                    sel.value = found.value;
                    sel.dispatchEvent(new Event('change', {{bubbles: true}}));
                    if (window.jQuery) {{
                        window.jQuery(sel).val(found.value)
                            .trigger('change').trigger('chosen:updated');
                    }}
                    return 'selected: ' + found.text;
                }}
            """)
            log.info("[%s] select → %s", doc_code, selected)
            await page.wait_for_timeout(1_000)
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)

            await page.evaluate(f"""
                () => {{
                    const begins = document.querySelectorAll(
                        'input.record-begin, input[class*="record-begin"]'
                    );
                    const ends = document.querySelectorAll(
                        'input.record-end, input[class*="record-end"]'
                    );
                    if (begins[0]) {{
                        begins[0].value = '{date_from}';
                        begins[0].dispatchEvent(new Event('change', {{bubbles: true}}));
                    }}
                    if (ends[0]) {{
                        ends[0].value = '{date_to}';
                        ends[0].dispatchEvent(new Event('change', {{bubbles: true}}));
                    }}
                }}
            """)
            await page.wait_for_timeout(500)

            await page.evaluate("""
                () => {
                    for (const el of document.querySelectorAll(
                            'input[type=submit], button')) {
                        if ((el.value || el.textContent || '').trim() === 'Search') {
                            el.click(); return;
                        }
                    }
                }
            """)
            await page.wait_for_load_state("domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(4_000)

            page_num = 1
            while True:
                html = await page.content()
                rows = _parse_html(html)
                results.extend(rows)
                log.info("[%s] pg %d: +%d rows (total %d)",
                         doc_code, page_num, len(rows), len(results))

                soup = BeautifulSoup(html, "lxml")
                if not soup.find("a", string=re.compile(r"^\s*(Next|>>)\s*$", re.I)):
                    break
                try:
                    await page.click("a:has-text('Next')", timeout=8_000)
                    await page.wait_for_load_state("domcontentloaded", timeout=20_000)
                    await page.wait_for_timeout(1_500)
                    page_num += 1
                except Exception:
                    break

            log.info("[%s] DONE — %d records", doc_code, len(results))
            return results

        except PWTimeout:
            log.warning("[%s] timeout attempt %d/3", doc_code, attempt)
        except Exception as exc:
            log.warning("[%s] error attempt %d: %s", doc_code, attempt, exc)
        await asyncio.sleep(3)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    date_to_dt   = datetime.utcnow()
    date_from_dt = date_to_dt - timedelta(days=LOOKBACK_DAYS)
    date_from    = date_from_dt.strftime("%m/%d/%Y")
    date_to      = date_to_dt.strftime("%m/%d/%Y")

    log.info("=" * 64)
    log.info("Hillsborough County Motivated Seller Scraper  v30")
    log.info("Range : %s → %s  (%d days)", date_from, date_to, LOOKBACK_DAYS)
    log.info("=" * 64)

    all_records: list[dict] = []
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
        clerk_page = await ctx.new_page()

        # ── Scrape clerk portal ───────────────────────────────────────────────
        for doc_code, (cat, cat_label) in DOC_TYPE_MAP.items():
            log.info("── [%s] %s", doc_code, cat_label)
            raw_rows = await scrape_one_doc_type(
                clerk_page, doc_code, date_from, date_to
            )
            for r in raw_rows:
                doc_type = r.get("doc_type", "").upper()
                if doc_type not in TARGET_TYPES:
                    continue

                grantor = _norm(r.get("grantor", ""))
                grantee = _norm(r.get("grantee", ""))

                # ─── THE CORE FIX ────────────────────────────────────────────
                # owner = homeowner (motivated seller), NOT the filing institution
                owner = _resolve_owner(doc_type, grantor, grantee)
                # ─────────────────────────────────────────────────────────────

                base_rec = {
                    "doc_num":      r.get("doc_num", ""),
                    "doc_type":     doc_type,
                    "filed":        r.get("filed", ""),
                    "cat":          cat,
                    "cat_label":    cat_label,
                    "owner":        owner,      # ← homeowner
                    "filer":        grantor,    # ← institution that filed
                    "grantee":      grantee,    # ← kept for reference
                    "amount":       r.get("amount"),
                    "legal":        r.get("legal", ""),
                    "prop_address": "",
                    "prop_city":    "",
                    "prop_state":   "FL",
                    "prop_zip":     "",
                    "mail_address": "",
                    "mail_city":    "",
                    "mail_state":   "FL",
                    "mail_zip":     "",
                    "clerk_url":    r.get("clerk_url", ""),
                    "flags":        [],
                    "score":        0,
                }
                score, flags = score_record(base_rec)
                base_rec["score"] = score
                base_rec["flags"] = flags
                all_records.append(base_rec)

        log.info("Scraped %d total records from clerk portal", len(all_records))
        await browser.close()

    # ── Address enrichment via HCPA REST API ─────────────────────────────────
    log.info("Enriching addresses via HCPA ArcGIS REST API…")
    enriched = 0

    for i, rec in enumerate(all_records):
        owner = rec.get("owner", "")
        if not owner or _is_institution(owner):
            continue
        try:
            addr = enrich_address_hcpa(owner)
            if addr.get("prop_address"):
                rec.update(addr)
                score, flags = score_record(rec)
                rec["score"] = score
                rec["flags"] = flags
                enriched += 1
        except Exception as e:
            log.debug("Enrich error [%s]: %s", owner[:40], e)

        if (i + 1) % 100 == 0:
            log.info("  %d / %d processed — %d with address",
                     i + 1, len(all_records), enriched)

    log.info("Enrichment done: %d / %d got addresses", enriched, len(all_records))

    # ── Sort and save ─────────────────────────────────────────────────────────
    all_records.sort(key=lambda x: x["score"], reverse=True)
    with_addr = sum(1 for r in all_records if r.get("prop_address"))

    payload = {
        "fetched_at":    datetime.utcnow().isoformat() + "Z",
        "source":        "Hillsborough County Clerk of Courts",
        "date_range":    {"from": date_from, "to": date_to},
        "lookback_days": LOOKBACK_DAYS,
        "total":         len(all_records),
        "with_address":  with_addr,
        "records":       all_records,
    }

    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        log.info("Saved → %s  (%d records, %d with address)",
                 path, len(all_records), with_addr)

    # ── GHL CSV export ────────────────────────────────────────────────────────
    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    GHL_HEADERS = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Filer", "Source", "Public Records URL",
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
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", "FL"),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", "FL"),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", ""),
                "Document Type":          r.get("doc_type", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       r.get("amount", ""),
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": " | ".join(r.get("flags", [])),
                "Filer":                  r.get("filer", ""),
                "Source":                 "Hillsborough County Clerk",
                "Public Records URL":     r.get("clerk_url", ""),
            })

    log.info("GHL CSV → %s", GHL_CSV_PATH)
    log.info("DONE — %d total leads | %d with address", len(all_records), with_addr)


if __name__ == "__main__":
    asyncio.run(main())
