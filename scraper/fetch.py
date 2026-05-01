"""
Hillsborough County Motivated Seller Lead Scraper v35
Change from v34:
  - Only enrich owners with score >= 70 (~150-200 lookups vs 2000)
  - Finishes well within 90 minute GitHub Actions timeout
  - Lower-scored records still appear in dashboard, just without addresses
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
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
CLERK_URL        = "https://publicaccess.hillsclerk.com/oripublicaccess/"
HCPA_BASE        = "https://gis.hcpafl.org/propertysearch"
LOOKBACK_DAYS    = int(os.getenv("LOOKBACK_DAYS", "7"))
ENRICH_MIN_SCORE = 70   # only look up addresses for top leads

# Forewarn credentials from GitHub Secrets
FOREWARN_EMAIL    = os.getenv("FOREWARN_EMAIL", "")
FOREWARN_PASSWORD = os.getenv("FOREWARN_PASSWORD", "")

OUTPUT_PATHS  = [Path("records.json"), Path("data/records.json")]
GHL_CSV_PATH  = Path("data/ghl_export.csv")

DOC_TYPE_MAP = {
    "LP":       ("foreclosure",  "Lis Pendens"),
    "TAXDEED":  ("tax",          "Tax Deed"),
    "JUD":      ("judgment",     "Judgment"),
    "CCJ":      ("judgment",     "Certified Judgment"),
    "DRJUD":    ("judgment",     "Domestic Judgment"),
    "LNCORPTX": ("lien",         "Corp Tax Lien"),
    "LN":       ("lien",         "Lien"),
    "MEDLN":    ("lien",         "Medicaid Lien"),
    "PRO":      ("probate",      "Probate"),
    "NOC":      ("construction", "Notice of Commencement"),
    "RELLP":    ("release",      "Release Lis Pendens"),
}

TARGET_TYPES = set(DOC_TYPE_MAP.keys())

CLERK_OPTION_VALUES = {
    "LP":       "(LP) LIS PENDENS",
    "TAXDEED":  "(TAXDEED) TAX DEED",
    "JUD":      "(JUD) JUDGMENT",
    "CCJ":      "(CCJ) CERTIFIED COPY OF A COURT JUDGMENT",
    "DRJUD":    "(DRJUD) DOMESTIC RELATIONS JUDGMENT",
    "LNCORPTX": "(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA",
    "LN":       "(LN) LIEN",
    "MEDLN":    "(MEDLN) MEDICAID LIEN",
    "PRO":      "(PRO) PROBATE DOCUMENTS",
    "NOC":      "(NOC) NOTICE OF COMMENCEMENT",
    "RELLP":    "(RELLP) RELEASE LIS PENDENS",
}

GRANTEE_IS_OWNER = {
    "LP", "TAXDEED", "LNCORPTX", "LN", "MEDLN", "CCJ", "DRJUD", "JUD",
}

INSTITUTION_KEYWORDS = [
    "BANK", "MORTGAGE", "LOAN", "LENDING", "FINANCIAL", "ASSOCIATION", "HOA",
    "LLC", "INC", "CORP", "LTD", "SERVICES", "NATIONAL", "FEDERAL",
    "PENNYMAC", "NEWREZ", "ROCKET", "SHELLPOINT", "CARRINGTON", "LAKEVIEW",
    "REGIONS", "TRUIST", "WELLS FARGO", "MIDFIRST", "FLAGSTAR", "PLANET HOME",
    "CROSSCOUNTRY", "NATIONSTAR", "HABITAT", "TRUSTEE", "AS TRUSTEE", "SUCCESSOR",
    "FREDDIE MAC", "FANNIE MAE", "DEUTSCHE", "JPMORGAN", "CITIMORTGAGE",
    "OCWEN", "PHH", "SPS ", "BSI ", "ARK ", "CENLAR", "LOANCARE", "SELENE",
    "RUSHMORE", "SERVIS", "SPECIALIZED", "ROUNDPOINT", "FREEDOM MORTGAGE",
    "MR. COOPER", "BROCK & SCOTT", "ROBERTSON ANSCHUTZ", "DIAZ ANSELMO",
    "FLORIDA DEFAULT", "LAW OFFICES", "ATTORNEYS", "PLLC", "PA ",
    "INTERNAL REVENUE", "DEPARTMENT OF", "STATE OF", "COUNTY OF", "UNITED STATES",
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
    n = _norm(full)
    if not n:
        return "", ""
    if "," in n:
        parts = n.split(",", 1)
        return parts[1].strip(), parts[0].strip()
    parts = n.split()
    if len(parts) == 1:
        return "", parts[0]
    return " ".join(parts[1:]), parts[0]

def _is_institution(name: str) -> bool:
    n = _norm(name)
    if not n:
        return False
    return any(k in n for k in INSTITUTION_KEYWORDS)

def _resolve_owner(doc_type: str, grantor: str, grantee: str) -> str:
    g1 = _norm(grantor)
    g2 = _norm(grantee)
    if doc_type in GRANTEE_IS_OWNER:
        if g2 and not _is_institution(g2):
            return g2
        if g1 and not _is_institution(g1):
            return g1
        return g2 or g1
    if g1 and not _is_institution(g1):
        return g1
    if g2 and not _is_institution(g2):
        return g2
    return g1

def _fix_clerk_url(href: str) -> str:
    if not href:
        return ""
    href = re.sub(r"javascript:.*$", "", href, flags=re.IGNORECASE).strip()
    if not href:
        return ""
    if href.startswith("http"):
        href = re.sub(
            r"(https://publicaccess\.hillsclerk\.com)(?!/)",
            r"\1/oripublicaccess/", href,
        )
        return href
    if href.startswith("/"):
        return "https://publicaccess.hillsclerk.com" + href
    return "https://publicaccess.hillsclerk.com/oripublicaccess/" + href

def _parse_doc_type(raw: str) -> str:
    m = re.match(r"\(([A-Z0-9]+)\)", raw.strip())
    return m.group(1) if m else raw.strip().upper()

def _is_legal_description(s: str) -> bool:
    """Return True if string looks like a legal description not a street address."""
    n = _norm(s)
    if not n:
        return False
    # Real addresses start with a house number
    if re.match(r"^\d+\s+[A-Z]", n):
        return False
    legal_patterns = [
        r"^L \d+", r"^LOT \d+", r"^PB \d+", r"^OR BK", r"^PT ",
        r"^SEC ", r"^BLK ", r"^TRACT ", r"^PARCEL", r"^SEE IMAGE",
        r"^\d{2}[A-Z]{2}\d+", r"^26[A-Z]{2}\d+",
    ]
    return any(re.match(p, n) for p in legal_patterns)


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────

def score_record(rec: dict):
    flags, s = [], 30
    doc   = rec.get("doc_type", "")
    owner = _norm(rec.get("owner", ""))
    filed = rec.get("filed", "")

    if doc == "LP":
        flags.append("Lis pendens");      s += 10
    if doc == "LP":
        flags.append("Pre-foreclosure");  s += 10
    if doc in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien");    s += 10
    if doc in ("LNCORPTX", "TAXDEED"):
        flags.append("Tax lien");         s += 10
    if doc == "MEDLN":
        flags.append("Medicaid lien");    s += 10
    if doc == "PRO":
        flags.append("Probate / estate"); s += 10
    if re.search(r"\b(LLC|INC|CORP|LTD|TRUST|LP)\b", owner):
        flags.append("LLC / corp owner")
        # Institutions are NOT motivated sellers — cap their score low
        return 35, flags
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        s += 20

    try:
        amt = float(rec.get("amount") or 0)
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
# HTML parser for clerk results
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
                clerk_url = ""
                for a in row.find_all("a", href=True):
                    href = a["href"].strip()
                    if href and "javascript" not in href.lower():
                        clerk_url = _fix_clerk_url(href)
                        break

                doc_num_raw = col(cells, "INSTRUMENT #", "INST #", "INST", "INSTRUMENT", "DOC #")
                if not clerk_url and doc_num_raw:
                    # Clerk site uses JS rendering — no direct doc links possible
                    clerk_url = "https://publicaccess.hillsclerk.com/oripublicaccess/"

                doc_type_raw = col(cells, "DOC TYPE", "TYPE", "DOCUMENT TYPE", "DOCTYPE")
                doc_code = _parse_doc_type(doc_type_raw)
                doc_num  = doc_num_raw or (
                    row.find("a").get_text(strip=True) if row.find("a") else ""
                )
                filed   = col(cells, "RECORDING DATE", "RECORD DATE", "DATE RECORDED", "DATE", "FILED")
                grantor = col(cells, "GRANTOR", "NAME", "PARTY 1", "OWNER")
                grantee = col(cells, "CROSS-PARTY NAME", "CROSS PARTY", "CROSS-PARTY", "GRANTEE", "PARTY 2")
                legal   = col(cells, "LEGAL DESCRIPTION", "LEGAL", "DESCRIPTION")

                amount_raw = col(cells, "AMOUNT", "CONSIDERATION", "DEBT")
                amount = None
                if amount_raw:
                    cleaned = re.sub(r"[^\d.]", "", amount_raw)
                    try:
                        amount = float(cleaned) if cleaned else None
                    except Exception:
                        pass

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
# HCPA address lookup via Playwright
# ─────────────────────────────────────────────────────────────────────────────

def _parse_hcpa_table(html: str, owner_parts: list[str]) -> dict:
    empty = {
        "prop_address": "", "prop_city": "", "prop_state": "FL", "prop_zip": "",
        "mail_address": "", "mail_city": "", "mail_state": "FL", "mail_zip": "",
    }

    soup = BeautifulSoup(html, "lxml")

    target_table = None
    for table in soup.find_all("table"):
        header_text = table.get_text(" ", strip=True).upper()
        if "PROPERTY ADDRESS" in header_text and "OWNER" in header_text:
            target_table = table
            break

    if not target_table:
        return empty

    rows = target_table.find_all("tr")
    if len(rows) < 2:
        return empty

    header_cells = rows[0].find_all(["th", "td"])
    headers = [c.get_text(" ", strip=True).upper() for c in header_cells]
    address_col = next((i for i, h in enumerate(headers) if "ADDRESS" in h), None)

    if address_col is None:
        return empty

    best_match = None
    best_score = 0

    for row in rows[1:]:
        cells = row.find_all("td")
        if not cells or address_col >= len(cells):
            continue

        addr_raw = cells[address_col].get_text(" ", strip=True).upper()
        if not addr_raw or addr_raw == "—":
            continue

        row_text = _norm(row.get_text(" ", strip=True))
        score = sum(1 for p in owner_parts if len(p) > 2 and p in row_text)

        if score > best_score:
            best_score = score
            best_match = addr_raw

    if not best_match or best_score == 0:
        return empty

    addr_parts = best_match.split(",")
    street = addr_parts[0].strip()
    city   = addr_parts[1].strip() if len(addr_parts) > 1 else "TAMPA"
    city   = re.sub(r"\s+FL\s*$", "", city).strip()

    return {
        "prop_address": street,
        "prop_city":    city or "TAMPA",
        "prop_state":   "FL",
        "prop_zip":     "",
        "mail_address": street,
        "mail_city":    city or "TAMPA",
        "mail_state":   "FL",
        "mail_zip":     "",
    }


async def hcpa_lookup(page, owner_name: str) -> dict:
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

    search_term = "+".join(parts[:2]) if len(parts) >= 2 else parts[0]
    search_url  = f"{HCPA_BASE}/#/search/basic/owner={search_term}"

    try:
        await page.goto(search_url, wait_until="networkidle", timeout=30_000)
        await page.wait_for_timeout(2_000)

        try:
            await page.wait_for_selector("table", timeout=6_000)
        except Exception:
            pass

        html = await page.content()
        addr = _parse_hcpa_table(html, parts)

        if addr.get("prop_address"):
            log.info("  ✓ HCPA '%s' → %s, %s",
                     n[:40], addr["prop_address"], addr["prop_city"])
            return addr

        # Fallback: last name only
        if len(parts) >= 2:
            search_url2 = f"{HCPA_BASE}/#/search/basic/owner={parts[0]}"
            await page.goto(search_url2, wait_until="networkidle", timeout=20_000)
            await page.wait_for_timeout(2_000)
            try:
                await page.wait_for_selector("table", timeout=5_000)
            except Exception:
                pass
            html2 = await page.content()
            addr = _parse_hcpa_table(html2, parts)
            if addr.get("prop_address"):
                log.info("  ✓ HCPA (fallback) '%s' → %s, %s",
                         n[:40], addr["prop_address"], addr["prop_city"])
                return addr

        return empty

    except PWTimeout:
        log.debug("HCPA timeout: %s", n[:40])
        return empty
    except Exception as e:
        log.debug("HCPA error [%s]: %s", n[:40], e)
        return empty


# ─────────────────────────────────────────────────────────────────────────────
# Clerk scraper
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_one_doc_type(
    page, doc_code: str, date_from: str, date_to: str
) -> list[dict]:
    option_value = CLERK_OPTION_VALUES.get(doc_code)
    if not option_value:
        return []

    results = []
    for attempt in range(1, 4):
        try:
            await page.goto(CLERK_URL, wait_until="networkidle", timeout=60_000)
            await page.wait_for_timeout(2_000)

            set_result = await page.evaluate(f"""
                () => {{
                    const sel = document.getElementById('OBKey__1285_1');
                    if (!sel) return 'ERROR: select not found';
                    let found = null;
                    for (const opt of sel.options) {{
                        if (opt.value === '{option_value}') {{ found = opt; break; }}
                    }}
                    if (!found) return 'ERROR: option not found';
                    sel.value = found.value;
                    sel.dispatchEvent(new Event('change', {{bubbles: true}}));
                    if (window.jQuery) window.jQuery(sel).trigger('chosen:updated');
                    return 'OK: ' + found.value;
                }}
            """)
            log.info("[%s] dropdown → %s", doc_code, set_result)
            if "ERROR" in str(set_result):
                return []

            await page.wait_for_timeout(500)
            await page.evaluate(f"""
                () => {{
                    const b = document.getElementById('OBKey__1634_1');
                    const e = document.getElementById('OBKey__1634_2');
                    if (b) {{ b.value = '{date_from}'; b.dispatchEvent(new Event('change', {{bubbles:true}})); }}
                    if (e) {{ e.value = '{date_to}';   e.dispatchEvent(new Event('change', {{bubbles:true}})); }}
                }}
            """)
            await page.wait_for_timeout(300)
            await page.evaluate("""
                () => {
                    const btn = document.getElementById('sub');
                    if (btn) { btn.click(); return; }
                    for (const el of document.querySelectorAll('button'))
                        if ((el.textContent||'').trim().toUpperCase()==='SEARCH') { el.click(); return; }
                }
            """)
            log.info("[%s] search clicked", doc_code)
            await page.wait_for_load_state("networkidle", timeout=30_000)
            await page.wait_for_timeout(3_000)

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
                    await page.wait_for_load_state("networkidle", timeout=20_000)
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


# ─────────────────────────────────────────────────────────────────────────────
# Forewarn phone number lookup via Playwright
# ─────────────────────────────────────────────────────────────────────────────

async def forewarn_login(page) -> bool:
    """Log into Forewarn. Returns True on success."""
    if not FOREWARN_EMAIL or not FOREWARN_PASSWORD:
        log.warning("Forewarn credentials not set — skipping phone lookup")
        return False
    try:
        await page.goto("https://app.forewarn.com/login", wait_until="networkidle", timeout=30_000)
        await page.wait_for_timeout(2_000)

        # Fill email
        await page.fill("input[type='email'], input[name='email'], input[placeholder*='mail']", FOREWARN_EMAIL)
        await page.wait_for_timeout(300)

        # Fill password
        await page.fill("input[type='password']", FOREWARN_PASSWORD)
        await page.wait_for_timeout(300)

        # Click login button
        await page.evaluate("""
            () => {
                for (const btn of document.querySelectorAll('button')) {
                    const t = btn.textContent.trim().toUpperCase();
                    if (t.includes('SIGN IN') || t.includes('LOG IN') || t.includes('LOGIN')) {
                        btn.click(); return;
                    }
                }
                // fallback — click first submit button
                const sub = document.querySelector('button[type=submit]');
                if (sub) sub.click();
            }
        """)
        await page.wait_for_load_state("networkidle", timeout=15_000)
        await page.wait_for_timeout(2_000)

        # Check if logged in — search page should be visible
        if "search" in page.url or "forewarn" in page.url:
            log.info("Forewarn login successful")
            return True
        log.warning("Forewarn login may have failed — URL: %s", page.url)
        return False
    except Exception as e:
        log.warning("Forewarn login error: %s", e)
        return False


async def forewarn_lookup(page, owner_name: str, zip_code: str) -> str:
    """
    Search Forewarn by name + zip and return the first mobile phone number.
    Returns phone number string or empty string.
    """
    n = _norm(owner_name)
    if not n or _is_institution(n):
        return ""

    first, last = _split_name(n)
    if not first or not last:
        return ""

    try:
        # Navigate to search page
        await page.goto("https://app.forewarn.com/search", wait_until="networkidle", timeout=20_000)
        await page.wait_for_timeout(1_500)

        # Fill first name
        for sel in ["input[placeholder*='First']", "input[name*='first']", "input[id*='first']"]:
            try:
                el = await page.query_selector(sel)
                if el:
                    await el.fill(first.title())
                    break
            except Exception:
                continue

        # Fill last name
        for sel in ["input[placeholder*='Last']", "input[name*='last']", "input[id*='last']"]:
            try:
                el = await page.query_selector(sel)
                if el:
                    await el.fill(last.title())
                    break
            except Exception:
                continue

        # Fill zip code
        if zip_code:
            for sel in ["input[placeholder*='Zip']", "input[placeholder*='zip']", "input[name*='zip']"]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.fill(zip_code[:5])
                        break
                except Exception:
                    continue

        await page.wait_for_timeout(300)

        # Click Search
        await page.evaluate("""
            () => {
                for (const btn of document.querySelectorAll('button')) {
                    const t = btn.textContent.trim().toUpperCase();
                    if (t === 'SEARCH' || t.includes('SEARCH')) { btn.click(); return; }
                }
            }
        """)

        await page.wait_for_load_state("networkidle", timeout=15_000)
        await page.wait_for_timeout(2_000)

        # On results page — find the best match and click it
        # Results show full name + address — match by last name at minimum
        html = await page.content()
        from bs4 import BeautifulSoup as BS
        soup = BS(html, "lxml")

        # Find result rows — look for elements containing the name
        last_upper = last.upper()
        first_upper = first.upper()

        # Try clicking the first result that contains our last name
        clicked = await page.evaluate(f"""
            () => {{
                const items = document.querySelectorAll(
                    '[class*="result"], [class*="card"], [class*="item"], li, tr'
                );
                for (const item of items) {{
                    const t = item.textContent.toUpperCase();
                    if (t.includes('{last_upper}') && t.includes('{first_upper}')) {{
                        const link = item.querySelector('a') || item;
                        link.click();
                        return true;
                    }}
                }}
                // fallback — click first result link
                const first = document.querySelector(
                    '[class*="result"] a, [class*="card"] a, .results a'
                );
                if (first) {{ first.click(); return true; }}
                return false;
            }}
        """)

        if not clicked:
            log.debug("Forewarn: no result found for %s %s", first, last)
            return ""

        await page.wait_for_load_state("networkidle", timeout=15_000)
        await page.wait_for_timeout(1_500)

        # Now on profile page — click Phone Records
        await page.evaluate("""
            () => {
                const els = document.querySelectorAll('a, button, [class*="record"], [class*="section"]');
                for (const el of els) {
                    if (el.textContent.toUpperCase().includes('PHONE')) {
                        el.click(); return;
                    }
                }
            }
        """)

        await page.wait_for_load_state("networkidle", timeout=10_000)
        await page.wait_for_timeout(1_500)

        # On phone records page — grab first mobile number
        phone_js = r"""
            () => {
                const rows = document.querySelectorAll('tr, [class*="row"], [class*="record"]');
                for (const row of rows) {
                    const text = row.textContent.toUpperCase();
                    if (text.includes('MOBILE')) {
                        const match = row.textContent.match(
                            /\b(\d{3}[-. ]\d{3}[-. ]\d{4})\b/
                        );
                        if (match) return match[0];
                    }
                }
                const match = document.body.textContent.match(/\b(\d{3}[-. ]\d{3}[-. ]\d{4})\b/);
                return match ? match[0] : '';
            }
        """
        phone = await page.evaluate(phone_js)

        if phone:
            log.info("  ✓ Forewarn '%s %s' → %s", first, last, phone)
        else:
            log.debug("  ✗ Forewarn no phone: %s %s", first, last)

        return phone or ""

    except Exception as e:
        log.debug("Forewarn lookup error [%s %s]: %s", first, last, e)
        return ""


async def main():
    date_to_dt   = datetime.utcnow()
    date_from_dt = date_to_dt - timedelta(days=LOOKBACK_DAYS)
    date_from    = date_from_dt.strftime("%m/%d/%Y")
    date_to      = date_to_dt.strftime("%m/%d/%Y")

    log.info("=" * 64)
    log.info("Hillsborough County Motivated Seller Scraper  v36")
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
        hcpa_page      = await ctx.new_page()
        forewarn_page  = await ctx.new_page()

        # ── Step 1: Scrape clerk portal ───────────────────────────────────────
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
                owner   = _resolve_owner(doc_type, grantor, grantee)
                base_rec = {
                    "doc_num":      r.get("doc_num", ""),
                    "doc_type":     doc_type,
                    "filed":        r.get("filed", ""),
                    "cat":          cat,
                    "cat_label":    cat_label,
                    "owner":        owner,
                    "filer":        grantor,
                    "grantee":      grantee,
                    "amount":       r.get("amount"),
                    "legal":        r.get("legal", ""),
                    "prop_address": "", "prop_city": "",
                    "prop_state":   "FL", "prop_zip": "",
                    "mail_address": "", "mail_city": "",
                    "mail_state":   "FL", "mail_zip": "",
                    "clerk_url":    r.get("clerk_url", ""),
                    "flags":        [], "score": 0, "phone": "",
                }
                score, flags = score_record(base_rec)
                base_rec["score"] = score
                base_rec["flags"] = flags
                all_records.append(base_rec)

        log.info("Scraped %d total records", len(all_records))

        # ── Step 2: Enrich only score >= 70 leads ────────────────────────────
        # Dedupe owners from qualifying records only
        unique_owners: dict[str, dict] = {}
        for rec in all_records:
            owner = rec.get("owner", "")
            score = rec.get("score", 0)
            if (owner and score >= ENRICH_MIN_SCORE
                    and not _is_institution(owner)
                    and owner not in unique_owners):
                unique_owners[owner] = {}

        log.info("Enriching %d unique owners (score >= %d) via HCPA…",
                 len(unique_owners), ENRICH_MIN_SCORE)

        enriched_count = 0
        for i, owner in enumerate(unique_owners):
            addr = await hcpa_lookup(hcpa_page, owner)
            unique_owners[owner] = addr
            if addr.get("prop_address"):
                enriched_count += 1
            if (i + 1) % 25 == 0:
                log.info("  HCPA: %d / %d — %d with address",
                         i + 1, len(unique_owners), enriched_count)
            await asyncio.sleep(0.5)

        log.info("HCPA done: %d / %d owners got addresses",
                 enriched_count, len(unique_owners))

        # Apply addresses back to all records with matching owner
        for rec in all_records:
            addr = unique_owners.get(rec.get("owner", ""), {})
            if addr.get("prop_address"):
                rec.update(addr)
                score, flags = score_record(rec)
                rec["score"] = score
                rec["flags"] = flags

        # ── Step 3: Forewarn phone lookup ─────────────────────────────────────
        if FOREWARN_EMAIL and FOREWARN_PASSWORD:
            logged_in = await forewarn_login(forewarn_page)
            if logged_in:
                # Only look up records that have an address (have zip code) and score >= 70
                to_call = [
                    r for r in all_records
                    if r.get("score", 0) >= ENRICH_MIN_SCORE
                    and not _is_institution(r.get("owner", ""))
                    and not r.get("phone")  # skip if already has phone
                ]
                # Dedupe by owner name
                seen_owners = set()
                unique_call = []
                for r in to_call:
                    if r["owner"] not in seen_owners:
                        seen_owners.add(r["owner"])
                        unique_call.append(r)

                log.info("Forewarn: looking up %d unique owners…", len(unique_call))
                phone_map: dict[str, str] = {}
                fw_found = 0

                for i, rec in enumerate(unique_call):
                    zip_code = rec.get("prop_zip") or rec.get("mail_zip") or ""
                    phone = await forewarn_lookup(forewarn_page, rec["owner"], zip_code)
                    phone_map[rec["owner"]] = phone
                    if phone:
                        fw_found += 1
                    if (i + 1) % 25 == 0:
                        log.info("  Forewarn: %d / %d — %d with phone",
                                 i + 1, len(unique_call), fw_found)
                    await asyncio.sleep(1)  # be respectful to Forewarn servers

                # Apply phone numbers to all matching records
                for rec in all_records:
                    phone = phone_map.get(rec.get("owner", ""), "")
                    if phone:
                        rec["phone"] = phone

                log.info("Forewarn done: %d / %d owners got phone numbers",
                         fw_found, len(unique_call))
        else:
            log.info("Forewarn credentials not configured — skipping phone lookup")

        await browser.close()

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

    # ── GHL CSV ───────────────────────────────────────────────────────────────
    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    GHL_HEADERS = [
        "First Name", "Last Name", "Phone",
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
                "Phone":                  r.get("phone", ""),
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
