"""
Hillsborough County Motivated Seller Lead Scraper v34
Address enrichment fix:
  - HCPA Basic Search URL confirmed: gis.hcpafl.org/propertysearch/#/nav/Basic Search
  - Owner Name input placeholder: "EX: SMITH, JOHN L"
  - After search, URL changes to: #/search/basic/owner=LASTNAME
  - Results table columns: Folio | Owner Name | Property Address | Sales Date | Sales Price | Homestead
  - Navigate directly via URL to bypass input/button issues
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
CLERK_URL      = "https://publicaccess.hillsclerk.com/oripublicaccess/"
HCPA_BASE      = "https://gis.hcpafl.org/propertysearch"
LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "7"))

OUTPUT_PATHS   = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV_PATH   = Path("data/ghl_export.csv")

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
        flags.append("LLC / corp owner"); s += 10
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
                    num = re.sub(r"\D", "", doc_num_raw)
                    if len(num) >= 8:
                        clerk_url = (
                            "https://publicaccess.hillsclerk.com/oripublicaccess/"
                            f"search.aspx?SearchType=OR&DocNumber={num}"
                        )

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
    """
    Parse HCPA search results table and find the best matching address.
    Table columns: Folio | Owner Name | Property Address | Sales Date | Sales Price | Homestead
    """
    empty = {
        "prop_address": "", "prop_city": "", "prop_state": "FL", "prop_zip": "",
        "mail_address": "", "mail_city": "", "mail_state": "FL", "mail_zip": "",
    }

    soup = BeautifulSoup(html, "lxml")

    # Find the results table — it has a "Property Address" column header
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

    # Find column indices
    header_cells = rows[0].find_all(["th", "td"])
    headers = [c.get_text(" ", strip=True).upper() for c in header_cells]

    owner_col   = next((i for i, h in enumerate(headers) if "OWNER" in h), None)
    address_col = next((i for i, h in enumerate(headers) if "ADDRESS" in h), None)

    if address_col is None:
        return empty

    best_match  = None
    best_score  = 0

    for row in rows[1:]:
        cells = row.find_all("td")
        if not cells or address_col >= len(cells):
            continue

        addr_raw = cells[address_col].get_text(" ", strip=True).upper()
        if not addr_raw or addr_raw == "—":
            continue

        # Score this row by how many owner name parts match
        row_text = _norm(row.get_text(" ", strip=True))
        score = sum(1 for p in owner_parts if len(p) > 2 and p in row_text)

        if score > best_score:
            best_score  = score
            best_match  = addr_raw

    if not best_match or best_score == 0:
        return empty

    # Parse address — format is "12345 STREET NAME, CITY" or "12345 STREET NAME, CITY ST"
    # Try to split city from street
    addr_parts = best_match.split(",")
    street = addr_parts[0].strip()
    city   = addr_parts[1].strip() if len(addr_parts) > 1 else "TAMPA"

    # Remove state abbreviation from city if present (e.g. "TAMPA FL" or "TAMPA")
    city = re.sub(r"\s+FL\s*$", "", city).strip()

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
    """
    Look up property address by navigating directly to the HCPA search URL.
    URL pattern confirmed: gis.hcpafl.org/propertysearch/#/search/basic/owner=LASTNAME
    """
    empty = {
        "prop_address": "", "prop_city": "", "prop_state": "FL", "prop_zip": "",
        "mail_address": "", "mail_city": "", "mail_state": "FL", "mail_zip": "",
    }

    n = _norm(owner_name)
    if not n or _is_institution(n):
        return empty

    # Clean name for URL — use first token (clerk stores as "LASTNAME FIRSTNAME")
    parts = n.replace(",", "").split()
    if not parts:
        return empty

    # Use first two tokens joined for a tighter search (e.g. "MANN FAIGY")
    search_term = "+".join(parts[:2]) if len(parts) >= 2 else parts[0]
    # URL-encode spaces as %20 or use + — the site uses the hash router
    search_url = f"{HCPA_BASE}/#/search/basic/owner={search_term}"

    try:
        await page.goto(search_url, wait_until="networkidle", timeout=30_000)
        await page.wait_for_timeout(3_000)

        # Wait for results table to appear
        try:
            await page.wait_for_selector("table", timeout=8_000)
        except Exception:
            pass

        await page.wait_for_timeout(1_000)
        html = await page.content()

        addr = _parse_hcpa_table(html, parts)

        if addr.get("prop_address"):
            log.info("  ✓ HCPA '%s' → %s, %s",
                     n[:40], addr["prop_address"], addr["prop_city"])
        else:
            # Try with just last name if two-token search failed
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

async def main():
    date_to_dt   = datetime.utcnow()
    date_from_dt = date_to_dt - timedelta(days=LOOKBACK_DAYS)
    date_from    = date_from_dt.strftime("%m/%d/%Y")
    date_to      = date_to_dt.strftime("%m/%d/%Y")

    log.info("=" * 64)
    log.info("Hillsborough County Motivated Seller Scraper  v34")
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
        hcpa_page  = await ctx.new_page()

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
                    "flags":        [], "score": 0,
                }
                score, flags = score_record(base_rec)
                base_rec["score"] = score
                base_rec["flags"] = flags
                all_records.append(base_rec)

        log.info("Scraped %d total records", len(all_records))

        # ── Step 2: Dedupe owners and enrich via HCPA ─────────────────────────
        unique_owners: dict[str, dict] = {}
        for rec in all_records:
            owner = rec.get("owner", "")
            if owner and not _is_institution(owner) and owner not in unique_owners:
                unique_owners[owner] = {}

        log.info("Enriching %d unique owners via HCPA Playwright…",
                 len(unique_owners))

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

        # Apply addresses back to records
        for rec in all_records:
            addr = unique_owners.get(rec.get("owner", ""), {})
            if addr.get("prop_address"):
                rec.update(addr)
                score, flags = score_record(rec)
                rec["score"] = score
                rec["flags"] = flags

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
