"""
Hillsborough County Motivated Seller Lead Scraper v31
Fixes:
  1. Clerk dropdown: targets Chosen.js via exact select id OBKey__1285_1
     Option values are "(LP) LIS PENDENS" not "(LP)" — fixed to match exactly
  2. Search button: id="sub"
  3. Date fields: id OBKey__1634_1 / OBKey__1634_2
  4. HCPA address enrichment: scrapes Downloads/Maps-Data page to find
     the real bulk DBF/ZIP download URL, then uses dbfread for owner lookup
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import zipfile
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

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
CLERK_URL     = "https://publicaccess.hillsclerk.com/oripublicaccess/"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

HCPA_MAPS_DATA_URL = "https://hcpafl.org/Downloads/Maps-Data"

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

# The option value on the clerk site is the full string e.g. "(LP) LIS PENDENS"
# This maps our short code → the exact option value on the page
CLERK_OPTION_VALUES = {
    "LP":       "(LP) LIS PENDENS",
    "NOFC":     "(NOFC) NOTICE OF FORECLOSURE",
    "TAXDEED":  "(TAXDEED) TAX DEED",
    "JUD":      "(JUD) JUDGMENT",
    "CCJ":      "(CCJ) CERTIFIED COPY OF A COURT JUDGMENT",
    "DRJUD":    "(DRJUD) DOMESTIC RELATIONS JUDGMENT",
    "LNCORPTX": "(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA",
    "LNIRS":    "(LNIRS) IRS LIEN",
    "LNFED":    "(LNFED) FEDERAL TAX LIEN",
    "LN":       "(LN) LIEN",
    "LNMECH":   "(LNMECH) MECHANIC LIEN",
    "LNHOA":    "(LNHOA) HOA LIEN",
    "MEDLN":    "(MEDLN) MEDICAID LIEN",
    "PRO":      "(PRO) PROBATE",
    "NOC":      "(NOC) NOTICE OF COMMENCEMENT",
    "RELLP":    "(RELLP) RELEASE LIS PENDENS",
}

# For these doc types: institution files AGAINST the homeowner
# grantee = homeowner, grantor = institution
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
    doc   = rec.get("doc_type", "")
    owner = _norm(rec.get("owner", ""))
    filed = rec.get("filed", "")

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

                doc_num_raw = col(cells, "INSTRUMENT #","INST #","INST","INSTRUMENT","DOC #")
                if not clerk_url and doc_num_raw:
                    num = re.sub(r"\D", "", doc_num_raw)
                    if len(num) >= 8:
                        clerk_url = (
                            "https://publicaccess.hillsclerk.com/oripublicaccess/"
                            f"search.aspx?SearchType=OR&DocNumber={num}"
                        )

                doc_type_raw = col(cells, "DOC TYPE","TYPE","DOCUMENT TYPE","DOCTYPE")
                doc_code = _parse_doc_type(doc_type_raw)
                doc_num  = doc_num_raw or (
                    row.find("a").get_text(strip=True) if row.find("a") else ""
                )
                filed   = col(cells, "RECORDING DATE","RECORD DATE","DATE RECORDED","DATE","FILED")
                grantor = col(cells, "GRANTOR","NAME","PARTY 1","OWNER")
                grantee = col(cells, "CROSS-PARTY NAME","CROSS PARTY","CROSS-PARTY","GRANTEE","PARTY 2")
                legal   = col(cells, "LEGAL DESCRIPTION","LEGAL","DESCRIPTION")

                amount_raw = col(cells, "AMOUNT","CONSIDERATION","DEBT")
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
# HCPA bulk DBF download + owner lookup
# ─────────────────────────────────────────────────────────────────────────────

class ParcelLookup:
    """
    Downloads the HCPA bulk NAL DBF file and builds an owner-name index.
    Falls back gracefully if the download fails.
    """

    def __init__(self):
        self._index: dict[str, dict] = {}   # normalised name token → parcel attrs
        self._loaded = False

    # ── find the download URL ─────────────────────────────────────────────────
    def _find_dbf_url(self) -> str:
        headers = {"User-Agent": "Mozilla/5.0"}
        log.info("Scraping HCPA Maps-Data page for bulk download link…")
        try:
            r = requests.get(HCPA_MAPS_DATA_URL, headers=headers,
                             timeout=20, verify=False)
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(" ", strip=True).upper()
                # Look for NAL, parcel, owner, bulk zip/dbf links
                if re.search(r"NAL|PARCEL|OWNER|BULK|NAME.?ADDRESS", text, re.I) or \
                   re.search(r"\.(zip|dbf)\b", href, re.I):
                    full = href if href.startswith("http") else "https://hcpafl.org" + href
                    log.info("  Candidate link: %s  (%s)", full, text[:60])
                    # Test if it's a real file
                    try:
                        head = requests.head(full, headers=headers, timeout=10,
                                             verify=False, allow_redirects=True)
                        ct = head.headers.get("Content-Type","")
                        cl = int(head.headers.get("Content-Length", 0))
                        if head.status_code == 200 and cl > 100_000:
                            log.info("  ✓ Found bulk file: %s (%d bytes)", full, cl)
                            return full
                    except Exception:
                        pass
        except Exception as e:
            log.warning("HCPA page scrape error: %s", e)

        # Last-resort hardcoded candidates
        candidates = [
            "https://hcpafl.org/LinkClick.aspx?link=%2fDownloads%2fMaps-Data%2fNAL.zip&tabid=97",
            "https://hcpafl.org/LinkClick.aspx?link=%2fDownloads%2fMaps-Data%2fNAL_OWNER.zip&tabid=97",
            "https://hcpafl.org/Portals/0/Downloads/NAL.zip",
            "https://hcpafl.org/Portals/0/Downloads/NAL_OWNER.zip",
            "https://hcpafl.org/Portals/0/NAL.zip",
        ]
        for url in candidates:
            try:
                head = requests.head(url, headers=headers, timeout=10,
                                     verify=False, allow_redirects=True)
                if head.status_code == 200:
                    cl = int(head.headers.get("Content-Length", 0))
                    if cl > 100_000:
                        log.info("  ✓ Fallback URL works: %s", url)
                        return url
            except Exception:
                pass

        log.warning("Could not find HCPA bulk DBF URL — address enrichment disabled")
        return ""

    # ── download + parse ──────────────────────────────────────────────────────
    def load(self):
        if self._loaded:
            return
        self._loaded = True

        if not HAS_DBF:
            log.warning("dbfread not installed — address enrichment disabled")
            return

        url = self._find_dbf_url()
        if not url:
            return

        log.info("Downloading HCPA bulk parcel file…")
        try:
            r = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=120,
                verify=False,
                stream=True,
            )
            r.raise_for_status()
            raw = r.content
            log.info("  Downloaded %d bytes", len(raw))
        except Exception as e:
            log.warning("HCPA download failed: %s", e)
            return

        # Unzip if needed
        dbf_bytes = None
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                for name in zf.namelist():
                    if name.upper().endswith(".DBF"):
                        dbf_bytes = zf.read(name)
                        log.info("  Extracted DBF: %s (%d bytes)", name, len(dbf_bytes))
                        break
        except zipfile.BadZipFile:
            # Maybe it's a raw DBF
            if raw[:4] in (b'\x03', b'\x04', b'\x83', b'\x8b'):
                dbf_bytes = raw

        if not dbf_bytes:
            log.warning("Could not extract DBF from download")
            return

        # Write to temp file (dbfread needs a file path)
        tmp = Path("data/_nal_tmp.dbf")
        tmp.parent.mkdir(exist_ok=True)
        tmp.write_bytes(dbf_bytes)

        log.info("  Parsing DBF records…")
        count = 0
        try:
            for rec in DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True):
                owner = _norm(
                    rec.get("OWNER") or rec.get("OWN1") or ""
                )
                if not owner:
                    continue
                site_addr = _norm(
                    rec.get("SITE_ADDR") or rec.get("SITEADDR") or ""
                )
                if not site_addr:
                    continue

                attrs = {
                    "prop_address": site_addr,
                    "prop_city":    _norm(rec.get("SITE_CITY") or "TAMPA"),
                    "prop_state":   "FL",
                    "prop_zip":     str(rec.get("SITE_ZIP") or "").strip()[:5],
                    "mail_address": _norm(
                        rec.get("ADDR_1") or rec.get("MAILADR1") or site_addr
                    ),
                    "mail_city":    _norm(
                        rec.get("CITY") or rec.get("MAILCITY") or "TAMPA"
                    ),
                    "mail_state":   _norm(rec.get("STATE") or "FL"),
                    "mail_zip":     str(
                        rec.get("ZIP") or rec.get("MAILZIP") or ""
                    ).strip()[:5],
                }

                # Index by every token in the owner name for fuzzy matching
                for token in owner.split():
                    if len(token) > 2:
                        self._index.setdefault(token, []).append(
                            (owner, attrs)
                        )
                count += 1

            log.info("  Indexed %d parcels", count)
        except Exception as e:
            log.warning("DBF parse error: %s", e)
        finally:
            try:
                tmp.unlink()
            except Exception:
                pass

    # ── lookup ────────────────────────────────────────────────────────────────
    def lookup(self, owner_name: str) -> dict:
        empty = {
            "prop_address":"","prop_city":"","prop_state":"FL","prop_zip":"",
            "mail_address":"","mail_city":"","mail_state":"FL","mail_zip":"",
        }
        if not self._index or not owner_name:
            return empty

        n = _norm(owner_name)
        parts = [p for p in n.split() if len(p) > 2]
        if not parts:
            return empty

        # Score candidates: count how many name tokens match
        candidates: dict[str, list] = {}
        for part in parts:
            for full_name, attrs in self._index.get(part, []):
                if full_name not in candidates:
                    candidates[full_name] = [0, attrs]
                candidates[full_name][0] += 1

        if not candidates:
            return empty

        # Best match = most token overlaps
        best_name, (best_score, best_attrs) = max(
            candidates.items(), key=lambda x: x[1][0]
        )

        # Require at least 2 matching tokens (or 1 if name is single word)
        min_score = 1 if len(parts) == 1 else 2
        if best_score < min_score:
            return empty

        log.info("  ✓ Parcel match [score=%d]: '%s' → '%s' %s %s",
                 best_score, n[:40], best_attrs["prop_address"],
                 best_attrs["prop_city"], best_attrs["prop_zip"])
        return best_attrs


# ─────────────────────────────────────────────────────────────────────────────
# Clerk scraper — Playwright with correct Chosen.js interaction
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_one_doc_type(
    page, doc_code: str, date_from: str, date_to: str
) -> list[dict]:
    """
    Scrape one doc type from the clerk portal.
    Uses the exact select id and option value format discovered by debug_both.py.
    """
    option_value = CLERK_OPTION_VALUES.get(doc_code)
    if not option_value:
        log.warning("[%s] No option value mapping — skipping", doc_code)
        return []

    results = []
    for attempt in range(1, 4):
        try:
            await page.goto(CLERK_URL, wait_until="networkidle", timeout=60_000)
            await page.wait_for_timeout(2_000)

            # ── Set the doc-type dropdown (Chosen.js hidden select) ───────────
            # The real <select> id is OBKey__1285_1 but it's hidden by Chosen.
            # We set it via JavaScript directly, then fire chosen:updated.
            set_result = await page.evaluate(f"""
                () => {{
                    const sel = document.getElementById('OBKey__1285_1');
                    if (!sel) return 'ERROR: select not found';
                    // Find the exact option value
                    let found = null;
                    for (const opt of sel.options) {{
                        if (opt.value === '{option_value}') {{
                            found = opt;
                            break;
                        }}
                    }}
                    if (!found) {{
                        // Try partial match on value
                        const code = '{doc_code}';
                        for (const opt of sel.options) {{
                            if (opt.value.includes('(' + code + ')')) {{
                                found = opt;
                                break;
                            }}
                        }}
                    }}
                    if (!found) return 'ERROR: option not found for {option_value}';
                    sel.value = found.value;
                    sel.dispatchEvent(new Event('change', {{bubbles: true}}));
                    // Trigger Chosen.js to update its display
                    if (window.jQuery) {{
                        window.jQuery(sel).trigger('chosen:updated');
                    }}
                    return 'OK: ' + found.value;
                }}
            """)
            log.info("[%s] dropdown → %s", doc_code, set_result)

            if "ERROR" in str(set_result):
                log.warning("[%s] Skipping — could not set dropdown", doc_code)
                return []

            await page.wait_for_timeout(500)

            # ── Set date range using the exact field IDs ──────────────────────
            await page.evaluate(f"""
                () => {{
                    const begin = document.getElementById('OBKey__1634_1');
                    const end   = document.getElementById('OBKey__1634_2');
                    if (begin) {{
                        begin.value = '{date_from}';
                        begin.dispatchEvent(new Event('change', {{bubbles: true}}));
                    }}
                    if (end) {{
                        end.value = '{date_to}';
                        end.dispatchEvent(new Event('change', {{bubbles: true}}));
                    }}
                }}
            """)
            await page.wait_for_timeout(300)

            # ── Click Search button (id="sub") ────────────────────────────────
            await page.evaluate("""
                () => {
                    const btn = document.getElementById('sub');
                    if (btn) { btn.click(); return 'clicked sub'; }
                    // fallback
                    for (const el of document.querySelectorAll('button, input[type=button]')) {
                        const t = (el.textContent || el.value || '').trim().toUpperCase();
                        if (t === 'SEARCH' || t.includes('SEARCH')) { el.click(); return 'clicked fallback'; }
                    }
                    return 'no search button found';
                }
            """)
            log.info("[%s] search clicked", doc_code)

            await page.wait_for_load_state("networkidle", timeout=30_000)
            await page.wait_for_timeout(3_000)

            # ── Paginate through results ──────────────────────────────────────
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
    log.info("Hillsborough County Motivated Seller Scraper  v31")
    log.info("Range : %s → %s  (%d days)", date_from, date_to, LOOKBACK_DAYS)
    log.info("=" * 64)

    all_records: list[dict] = []
    Path("data").mkdir(exist_ok=True)

    # ── Load HCPA parcel data upfront ─────────────────────────────────────────
    parcel = ParcelLookup()
    parcel.load()

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

        # ── Scrape each doc type from clerk portal ────────────────────────────
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

                # Address enrichment from parcel index
                addr = parcel.lookup(owner) if owner else {}

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
                    "prop_address": addr.get("prop_address", ""),
                    "prop_city":    addr.get("prop_city", ""),
                    "prop_state":   "FL",
                    "prop_zip":     addr.get("prop_zip", ""),
                    "mail_address": addr.get("mail_address", ""),
                    "mail_city":    addr.get("mail_city", ""),
                    "mail_state":   addr.get("mail_state", "FL"),
                    "mail_zip":     addr.get("mail_zip", ""),
                    "clerk_url":    r.get("clerk_url", ""),
                    "flags":        [],
                    "score":        0,
                }
                score, flags = score_record(base_rec)
                base_rec["score"] = score
                base_rec["flags"] = flags
                all_records.append(base_rec)

        await browser.close()

    log.info("Scraped %d total records", len(all_records))

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
        "First Name","Last Name",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount/Debt Owed","Seller Score","Motivated Seller Flags",
        "Filer","Source","Public Records URL",
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
