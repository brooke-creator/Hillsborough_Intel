"""
Hillsborough County Motivated Seller Lead Scraper v13
Fix: Trigger jQuery chosen:updated after JS selection to make it stick.
"""

import asyncio
import csv
import io
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

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

CLERK_URL     = "https://publicaccess.hillsclerk.com/oripublicaccess/"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

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

def _index_parcel(rec: dict, lookup: dict):
    R = {k.upper(): (v or "") for k, v in rec.items()}
    owner = _norm(R.get("OWNER") or R.get("OWN1") or R.get("OWNERNAME") or "")
    if not owner: return
    entry = {
        "prop_address": _norm(R.get("SITE_ADDR") or R.get("SITEADDR") or ""),
        "prop_city":    _norm(R.get("SITE_CITY") or R.get("SITECITY") or "TAMPA"),
        "prop_state":   "FL",
        "prop_zip":     _norm(R.get("SITE_ZIP")  or R.get("SITEZIP")  or ""),
        "mail_address": _norm(R.get("ADDR_1")    or R.get("MAILADR1") or ""),
        "mail_city":    _norm(R.get("CITY")       or R.get("MAILCITY") or ""),
        "mail_state":   _norm(R.get("STATE")      or "FL"),
        "mail_zip":     _norm(R.get("ZIP")        or R.get("MAILZIP")  or ""),
    }
    parts = owner.split()
    variants = {owner}
    if len(parts) >= 2:
        variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")
        variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")
    for v in variants:
        if v: lookup[v] = entry

def build_parcel_lookup() -> dict:
    lookup: dict = {}
    dbf_path = Path("data/parcels.dbf")
    if HAS_DBF and dbf_path.exists():
        try:
            for rec in DBF(str(dbf_path), ignore_missing_memofile=True):
                _index_parcel(dict(rec), lookup)
            return lookup
        except Exception: pass
    csv_path = Path("data/parcels.csv")
    if csv_path.exists():
        try:
            with open(csv_path, newline="", encoding="utf-8", errors="replace") as fh:
                for row in csv.DictReader(fh): _index_parcel(row, lookup)
            return lookup
        except Exception: pass
    log.warning("No parcel data — addresses will be empty")
    return lookup

def match_parcel(owner: str, lookup: dict) -> dict:
    n = _norm(owner)
    if n in lookup: return lookup[n]
    parts = n.split()
    if len(parts) >= 2:
        for v in (f"{parts[-1]} {' '.join(parts[:-1])}", f"{parts[-1]}, {' '.join(parts[:-1])}"):
            if v in lookup: return lookup[v]
    return {}

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

def _parse_html_table(html: str) -> list[dict]:
    records = []
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table: return records
    rows = table.find_all("tr")
    if len(rows) < 2: return records
    headers = [th.get_text(" ", strip=True).upper() for th in rows[0].find_all(["th","td"])]
    log.info("Table headers: %s", headers)
    def col(cells, *names):
        for name in names:
            for i, h in enumerate(headers):
                if name in h and i < len(cells):
                    t = cells[i].get_text(" ", strip=True)
                    if t: return t
        return ""
    for row in rows[1:]:
        cells = row.find_all("td")
        if not cells: continue
        try:
            link_tag  = row.find("a", href=True)
            clerk_url = ""
            if link_tag:
                href = link_tag["href"]
                clerk_url = href if href.startswith("http") else "https://publicaccess.hillsclerk.com" + href
            doc_type_raw = col(cells, "DOC TYPE","TYPE","DOCUMENT")
            doc_code     = _parse_doc_type(doc_type_raw)
            doc_num  = col(cells, "INST","INSTRUMENT","DOC #","NUMBER") or (link_tag.get_text(strip=True) if link_tag else "")
            filed    = col(cells, "RECORDING DATE","DATE","FILED")
            grantor  = col(cells, "NAME","GRANTOR","PARTY 1","OWNER")
            grantee  = col(cells, "CROSS","GRANTEE","PARTY 2")
            legal    = col(cells, "LEGAL","DESCRIPTION")
            if not doc_num and not grantor: continue
            records.append({
                "doc_num": doc_num, "doc_type": doc_code,
                "filed": _norm_date(filed), "owner": grantor,
                "grantee": grantee, "amount": None,
                "legal": legal, "clerk_url": clerk_url,
            })
        except Exception: continue
    return records

def _parse_csv_direct(csv_text: str, doc_code: str) -> list[dict]:
    records = []
    try:
        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            try:
                R = {k.upper().strip(): (v or "").strip() for k, v in row.items()}
                doc_type_raw = R.get("DOC TYPE") or R.get("TYPE") or doc_code
                doc_type = _parse_doc_type(doc_type_raw)
                name   = R.get("NAME") or R.get("GRANTOR") or ""
                xname  = R.get("CROSS-PARTY NAME") or R.get("GRANTEE") or ""
                filed  = R.get("RECORDING DATE") or R.get("DATE") or ""
                legal  = R.get("LEGAL DESCRIPTION") or R.get("LEGAL") or ""
                inst   = R.get("INST #") or R.get("INSTRUMENT") or ""
                clerk_url = f"https://publicaccess.hillsclerk.com/oripublicaccess/?instrument={inst}" if inst else ""
                if not name and not inst: continue
                records.append({
                    "doc_num": inst, "doc_type": doc_type,
                    "filed": _norm_date(filed), "owner": name,
                    "grantee": xname, "amount": None,
                    "legal": legal, "clerk_url": clerk_url,
                })
            except Exception: continue
    except Exception as e:
        log.warning("CSV parse error: %s", e)
    return records


async def scrape_one_doc_type(page, doc_code: str, date_from: str, date_to: str) -> list[dict]:
    results = []
    for attempt in range(1, 4):
        try:
            await page.goto(CLERK_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(3_000)

            # Click Document Type in left nav using exact ID from inspection
            await page.click('#ORI-Document\\ Type', timeout=10_000)
            await page.wait_for_timeout(2_000)
            log.info("[%s] clicked Document Type nav", doc_code)

            # Use JavaScript to select the option AND trigger jQuery chosen update
            selected = await page.evaluate(f"""
                () => {{
                    const sel = document.querySelector('select.doc-type, select.for-chosen, select[class*="doc-type"], select[id*="OBKey"]');
                    if (!sel) return 'no select found';
                    
                    let found = null;
                    for (const opt of sel.options) {{
                        if (opt.text.includes('({doc_code})')) {{
                            found = opt;
                            break;
                        }}
                    }}
                    if (!found) return 'option not found for {doc_code}';
                    
                    // Set the value
                    sel.value = found.value;
                    
                    // Fire native events
                    sel.dispatchEvent(new Event('change', {{bubbles: true}}));
                    sel.dispatchEvent(new Event('input', {{bubbles: true}}));
                    
                    // Fire jQuery events to update chosen.js widget
                    if (window.jQuery) {{
                        const $sel = window.jQuery(sel);
                        $sel.val(found.value);
                        $sel.trigger('change');
                        $sel.trigger('chosen:updated');
                    }}
                    
                    return 'selected: ' + found.text;
                }}
            """)
            log.info("[%s] JS result: %s", doc_code, selected)
            await page.wait_for_timeout(2_000)
            await page.screenshot(path=f"data/selected_{doc_code}.png")

            # Fill date fields
            all_inputs = page.locator('input[type="text"]')
            n = await all_inputs.count()
            begin_filled = end_filled = False
            for i in range(n):
                try:
                    el  = all_inputs.nth(i)
                    val = (await el.input_value() or "")
                    pid = (await el.get_attribute("id") or "").lower()
                    cls = (await el.get_attribute("class") or "").lower()
                    if "record-begin" in cls or "begin" in pid:
                        await el.click()
                        await el.press("Control+a")
                        await el.fill(date_from)
                        begin_filled = True
                        log.info("[%s] filled begin date", doc_code)
                    elif "record-end" in cls or "end" in pid:
                        await el.click()
                        await el.press("Control+a")
                        await el.fill(date_to)
                        end_filled = True
                        log.info("[%s] filled end date", doc_code)
                except Exception: continue

            # Fallback positional fill if class-based didn't work
            if not begin_filled or not end_filled:
                for i in range(n):
                    try:
                        el  = all_inputs.nth(i)
                        val = (await el.input_value() or "")
                        if re.match(r"\d{1,2}/\d{1,2}/\d{4}", val):
                            if not begin_filled:
                                await el.click()
                                await el.press("Control+a")
                                await el.fill(date_from)
                                begin_filled = True
                            elif not end_filled:
                                await el.click()
                                await el.press("Control+a")
                                await el.fill(date_to)
                                end_filled = True
                    except Exception: continue

            log.info("[%s] dates filled: begin=%s end=%s", doc_code, begin_filled, end_filled)
            await page.wait_for_timeout(500)
            await page.screenshot(path=f"data/dates_{doc_code}.png")

            # Click Search
            for selector in ['input[value="Search"]', 'button:has-text("Search")', 'input[type="submit"]']:
                try:
                    await page.click(selector, timeout=8_000)
                    log.info("[%s] clicked Search", doc_code)
                    break
                except Exception: continue

            await page.wait_for_load_state("domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(3_000)
            await page.screenshot(path=f"data/results_{doc_code}.png")

            # Try Export to Spreadsheet
            try:
                async with page.expect_download(timeout=15_000) as dl:
                    for selector in ['text=Export to Spreadsheet', 'button:has-text("Export")', 'a:has-text("Export")']:
                        try:
                            await page.click(selector, timeout=5_000)
                            log.info("[%s] clicked Export", doc_code)
                            break
                        except Exception: continue
                download = await dl.value
                dl_path = Path(f"data/export_{doc_code}.csv")
                await download.save_as(str(dl_path))
                csv_text = dl_path.read_text(encoding="utf-8", errors="replace")
                log.info("[%s] CSV: %d chars", doc_code, len(csv_text))
                records = _parse_csv_direct(csv_text, doc_code)
                log.info("[%s] CSV records: %d", doc_code, len(records))
                if records:
                    return records
            except Exception as e:
                log.warning("[%s] Export failed: %s", doc_code, e)

            # Fallback HTML table
            page_num = 1
            while True:
                html  = await page.content()
                rows  = _parse_html_table(html)
                results.extend(rows)
                log.info("[%s] page %d: %d rows", doc_code, page_num, len(rows))
                soup2     = BeautifulSoup(html, "lxml")
                next_link = soup2.find("a", string=re.compile(r"^\s*(Next|>>)\s*$", re.I))
                if not next_link: break
                try:
                    await page.click("a:has-text('Next')", timeout=8_000)
                    await page.wait_for_load_state("domcontentloaded", timeout=20_000)
                    await page.wait_for_timeout(1_000)
                    page_num += 1
                except Exception: break

            log.info("[%s] total: %d", doc_code, len(results))
            return results

        except PWTimeout:
            log.warning("[%s] timeout attempt %d/3", doc_code, attempt)
        except Exception as exc:
            log.warning("[%s] error attempt %d: %s", doc_code, attempt, exc)
        await asyncio.sleep(3)
    return results


async def main():
    date_to_dt    = datetime.utcnow()
    date_from_dt  = date_to_dt - timedelta(days=LOOKBACK_DAYS)
    date_from_str = date_from_dt.strftime("%m/%d/%Y")
    date_to_str   = date_to_dt.strftime("%m/%d/%Y")

    log.info("=" * 64)
    log.info("Hillsborough County Motivated Seller Scraper  v13")
    log.info("Range  : %s  →  %s  (%d days)", date_from_str, date_to_str, LOOKBACK_DAYS)
    log.info("=" * 64)

    parcel_lookup = build_parcel_lookup()
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
        page = await ctx.new_page()
        Path("data").mkdir(exist_ok=True)

        # Test LP only
        test_types = {"LP": ("foreclosure", "Lis Pendens")}

        for doc_code, (cat, cat_label) in test_types.items():
            log.info("── Fetching [%s] %s", doc_code, cat_label)
            raw = await scrape_one_doc_type(page, doc_code, date_from_str, date_to_str)
            for r in raw:
                doc_type = r.get("doc_type","").upper()
                if doc_type not in TARGET_TYPES: continue
                parcel = match_parcel(r.get("owner",""), parcel_lookup)
                score, flags = score_record({**r, "doc_type": doc_type, "cat": cat, **parcel})
                all_records.append({
                    "doc_num": r.get("doc_num",""), "doc_type": doc_type,
                    "filed": r.get("filed",""), "cat": cat, "cat_label": cat_label,
                    "owner": r.get("owner",""), "grantee": r.get("grantee",""),
                    "amount": r.get("amount"), "legal": r.get("legal",""),
                    "prop_address": parcel.get("prop_address",""),
                    "prop_city": parcel.get("prop_city",""),
                    "prop_state": "FL",
                    "prop_zip": parcel.get("prop_zip",""),
                    "mail_address": parcel.get("mail_address",""),
                    "mail_city": parcel.get("mail_city",""),
                    "mail_state": parcel.get("mail_state","FL"),
                    "mail_zip": parcel.get("mail_zip",""),
                    "clerk_url": r.get("clerk_url",""),
                    "flags": flags, "score": score,
                })

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
        log.info("Saved → %s  (%d records)", path, len(all_records))

    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    GHL_HEADERS = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip","Property Address","Property City","Property State","Property Zip","Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed","Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    with open(GHL_CSV_PATH, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=GHL_HEADERS, extrasaction="ignore")
        w.writeheader()
        for r in all_records:
            first, last = _split_name(r.get("owner",""))
            w.writerow({"First Name": first, "Last Name": last, "Mailing Address": r.get("mail_address",""), "Mailing City": r.get("mail_city",""), "Mailing State": r.get("mail_state","FL"), "Mailing Zip": r.get("mail_zip",""), "Property Address": r.get("prop_address",""), "Property City": r.get("prop_city",""), "Property State": r.get("prop_state","FL"), "Property Zip": r.get("prop_zip",""), "Lead Type": r.get("cat_label",""), "Document Type": r.get("doc_type",""), "Date Filed": r.get("filed",""), "Document Number": r.get("doc_num",""), "Amount/Debt Owed": r.get("amount",""), "Seller Score": r.get("score",0), "Motivated Seller Flags": " | ".join(r.get("flags",[])), "Source": "Hillsborough County Clerk", "Public Records URL": r.get("clerk_url","")})

    log.info("DONE — %d total leads  |  %d with address", len(all_records), with_addr)

if __name__ == "__main__":
    asyncio.run(main())
