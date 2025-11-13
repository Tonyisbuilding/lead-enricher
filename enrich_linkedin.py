# enrich_linkedin.py
import os, re, sys, time, random, json, urllib.parse, html, datetime, contextlib
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2 import service_account
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except Exception:
    sync_playwright = None
    PlaywrightTimeoutError = Exception

# ------------------ config ------------------
BATCH_SIZE = 25
TIMEOUT    = 12
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")

CREDS_PATH = os.path.expanduser(
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "~/sheet-bot-key.json")
)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Default sheet wiring (env vars can still override)
DEFAULT_SHEET_PROFILE = "anthony_directory"
SHEET_PROFILES = {
    "anthony_directory": {
        "sheet_id": "1pwBp7c2ou5007RgMRc_wxQO9J9k9AnTat0_SGunTDdA",
        "tab_name": "Main",
    },
}

# Column headers we use/create
COL_COMPANY        = "Company"
COL_LINKEDIN       = "LinkedIn"
COL_STATUS         = "LinkedIn status"
COL_LAST_CHECKED   = "Last checked"
COL_EMP_COUNT      = "LinkedIn employees"
COL_EMP_LINK       = "LinkedIn people link"
COL_COMPANY_ID     = "LinkedIn company ID"

STATUS_FOUND = "FOUND"
STATUS_NONE  = "NONE"
# --------------------------------------------

def err_exit(msg: str):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def resolve_sheet_target() -> tuple[str, str]:
    env_sheet_id = os.environ.get("SHEET_ID")
    env_tab_name = os.environ.get("TAB_NAME")
    profile_name = os.environ.get("SHEET_PROFILE", DEFAULT_SHEET_PROFILE)
    profile = SHEET_PROFILES.get(profile_name)

    if env_sheet_id:
        tab = env_tab_name or (profile["tab_name"] if profile else "Main")
        return env_sheet_id, tab

    if profile:
        tab = env_tab_name or profile.get("tab_name", "Main")
        return profile["sheet_id"], tab

    err_exit(
        "Set SHEET_ID env var or add the sheet to SHEET_PROFILES "
        f"(unknown sheet profile '{profile_name}')."
    )

SHEET_ID, TAB_NAME = resolve_sheet_target()

if not SHEET_ID:
    err_exit("Set SHEET_ID env var or define a profile in SHEET_PROFILES.")
if not os.path.exists(CREDS_PATH):
    err_exit(f"Service-account key not found at: {CREDS_PATH}")

def auth_client():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_PATH, scopes=SCOPES
    )
    return gspread.authorize(creds)

# ===== meta helpers (progress pointer) =====
META_SHEET = "_meta"
META_KEY_NEXTROW = "next_row"

def get_or_create_meta(sh: gspread.Spreadsheet):
    meta = sh.worksheet(META_SHEET) if META_SHEET in [w.title for w in sh.worksheets()] else sh.add_worksheet(META_SHEET, 10, 2)
    try:
        sh.batch_update([{"updateSheetProperties": {
            "properties": {"sheetId": meta.id, "hidden": True},
            "fields": "hidden"
        }}])
    except Exception:
        pass
    if meta.acell("A1").value != "key":
        meta.update("A1:B1", [["key", "value"]])
    return meta

def meta_get_next_row(meta) -> int:
    cells = meta.findall(META_KEY_NEXTROW)
    if cells:
        row = cells[0].row
        val = meta.cell(row, 2).value
        try:
            return max(2, int(val))
        except Exception:
            return 2
    meta.append_row([META_KEY_NEXTROW, "2"])
    return 2

def meta_set_next_row(meta, next_row: int):
    cells = meta.findall(META_KEY_NEXTROW)
    if cells:
        meta.update_cell(cells[0].row, 2, str(max(2, next_row)))
    else:
        meta.append_row([META_KEY_NEXTROW, str(max(2, next_row))])


class LinkedInCompanyInspector:
    """Lightweight wrapper around Playwright to extract employee stats."""

    _warned = False

    def __init__(self):
        self._play = None
        self._browser = None
        self._page = None

    def _ensure(self, fresh: bool = False) -> bool:
        if fresh:
            self.close()
        if not sync_playwright:
            if not LinkedInCompanyInspector._warned:
                print("⚠️ Playwright not installed; skipping employee stats scrape.")
                LinkedInCompanyInspector._warned = True
            return False
        if self._page:
            return True
        try:
            self._play = sync_playwright().start()
            self._browser = self._play.chromium.launch(headless=True)
            self._page = self._browser.new_page()
            self._page.set_default_timeout(15000)
            return True
        except Exception as exc:
            if not LinkedInCompanyInspector._warned:
                print(f"⚠️ Could not start Playwright (employee stats skipped): {exc}")
                LinkedInCompanyInspector._warned = True
            self.close()
            return False

    def close(self):
        if self._page:
            with contextlib.suppress(Exception):
                self._page.close()
            self._page = None
        if self._browser:
            with contextlib.suppress(Exception):
                self._browser.close()
            self._browser = None
        if self._play:
            with contextlib.suppress(Exception):
                self._play.stop()
            self._play = None

    @staticmethod
    def _deep_unquote(value: str, rounds: int = 3) -> str:
        result = value or ""
        for _ in range(rounds):
            new = urllib.parse.unquote(result)
            if new == result:
                break
            result = new
        return result

    @staticmethod
    def _extract_company_id(text: str) -> str:
        if not text:
            return ""
        match = re.search(r"(?:currentCompany|facetCurrentCompany)=%5B(\d+)%5D", text)
        if match:
            return match.group(1)
        match = re.search(r"(?:currentCompany|facetCurrentCompany)=\[(\d+)\]", text)
        return match.group(1) if match else ""

    @staticmethod
    def _extract_people_url(*candidates: str, company_id: str = "") -> str:
        for c in candidates:
            if c and "linkedin.com/search/results/people" in c:
                return c
        if company_id:
            return f"https://www.linkedin.com/search/results/people/?currentCompany=%5B{company_id}%5D"
        return ""

    @staticmethod
    def _parse_employee_count(text: str):
        if not text:
            return None
        match = re.search(r"([\d.,]+)\s+employee", text, flags=re.I)
        if not match:
            return None
        digits = re.sub(r"[^\d]", "", match.group(1))
        if not digits:
            return None
        try:
            return int(digits)
        except Exception:
            return None

    def fetch(self, company_url: str):
        if not company_url:
            return None
        if "linkedin.com" not in company_url.lower():
            return None
        if not self._ensure(fresh=True):
            return None
        try:
            self._page.goto(company_url, wait_until="domcontentloaded")
            locator = self._page.locator("a").filter(has_text=re.compile("employees?", re.I)).first
            locator.wait_for(state="visible")
            text = locator.inner_text().strip()
            href = locator.get_attribute("href") or ""
            decoded = self._deep_unquote(href)
            company_id = self._extract_company_id(decoded) or self._extract_company_id(company_url)
            people_url = self._extract_people_url(decoded, href, company_id=company_id)
            return {
                "employee_text": text,
                "employee_count": self._parse_employee_count(text),
                "company_id": company_id,
                "people_url": people_url,
            }
        except PlaywrightTimeoutError:
            return None
        except Exception as exc:
            print(f"⚠️ Employee scrape failed for {company_url}: {exc}")
            return None
        finally:
            self.close()

# =========== LINKEDIN FINDER CORE ===========
REQ_HEADERS = {
    "User-Agent": UA,
    "Accept-Language": "en-US,en;q=0.9,nl;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
ALLOW_PERSON_FALLBACK = True

RE_LINKEDIN_URL = re.compile(
    r'(https?:\/\/(?:www\.)?linkedin\.com\/[^\s"\'<>\]\}\),]+)',
    re.I
)

def _normalize_site_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if not re.match(r"^https?://", url, flags=re.I):
        url = "https://" + url
    parts = urllib.parse.urlsplit(url)
    parts = parts._replace(fragment="")
    return urllib.parse.urlunsplit(parts)

def _clean_and_normalize_linkedin(raw: str) -> str:
    if not raw:
        return ""
    txt = html.unescape(raw)
    m = RE_LINKEDIN_URL.search(txt)
    if not m:
        return ""
    url = m.group(1)
    if url.startswith("//"):
        url = "https:" + url
    try:
        u = urllib.parse.urlsplit(url)
    except Exception:
        return ""
    host = u.netloc.lower()
    if "linkedin.com" not in host:
        return ""
    path = (u.path or "/")
    if ("share" in path) or ("shareArticle" in path) or ("embed" in path):
        return ""
    p = path.lower()
    is_company = ("/company/" in p) or ("/showcase/" in p)
    is_person  = ("/in/" in p)
    if not is_company and not (ALLOW_PERSON_FALLBACK and is_person):
        return ""
    clean_path = re.sub(r"/+$", "", path)
    if is_company:
        parts = [p for p in clean_path.split("/") if p]
        pivot = None
        for marker in ("company", "showcase"):
            if marker in parts:
                pivot = marker
                break
        if pivot:
            idx = parts.index(pivot)
            if idx + 1 < len(parts):
                clean_path = "/" + "/".join(parts[:idx + 2])
            else:
                clean_path = "/" + "/".join(parts[:idx + 1])
    return urllib.parse.urlunsplit(("https", "www.linkedin.com", clean_path, "", ""))

def _extract_candidates_from_html(html_text: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    found = []
    for a in soup.find_all("a", href=True):
        href = urllib.parse.urljoin(base_url, a["href"].strip())
        norm = _clean_and_normalize_linkedin(href)
        if norm:
            found.append(norm)
    for tag in soup.find_all(True):
        for _attr, val in (tag.attrs or {}).items():
            vals = val if isinstance(val, (list, tuple)) else [val]
            for v in vals:
                if isinstance(v, str) and "linkedin.com" in v.lower():
                    absolute = urllib.parse.urljoin(base_url, html.unescape(v.strip()))
                    norm = _clean_and_normalize_linkedin(absolute)
                    if norm:
                        found.append(norm)
    for s in soup.find_all("script", type=lambda t: t and "ld+json" in t.lower()):
        try:
            data = json.loads(s.string or "")
        except Exception:
            continue
        def walk(x):
            if isinstance(x, dict):
                if "sameAs" in x:
                    items = x["sameAs"] if isinstance(x["sameAs"], list) else [x["sameAs"]]
                    for u in items:
                        n = _clean_and_normalize_linkedin(str(u))
                        if n: found.append(n)
                for v in x.values(): walk(v)
            elif isinstance(x, list):
                for i in x: walk(i)
        walk(data)
    for m in RE_LINKEDIN_URL.finditer(html_text):
        n = _clean_and_normalize_linkedin(m.group(0))
        if n:
            found.append(n)
    seen, unique = set(), []
    for u in found:
        if u not in seen:
            unique.append(u); seen.add(u)
    def sort_key(u: str):
        ul = u.lower()
        score = 0
        if "/company/" in ul or "/showcase/" in ul: score -= 10
        if "/in/" in ul: score += 5
        return (score, len(u))
    return sorted(unique, key=sort_key)

def _fetch(url: str) -> requests.Response | None:
    try:
        r = requests.get(url, headers=REQ_HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if 200 <= r.status_code < 400 and r.text:
            return r
    except Exception:
        pass
    return None

COMMON_INTERNAL = [
    "/", "/about", "/about-us", "/over", "/over-ons", "/team", "/contact",
    "/contact-us", "/company", "/who-we-are", "/wie-zijn-wij", "/overons",
    "/ons-team", "/social", "/connect", "/footer"
]

def find_linkedin_on_site(site_url: str) -> str:
    base = _normalize_site_url(site_url)
    if not base:
        return ""
    try:
        parts = urllib.parse.urlsplit(base)
        host_root = f"{parts.scheme}://{parts.netloc}"
    except Exception:
        host_root = base
    queue = [host_root + p for p in COMMON_INTERNAL]
    for url in queue:
        r = _fetch(url)
        if not r:
            continue
        cands = _extract_candidates_from_html(r.text, r.url)
        if cands:
            return cands[0]
        time.sleep(0.2 + random.random() * 0.3)
    return ""

# ----- helper: build Google search formula for a given company cell -----
def build_google_formula(company_cell_a1: str) -> str:
    # One-line USER_ENTERED formula, US-style commas.
    # - Removes " and ' from the name
    # - Strips common company suffixes
    # - Collapses whitespace
    # - No quotes around the company name
    return (
        '=HYPERLINK('
        '"https://www.google.com/search?q=" & '
        'ENCODEURL('
        'REGEXREPLACE('
        'TRIM('
        'REGEXREPLACE('
        # remove punctuation quotes first
        f'SUBSTITUTE(SUBSTITUTE({company_cell_a1},CHAR(34),""),"\'",""),'
        # strip common suffixes
        '"(?i)\\\\b(bv|b\\\\.v\\\\.|nv|n\\\\.v\\\\.|vof|v\\\\.o\\\\.f\\\\.|cv|coöperatie|cooperative|holding|holdings?|groep|group|ltd|limited|inc|inc\\\\.|co\\\\.?|company|ag|sa|gmbh|plc)\\\\b",'
        '""'
        ')'
        '),'
        '"\\\\s+"," "'
        ') & " Netherlands LinkedIn"'
        '),'
        '"🔎 Google: Company + Netherlands + LinkedIn"'
        ')'
    )


# =========== MAIN ===========
def main():
    inspector = LinkedInCompanyInspector()
    try:
        gc = auth_client()
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.worksheet(TAB_NAME)

        meta = get_or_create_meta(sh)
        start_row = meta_get_next_row(meta)

        headers = [h.strip() for h in ws.row_values(1)]
        header_lc = {h.lower(): i+1 for i, h in enumerate(headers)}

        def col_index(*names):
            for n in names:
                idx = header_lc.get(n.lower())
                if idx: return idx
            return None

        col_company    = col_index(COL_COMPANY, "Name", "Company Name")
        col_website    = col_index("Company's website", "Website", "Site", "URL")
        col_linkedin   = col_index(COL_LINKEDIN, "Find LinkedIn", "Company LinkedIn")
        col_status     = col_index(COL_STATUS)
        col_checked    = col_index(COL_LAST_CHECKED)
        col_emp_count  = col_index(COL_EMP_COUNT)
        col_emp_link   = col_index(COL_EMP_LINK)
        col_company_id = col_index(COL_COMPANY_ID)

        if not col_website:
            err_exit("Could not find a website column.")
        if not col_company:
            err_exit("Could not find a Company column.")

        # Ensure LinkedIn/Status/Checked columns exist
        need_updates = []
        def ensure_column(current_idx, header_label):
            nonlocal need_updates
            if current_idx:
                return current_idx
            ws.add_cols(1)
            idx = ws.col_count
            need_updates.append((1, idx, header_label))
            return idx

        col_linkedin   = ensure_column(col_linkedin, COL_LINKEDIN)
        col_status     = ensure_column(col_status, COL_STATUS)
        col_checked    = ensure_column(col_checked, COL_LAST_CHECKED)
        col_emp_count  = ensure_column(col_emp_count, COL_EMP_COUNT)
        col_emp_link   = ensure_column(col_emp_link, COL_EMP_LINK)
        col_company_id = ensure_column(col_company_id, COL_COMPANY_ID)

        if need_updates:
            for r, c, v in need_updates:
                ws.update_cell(r, c, v)
            headers = [h.strip() for h in ws.row_values(1)]
            header_lc = {h.lower(): i+1 for i, h in enumerate(headers)}

        # Read all values once
        values = ws.get_all_values()
        if not values:
            print("No data.")
            return

        # Build candidate rows list (wrap from pointer)
        all_rows = list(range(start_row, ws.row_count + 1)) + list(range(2, start_row))
        work_rows, processed = [], 0
        for r in all_rows:
            if r > len(values):  # outside filled area
                continue
            row = values[r-1]
            def v(col):
                return (row[col-1] if col and col-1 < len(row) else "").strip()
            website      = v(col_website)
            linkedin_val = v(col_linkedin)
            status_val   = v(col_status)
            emp_count_v  = v(col_emp_count)
            emp_link_v   = v(col_emp_link)
            company_id_v = v(col_company_id)
            has_formula  = linkedin_val.startswith("=")
            has_real_li  = bool(linkedin_val) and not has_formula and "linkedin.com" in linkedin_val.lower()
            is_none      = status_val == STATUS_NONE
            needs_link_lookup = (not has_real_li) and not is_none
            employee_missing = not (emp_count_v and emp_link_v and company_id_v)
            needs_backfill = has_real_li and employee_missing

            if not website:
                continue
            if not needs_link_lookup and not needs_backfill:
                continue
            work_rows.append(r)
            processed += 1
            if processed >= BATCH_SIZE:
                break

        if not work_rows:
            print("Nothing to update.")
            meta_set_next_row(meta, start_row if start_row <= 3 else 2)
            return

        updates = []
        def queue_cell(row_num: int, col_num: int, value):
            if not col_num or value is None:
                return
            a1 = gspread.utils.rowcol_to_a1(row_num, col_num)
            updates.append({"range": f"{ws.title}!{a1}", "values": [[value]]})

        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        last_touched = work_rows[-1]

        for r in work_rows:
            row = values[r-1]
            def v(col):
                return (row[col-1] if col and col-1 < len(row) else "").strip()
            website      = v(col_website)
            linkedin_val = v(col_linkedin)
            status_val   = v(col_status)
            emp_count_v  = v(col_emp_count)
            emp_link_v   = v(col_emp_link)
            company_id_v = v(col_company_id)
            has_formula  = linkedin_val.startswith("=")
            has_real_li  = bool(linkedin_val) and not has_formula and "linkedin.com" in linkedin_val.lower()
            is_none      = status_val == STATUS_NONE
            employee_missing = not (emp_count_v and emp_link_v and company_id_v)

            company_cell = gspread.utils.rowcol_to_a1(r, col_company)  # e.g., A12
            print(f"[{r}] checking {website} …")
            li = linkedin_val if has_real_li else ""
            looked_up = False

            if not has_real_li and not is_none:
                li = find_linkedin_on_site(website)
                looked_up = True
                if li:
                    queue_cell(r, col_linkedin, li)
                    queue_cell(r, col_status, STATUS_FOUND)
                    print(f"  -> {li}")
                    has_real_li = True
                else:
                    formula = build_google_formula(company_cell)
                    queue_cell(r, col_linkedin, formula)
                    queue_cell(r, col_status, STATUS_NONE)
                    queue_cell(r, col_emp_count, "")
                    queue_cell(r, col_emp_link, "")
                    queue_cell(r, col_company_id, "")
                    print("  -> (none) — inserted Google search formula")
                    queue_cell(r, col_checked, now)
                    time.sleep(0.25)
                    continue

            if has_real_li and (employee_missing or looked_up):
                stats = inspector.fetch(li)
                if stats:
                    count_value = stats.get("employee_count")
                    if count_value is None:
                        count_value = stats.get("employee_text")
                    queue_cell(r, col_emp_count, count_value or "")
                    queue_cell(r, col_emp_link, stats.get("people_url") or "")
                    queue_cell(r, col_company_id, stats.get("company_id") or "")
                    print(f"  -> employees: {stats.get('employee_text') or stats.get('employee_count','?')} | people link: {stats.get('people_url','')}")
                else:
                    print("  -> employee stats unavailable (Playwright or page issue).")

            queue_cell(r, col_checked, now)
            time.sleep(0.25)

        if updates:
            ws.spreadsheet.values_batch_update({"valueInputOption": "USER_ENTERED", "data": updates})
            print(f"✅ wrote {len(work_rows)} rows (URL or formula + status + timestamp).")

        # Advance pointer
        next_row = last_touched + 1
        if next_row > len(values):
            next_row = 2
        meta_set_next_row(meta, next_row)
    finally:
        inspector.close()

# Optional: --loop mode
# Optional: loop + reset controls
if __name__ == "__main__":
    import argparse, os, sys, time
    import gspread
    from google.oauth2 import service_account

    p = argparse.ArgumentParser()
    p.add_argument("--loop", action="store_true", help="keep running forever")
    p.add_argument("--interval", type=int, default=300, help="seconds between passes in --loop mode")
    p.add_argument("--reset-pointer", action="store_true",
                   help="set _meta.next_row back to 2 (start from top)")
    p.add_argument("--reset-hard", action="store_true",
                   help="also clear LinkedIn status/Last checked (does NOT touch company/website)")
    args = p.parse_args()

    # Run resets if requested
    if args.reset_pointer or args.reset_hard:
        creds = service_account.Credentials.from_service_account_file(
            os.path.expanduser(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "~/sheet-bot-key.json")),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)

        meta = get_or_create_meta(sh)
        meta_set_next_row(meta, 2)
        print("🔁 Pointer reset to row 2.")

        if args.reset_hard:
            ws = sh.worksheet(TAB_NAME)
            headers = [h.strip() for h in ws.row_values(1)]
            hdr = {h.lower(): i+1 for i, h in enumerate(headers)}

            def idx(*names):
                for n in names:
                    i = hdr.get(n.lower())
                    if i:
                        return i

            col_linkedin   = idx("LinkedIn", "Find LinkedIn", "Company LinkedIn")
            col_status     = idx("LinkedIn status")
            col_checked    = idx("Last checked")
            col_emp_count  = idx(COL_EMP_COUNT)
            col_emp_link   = idx(COL_EMP_LINK)
            col_company_id = idx(COL_COMPANY_ID)

            # clear values in these columns (rows 2..last)
            vals = ws.get_all_values()
            last_row = max(2, len(vals))
            clears = []
            def clear_col(ci):
                if not ci: return
                start = gspread.utils.rowcol_to_a1(2, ci)
                end   = gspread.utils.rowcol_to_a1(last_row, ci)
                clears.append({"range": f"{ws.title}!{start}:{end}",
                               "values": [[""]]*(last_row-1)})
            clear_col(col_linkedin)
            clear_col(col_status)
            clear_col(col_checked)
            clear_col(col_emp_count)
            clear_col(col_emp_link)
            clear_col(col_company_id)

            if clears:
                ws.spreadsheet.values_batch_update({"valueInputOption": "RAW", "data": clears})
                print("🧹 Cleared LinkedIn/status/employee columns.")

        # if we only wanted to reset and not loop, stop here
        if not args.loop:
            sys.exit(0)

    # Normal execution
    if args.loop:
        while True:
            try:
                main()
            except Exception as e:
                print("Run error:", e)
            time.sleep(max(5, args.interval))
    else:
        main()
