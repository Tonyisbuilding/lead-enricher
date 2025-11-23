import os
from typing import List, Tuple

import gspread
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

# Keywords to look for
KEYWORDS = [
    "capital",
    "fund",
    "asset",
    "wealth",
    "investment",
    "partners",
    "equity",
    "interest rates",
    "mortgage",
]

# Sheet config (override via env vars)
CREDS_PATH = os.path.expanduser(
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "~/sheet-bot-key.json")
)
SHEET_ID = os.environ.get(
    "TEST_SHEET_ID",
    os.environ.get("SHEET_ID", "1KrKoeun-h6eEzSK6-cc4_MUHriuabg8GyAR9b8NR7fc"),
)
TAB_NAME = os.environ.get("TEST_TAB_NAME", "Ultra_validated")
WEBSITE_COLUMN = os.environ.get("TEST_WEBSITE_COLUMN", "website")
STATUS_COLUMN = os.environ.get("TEST_STATUS_COLUMN", "Keyword status")
MAX_ROWS = int(os.environ.get("TEST_MAX_ROWS", "0"))  # 0 = no limit

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def load_sheet_data():
    """Return worksheet, status column index, and list of (row_idx, url)."""
    if not SHEET_ID:
        return None, None, []
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPES)
    client = gspread.authorize(creds)
    ws = client.open_by_key(SHEET_ID).worksheet(TAB_NAME)

    headers = [h.strip() for h in ws.row_values(1)]
    header_map = {h.lower(): idx + 1 for idx, h in enumerate(headers)}

    website_col = header_map.get(WEBSITE_COLUMN.lower())
    if not website_col:
        raise RuntimeError(f"Website column '{WEBSITE_COLUMN}' not found in sheet.")

    status_col = header_map.get(STATUS_COLUMN.lower())
    if not status_col:
        ws.add_cols(1)
        status_col = ws.col_count
        ws.update_cell(1, status_col, STATUS_COLUMN)
        add_dropdown_validation(ws, status_col)
    else:
        add_dropdown_validation(ws, status_col)

    values = ws.get_all_values()
    rows: List[Tuple[int, str]] = []
    for row_idx in range(2, len(values) + 1):
        row = values[row_idx - 1]
        cell = row[website_col - 1] if website_col - 1 < len(row) else ""
        url = cell.strip()
        if not url:
            continue
        rows.append((row_idx, url))
        if MAX_ROWS and len(rows) >= MAX_ROWS:
            break
    return ws, status_col, rows


def add_dropdown_validation(ws, col_idx):
    """Apply Cat 1/Cat 2 dropdown validation to the status column."""
    try:
        ws.spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "setDataValidation": {
                            "range": {
                                "sheetId": ws.id,
                                "startRowIndex": 1,
                                "startColumnIndex": col_idx - 1,
                                "endRowIndex": ws.row_count,
                                "endColumnIndex": col_idx,
                            },
                            "rule": {
                                "condition": {
                                    "type": "ONE_OF_LIST",
                                    "values": [
                                        {"userEnteredValue": "Cat 1"},
                                        {"userEnteredValue": "Cat 2"},
                                    ],
                                },
                                "showCustomUi": True,
                                "strict": True,
                            },
                        }
                    }
                ]
            }
        )
    except Exception:
        pass


def keyword_check(url: str):
    try:
        response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True).lower()
        word_count = len(text.split())
        found = [kw for kw in KEYWORDS if kw in text]
        return {
            "found": found,
            "word_count": word_count,
            "js_rendered": word_count < 100,
        }
    except Exception as exc:
        return {
            "found": [],
            "word_count": 0,
            "js_rendered": False,
            "error": str(exc),
        }


def main():
    ws, status_col, rows = load_sheet_data()
    if not rows:
        print("⚠️ No sheet URLs found; provide TEST_SHEET_ID/TAB_NAME or ensure data exists.")
        return

    updates = []
    for row_idx, url in rows:
        result = keyword_check(url)
        print(url)
        if "error" in result:
            print(f"❌ Error: {result['error']}")
            status = ""
        else:
            print(f"→ Found: {result['found']}")
            print(f"→ Word count: {result['word_count']}")
            if result["js_rendered"]:
                print("⚠️ Possibly JS-rendered (needs Selenium for full content)")
            status = "Cat 1" if result["found"] else "Cat 2"
        a1 = rowcol_to_a1(row_idx, status_col)
        updates.append({"range": f"{ws.title}!{a1}", "values": [[status]]})
        print("-" * 60)

    if updates:
        ws.spreadsheet.values_batch_update(
            {"valueInputOption": "USER_ENTERED", "data": updates}
        )
        print(f"✅ Updated {len(updates)} rows in '{STATUS_COLUMN}'.")


if __name__ == "__main__":
    main()
