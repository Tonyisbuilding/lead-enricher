import os
import gspread
from google.oauth2.service_account import Credentials

# === CONFIG ===
CREDS_PATH = os.path.expanduser("~/sheet-bot-key.json")

SOURCE_SHEET_ID = "1KrKoeun-h6eEzSK6-cc4_MUHriuabg8GyAR9b8NR7fc"
SOURCE_TAB_NAME = "Ultra_validated"

DEST_SHEET_ID = "1YF8WvLTPu-Raf22rHaaphauj-w-n64CRu83ZeAtHYAc"

# Updated with all your new categories
DEST_TABS = {
    "Alpha": "Alpha",
    "Beta": "Beta",
    "Gamma": "Gamma",
    "I-gamma": "I-gamma",
    "pBin": "pBin",
    "Hydro": "Hydro"
}

def classify(kw_status: str, team_size: int, linkedin_url: str) -> str:
    """
    Applies the new classification logic based on keyword status and team size.
    """
    # 1. Check for Hydro (broken links) first
    if linkedin_url.startswith("="):
        return "Hydro"
    
    # 2. Check for pBin (all cat 2)
    if kw_status == "cat 2":
        return "pBin"

    # 3. Apply the waterfall logic for all "cat 1"
    if kw_status == "cat 1":
        if team_size < 15:
            return "Alpha"
        elif team_size < 65:
            return "Beta"
        elif team_size < 250:
            return "Gamma"
        else: # team_size >= 250
            return "I-gamma"
            
    # 4. If not Hydro, cat 1, or cat 2 (e.g., blank, "ERROR"),
    #    put it in the pBin for review.
    return "pBin"


# === AUTH ===
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scopes)
client = gspread.authorize(creds)

source_ws = client.open_by_key(SOURCE_SHEET_ID).worksheet(SOURCE_TAB_NAME)

# Get all data as a list of lists, skipping the header row (row 1)
source_data = source_ws.get_all_values()[1:] 
dest_book = client.open_by_key(DEST_SHEET_ID)

# Updated results dictionary
results = {
    "Alpha": [],
    "Beta": [],
    "Gamma": [],
    "I-gamma": [],
    "pBin": [],
    "Hydro": []
}

# Your column indexes from the screenshot:
# A = 0, B = 1, C = 2, D = 3, E = 4
# *** ASSUMING Keyword Status is in F = 5 ***
for row in source_data:
    try:
        name = str(row[0]).strip()           # Column A
        website = str(row[1]).strip()       # Column B
        linkedin = str(row[2]).strip()      # Column C
        team_raw = row[3]                   # Column D
        people_link = str(row[4]).strip()   # Column E
        kw_status = str(row[5]).strip().lower() # !! ASSUMING COLUMN F (index 5) !!
    except IndexError:
        # Skips rows that are too short or empty
        continue

    # Try to convert team size, default to 0 if blank
    try:
        team_size = int(str(team_raw).strip())
    except:
        team_size = 0

    # We only skip rows if they are truly empty
    if not all([name, website, linkedin, people_link]):
        continue
    
    # --- NO SCRAPING, NO SCORING ---
    # Just classify based on the pre-filled data
    label = classify(kw_status, team_size, linkedin)
    
    # Add the row to the correct list for batch updating
    results[label].append([name, website, linkedin, team_size, people_link])


# === WRITE TO DESTINATION TABS ===
for label, rows in results.items():
    if not rows:
        continue
    try:
        ws = dest_book.worksheet(DEST_TABS[label])
    except gspread.exceptions.WorksheetNotFound:
        # Fixed typo: add_worksheet
        ws = dest_book.add_worksheet(title=DEST_TABS[label], rows="3000", cols="10")
        ws.append_row(["name", "website", "LinkedIn", "LinkedIn employees", "LinkedIn people link"])
    
    ws.append_rows(rows, value_input_option="USER_ENTERED")

# === FINAL REPORT ===
print("======================================================")
print("  SCORING & CLASSIFICATION COMPLETE")
print("======================================================")
# Print all new categories
for k in DEST_TABS.keys():
    print(f" {k:<8}: {len(results[k])}")
print("======================================================")