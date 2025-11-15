import os
import re
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# === CONFIG ===
CREDS_PATH = os.path.expanduser("~/sheet-bot-key.json")

SOURCE_SHEET_ID = "1KrKoeun-h6eEzSK6-cc4_MUHriuabg8GyAR9b8NR7fc"
SOURCE_TAB_NAME = "Ultra_validated"

DEST_SHEET_ID = "1YF8WvLTPu-Raf22rHaaphauj-w-n64CRu83ZeAtHYAc"
DEST_TABS = {
    "Alpha": "Alpha",
    "Beta": "Beta",
    "Gamma": "Gamma",
    "Hydro": "Hydro"
}

# === KEYWORD CATEGORIES ===
CATEGORY_1 = [
    "capital", "asset", "fund", "wealth", "beheer", "vermogensbeheer", "investment",
    "family office", "hedge", "private equity", "reit", "real estate investment", "venture capital"
]

CATEGORY_3 = [
    "hotel", "restaurant", "ngo", "church", "mosque", "temple", "event", "catering", "hostel",
    "worship", "conference", "festival", "kerk", "moskee", "evenement", "geloof",
    "nonprofit", "non-profit"
]

# === SCORING RULES ===
ALPHA_SCORE_THRESHOLD = 5.0
BETA_SCORE_THRESHOLD = 4.5


# ----------------------------
#   Extract Website Content
# ----------------------------
def extract_visible_text(url):
    """Fetch visible text content from the website."""
    try:
        url = url.strip()
        if not url.startswith("http"):
            url = "https://" + url

        response = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.9"
        })

        if not (200 <= response.status_code < 400):
            return ""

        soup = BeautifulSoup(response.text, "html.parser")

        # remove junk
        for tag in soup(["script", "style", "noscript", "header", "footer"]):
            tag.decompose()

        text = soup.get_text(separator=" ", strip=True).lower()
        return re.sub(r"\s+", " ", text)
    except:
        return ""


# ----------------------------
#   Keyword Category From Content
# ----------------------------
def keyword_category_from_content(url):
    text = extract_visible_text(url)
    if not text:
        return 2  # fallback neutral

    if any(kw in text for kw in CATEGORY_3):
        return 3

    if any(kw in text for kw in CATEGORY_1):
        return 1

    return 2


# ----------------------------
#   Team Size Scoring
# ----------------------------
def score_team_size(team_size):
    try:
        n = int(team_size)
        if n <= 15:
            return 1.5
        elif 60 <= n <= 260:
            return 1.0
        else:
            return 0.0
    except:
        return 0.0


# ----------------------------
#   Classification Logic
# ----------------------------
def classify(score, team_size, linkedin_url):
    if linkedin_url.startswith("="):
        return "Hydro"   # formula = no real LinkedIn detected

    if score >= ALPHA_SCORE_THRESHOLD and team_size <= 15:
        return "Alpha"

    if score >= BETA_SCORE_THRESHOLD and 60 <= team_size <= 260:
        return "Beta"

    return "Gamma"


# === AUTH ===
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scopes)
client = gspread.authorize(creds)

# === READ SOURCE ===
source_ws = client.open_by_key(SOURCE_SHEET_ID).worksheet(SOURCE_TAB_NAME)
source_data = source_ws.get_all_records()

dest_book = client.open_by_key(DEST_SHEET_ID)

results = {
    "Alpha": [],
    "Beta": [],
    "Gamma": [],
    "Hydro": []
}

# === PROCESS ROWS ===
for row in source_data:

    # clean getter
    def get_str(key):
        val = row.get(key, "")
        return str(val).strip() if val is not None else ""

    name = get_str("name")
    website = get_str("website")
    linkedin = get_str("LinkedIn")
    people_link = get_str("LinkedIn people link")

    team_raw = row.get("LinkedIn employees", "")
    try:
        team_size = int(str(team_raw).strip())
    except:
        team_size = 0

    # Skip incomplete rows
    if not all([name, website, linkedin, people_link, team_size]):
        continue

    # Website content keyword category
    kw_cat = keyword_category_from_content(website)

    if kw_cat == 3:
        results["Gamma"].append([name, website, linkedin, team_size, people_link])
        continue

    score = 0
    if kw_cat == 1:
        score += 3.5

    score += score_team_size(team_size)

    label = classify(score, team_size, linkedin)
    results[label].append([name, website, linkedin, team_size, people_link])


# === WRITE TO DESTINATION TABS ===
for label, rows in results.items():
    if not rows:
        continue

    try:
        ws = dest_book.worksheet(DEST_TABS[label])
    except:
        ws = dest_book.add_worksheet(title=DEST_TABS[label], rows="3000", cols="10")
        ws.append_row(["name", "website", "LinkedIn", "LinkedIn employees", "LinkedIn people link"])

    ws.append_rows(rows, value_input_option="USER_ENTERED")

print("======================================================")
print("  SCORING & CLASSIFICATION COMPLETE")
print("======================================================")
print(f" Alpha: {len(results['Alpha'])}")
print(f" Beta : {len(results['Beta'])}")
print(f" Gamma: {len(results['Gamma'])}")
print(f" Hydro: {len(results['Hydro'])}")
print("======================================================")
