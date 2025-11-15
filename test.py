import requests
from bs4 import BeautifulSoup

# Category 1 Keywords (adjust as needed)
KEYWORDS = ["capital", "fund", "asset", "wealth", "investment", "partners", "equity"]

# Sample list (replace with actual rows from Gamma)
gamma_websites = [
    "http://www.hollandcapital.nl/",
    "https://www.morganstanley.com/about-us/global-offices/netherlands",
    "http://www.ffadministraties.nl/",
    "https://iqeq.com/nl/",
    "http://www.waterland.co.nl/",
]

def keyword_check(url):
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text(separator=' ', strip=True).lower()
        found = [kw for kw in KEYWORDS if kw.lower() in text]
        return found
    except Exception as e:
        return f"❌ Error: {e}"

# Run test
for site in gamma_websites:
    result = keyword_check(site)
    print(f"{site}\n→ Found: {result}\n")
