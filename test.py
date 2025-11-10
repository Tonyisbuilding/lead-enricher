from playwright.sync_api import sync_playwright
import re, urllib.parse

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    url = "https://www.linkedin.com/company/groupe-atland/"
    page.goto(url)
    page.wait_for_selector("a:has-text('employees')")

    # Get the visible text
    employee_text = page.locator("a:has-text('employees')").first.inner_text()

    # Get the href link (redirects to signup)
    employee_link = page.locator("a:has-text('employees')").first.get_attribute("href")

    # Decode the redirect URL
    decoded_url = urllib.parse.unquote(employee_link or "")

    # Extract company ID from inside the redirect
    id_match = re.search(r'facetCurrentCompany=%5B(\d+)%5D', decoded_url) or \
               re.search(r'facetCurrentCompany=\[(\d+)\]', decoded_url)

    company_id = id_match.group(1) if id_match else "Not Found"

    print(f"✅ Employee Info: {employee_text}")
    print(f"🔗 Link to people: {employee_link}")
    print(f"🏷️  Company ID: {company_id}")

    browser.close()
