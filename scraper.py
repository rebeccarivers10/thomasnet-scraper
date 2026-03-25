"""
thomasnet.com Sponsored Ad Scraper
Usage: python scraper.py "CNC MACHINE"
       python scraper.py "injection molding" --all   (include organic results too)
       python scraper.py "hydraulic pumps" --output results.csv
"""

import argparse
import csv
import re
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


BASE_URL = "https://www.thomasnet.com/suppliers/search"


def build_url(search_term: str) -> str:
    encoded = urllib.parse.quote(search_term)
    return f"{BASE_URL}?searchterm={encoded}"


def get_driver(headless: bool = False) -> uc.Chrome:
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )
    if headless:
        options.add_argument("--headless=new")
    driver = uc.Chrome(options=options, version_main=145)
    return driver


def wait_for_results(driver: uc.Chrome, timeout: int = 30) -> bool:
    """Wait for the results list to load. Returns True if found."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "ul[data-ref='srp.results']"))
        )
        return True
    except Exception:
        return False


def extract_text(element, selector: str, default: str = "") -> str:
    """Find a child element by CSS selector and return its stripped text."""
    found = element.select_one(selector)
    return found.get_text(strip=True) if found else default


def extract_icon_label(soup_card, icon_name: str) -> str:
    """
    thomasnet uses <l-icon name="user-group"> etc. followed by a sibling .txt-label.
    Since BeautifulSoup doesn't handle custom elements natively, we search by
    the icon's name attribute, then grab the adjacent label span.
    """
    icon = soup_card.find("l-icon", attrs={"name": icon_name})
    if not icon:
        return ""
    # The label is typically the next sibling span with class txt-label
    sibling = icon.find_next_sibling(class_="txt-label")
    if sibling:
        return sibling.get_text(strip=True)
    # Fallback: check parent's text excluding the icon
    parent = icon.parent
    if parent:
        return parent.get_text(strip=True)
    return ""


def parse_card(li_element) -> dict:
    """Parse a single supplier <li> card into a dict."""
    soup = BeautifulSoup(str(li_element.get_attribute("outerHTML")), "html.parser")
    card = soup.find("li")

    # Sponsored flag
    sponsored_badge = card.select_one("[class*='sponsoredText']")
    is_sponsored = sponsored_badge is not None and "Sponsored" in sponsored_badge.get_text()

    # Company name
    name_btn = card.select_one("button[data-testid='supplier-name-link']")
    company_name = name_btn.get_text(strip=True) if name_btn else ""

    # Website URL
    visit_link = card.select_one("a[rel='nofollow'][class*='visitWebsiteButton']")
    website = visit_link.get("href", "") if visit_link else ""

    # Location (first location link)
    loc_link = card.select_one("a[data-testid='srp.supplier-location-link'] span.txt-underline")
    location = loc_link.get_text(strip=True) if loc_link else ""

    # Metadata via icon labels
    employees = extract_icon_label(card, "user-group")
    revenue = extract_icon_label(card, "landmark")
    year_established = extract_icon_label(card, "calendar")
    industry_type = extract_icon_label(card, "industry")

    # Description
    desc_el = card.select_one("[data-sentry-component='TrimmedDescription'] p")
    description = desc_el.get_text(strip=True) if desc_el else ""

    # Capability tags — use span to avoid matching wrapper divs, then deduplicate
    seen = {}
    for t in card.select("span[class*='tag-with-check_tag']"):
        text = t.get_text(strip=True)
        if text:
            seen[text] = None  # dict preserves insertion order, drops duplicates
    tags_str = "; ".join(seen.keys())

    return {
        "sponsored": is_sponsored,
        "company_name": company_name,
        "website": website,
        "location": location,
        "employees": employees,
        "revenue": revenue,
        "year_established": year_established,
        "industry_type": industry_type,
        "description": description,
        "tags": tags_str,
    }


def scrape(search_term: str, sponsored_only: bool = True, headless: bool = False) -> list[dict]:
    url = build_url(search_term)
    print(f"Navigating to: {url}")

    driver = get_driver(headless=headless)
    results = []

    try:
        driver.get(url)

        # Give DataDome's JS challenge time to resolve before checking for results
        time.sleep(5)

        if not wait_for_results(driver):
            print("ERROR: Results list did not load. The page may be blocked or have changed.")
            print("       Try running without --headless if you added that flag.")
            return results

        # Let dynamic content settle fully
        time.sleep(3)

        # Grab all supplier cards
        cards = driver.find_elements(By.CSS_SELECTOR, "li[data-sentry-component='SearchResultSupplier']")
        print(f"Found {len(cards)} supplier cards total.")

        for card in cards:
            data = parse_card(card)
            if sponsored_only and not data["sponsored"]:
                continue
            results.append(data)

        sponsored_count = sum(1 for r in results if r["sponsored"])
        print(f"Extracted {len(results)} results ({sponsored_count} sponsored).")

    finally:
        driver.quit()

    return results


# ---------------------------------------------------------------------------
# Contact enrichment
# ---------------------------------------------------------------------------

# Common paths to try for contact info, in priority order
_CONTACT_PATHS = ["/contact", "/contact-us", "/contacts", "/contact-us/", "/about", "/about-us"]

# Emails: standard pattern; we'll filter out obvious non-contacts afterward
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# US phone numbers with separators (requires at least one separator between groups)
_PHONE_RE = re.compile(
    r"(?:\+?1[\s.\-]?)?"           # optional country code
    r"\(?\d{3}\)?"                  # area code
    r"[\s.\-]"                      # separator
    r"\d{3}"                        # exchange
    r"[\s.\-]"                      # separator
    r"\d{4}"                        # subscriber
)

# Email domains to ignore (JS libraries, image names, tracker domains, etc.)
_IGNORED_EMAIL_DOMAINS = {
    "example.com", "domain.com", "yourcompany.com", "company.com",
    "email.com", "test.com", "sentry.io", "sentry-cdn.com",
    "w3.org", "schema.org", "google.com", "googleapis.com",
    "jquery.com", "bootstrapcdn.com", "cloudflare.com",
}

_ENRICH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _fetch_text(url: str, session: requests.Session, timeout: int = 10) -> str:
    """Fetch a URL and return its visible text (via BeautifulSoup), empty string on failure."""
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", ""):
            soup = BeautifulSoup(r.text, "html.parser")
            # Remove scripts/styles so regex doesn't catch JS strings
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            return soup.get_text(" ")
    except Exception:
        pass
    return ""


def _clean_emails(raw: list[str], site_domain: str = "") -> list[str]:
    """
    Return cleaned, deduplicated emails.
    When site_domain is provided, emails matching that domain are returned first
    and emails from other domains are only included if no on-domain email was found.
    This prevents partner/distributor emails scraped from the page from being
    attributed to the wrong company.
    """
    seen: set[str] = set()
    on_domain: list[str] = []
    off_domain: list[str] = []

    for e in raw:
        e = e.lower().strip(".")
        domain = e.split("@")[-1]
        if domain in _IGNORED_EMAIL_DOMAINS:
            continue
        if any(e.endswith(ext) for ext in (".png", ".jpg", ".gif", ".js", ".css", ".svg")):
            continue
        if e in seen:
            continue
        seen.add(e)
        if site_domain and site_domain in domain:
            on_domain.append(e)
        else:
            off_domain.append(e)

    # Prefer domain-matched emails; fall back to others only if none found
    result = on_domain if on_domain else off_domain
    return result[:5]


def _clean_phones(raw: list[str]) -> list[str]:
    seen, out = set(), []
    for p in raw:
        digits = re.sub(r"\D", "", p)
        # Must be 10 digits (US) or 11 starting with 1
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) != 10:
            continue
        if digits not in seen:
            seen.add(digits)
            # Format as (XXX) XXX-XXXX
            out.append(f"({digits[:3]}) {digits[3:6]}-{digits[6:]}")
    return out[:3]


def enrich_contact(website_url: str) -> dict:
    """
    Visit the supplier's website (and common contact-page paths) and extract
    any email addresses and phone numbers found in visible text.

    Returns {"emails": [...], "phones": [...]}
    """
    if not website_url or not website_url.startswith("http"):
        return {"emails": [], "phones": []}

    parsed = urlparse(website_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    session = requests.Session()
    session.headers.update(_ENRICH_HEADERS)

    all_text = ""

    # Main page
    all_text += _fetch_text(website_url, session)

    # Try contact/about pages until one loads
    for path in _CONTACT_PATHS:
        contact_url = urljoin(base, path)
        text = _fetch_text(contact_url, session)
        if text:
            all_text += " " + text
            break  # one contact page is enough

    site_domain = parsed.netloc.lower().replace("www.", "")
    emails = _clean_emails(_EMAIL_RE.findall(all_text), site_domain=site_domain)
    phones = _clean_phones(_PHONE_RE.findall(all_text))

    return {"emails": emails, "phones": phones}


def save_csv(results: list[dict], output_path: str) -> None:
    if not results:
        print("No results to save.")
        return

    fieldnames = [
        "sponsored", "company_name", "website", "location",
        "employees", "revenue", "year_established", "industry_type",
        "description", "tags", "emails", "phones",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"Saved {len(results)} rows to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Scrape thomasnet.com supplier search results.")
    parser.add_argument("search_term", help='Search term, e.g. "CNC MACHINE"')
    parser.add_argument(
        "--all", dest="include_all", action="store_true",
        help="Include organic (non-sponsored) results in addition to sponsored ads"
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run Chrome headlessly (may be blocked by DataDome CAPTCHA)"
    )
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output CSV filename (default: auto-generated with timestamp)"
    )
    args = parser.parse_args()

    sponsored_only = not args.include_all

    if args.output:
        output_path = args.output
    else:
        safe_term = args.search_term.replace(" ", "_").replace("/", "-")[:40]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"thomasnet_{safe_term}_{timestamp}.csv"

    results = scrape(args.search_term, sponsored_only=sponsored_only, headless=args.headless)
    save_csv(results, output_path)


if __name__ == "__main__":
    main()
