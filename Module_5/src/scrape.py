"""
Scrape GradCafe survey pages into a JSON dataset.

This module uses robots.txt checks, page iteration, and BeautifulSoup
to extract structured applicant records from the public survey pages.
"""

import ssl
from urllib import parse, robotparser, error, request
import re
import json
import time
from bs4 import BeautifulSoup

try:
    import certifi
except ImportError:  # pragma: no cover - optional dependency
    certifi = None


URL = "https://www.thegradcafe.com/"
USER_AGENT = "Mozilla/5.0 (compatible; zhang/1.0)"
AGENT = "zhang"
OUTPUT_FILE = "applicant_data.json"


def _build_ssl_context():
    """
    Build a TLS context for HTTPS fetches.

    Prefer certifi's CA bundle when available so macOS/Homebrew Python
    environments without system trust integration can still verify certs.
    """

    cafile = certifi.where() if certifi is not None else None
    if cafile:
        return ssl.create_default_context(cafile=cafile)
    return ssl.create_default_context()


def _urlopen_with_tls(req):
    """
    Open a URL request using the module's TLS context.

    Falls back to the one-argument urlopen signature for test doubles that
    do not accept the ``context`` keyword.
    """

    context = _build_ssl_context()
    try:
        return request.urlopen(req, context=context)
    except TypeError:
        return request.urlopen(req)


def _is_cert_verification_error(err):
    """
    Return True when a URLError wraps a certificate verification failure.
    """

    reason = getattr(err, "reason", None)
    if isinstance(reason, ssl.SSLCertVerificationError):
        return True
    if isinstance(reason, ssl.SSLError):
        return "CERTIFICATE_VERIFY_FAILED" in str(reason)
    return False


def _fetch_text(url):
    """
    Fetch UTF-8 text content from a URL using module TLS settings.
    """

    req = request.Request(url, headers={"User-Agent": USER_AGENT})
    with _urlopen_with_tls(req) as response:
        return response.read().decode("utf-8", errors="replace")


def url_check():
    """
    Create and initialize a robots.txt parser for the GradCafe site.

    :returns: A configured :class:`robotparser.RobotFileParser` instance.
    """

    # Join robots.txt URL and run it through the parser
    parser = robotparser.RobotFileParser()
    robots_url = parse.urljoin(URL, "robots.txt")
    parser.set_url(robots_url)

    # Read robots.txt with default parser behavior first, then retry once
    # with an explicit CA bundle on certificate verification failures.
    try:
        parser.read()
    except error.URLError as err:
        if not _is_cert_verification_error(err):
            raise
        print("Fetch results: robots.txt TLS verify failed; retrying with CA bundle")
        robots_text = _fetch_text(robots_url)
        parser.parse(robots_text.splitlines())

    # Print that robots.txt was checked
    print("Fetch results: robots.txt checked")

    # Return parser to check URLs
    return parser


def check_url (page_url, parser):
    """
    Fetch a page if allowed by robots.txt and return a BeautifulSoup object.

    :param page_url: Page URL to fetch.
    :param parser: Robots parser from :func:`url_check`.
    :returns: :class:`bs4.BeautifulSoup` or None if blocked/error.
    """

    # Check if robots.txt allows for agent to fetch the URL (T/F)
    allowed = parser.can_fetch(USER_AGENT, page_url)
    print(f"Fetch results: robots.txt check - {allowed}")

    # If not allowed to fetch, then do not scrape URL
    if not allowed:
        print("Fetch results: NOT allowed to fetch URL")
        return None

    # Review of any HTTP errors
    try:
        # Create user header to avoid 403 error when scrapping
        new_header = request.Request(page_url,
            headers={"User-Agent": USER_AGENT})

        # Requests and decodes the HTML
        with _urlopen_with_tls(new_header) as response:
            html = response.read().decode("utf-8")

        # Converting HTML into object
        soup = BeautifulSoup(html, "html.parser")
        return soup

    # Print any errors that occurred
    except (error.HTTPError, error.URLError) as err:
        print(f"An error has occurred - {err}")
        return None


def _empty_record():
    """Return a new record skeleton."""
    return {
        "program_name": None,
        "university": None,
        "masters_or_phd": None,
        "comments": None,
        "date_added": None,
        "url": None,
        "applicant_status": None,
        "decision_date": None,
        "semester_year_start": None,
        "citizenship": None,
        "gpa": None,
        "gre": None,
        "gre_v": None,
        "gre_aw": None,
    }


def _parse_decision(decision_text):
    """Split a decision string into status and optional date."""
    if not decision_text:
        return None, None
    normalized = re.sub(r"\s+", " ", decision_text).strip()
    match = re.match(
        r"^(?P<status>.+?)(?:\s+on\s+(?P<date>.+))?$",
        normalized,
        flags=re.IGNORECASE,
    )
    if not match:
        return None, None
    status = match.group("status").strip()
    decision_date = match.group("date").strip() if match.group("date") else None
    return status, decision_date


def _apply_detail_text(record, text):
    """Apply one detail text token to the current record."""
    if re.match(r"^(Fall|Spring|Summer|Winter)\s+\d{4}$", text):
        record["semester_year_start"] = text
        return
    if text in ("American", "International"):
        record["citizenship"] = text
        return
    if re.match(r"^GPA\s+[\d.]+$", text):
        record["gpa"] = text
        return
    if re.match(r"^GRE\s+AW\s+[\d.]+$", text):
        record["gre_aw"] = text
        return
    if re.match(r"^GRE\s+V\s+\d+$", text):
        record["gre_v"] = text
        return
    if re.match(r"^GRE\s+\d+$", text):
        record["gre"] = text


def _fill_following_rows(cases, start_index, record):
    """Read detail rows after a main row and return the next main-row index."""
    idx = start_index
    while idx < len(cases):
        next_tds = cases[idx].find_all("td")
        if len(next_tds) >= 4:
            break

        for div in cases[idx].find_all("div"):
            text = div.get_text(" ", strip=True)
            if text:
                _apply_detail_text(record, text)

        paragraph = cases[idx].find("p")
        if paragraph and not record["comments"]:
            comment_text = paragraph.get_text(" ", strip=True)
            if comment_text:
                record["comments"] = comment_text

        idx += 1
    return idx


def _parse_main_row(case):
    """Parse one main row and return a record, or None for non-main rows."""
    tds = case.find_all("td")
    if len(tds) < 4:
        return None

    record = _empty_record()
    record["university"] = tds[0].get_text(strip=True)
    spans = tds[1].find_all("span")
    if spans:
        record["program_name"] = spans[0].get_text(strip=True)
    if len(spans) >= 2:
        record["masters_or_phd"] = spans[1].get_text(strip=True)
    record["date_added"] = tds[2].get_text(strip=True)

    status, decision_date = _parse_decision(tds[3].get_text(" ", strip=True))
    record["applicant_status"] = status
    record["decision_date"] = decision_date

    link = case.find("a", href=lambda value: value and value.startswith("/result/"))
    if link:
        record["url"] = f"{URL.rstrip('/')}{link['href']}"
    return record


def scrape_data(soup):
    """
    Parse the GradCafe results table into structured records.

    :param soup: Parsed HTML document.
    :returns: List of record dicts (possibly empty).
    """

    # Store data as a list
    results = []

    # Find table in the HTML to pull data
    table = soup.find("table")
    if not table:
        return results

    # Find the table body for the specific cases
    tbody = table.find("tbody")
    if not tbody:
        return results

    # Look through all of the table rows
    cases = tbody.find_all("tr")
    idx = 0
    while idx < len(cases):
        record = _parse_main_row(cases[idx])
        if record is None:
            idx += 1
            continue

        idx = _fill_following_rows(cases, idx + 1, record)
        results.append(record)

    # Return all of the data
    return results


def create_pages(page_num):
    """
    Build the GradCafe survey URL for a given page.

    :param page_num: Page index (1-based).
    :returns: Fully qualified URL for the survey page.
    """

    # First page does not need page number
    if page_num <= 1:
        return parse.urljoin(URL, "survey/")

    # Add page numbers after page 1
    return parse.urljoin(URL, f"survey/?page={page_num}")



def pull_pages(target_n= 50, start_page=1):
    """
    Pull records from paginated survey pages and save to JSON.

    :param target_n: Maximum number of records to collect.
    :param start_page: First page to start scraping from.
    :returns: None. Writes ``OUTPUT_FILE`` as a JSON array.
    """

    # Parses through robots.txt
    robot = url_check()
    # Track page of URL
    page = start_page
    num_rec = 0
    all_records = []

    # Run until you've met your target number of records
    while num_rec < target_n:

        # Create url for the specific page
        page_url = create_pages(page)

        # Check robots.txt and uses BeautifulSoup to pull page
        soup = check_url(page_url, robot)
        if soup is None:
            break

        # Pull rows from the page
        page_records = scrape_data(soup)
        if not page_records:
            break

        # Keep adding records from the page until target n is met
        for record in page_records:
            if num_rec >= target_n:
                break
            all_records.append(record)

            num_rec += 1

        # Check on progress for reference
        print(f"Page {page}: saved {num_rec}")
        page += 1

        # Slow the speed of requests to ensure we can reach target n
        time.sleep(10)

    # Save the list as a JSON array
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    # Confirm number of records in applicant_data
    print("Finished. Total records saved:", num_rec)


#
def main():
    """
    Run the scraper with the default target size.

    Intended for command-line use.
    """
    pull_pages(target_n=500)

# Run only if executed directly
if __name__ == "__main__":
    main()
