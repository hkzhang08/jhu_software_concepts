"""
Scrape GradCafe survey pages into a JSON dataset.

This module uses robots.txt checks, page iteration, and BeautifulSoup
to extract structured applicant records from the public survey pages.
"""



from urllib import parse, robotparser, error, request
import os
import re
import json
import time
from bs4 import BeautifulSoup


URL = "https://www.thegradcafe.com/"
USER_AGENT = "Mozilla/5.0 (compatible; zhang/1.0)"
AGENT = "zhang"
OUTPUT_FILE = "applicant_data.json"


def url_check():
    """
    Create and initialize a robots.txt parser for the GradCafe site.

    :returns: A configured :class:`robotparser.RobotFileParser` instance.
    """

    # Join robots.txt URL and run it through the parser
    parser = robotparser.RobotFileParser()
    parser.set_url(parse.urljoin(URL, 'robots.txt'))

    # Reading the parser
    parser.read()

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
        with request.urlopen(new_header) as response:
            html = response.read().decode("utf-8")

        # Converting HTML into object
        soup = BeautifulSoup(html, "html.parser")
        return soup

    # Print any errors that occurred
    except error.HTTPError as err:
        print(f"An error has occurred - {err}")
        return None


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
    i = 0

    # Loop through table rows
    while i < len(cases):
        case = cases[i]
        tds = case.find_all("td")

        # Only pull records with at least 4 table rows
        if len(tds) >=4:

            # Initailize the dictionary we will use
            record = {
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

            # Pull university
            record["university"] = tds[0].get_text(strip=True)

            # Pull program name and masters/PhD
            spans = tds[1].find_all("span")
            if len(spans) >= 1:
                record["program_name"] = spans[0].get_text(strip=True)
            if len(spans) >= 2:
                record["masters_or_phd"] = spans[1].get_text(strip=True)

            # Pull date infomration was added to grad cafe
            record["date_added"] = tds[2].get_text(strip=True)

            # Pull applicant status and decision date
            decision_text = tds[3].get_text(" ", strip=True)

            # Use re to separate status and date
            if decision_text:
                decision_text = re.sub(r"\s+", " ", decision_text).strip()

                m = re.match(r"^(?P<status>.+?)(?:\s+on\s+(?P<date>.+))?$",
                             decision_text, flags=re.IGNORECASE)
                if m:
                    record["applicant_status"] = m.group("status").strip()
                    record["decision_date"] = m.group("date").strip() if m.group("date") else None

            # Pull URL link to applicant entry
            link = case.find("a", href=lambda x: x and x.startswith("/result/"))
            if link:
                record["url"] = "https://www.thegradcafe.com" + link["href"]

            # Look through next rows
            j = i + 1
            while j < len(cases):
                next_tds = cases[j].find_all("td")

                # Do not continue if this is a new case
                if len(next_tds)>=4:
                    break

                # Look through data in div
                for div in cases[j].find_all("div"):
                    text = div.get_text(" ", strip=True)
                    if not text:
                        continue

                    # Pull semester and year of program start (if available)
                    if re.match(r"^(Fall|Spring|Summer|Winter)\s+\d{4}$", text):
                        record["semester_year_start"] = text

                    # Pull if International / American Student (if available)
                    elif text in ("American", "International"):
                        record["citizenship"] = text

                    #Pull GPA (if available)
                    elif re.match(r"^GPA\s+[\d.]+$", text):
                        record["gpa"] = text

                    # Pull GRE Score, GRE V Score, GRE AW (if available)
                    elif re.match(r"^GRE\s+AW\s+[\d.]+$", text):
                        record["gre_aw"] = text
                    elif re.match(r"^GRE\s+V\s+\d+$", text):
                        record["gre_v"] = text
                    elif re.match(r"^GRE\s+\d+$", text):
                        record["gre"] = text

                # Pull Comments (if available)
                p = cases[j].find("p")

                if p and not record["comments"]:
                    comment_text = p.get_text(" ", strip=True)

                    if comment_text:
                        record["comments"] = comment_text

                j += 1

            # Store the record and then review next record
            results.append(record)
            i = j

        # Not a new record - keep reviewing the next row
        else:
            i += 1

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
