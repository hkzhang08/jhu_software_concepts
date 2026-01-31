
from urllib import parse, robotparser, error, request
import re
import json
import time
from bs4 import BeautifulSoup



URL = "https://www.thegradcafe.com/"
USER_AGENT = "Mozilla/5.0 (compatible; zhang/1.0)"
AGENT = "zhang"



def url_check():
    # Confirm the robot.txt file permits scraping

    parser = robotparser.RobotFileParser(URL)
    parser.set_url(parse.urljoin(URL, 'robots.txt'))
    parser.read()
    return parser


def check_url (page_url, parser):
    if not parser.can_fetch(USER_AGENT, page_url):
        print("Fetch results: NOT allowed to fetch URL")
        return None

    try:
        # Create user header to avoid 403 error
        new_header = request.Request(page_url,
            headers={"User-Agent": USER_AGENT})

        with request.urlopen(new_header) as response:
            html = response.read().decode("utf-8")

        soup = BeautifulSoup(html, "html.parser")
        return soup
        # print(html[:50000])

    except error.HTTPError as err:
        print(f"An error has occurred - {err}")
        return None


    # Use urllib to request data from Grad Cafe
# Use beautifulSoup/regex/string search methods to find admissions data.
def scrape_data(soup):
    results = []

    table = soup.find("table")
    if not table:
        return results

    tbody = table.find("tbody")
    if not tbody:
        return results

    cases = tbody.find_all("tr")
    i = 0

    while i < len(cases):
        case = cases[i]
        tds = case.find_all("td")

        if len(tds) >=4:
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

            # University
            record["university"] = tds[0].get_text(strip=True)

            # Program name and masters/PhD
            spans = tds[1].find_all("span")
            if len(spans) >= 1:
                record["program_name"] = spans[0].get_text(strip=True)
            if len(spans) >= 2:
                record["masters_or_phd"] = spans[1].get_text(strip=True)

            # Date of Information Added to Grad Café
            record["date_added"] = tds[2].get_text(strip=True)

            # Applicant Status + Date
            decision_text = tds[3].get_text(" ", strip=True)

            if decision_text:
                decision_text = re.sub(r"\s+", " ", decision_text).strip()

                m = re.match(r"^(?P<status>.+?)(?:\s+on\s+(?P<date>.+))?$",
                             decision_text, flags=re.IGNORECASE)
                if m:
                    record["applicant_status"] = m.group("status").strip()
                    record["decision_date"] = m.group("date").strip() if m.group("date") else None

            # URL link to applicant entry
            link = case.find("a", href=lambda x: x and x.startswith("/result/"))
            if link:
                record["url"] = "https://www.thegradcafe.com" + link["href"]


            j = i + 1
            while j < len(cases):
                next_tds = cases[j].find_all("td")

                if len(next_tds)>=4:
                    break

                for div in cases[j].find_all("div"):
                    text = div.get_text(" ", strip=True)
                    if not text:
                        continue

                    # Semester and Year of Program Start (if available)
                    if re.match(r"^(Fall|Spring|Summer|Winter)\s+\d{4}$", text):
                        record["semester_year_start"] = text

                    # International / American Student (if available)
                    elif text in ("American", "International"):
                        record["citizenship"] = text

                    #GPA (if available)
                    elif re.match(r"^GPA\s+[\d.]+$", text):
                        record["gpa"] = text

                    # GRE Score, GRE V Score, GRE AW (if available)
                    elif re.match(r"^GRE\s+AW\s+[\d.]+$", text):
                        record["gre_aw"] = text
                    elif re.match(r"^GRE\s+V\s+\d+$", text):
                        record["gre_v"] = text
                    elif re.match(r"^GRE\s+\d+$", text):
                        record["gre"] = text

                # Comments (if available)
                p = cases[j].find("p")

                if p and not record["comments"]:
                    comment_text = p.get_text(" ", strip=True)

                    if comment_text:
                        record["comments"] = comment_text

                j += 1

            results.append(record)
            i = j

        else:
            i += 1

    return results



def create_pages(page_num):
    if page_num <= 1:
        return parse.urljoin(URL, "survey/")
    return parse.urljoin(URL, f"survey/?page={page_num}")



def pull_pages(target_n= 50, start_page=1):
    robot = url_check()
    page = start_page
    num_rec = 0
    all_records = []

    while num_rec < target_n:
        page_url = create_pages(page)

        soup = check_url(page_url, robot)
        if soup is None:
            break

        page_records = scrape_data(soup)
        if not page_records:
            break

        for record in page_records:
            if num_rec >= target_n:
                break
            all_records.append(record)

            num_rec += 1

        print(f"Page {page}: saved {num_rec}")

        page += 1
        time.sleep(10)

    with open("applicant_data.json", "w") as f:
        json.dump(all_records, f, indent=2)

    print("Finished. Total records saved:", num_rec)



def main():
    pull_pages(target_n=50000)


if __name__ == "__main__":
    main()

