Name: Helen Zhang (hzhan308)Module 2 Assignment: Web Scraping Due on 2/1/2026 at 11:59 ESTApproach:
The goal of this assignment is to scrape data from GradCafe data and then set up the dataset so that it is easier to clean using an LLM.


Running the Application:
1. Install required python packages from requirements.txt
pip install -r requirements.txt

2. Scrape data from GradCafe site using scrape.py program
python3 scrape.py

3. Clean the scraped data from step 2 using clean.py program
python3 clean.py

4. Run the LLM cleaning from the LLM_hosting folder. Follow the instructions from the readme file within that folder which includes
- uploading the files
- installing the requirements
- running the API server
- running the app.py program through your terminal
python3 llm_hosting/app.py \
  --file applicant_data.json \
  --stdout > llm_extend_applicant_data.json

Programs Details:scrape.py – The goal of this program is to scrape GradCafe results pages and save the raw output so it can be used later. We begin by importing all required libraries for this request, including urllib, which is required for this assignment. To make the program easier to follow and maintain, we define the base URL and user agent at the start of the script so they can be referenced consistently throughout the program.

We first define url_check, which confirms whether the site’s robots.txt file allows scraping using Python’s robotparser. In this function, we pass the base URL to the parser, read the rules defined in robots.txt, and return the parser object so it can be reused in later steps.

Next we define check_url to see whether a specific page can be fetched and, if so, parsing the HTML output using BeautifulSoup. We first use the robot parser to confirm that the user agent is allowed to fetch the requested URL, which returns a true/false response. If access is not allowed, the function exits and the page is not scraped. If access is allowed, we proceed by creating a request header with the user agent, requesting and decoding the HTML content, and then converting the HTML into a BeautifulSoup object. If any errors occur during this process, they are printed so they can be reviewed and debugged.

Next, we use a combination of BeautifulSoup, regex, and string search methods to scrape the data from the URL. We find the table and table body for the specific records, then look through the rows so we can pull legitimate non-missing records. We initialize the dictionary and pull all of the data categories needed (ex. Program name, university). For the decision text, we use re to more accurately parse out the decision from the decision date. Some records include optional fields, so we then look through those subsequent rows associated with the same record and extract additional information when available (such as GPA, GRE scores, and other metadata). Each completed record is stored and added to a list that is returned once all records on the page have been processed.

Following that, we need to create a way to run through the different pages of the URL. Since GradCafe limits results to a maximum of 20 records per page, the program needs to loop through multiple pages to collect a larger dataset. We define create_pages, which handles creating the URLs by page by noting that the first page does not require a page number parameter, while subsequent pages do. Using this function, we then define pull_pages, which controls the overall scraping process. This function runs in a loop until the target number of records is reached. For each page, it builds the page-specific URL, uses BeautifulSoup to scrape the page, and continues adding records until the target count is met.

A progress message is printed after each page is processed so progress can be monitored and number of results are visible in case the scraper stops unexpectedly. Additionally, we introduce a 10 second delay between requests to slow the scraping speed and reduce the risk of being blocked by the site. The collected records are saved as a JSON array

Finally, the main function runs pull_pages and defines the target number of records to collect, which is currently set to 50,000. This function only executes when the script is run directly, ensuring the scraping process does not start unintentionally when the file is imported elsewhere.
clean.py – The goal of this program is to read the scraped results produced by scrape.py, clean key fields, and save a cleaner output that can be passed to the LLM for standardization. We begin by defining references to the input file name and to text values that represent missing or unavailable data so they can be reused consistently throughout the script.

We first define load_data, which loads the JSON output from scrape.py (applicant_data.json) and returns the data as a Python object. While reviewing the data in applicant_data.json, we observed cases where either the program name or the university was missing, indicating that the record was not a legitimate application entry. To identify and flag these cases, we define is_missing, which checks whether a value is null or contains placeholder text indicating unavailable data so we can identify records that should be removed during cleaning.

Next, we define clean_data, which creates a standardized "program: field for each valid record so the LLM has a single text field to review and clean. As we loop through each record in the dataset, we extract the "program_name" and "university", remove records where either field is missing, and combine the two values into a comma-separated string called "program". This new field is added to the record, and the cleaned record is appended to the output list. The number of records removed during this step is printed to the console (40 records were removed).

We then define save_data, which writes the cleaned dataset to a new JSON file. The output is formatted using indent=2 to make the file easier to read. Finally, we define reorder_data, which removes the original program_name and university fields and moves the newly created program field to the top of each record to match the assignment example.

Using the main function, we tie all of these steps together and ensure the cleaning process only runs when the script is executed directly.


LLM_hosting Folder:
This folder contains the files and instructions needed to use the LLM to further clean and standardize the program field. It includes the app.py used to run the LLM as well as canonical reference lists for both programs and universities, which are used to guide and improve the model’s outputs.

For universities, the original canonical list was missing many medical schools and affiliated institutions (for example, Mount Sinai and Cornell Medical College). These were manually added after reviewing the dataset to help the LLM more accurately recognize and standardize university names.

Similarly, the canonical programs list was improved. Several common degree programs were missing from the original file and were added after review. For example, Artificial Intelligence and Machine Learning were included, as they are now common master’s and PhD programs, even though they are newer than many of the traditional programs already listed. These additions were made to improve the overall accuracy and consistency of the LLM cleaning.
Known Bugs:There are no known bugs