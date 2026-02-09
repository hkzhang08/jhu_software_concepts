Name: Helen Zhang (hzhan308)
Module 3 Assignment: Database Queries Assignment Experiment Due on 2/8/2026 at 11:59 EST



Approach:
The goal of this assignment is to load GradCafe data into a PostgreSQL database, answer analysis questions using SQL, and present results on a simple Flask webpage. The workflow also includes an option to pull additional data from GradCafe, clean those results, and then add the new records to the current database to improve the responses to the questions.


Running the Application:
1. Install required python packages from requirements.txt
pip install -r requirements.txt

2. Ensure to install the LLM dependencies if you plan to run the LLM standardizer
pip install -r llm_hosting/requirements.txt

3. Load new records into PostgreSQL using load_data.py
python3 load_data.py

4. Run the Flask website to view the analysis (and optionally pull new data from the UI)
python3 website.py


Programs Details:



load_data.py – The goal of this program is to update the PostgreSQL database. It loads llm_extend_applicant_data.json first, which is the original database to include from module_2. Then it loads loads llm_new_applicant.json which are the new rows collected from web scrapping and cleaning. Then it removes and skips URLs already in the database, and inserts only new rows into the applicants table. This allows incremental updates without duplicating data.


query_table.py – The goal of this program is to run SQL queries against the PostgreSQL database and print answers to the assignment questions. It uses psycopg to connect to the database, runs each query, and prints the resulting metrics for assignment screenshots.


website.py – The goal of this program is to provide a Flask webpage that displays the analysis results and offers two actions. Pull Data runs the full pipeline (scrape.py then clean.py then load_data.py) to fetch and load new GradCafe records. Update Analysis refreshes the page to show the latest results in the database, and is disabled if a pull is already running.

Programs from Module_2 Assignment:
scrape.py / clean.py – Refer to read me file from Module_2 Assignment

LLM_hosting Folder:
The same LLM program and files from Module_2 folder and assignment. This folder contains the files and instructions needed to use the LLM to further clean and standardize the program field. It includes the app.py used to run the LLM as well as canonical reference lists for both programs and universities, which are used to guide and improve the model’s outputs.

Known Bugs:
There are no known bugs
