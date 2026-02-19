# Grad Cafe Analytics Application (Module_5)

This project is a Flask-based web application that allows users to view and analyze graduate admissions data. It includes a scraping/cleaning/loading pipeline, a PostgreSQL-backed analysis view, and a comprehensive pytest suite with full coverage.

## Project Structure

- `src/`: Application code (Flask app, ETL, DB helpers, LLM standardizer)
- `tests/`: Pytest suite with required markers
- `docs/`: Sphinx documentation source and build output
- `deliverable/`: Submission artifacts (coverage summary, operational notes, repo link)
- `.github/`: GitHub Actions workflows (CI + Pages)
- `.venv/`: Local Python virtual environment (optional, recommended)

## Requirements

- Python 3.13 (matches CI)
- PostgreSQL (local or CI)
- Database environment configuration (choose one):
  - `DATABASE_URL` (PostgreSQL connection string), or
  - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

Examples:

```bash
export DATABASE_URL="postgresql://localhost/grad_cafe"
```

```bash
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="grad_cafe"
export DB_USER="your_user"
export DB_PASSWORD="your_password"
```

## Setup

```bash
# From the Module_5 directory
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r requirements.txt
export DATABASE_URL="postgresql://localhost/grad_cafe"
```

Or set the split DB variables instead of `DATABASE_URL`:

```bash
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="grad_cafe"
export DB_USER="your_user"
export DB_PASSWORD="your_password"
```

Do not hard-code DB credentials in source files; provide them via environment variables.

## Run the App

```bash
flask --app src run
```

Or:

```bash
python -m src
```

## Run Tests

All tests are marked. Run the full suite with:

```bash
pytest -m "web or buttons or analysis or db or integration"
```

With coverage (100% required):

```bash
pytest -m "web or buttons or analysis or db or integration" --cov=src --cov-report=term-missing --cov-fail-under=100
```

## Notes

- `POST /pull-data` returns JSON and triggers the data pipeline.
- `POST /update-analysis` returns JSON and refreshes analysis.
- Busy gating returns HTTP 409 with `{"busy": true}`.
- Analysis percentages are formatted to two decimals.

## Approach

The goal of this assignment is to load GradCafe data into a PostgreSQL database,
answer analysis questions using SQL, and present results on a simple Flask webpage.
The workflow also includes an option to pull additional data from GradCafe, clean
those results, and then add the new records to the current database to improve the
responses to the questions.

## Running the Application (scripted workflow)

1. Install required Python packages from `requirements.txt`

```bash
python3 -m pip install -r requirements.txt
```

2. Ensure to install the LLM dependencies if you plan to run the LLM standardizer

```bash
python3 -m pip install -r src/llm_hosting/requirements.txt
```

3. Load new records into PostgreSQL using `load_data.py`

```bash
python3 src/load_data.py
```

4. Run the Flask website to view the analysis (and optionally pull new data from the UI)

```bash
flask --app src run
```

Alternatively:

```bash
python -m src
```

## Program Details

**load_data.py** – The goal of this program is to update the PostgreSQL database.
It loads `llm_extend_applicant_data.json` first, which is the original database
to include from module_2. Then it loads `llm_new_applicant.json` which are the new
rows collected from web scraping and cleaning. Then it removes and skips URLs already
in the database, and inserts only new rows into the applicants table. This allows
incremental updates without duplicating data.

**query_table.py** – The goal of this program is to run SQL queries against the
PostgreSQL database and print answers to the assignment questions. It uses psycopg
to connect to the database using environment variables, runs each query, and prints
the resulting metrics for assignment screenshots.

**website.py** – The goal of this program is to provide a Flask webpage that displays
the analysis results and offers two actions. Pull Data runs the full pipeline
(`scrape.py` then `clean.py` then `load_data.py`) to fetch and load new GradCafe
records. Update Analysis refreshes the page to show the latest results in the
database, and is disabled if a pull is already running.

**Programs from Module_2 Assignment:**  
`scrape.py` / `clean.py` – Refer to the README file from the Module_2 Assignment.

**LLM_hosting Folder:**  
The same LLM program and files from Module_2 folder and assignment. This folder
contains the files and instructions needed to use the LLM to further clean and
standardize the program field. It includes the `app.py` used to run the LLM as well
as canonical reference lists for both programs and universities, which are used to
guide and improve the model’s outputs.

## Known Bugs

There are no known bugs.

## Documentation

Sphinx docs live in `docs/` and build output is in `docs/_build/html`.

Build docs:

```bash
cd docs
make html SPHINXBUILD=/Users/zhang8/PycharmProjects/jhu_software_concepts/Module_5/.venv/bin/sphinx-build
```

Open docs:

```bash
open docs/_build/html/index.html
```
