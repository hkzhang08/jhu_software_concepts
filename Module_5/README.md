# Grad Cafe Analytics Application (Module_5)

This README reflects the current Module_5 implementation.

## Module_5 Updates

Compared with earlier modules, this version includes:

- A Flask analysis UI with busy-state controls and status polling.
- A full scrape -> clean -> load pipeline wired to the UI.
- PostgreSQL-backed analytics queries for 11 assignment questions.
- Optional LLM standardization support (with deterministic fallback behavior).
- Least-privilege SQL setup scripts for runtime DB access.
- A fully marked pytest suite with enforced 100% coverage.
- GitHub Actions workflows for lint, dependency graph, Snyk scan, pytest, and docs pages.

## Project Structure

- `src/` application code (Flask app, ETL pipeline, DB helpers, LLM standardizer)
- `src/sql/` SQL scripts for table bootstrap and least-privilege grants
- `tests/` pytest suite (`web`, `buttons`, `analysis`, `db`, `integration` markers)
- `docs/` Sphinx documentation source and build output
- `deliverable/` submission artifacts
- `.github/workflows/` CI and docs deployment workflows

## Requirements

- Python 3.13 (matches CI and `setup.py`)
- PostgreSQL (CI uses PostgreSQL 15)
- `pip` (or `uv`, optional)

Install core dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install -r requirements.txt
```

Optional (isolated) dependencies for `src/llm_hosting/`:

```bash
python3 -m venv .venv-llm
. .venv-llm/bin/activate
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install -r src/llm_hosting/requirements.txt
deactivate
. .venv/bin/activate
```

## Environment Configuration

Set either `DATABASE_URL` or all DB split fields.

Option A:

```bash
export DATABASE_URL="postgresql://localhost/grad_cafe"
```

Option B:

```bash
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="grad_cafe"
export DB_USER="grad_cafe_app"
export DB_PASSWORD="your_password"
```

Additional environment variables used by Module_5:

- `QUERY_LIMIT` (default `100`, clamped to `1..100`)
- `STANDARDIZE_MAX_ROWS` (default `100`)
- `STANDARDIZE_MAX_PROGRAM_CHARS` (default `512`)
- `LLM_NEW_APPLICANT_PATH` (optional override for `src/llm_new_applicant.json`)
- `LLM_EXTEND_APPLICANT_PATH` (optional override for `src/llm_extend_applicant_data.json`)

Use `.env.example` as a template.

## Database Setup (Least Privilege)

Create schema/table once as an owner/admin:

```bash
psql -d grad_cafe -f src/sql/bootstrap_applicants_table.sql
```

Create or rotate the runtime app user with only required permissions:

```bash
APP_DB_USER="grad_cafe_app"
APP_DB_PASSWORD="$(openssl rand -base64 24)"

psql -d grad_cafe \
  -v app_user="$APP_DB_USER" \
  -v app_password="$APP_DB_PASSWORD" \
  -f src/sql/create_least_privilege_app_user.sql
```

Granted to app user:

- `CONNECT` on the target DB
- `USAGE` on schema `public`
- `SELECT, INSERT` on `public.applicants`
- `USAGE, SELECT` on `public.applicants_p_id_seq`

Not granted:

- superuser / createdb / createrole
- `UPDATE`, `DELETE`, `ALTER`, `DROP`

## Run the App

```bash
flask --app src run
```

Alternative:

```bash
python -m src
```

## Web Routes and Behavior

- `GET /` and `GET /analysis`: render analysis page
- `POST /pull-data`:
  - JSON callers (default when `Accept` is not `text/html`): run pipeline immediately, return `202 {"ok": true}` on success
  - HTML form callers: start background pull thread and `303` redirect back to `/`
  - busy state: returns `409 {"busy": true}`
- `POST /update-analysis`:
  - idle/done: `303` redirect to `/`
  - busy state: `409 {"busy": true}`
- `GET /pull-status`: JSON status payload for polling (`idle|running|done|error` + message)

## Data Pipeline Scripts

### 1) Scrape

```bash
python src/scrape.py
```

- Pulls GradCafe survey pages (default target `500` rows in `main()`)
- Writes JSON array to `src/applicant_data.json`

### 2) Clean + LLM Standardization

```bash
python src/clean.py
```

- Cleans scraped records, builds combined `program` field
- Reorders fields to expected schema
- Deduplicates new rows by URL against historical LLM output
- Runs `src/llm_hosting/app.py` for standardization output
- Produces/updates:
  - `src/applicant_data.json` (cleaned JSON)
  - `src/new_applicant_data.json` (new rows for LLM input)
  - `src/llm_new_applicant.json` (latest standardized JSONL rows)
  - `src/llm_extend_applicant_data.json` (master JSONL append target)

### 3) Load into PostgreSQL

```bash
python src/load_data.py
```

- Requires `public.applicants` table to already exist
- Loads from both LLM JSONL sources
- Deduplicates by URL against existing DB rows
- Prints inserted row count (`Inserted rows: N`)

### 4) Query Metrics (CLI)

```bash
python src/query_table.py
```

- Executes SQL analytics queries used by the Flask page
- Prints formatted answers to stdout

## Run Tests

All tests are marker-based and coverage is enforced by `pytest.ini`.

```bash
pytest -m "web or buttons or analysis or db or integration"
```

Coverage gate configured in `pytest.ini`:

- `--cov=src`
- `--cov-report=term-missing`
- `--cov-fail-under=100`

## Lint

```bash
pylint src --fail-under=10
```

## CI/CD Workflows

`.github/workflows/ci.yml` runs:

1. `action-1-pylint`
2. `action-2-dependency-graph` (uploads `dependency.svg` artifact)
3. `action-3-snyk-test` (optional, token-gated with optional failure enforcement)
4. `action-4-pytest` (with PostgreSQL service container)

`.github/workflows/pages.yml` builds Sphinx docs and deploys to GitHub Pages on pushes to `main`.

## Documentation (Sphinx)

Build locally:

```bash
make -C docs html
```

Open:

```bash
open docs/_build/html/index.html
```

## Notes

- `src/load_data.py` is script-oriented and performs its load at module execution time.
- Deduplication is URL-based for both pipeline and DB inserts.
- Pull/update button busy-state behavior is enforced in both backend responses and frontend button disabling.
