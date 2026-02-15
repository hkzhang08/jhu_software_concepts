About
=====

Overview
--------

This project pulls GradCafe results, cleans the data, loads it into PostgreSQL,
and renders analysis in a Flask web app.

Architecture
------------

Web layer responsibilities:

- Exposes ``/`` and ``/analysis`` for rendering the analysis UI.
- Exposes ``/pull-data`` and ``/update-analysis`` to trigger data refreshes.
- Enforces busy-state gating so only one pull runs at a time.
- Uses dependency injection in ``create_app()`` for testability.

ETL layer responsibilities:

- Scrapes GradCafe survey pages into raw JSON.
- Cleans and normalizes records (program/university, required fields).
- Runs optional LLM standardization and writes JSONL outputs.

DB layer responsibilities:

- Creates and maintains the ``applicants`` table schema.
- Inserts new rows idempotently using URL-based deduplication.
- Provides SQL metrics for the analysis page.

API reference
-------------

The autodoc pages will cover the following key modules:

- ``scrape.py``: scraping helpers and page parsing.
- ``clean.py``: data cleaning and LLM standardization pipeline.
- ``load_data.py``: JSONL ingestion and database inserts.
- ``query_table.py``: SQL queries for analysis metrics.
- ``website.py``: Flask routes (``/``, ``/analysis``, ``/pull-data``, ``/update-analysis``).

Testing guide
-------------

All tests are marked. Run the full suite with:

::

      pytest -m "web or buttons or analysis or db or integration"

For coverage (100% required):

::

      pytest -m "web or buttons or analysis or db or integration" --cov=src --cov-report=term-missing --cov-fail-under=100

Expected UI selectors
---------------------

The following ``data-testid`` attributes are provided for UI tests:

- ``pull-data-btn``
- ``update-analysis-btn``

Test doubles and fixtures
-------------------------

Common fakes used in tests:

- Fake DB connections/cursors to avoid real Postgres calls.
- Injected pipeline/metrics functions via ``create_app(...)`` for deterministic routes.
- Monkeypatched ``subprocess.run`` in pull pipeline tests to avoid executing scripts.
- Stubbed LLM and hub clients in ``test_llm_hosting.py``.

Required environment variables
------------------------------

- ``DATABASE_URL``: PostgreSQL connection string used by the app and scripts.

Setup
-----

1. Create and activate a virtual environment (optional but recommended)::

      python3 -m venv .venv
      . .venv/bin/activate

2. Install dependencies::

      python3 -m pip install -r requirements.txt

3. Set ``DATABASE_URL`` (required)::

      export DATABASE_URL="postgresql://localhost/grad_cafe"

4. Run the Flask app::

      flask --app src run

   Alternatively::

      python -m src

5. Run tests (full marked suite)::

      pytest -m "web or buttons or analysis or db or integration"

   Run with coverage (100% required)::

      pytest -m "web or buttons or analysis or db or integration" --cov=src --cov-report=term-missing --cov-fail-under=100

Planned content
---------------

- Data pipeline overview (scrape -> clean -> load)
- Testing strategy and markers
