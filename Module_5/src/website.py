"""
Flask application and data pipeline helpers for GradCafe analysis.

This module exposes a Flask app factory, database utilities, and the
pull/update endpoints used by the web UI. It is designed for both
interactive use and automated testing (via dependency injection).
"""


# Imports to display analysis results from a PostgreSQL database
import json
import logging
import os
import re
import subprocess
import sys
import psycopg
from psycopg import sql
from flask import Flask, current_app, jsonify, render_template

try:
    from . import db_builders
except ImportError:  # pragma: no cover - script execution path
    import db_builders

try:
    from .query_table import fetch_metrics as query_table_fetch_metrics
except ImportError:  # pragma: no cover - script execution path
    from query_table import fetch_metrics as query_table_fetch_metrics

# Create connections and paths to database
DSN = db_builders.get_db_dsn()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.abspath(os.path.join(BASE_DIR, "templates"))
STATIC_DIR = os.path.abspath(os.path.join(BASE_DIR, "static"))
RAW_FILE = os.path.join(BASE_DIR, "applicant_data.json")
APPLICANTS_TABLE = db_builders.APPLICANTS_TABLE
APPLICANTS_REGCLASS = db_builders.APPLICANTS_REGCLASS
applicants_sql = db_builders.applicants_sql
fnum = db_builders.fnum
fdate = db_builders.fdate
fdegree = db_builders.fdegree
ftext = db_builders.ftext
APPLICANTS_COLUMN_NAMES = (
    "p_id",
    "program",
    "comments",
    "date_added",
    "url",
    "status",
    "term",
    "us_or_international",
    "gpa",
    "gre",
    "gre_v",
    "gre_aw",
    "degree",
    "llm_generated_program",
    "llm_generated_university",
)
APPLICANTS_COLUMNS = {
    column_name: sql.Identifier(column_name)
    for column_name in APPLICANTS_COLUMN_NAMES
}
APPLICANTS_SCHEMA = (
    ("p_id", sql.SQL("SERIAL PRIMARY KEY")),
    ("program", sql.SQL("TEXT")),
    ("comments", sql.SQL("TEXT")),
    ("date_added", sql.SQL("DATE")),
    ("url", sql.SQL("TEXT")),
    ("status", sql.SQL("TEXT")),
    ("term", sql.SQL("TEXT")),
    ("us_or_international", sql.SQL("TEXT")),
    ("gpa", sql.SQL("DOUBLE PRECISION")),
    ("gre", sql.SQL("DOUBLE PRECISION")),
    ("gre_v", sql.SQL("DOUBLE PRECISION")),
    ("gre_aw", sql.SQL("DOUBLE PRECISION")),
    ("degree", sql.SQL("DOUBLE PRECISION")),
    ("llm_generated_program", sql.SQL("TEXT")),
    ("llm_generated_university", sql.SQL("TEXT")),
)
APPLICANTS_INSERT_COLUMN_NAMES = (
    "program",
    "comments",
    "date_added",
    "url",
    "status",
    "term",
    "us_or_international",
    "gpa",
    "gre",
    "gre_v",
    "gre_aw",
    "degree",
    "llm_generated_program",
    "llm_generated_university",
)

# App config
PULL_TARGET_N = 200
QUERY_LIMIT_MIN = db_builders.MIN_QUERY_LIMIT
QUERY_LIMIT_MAX = db_builders.MAX_QUERY_LIMIT
QUERY_LIMIT_DEFAULT = db_builders.MAX_QUERY_LIMIT
QUERY_LIMIT_ENV_VAR = "QUERY_LIMIT"
PULL_ERROR_MESSAGE = "Pull failed due to an internal error."
ANALYSIS_ERROR_MESSAGE = "Analysis is temporarily unavailable. Please try again later."
LOGGER = logging.getLogger(__name__)
PULL_STATE = {"status": "idle", "message": ""}
PIPELINE_ERRORS = (
    OSError,
    subprocess.SubprocessError,
    ValueError,
    TypeError,
    AttributeError,
    RuntimeError,
)
ANALYSIS_ERRORS = (
    psycopg.Error,
    OSError,
    ValueError,
    TypeError,
    AttributeError,
    RuntimeError,
    KeyError,
)
# Keep cleaner helpers available as part of the module API used by tests.
CLEANER_EXPORTS = (fnum, fdate, fdegree, ftext)

def fmt_pct(value):
    """
    Format percentages with two decimal places.

    :param value: Numeric-like percentage value.
    :returns: Two-decimal string, or "N/A" if missing.
    """

    if value is None:
        return "N/A"
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return str(value)


def clamp_query_limit(value=None):
    """
    Clamp query LIMIT values to the allowed range.

    :param value: Optional candidate LIMIT value.
    :returns: Integer LIMIT clamped to [QUERY_LIMIT_MIN, QUERY_LIMIT_MAX].
    """

    if value is None:
        value = os.environ.get(QUERY_LIMIT_ENV_VAR, QUERY_LIMIT_DEFAULT)
    return db_builders.clamp_limit(value)


def build_insert_row(row: dict, url):
    """
    Convert one input record into the applicants INSERT tuple.

    :param row: Raw row dictionary from cleaned JSON.
    :param url: Pre-normalized URL value.
    :returns: Tuple matching ``APPLICANTS_INSERT_COLUMN_NAMES`` order.
    """

    return db_builders.build_applicant_insert_row(row, url=url, include_llm=False)


def build_insert_rows(rows: list, existing_urls: set):
    """
    Deduplicate by URL and build INSERT rows for new records only.

    :param rows: Raw JSON rows from cleaned file.
    :param existing_urls: URLs already stored in the database.
    :returns: List of row tuples ready for ``executemany``.
    """

    seen_urls = set(existing_urls)
    inserts = []

    for row in rows:
        url = db_builders.register_unique_url(row, seen_urls)
        if url is None:
            continue
        inserts.append(build_insert_row(row, url))

    return inserts


def insert_rows(cur, inserts: list):
    """
    Insert prepared rows into the applicants table.

    :param cur: Database cursor.
    :param inserts: List of tuples to insert.
    """

    if not inserts:
        return

    insert_columns = sql.SQL(", ").join(
        APPLICANTS_COLUMNS[column_name] for column_name in APPLICANTS_INSERT_COLUMN_NAMES
    )
    placeholders = sql.SQL(", ").join(
        sql.Placeholder() for _ in APPLICANTS_INSERT_COLUMN_NAMES
    )
    stmt = applicants_sql(
        "INSERT INTO {table} ({insert_columns}) VALUES ({values});",
        insert_columns=insert_columns,
        values=placeholders,
    )
    cur.executemany(stmt, inserts)


def load_cleaned_data_to_db(file_path: str, query_limit=None) -> int:
    """
    Load cleaned records from a JSON file into the applicants table.

    :param file_path: Path to a JSON array file of cleaned rows.
    :returns: Number of inserted rows.
    """

    query_limit = clamp_query_limit(query_limit)

    # If file does not exist, return 0
    if not os.path.exists(file_path):
        return 0

    # Open JSON file and load records
    with open(file_path, "r", encoding="utf-8") as handle:
        rows = json.load(handle)

    inserts = []
    # Insert new rows into PostgreSQL database
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            db_builders.ensure_table_exists(
                cur,
                APPLICANTS_REGCLASS,
                (
                    "Required table public.applicants is missing. "
                    "Create it with a schema owner before running the app."
                ),
            )
            existing_urls = db_builders.fetch_existing_urls(
                cur,
                query_limit,
                table_identifier=APPLICANTS_TABLE,
                url_identifier=APPLICANTS_COLUMNS["url"],
                order_identifier=APPLICANTS_COLUMNS["url"],
            )
            inserts = build_insert_rows(rows, existing_urls)
            insert_rows(cur, inserts)

        # Commit and save new records
        conn.commit()

    # Return number of new records inserted
    return len(inserts)


def run_pull_pipeline():
    """
    Run the full scrape → clean → load pipeline.

    :returns: True on success, False on failure.
    """

    # Update status of data pull
    try:
        PULL_STATE["status"] = "running"
        PULL_STATE["message"] = "Pull in progress..."

        # Check that output directory exists
        if os.path.dirname(RAW_FILE):
            os.makedirs(os.path.dirname(RAW_FILE), exist_ok=True)

        # Call on scrape.py and use subprocesses to run
        subprocess.run(
            [
                sys.executable,
                "-c",
                f"import scrape; scrape.pull_pages(target_n={PULL_TARGET_N})",
            ],
            cwd=BASE_DIR,
            check=True,
        )

        # Run clean.py to clean the raw data and then load_data.py to insert into the database
        subprocess.run(
            [sys.executable, os.path.join(BASE_DIR, "clean.py")],
            cwd=BASE_DIR,
            check=True,
        )
        load_result = subprocess.run(
            [sys.executable, os.path.join(BASE_DIR, "load_data.py")],
            cwd=BASE_DIR,
            check=True,
            capture_output=True,
            text=True,
        )

        # Parse output to determine number of rows inserted
        inserted = None
        match = re.search(r"Inserted rows:\s*(\d+)", load_result.stdout)
        if match:
            inserted = int(match.group(1))

        # When pulling data is done
        PULL_STATE["status"] = "done"
        if inserted is None:
            PULL_STATE["message"] = "Pull complete. Database updated."
        else:
            PULL_STATE["message"] = f"Pull complete. Inserted {inserted} new rows."
        return True

    # Store any errors for the website
    except PIPELINE_ERRORS:
        LOGGER.exception("Pull pipeline failed")
        PULL_STATE["status"] = "error"
        PULL_STATE["message"] = PULL_ERROR_MESSAGE
        return False


def fetch_metrics(query_limit=None) -> dict:
    """
    Query analysis metrics via ``query_table.fetch_metrics``.

    :param query_limit: Optional per-query LIMIT value.
    :returns: Dict of computed metrics for the analysis template.
    """

    return query_table_fetch_metrics(
        query_limit=clamp_query_limit(query_limit),
        connect_fn=psycopg.connect,
    )


def pull_data():
    """
    Trigger the data pull pipeline.

    :returns: JSON response with status code.
    """

    # If data is running, do not start a second data pull
    if PULL_STATE["status"] == "running":
        return jsonify({"busy": True}), 409

    # Run the pipeline to pull data and update the database
    run_fn = current_app.config.get("RUN_PULL_PIPELINE", run_pull_pipeline)
    try:
        result = run_fn()
    except PIPELINE_ERRORS:
        current_app.logger.exception("Pull request failed before completion")
        PULL_STATE["status"] = "error"
        PULL_STATE["message"] = PULL_ERROR_MESSAGE
        return jsonify({"ok": False, "error": PULL_ERROR_MESSAGE}), 500

    if result is False or PULL_STATE["status"] == "error":
        return jsonify({"ok": False, "error": PULL_ERROR_MESSAGE}), 500

    return jsonify({"ok": True}), 202


def update_analysis():
    """
    Refresh analysis results (no-op when busy).

    :returns: JSON response with status code.
    """

    # If data is running, do not update analysis
    if PULL_STATE["status"] == "running":
        return jsonify({"busy": True}), 409

    return jsonify({"ok": True}), 200


def index():
    """
    Render the analysis page using current metrics.

    :returns: Rendered HTML response.
    """

    fetch_fn = current_app.config.get("FETCH_METRICS", fetch_metrics)
    try:
        metrics = fetch_fn()
    except ANALYSIS_ERRORS:
        current_app.logger.exception("Failed to fetch analysis metrics")
        questions = [
            {
                "question": "Analysis currently unavailable.",
                "answer": ANALYSIS_ERROR_MESSAGE,
            }
        ]
        return render_template("index.html", questions=questions, pull_state=PULL_STATE), 503

    # Format the SQL questions
    questions = [
        {
            "question": (
                "How many entries do you have in your database who have applied for Fall 2026?"
            ),
            "answer": f"Fall 2026 Applicants: {metrics['fall_2026_count']}",
        },
        {
            "question": (
                "What percentage of entries are from international students "
                "(not American or Other)?"
            ),
            "answer": f"Percentage International: {fmt_pct(metrics['intl_pct'])}",
        },
        {
            "question": (
                "What is the average GPA, GRE, GRE V, GRE AW of applicants "
                "who provide these metrics?"
            ),
            "answer_lines": [
                f"Average GPA: {metrics['avg_gpa']}",
                f"Average GRE: {metrics['avg_gre']}",
                f"Average GRE V: {metrics['avg_gre_v']}",
                f"Average GRE AW: {metrics['avg_gre_aw']}",
            ],
        },
        {
            "question": "What is their average GPA of American students in Fall 2026?",
            "answer": f"Average American GPA: {metrics['avg_gpa_american_fall_2026']}",
        },
        {
            "question": "What percent of entries for Fall 2026 are Acceptances?",
            "answer": (
                "Fall 2026 Acceptance percent: "
                f"{fmt_pct(metrics['acceptance_pct_fall_2026'])}"
            ),
        },
        {
            "question": (
                "What is the average GPA of applicants who applied for "
                "Fall 2026 who are Acceptances?"
            ),
            "answer": (
                "Average GPA of Fall 2026 Acceptances: "
                f"{metrics['avg_gpa_accepted_fall_2026']}"
            ),
        },
        {
            "question": "How many entries are from applicants who applied to JHU for a "
                        "masters degrees in Computer Science?",
            "answer": f"JHU Masters of Computer Science Applicants: {metrics['jhu_ms_cs_count']}",
        },
        {
            "question": (
                "How many entries from 2026 are acceptances from applicants "
                "who applied to Georgetown University, "
                "MIT, Stanford University, or Carnegie Mellon University "
                "for a PhD in Computer Science?"
            ),
            "answer": (
                "Accepted 2026 PhD CS applicants at Georgetown/MIT/Stanford/CMU: "
                f"{metrics['cs_phd_accept_2026']}"
            ),
        },
        {
            "question": "Do you numbers for question 8 change if you use LLM Generated Fields?",
            "answer": (
                "Accepted 2026 PhD CS at Georgetown/MIT/Stanford/CMU (LLM Version): "
                f"{metrics['cs_phd_accept_2026_llm']}"
            ),
        },
        {
            "question": (
                "For UNC Chapel Hill Fall 2026 semester, how many accepted "
                "Master’s applicants were there by program?"
            ),
            "answer_lines": [
                f"{program_name or 'Unknown'}: {count}"
                for program_name, count in metrics["unc_masters_program_rows"]
            ] or ["No rows found."],
        },
        {
            "question": (
                "For UNC Chapel Hill Fall 2026 semester, how many PhD "
                "applicants in Biostatics or Epidemiology "
                "were there by program?"
            ),
            "answer_lines": [
                f"{program_name or 'Unknown'}: {count}"
                for program_name, count in metrics["unc_phd_program_rows"]
            ] or ["No rows found."],
        },
    ]
    return render_template("index.html", questions=questions, pull_state=PULL_STATE)


def create_app(*, run_pull_pipeline_fn=None, fetch_metrics_fn=None):
    """
    Create and configure the Flask application.

    Dependency injection hooks are exposed for testability.

    :param run_pull_pipeline_fn: Optional callable to replace the pipeline.
    :param fetch_metrics_fn: Optional callable to replace metric queries.
    :returns: Configured Flask app instance.
    """

    app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
    if run_pull_pipeline_fn is not None:
        app.config["RUN_PULL_PIPELINE"] = run_pull_pipeline_fn
    if fetch_metrics_fn is not None:
        app.config["FETCH_METRICS"] = fetch_metrics_fn
    app.add_url_rule("/pull-data", "pull_data", pull_data, methods=["POST"])
    app.add_url_rule("/update-analysis", "update_analysis", update_analysis, methods=["POST"])
    app.add_url_rule("/", "index", index)
    app.add_url_rule("/analysis", "analysis", index)
    return app
