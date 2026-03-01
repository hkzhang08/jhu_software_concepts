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
import threading
import psycopg
from psycopg import sql
from flask import Flask, current_app, jsonify, redirect, render_template, request, url_for

try:
    from . import db_builders
except ImportError:  # pragma: no cover - script execution path
    import db_builders

try:
    from .query_table import fetch_metrics as query_table_fetch_metrics
except ImportError:  # pragma: no cover - script execution path
    from query_table import fetch_metrics as query_table_fetch_metrics

try:
    from web.publisher import PublishError, publish_task as publish_queue_task
except ImportError:  # pragma: no cover - script execution path
    class PublishError(RuntimeError):
        """Fallback publish error when the RabbitMQ publisher cannot be imported."""

    def publish_queue_task(*_args, **_kwargs):
        raise PublishError("RabbitMQ publisher is unavailable.")

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
PULL_RUNNING_MESSAGE = (
    "Data pull is running. Please wait for a follow-up message when complete."
)
PULL_QUEUED_MESSAGE = "Request queued. Data pull will continue in the background."
ANALYSIS_QUEUED_MESSAGE = (
    "Request queued. Analysis refresh will run after the background worker picks it up."
)
SCRAPE_TASK_NAME = "scrape_new_data"
RECOMPUTE_TASK_NAME = "recompute_analytics"
PUBLISH_FAILED_ERROR = "publish_failed"
ANALYSIS_REFRESHED_MESSAGE = "Analysis refreshed with latest data pull results."
ANALYSIS_ERROR_MESSAGE = "Analysis is temporarily unavailable. Please try again later."
LOGGER = logging.getLogger(__name__)
PULL_STATE = {"status": "idle", "message": ""}
ANALYSIS_STATE = {"message": ""}
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


def prefers_json_response() -> bool:
    """
    Return True when the caller explicitly prefers JSON over HTML.

    Keep JSON as the default for backwards compatibility with API/test callers
    that omit ``Accept``. Browser form submissions typically include
    ``text/html`` and should get redirects back to the page.
    """

    accept = (request.headers.get("Accept") or "").lower()
    if "text/html" in accept:
        return False
    if "application/json" in accept:
        return True
    return True

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
        PULL_STATE["message"] = PULL_RUNNING_MESSAGE

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
            PULL_STATE["message"] = "Data Pull Complete. Database updated."
        else:
            PULL_STATE["message"] = f"Data Pull Complete. Inserted {inserted} new rows."
        return True

    # Store any errors for the website
    except PIPELINE_ERRORS:
        LOGGER.exception("Pull pipeline failed")
        PULL_STATE["status"] = "error"
        PULL_STATE["message"] = PULL_ERROR_MESSAGE
        return False


def _start_background_pull(run_fn):
    """
    Execute a pull pipeline function in a daemon thread.

    :param run_fn: Callable that performs the pull pipeline.
    """

    def _worker():
        """Run the pull callable and normalize state transitions on completion."""
        try:
            result = run_fn()
        except PIPELINE_ERRORS:
            LOGGER.exception("Background pull request failed before completion")
            PULL_STATE["status"] = "error"
            PULL_STATE["message"] = PULL_ERROR_MESSAGE
            return

        if result is False or PULL_STATE["status"] == "error":
            PULL_STATE["status"] = "error"
            PULL_STATE["message"] = PULL_ERROR_MESSAGE
            return

        if PULL_STATE["status"] == "running":
            PULL_STATE["status"] = "done"
            if (
                not PULL_STATE["message"]
                or PULL_STATE["message"] == PULL_RUNNING_MESSAGE
            ):
                PULL_STATE["message"] = "Data Pull Complete. Database updated."

    threading.Thread(target=_worker, daemon=True).start()


def fetch_metrics(query_limit=None) -> dict:
    """
    Query analysis metrics via ``query_table.fetch_metrics``.

    :param query_limit: Optional per-query LIMIT value.
    :returns: Dict of computed metrics for the analysis template.
    """

    try:
        return query_table_fetch_metrics(
            query_limit=clamp_query_limit(query_limit),
            connect_fn=psycopg.connect,
        )
    except ANALYSIS_ERRORS:
        LOGGER.warning(
            "Falling back to simplified applicant metrics because the full "
            "analytics schema is unavailable.",
            exc_info=True,
        )
        return fetch_simplified_metrics()


def _empty_metrics() -> dict:
    """Return a complete metrics payload with safe defaults."""

    return {
        "fall_2026_count": 0,
        "intl_pct": None,
        "avg_gpa": None,
        "avg_gre": None,
        "avg_gre_v": None,
        "avg_gre_aw": None,
        "avg_gpa_american_fall_2026": None,
        "acceptance_pct_fall_2026": 0.0,
        "avg_gpa_accepted_fall_2026": None,
        "jhu_ms_cs_count": 0,
        "cs_phd_accept_2026": 0,
        "cs_phd_accept_2026_llm": 0,
        "unc_masters_program_rows": [],
        "unc_phd_program_rows": [],
    }


def fetch_simplified_metrics() -> dict:
    """
    Compute a compatibility metrics payload from the simplified applicants table.

    This stack's Compose database uses a smaller schema than the legacy analytics
    queries expect. When those richer columns are missing, derive whatever can be
    computed from the current table and fill the remaining slots with defaults so
    the HTML analysis page can still render.
    """

    metrics = _empty_metrics()
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants")
            total_row = cur.fetchone()
            total_applicants = int(total_row[0] or 0) if total_row else 0
            metrics["fall_2026_count"] = total_applicants

            cur.execute(
                """
                SELECT
                    ROUND(
                        100.0 * SUM(CASE WHEN status = %s THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0),
                        2
                    )
                FROM applicants
                """,
                ("Accepted",),
            )
            accepted_row = cur.fetchone()
            metrics["acceptance_pct_fall_2026"] = (
                float(accepted_row[0]) if accepted_row and accepted_row[0] is not None else 0.0
            )

            cur.execute(
                """
                SELECT COUNT(*)
                FROM applicants
                WHERE program ILIKE %s
                  AND (
                        university ILIKE %s
                     OR university ILIKE %s
                     OR university ILIKE %s
                  )
                """,
                ("%Computer Science%", "%Johns Hopkins%", "%John Hopkins%", "%JHU%"),
            )
            jhu_row = cur.fetchone()
            metrics["jhu_ms_cs_count"] = int(jhu_row[0] or 0) if jhu_row else 0

    return metrics


def _publish_button_task(task_name: str, *, payload=None):
    """
    Publish a button-triggered task request through RabbitMQ.

    :param task_name: Logical task name for the background worker.
    :param payload: Optional task metadata dictionary.
    :returns: Published message payload.
    :raises PublishError: When the task cannot be published.
    """

    publish_fn = current_app.config.get("PUBLISH_TASK", publish_queue_task)
    return publish_fn(task_name, payload=payload or {})


def pull_data():
    """
    Trigger the data pull pipeline.

    :returns: Redirect for browser forms, JSON for API callers.
    """

    expects_json = prefers_json_response()
    response = None
    status_code = None

    # If data is running, do not start a second data pull
    if PULL_STATE["status"] == "running":
        PULL_STATE["message"] = PULL_RUNNING_MESSAGE
        if expects_json:
            response = jsonify({"busy": True, "message": PULL_STATE["message"]})
            status_code = 409
        else:
            response = redirect(url_for("index"), code=303)
    else:
        run_fn = current_app.config.get("RUN_PULL_PIPELINE")
        if run_fn is None:
            try:
                _publish_button_task(SCRAPE_TASK_NAME)
            except PublishError:
                current_app.logger.exception("Failed to publish %s", SCRAPE_TASK_NAME)
                PULL_STATE["status"] = "error"
                PULL_STATE["message"] = PULL_ERROR_MESSAGE
                if expects_json:
                    response = jsonify({"error": PUBLISH_FAILED_ERROR})
                    status_code = 503
                else:
                    response = redirect(url_for("index"), code=303)
            else:
                PULL_STATE["status"] = "idle"
                PULL_STATE["message"] = PULL_QUEUED_MESSAGE
                if expects_json:
                    response = jsonify({"status": "queued", "task": SCRAPE_TASK_NAME})
                    status_code = 202
                else:
                    response = redirect(url_for("index"), code=303)
        else:
            # Run the pipeline to pull data and update the database
            if expects_json:
                try:
                    result = run_fn()
                except PIPELINE_ERRORS:
                    current_app.logger.exception("Pull request failed before completion")
                    PULL_STATE["status"] = "error"
                    PULL_STATE["message"] = PULL_ERROR_MESSAGE
                    result = False

                if result is False or PULL_STATE["status"] == "error":
                    response = jsonify({"ok": False, "error": PULL_ERROR_MESSAGE})
                    status_code = 500
                else:
                    response = jsonify({"ok": True})
                    status_code = 202
            else:
                PULL_STATE["status"] = "running"
                PULL_STATE["message"] = PULL_RUNNING_MESSAGE
                _start_background_pull(run_fn)
                response = redirect(url_for("index"), code=303)

    if status_code is None:
        return response
    return response, status_code


def update_analysis():
    """
    Refresh analysis results by redirecting to the analysis page.

    :returns: Redirect response when idle, busy JSON with 409 when running.
    """

    expects_json = "application/json" in (request.headers.get("Accept") or "").lower()

    # If data is running, do not update analysis
    if PULL_STATE["status"] == "running":
        PULL_STATE["message"] = PULL_RUNNING_MESSAGE
        if expects_json:
            return (
                jsonify({"busy": True, "message": PULL_STATE["message"] or PULL_RUNNING_MESSAGE}),
                409,
            )
        return redirect(url_for("index"), code=303)

    run_fn = current_app.config.get("RUN_PULL_PIPELINE")
    if run_fn is None:
        try:
            _publish_button_task(
                RECOMPUTE_TASK_NAME,
            )
        except PublishError:
            current_app.logger.exception("Failed to publish %s", RECOMPUTE_TASK_NAME)
            ANALYSIS_STATE["message"] = PULL_ERROR_MESSAGE
            if expects_json:
                return jsonify({"error": PUBLISH_FAILED_ERROR}), 503
            return redirect(url_for("index"), code=303)

        ANALYSIS_STATE["message"] = ANALYSIS_QUEUED_MESSAGE
        if expects_json:
            return jsonify({"status": "queued", "task": RECOMPUTE_TASK_NAME}), 202
    elif PULL_STATE["status"] == "done":
        ANALYSIS_STATE["message"] = ANALYSIS_REFRESHED_MESSAGE
    else:
        ANALYSIS_STATE["message"] = ""

    return redirect(url_for("index"), code=303)


def pull_status():
    """
    Return the current pull pipeline state for browser polling.

    :returns: JSON payload with pull ``status`` and ``message``.
    """

    return jsonify(PULL_STATE), 200


def health():
    """
    Return a lightweight health response for container checks.

    :returns: JSON payload indicating the app is reachable.
    """

    return jsonify({"status": "healthy"}), 200


def index():
    """
    Render the analysis page using current metrics.

    :returns: Rendered HTML response.
    """

    analysis_message = ANALYSIS_STATE.get("message", "")
    ANALYSIS_STATE["message"] = ""

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
        return render_template(
            "index.html",
            questions=questions,
            pull_state=PULL_STATE,
            analysis_message=analysis_message,
        )

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
    return render_template(
        "index.html",
        questions=questions,
        pull_state=PULL_STATE,
        analysis_message=analysis_message,
    )


def create_app(*, run_pull_pipeline_fn=None, fetch_metrics_fn=None, publish_task_fn=None):
    """
    Create and configure the Flask application.

    Dependency injection hooks are exposed for testability.

    :param run_pull_pipeline_fn: Optional callable to replace the pipeline.
    :param fetch_metrics_fn: Optional callable to replace metric queries.
    :param publish_task_fn: Optional callable to replace RabbitMQ task publishing.
    :returns: Configured Flask app instance.
    """

    app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
    if run_pull_pipeline_fn is not None:
        app.config["RUN_PULL_PIPELINE"] = run_pull_pipeline_fn
    if fetch_metrics_fn is not None:
        app.config["FETCH_METRICS"] = fetch_metrics_fn
    if publish_task_fn is not None:
        app.config["PUBLISH_TASK"] = publish_task_fn
    app.add_url_rule("/pull-data", "pull_data", pull_data, methods=["POST"])
    app.add_url_rule("/pull-status", "pull_status", pull_status, methods=["GET"])
    app.add_url_rule("/update-analysis", "update_analysis", update_analysis, methods=["POST"])
    app.add_url_rule("/health", "health", health, methods=["GET"])
    app.add_url_rule("/", "index", index)
    app.add_url_rule("/analysis", "analysis", index)
    return app
