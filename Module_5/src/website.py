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
from datetime import datetime
import psycopg
from psycopg import sql
from flask import Flask, current_app, jsonify, render_template

try:
    from .db_config import get_db_dsn
except ImportError:  # pragma: no cover - script execution path
    from db_config import get_db_dsn

# Create connections and paths to database
DSN = get_db_dsn()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.abspath(os.path.join(BASE_DIR, "templates"))
STATIC_DIR = os.path.abspath(os.path.join(BASE_DIR, "static"))
RAW_FILE = os.path.join(BASE_DIR, "applicant_data.json")
APPLICANTS_TABLE = sql.Identifier("applicants")
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
QUERY_LIMIT_MIN = 1
QUERY_LIMIT_MAX = 100
QUERY_LIMIT_DEFAULT = 100
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


def fnum(value):
    """
    Convert numeric-like values to float.

    :param value: Any numeric-like value (str/int/float).
    :returns: Float if parsable, otherwise None.
    """

    # Set to none if already missing
    if value is None:
        return None

    # If numeric, covert to float
    if isinstance(value, (int, float)):
        return float(value)

    # Clean the number within the string if needed
    match = re.search(r"[-+]?\d*\.?\d+", str(value))
    return float(match.group(0)) if match else None


def fdate(value):
    """
    Convert date strings to ``datetime.date``.

    :param value: Date string.
    :returns: ``datetime.date`` or None if parsing fails.
    """

    # If missing, then keep as none
    if not value:
        return None

    # Try different date formats as needed
    cleaned = str(value).strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue

    # If not date, then return none
    return None


def fdegree(value):
    """
    Normalize degree labels to numeric categories.

    :param value: Degree label (e.g., "PhD", "Masters").
    :returns: 2.0 for PhD/doctorate, 1.0 for masters, else None.
    """

    # If not value, then return none
    if not value:
        return None

    # Update phd/doctorate to 2.0
    v = str(value).strip().lower()
    if "phd" in v or "doctor" in v:
        return 2.0

    # Update masters to 1.0
    if "master" in v or v in {"ms", "ma", "msc", "mba"}:
        return 1.0

    # Return none if not matched above
    return None


def ftext(value):
    """
    Normalize text values to strings without NULL bytes.

    :param value: Input value.
    :returns: Cleaned string or None.
    """

    # Return none if missing
    if value is None:
        return None

    # Return string and remove null bytes
    return str(value).replace("\x00", "")


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
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = QUERY_LIMIT_DEFAULT
    return max(QUERY_LIMIT_MIN, min(QUERY_LIMIT_MAX, parsed))


def applicants_sql(query_template: str, **identifiers):
    """
    Compose SQL with safely quoted identifier placeholders.

    :param query_template: SQL string template containing ``{table}`` and
        optional identifier placeholders.
    :param identifiers: Extra identifier/value placeholders for SQL ``format``.
    :returns: Composed SQL object safe for execution.
    """

    format_args = {"table": APPLICANTS_TABLE}
    format_args.update(identifiers)
    return sql.SQL(query_template).format(**format_args)


def ensure_applicant_table(cur):
    """
    Ensure the applicants table exists.

    :param cur: Database cursor.
    :returns: None.
    """

    column_defs = [
        sql.SQL("{} {}").format(APPLICANTS_COLUMNS[column_name], column_type)
        for column_name, column_type in APPLICANTS_SCHEMA
    ]
    stmt = applicants_sql(
        "CREATE TABLE IF NOT EXISTS {table} ({column_defs});",
        column_defs=sql.SQL(", ").join(column_defs),
    )
    cur.execute(stmt)


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

    # Insert new rows into PostgreSQL database
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:

            # Check if table exists
            ensure_applicant_table(cur)

            # Create set of URLS to remove duplicates
            existing_urls = set()
            offset = 0
            while True:
                stmt = applicants_sql(
                    """
                    SELECT {url_col}
                    FROM {table}
                    WHERE {url_col} IS NOT NULL
                    ORDER BY {url_col}
                    LIMIT %s OFFSET %s;
                    """,
                    url_col=APPLICANTS_COLUMNS["url"],
                )
                params = (query_limit, offset)
                cur.execute(stmt, params)
                url_rows = cur.fetchall()
                if not url_rows:
                    break
                existing_urls.update(row[0] for row in url_rows)
                if len(url_rows) < query_limit:
                    break
                offset += query_limit

            seen_urls = set(existing_urls)

            # Check urls of data
            inserts = []
            for row in rows:
                url = ftext(row.get("url"))
                if url and url in seen_urls:
                    continue
                if url:
                    seen_urls.add(url)

                # Insert new rows, insert none for llm generated fields
                inserts.append(
                    (
                        ftext(row.get("program")),
                        ftext(row.get("comments")),
                        fdate(row.get("date_added")),
                        url,
                        ftext(row.get("applicant_status")),
                        ftext(row.get("semester_year_start")),
                        ftext(row.get("citizenship")),
                        fnum(row.get("gpa")),
                        fnum(row.get("gre")),
                        fnum(row.get("gre_v")),
                        fnum(row.get("gre_aw")),
                        fdegree(row.get("masters_or_phd")),
                        None,
                        None,
                    )
                )

            # Bulk insert new rows
            if inserts:
                insert_columns = sql.SQL(", ").join(
                    APPLICANTS_COLUMNS[column_name]
                    for column_name in APPLICANTS_INSERT_COLUMN_NAMES
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
    Query the database and return metrics used by the analysis page.

    :param query_limit: Optional per-query LIMIT value.
    :returns: Dict of computed metrics for the analysis template.
    """

    query_limit = clamp_query_limit(query_limit)

    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            metrics = {}

            # Question 1
            stmt = applicants_sql(
                """
                SELECT COUNT(*)
                FROM {table}
                WHERE term ILIKE %s
                LIMIT %s;
                """
            )
            params = ("Fall 2026", query_limit)
            cur.execute(stmt, params)
            metrics["fall_2026_count"] = cur.fetchone()[0]

            # Question 2
            stmt = applicants_sql(
                """
                SELECT
                    ROUND(
                        100.0 * SUM(
                            CASE
                                WHEN us_or_international = %s THEN 1
                                ELSE 0
                            END
                        )
                        / NULLIF(COUNT(*), 0),
                        2
                    )
                FROM {table}
                LIMIT %s;
                """
            )
            params = ("International", query_limit)
            cur.execute(stmt, params)
            metrics["intl_pct"] = cur.fetchone()[0]

            # Question 3
            stmt = applicants_sql(
                """
                SELECT
                    ROUND(
                        (AVG(gpa) FILTER (
                            WHERE gpa IS NOT NULL
                              AND gpa BETWEEN %s AND %s
                        ))::numeric,
                        2
                    ) AS avg_gpa,
                    ROUND(
                        (AVG(gre) FILTER (
                            WHERE gre IS NOT NULL
                              AND gre BETWEEN %s AND %s
                        ))::numeric,
                        2
                    ) AS avg_gre,
                    ROUND(
                        (AVG(gre_v) FILTER (
                            WHERE gre_v IS NOT NULL
                              AND gre_v BETWEEN %s AND %s
                        ))::numeric,
                        2
                    ) AS avg_gre_v,
                    ROUND(
                        (AVG(gre_aw) FILTER (
                            WHERE gre_aw IS NOT NULL
                              AND gre_aw BETWEEN %s AND %s
                        ))::numeric,
                        2
                    ) AS avg_gre_aw
                FROM {table}
                LIMIT %s;
                """
            )
            params = (0, 4.33, 0, 340, 0, 170, 0, 6.0, query_limit)
            cur.execute(stmt, params)
            row = cur.fetchone()
            metrics["avg_gpa"] = row[0]
            metrics["avg_gre"] = row[1]
            metrics["avg_gre_v"] = row[2]
            metrics["avg_gre_aw"] = row[3]

            # Question 4
            stmt = applicants_sql(
                """
                SELECT
                    ROUND((AVG(gpa) FILTER (WHERE gpa IS NOT NULL))::numeric, 2) AS avg_gpa
                FROM {table}
                WHERE us_or_international = %s
                  AND term ILIKE %s
                  AND gpa IS NOT NULL
                  AND gpa <= %s
                LIMIT %s;
                """
            )
            params = ("American", "%Fall 2026%", 4.33, query_limit)
            cur.execute(stmt, params)
            metrics["avg_gpa_american_fall_2026"] = cur.fetchone()[0]

            # Question 5
            stmt = applicants_sql(
                """
                SELECT
                    ROUND(
                        100.0 * SUM(CASE WHEN status = %s THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0),
                        2
                    )
                FROM {table}
                WHERE term ILIKE %s
                LIMIT %s;
                """
            )
            params = ("Accepted", "Fall 2026", query_limit)
            cur.execute(stmt, params)
            metrics["acceptance_pct_fall_2026"] = cur.fetchone()[0]

            # Question 6
            stmt = applicants_sql(
                """
                SELECT
                    ROUND(AVG(gpa)::numeric, 2) AS avg_gpa
                FROM {table}
                WHERE status = %s
                  AND term ILIKE %s
                  AND gpa IS NOT NULL
                  AND gpa <= %s
                LIMIT %s;
                """
            )
            params = ("Accepted", "Fall 2026", 4.33, query_limit)
            cur.execute(stmt, params)
            metrics["avg_gpa_accepted_fall_2026"] = cur.fetchone()[0]

            # Question 7
            cs_patterns = ["%Computer Science%"]
            jhu_patterns = ["%Johns Hopkins%", "%John Hopkins%", "%JHU%"]
            stmt = applicants_sql(
                """
                SELECT COUNT(*)
                FROM {table}
                WHERE degree = %s
                  AND (
                        program ILIKE ANY (%s)
                     OR llm_generated_program ILIKE ANY (%s)
                  )
                  AND (
                        program ILIKE ANY (%s)
                     OR llm_generated_university ILIKE ANY (%s)
                  )
                LIMIT %s;
                """
            )
            params = (1.0, cs_patterns, cs_patterns, jhu_patterns, jhu_patterns, query_limit)
            cur.execute(stmt, params)
            metrics["jhu_ms_cs_count"] = cur.fetchone()[0]

            # Question 8
            cs_patterns = ["%Computer Science%"]
            uni_patterns = [
                "%Georgetown%",
                "%Massachusetts Institute of Technology%",
                "%MIT%",
                "%Stanford%",
                "%Carnegie Mellon%",
                "%CMU%",
            ]
            stmt = applicants_sql(
                """
                SELECT COUNT(*)
                FROM {table}
                WHERE status = %s
                  AND degree = %s
                  AND term in (%s, %s)
                  AND program ILIKE ANY (%s)
                  AND program ILIKE ANY (%s)
                LIMIT %s;
                """
            )
            params = (
                "Accepted",
                2.0,
                "Fall 2026",
                "Spring 2026",
                cs_patterns,
                uni_patterns,
                query_limit,
            )
            cur.execute(stmt, params)
            metrics["cs_phd_accept_2026"] = cur.fetchone()[0]

            # Question 9
            stmt = applicants_sql(
                """
                SELECT COUNT(*)
                FROM {table}
                WHERE status = %s
                  AND degree = %s
                  AND term in (%s, %s)
                  AND llm_generated_program ILIKE ANY (%s)
                  AND llm_generated_university ILIKE ANY (%s)
                LIMIT %s;
                """
            )
            params = (
                "Accepted",
                2.0,
                "Fall 2026",
                "Spring 2026",
                cs_patterns,
                uni_patterns,
                query_limit,
            )
            cur.execute(stmt, params)
            metrics["cs_phd_accept_2026_llm"] = cur.fetchone()[0]

            # Question 10
            unc_patterns = [
                "%UNC%",
                "%UNC-CH%",
                "%UNC CH%",
                "%University of North Carolina at Chapel Hill%",
                "%Chapel Hill%",
            ]
            stmt = applicants_sql(
                """
                SELECT
                    COALESCE(llm_generated_program, program) AS program_name,
                    COUNT(*) AS n
                FROM {table}
                WHERE degree = %s
                  AND status = %s
                  AND term = %s
                  AND (
                        program ILIKE ANY (%s)
                     OR llm_generated_university ILIKE ANY (%s)
                  )
                GROUP BY program_name
                ORDER BY n DESC, program_name
                LIMIT %s;
                """
            )
            params = (1.0, "Accepted", "Fall 2026", unc_patterns, unc_patterns, query_limit)
            cur.execute(stmt, params)
            metrics["unc_masters_program_rows"] = cur.fetchall()

            # Question 11
            unc_patterns = [
                "%UNC%",
                "%UNC-CH%",
                "%UNC CH%",
                "%University of North Carolina%",
                "%Chapel Hill%",
            ]
            program_patterns = ["%Biostat%", "%Epidemiolog%"]
            stmt = applicants_sql(
                """
                SELECT
                    llm_generated_program,
                    COUNT(*) AS n
                FROM {table}
                WHERE degree = %s
                  AND term = %s
                  AND (
                        llm_generated_program ILIKE ANY (%s)
                  )
                      AND (
                            llm_generated_university ILIKE ANY (%s)
                      )
                GROUP BY llm_generated_program
                ORDER BY n DESC, llm_generated_program
                LIMIT %s;
                """
            )
            params = (2.0, "Fall 2026", program_patterns, unc_patterns, query_limit)
            cur.execute(stmt, params)
            metrics["unc_phd_program_rows"] = cur.fetchall()

    # Return the answers to the questions
    return metrics


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
    except Exception:
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
