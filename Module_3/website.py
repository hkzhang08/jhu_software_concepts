"""
Class:         EP 605 2256 81 - Spring 2026
Assignment:    Module_3 - Database Queries Assignment Experiment
Due Date:      February 8th by 11:59AM
Name:          Helen Zhang
Email:         hzhan308@jh.edu
"""


# Imports to display analysis results from a PostgreSQL database
import json
import os
import re
import subprocess
import sys
from datetime import datetime
import psycopg
from flask import Flask, redirect, render_template, url_for

# Create flask app
app = Flask(__name__)

# Create connections and paths to database
DSN = "dbname=grad_cafe user=zhang8 host=localhost"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_FILE = os.path.join(BASE_DIR, "applicant_data.json")
PULL_TARGET_N = 200
PULL_STATE = {"status": "idle", "message": ""}


def fnum(value):
    """
    Purpose: Convert values to a float if needed
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
    Purpose: Convert date strings to date objects
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
    Purpose: update PhDs (2.0) and Masters (1.0) to floats
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
    Purpose: Convert text values to string
    """

    # Return none if missing
    if value is None:
        return None

    # Return string and remove null bytes
    return str(value).replace("\x00", "")


def ensure_applicant_table(cur):
    """
    Purpose: create applicants table if not exists
    """

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS applicants (
            p_id SERIAL PRIMARY KEY,
            program TEXT,
            comments TEXT,
            date_added DATE,
            url TEXT,
            status TEXT,
            term TEXT,
            us_or_international TEXT,
            gpa DOUBLE PRECISION,
            gre DOUBLE PRECISION,
            gre_v DOUBLE PRECISION,
            gre_aw DOUBLE PRECISION,
            degree DOUBLE PRECISION,
            llm_generated_program TEXT,
            llm_generated_university TEXT
        );
        """
    )


def load_cleaned_data_to_db(file_path: str) -> int:
    """
    Purpose: load cleaned records from a JSON file into the applicants table
    """

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
            cur.execute("SELECT url FROM applicants WHERE url IS NOT NULL;")
            existing_urls = {row[0] for row in cur.fetchall()}

            # Check urls of data
            inserts = []
            for row in rows:
                url = ftext(row.get("url"))
                if url and url in existing_urls:
                    continue

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
                cur.executemany(
                    """
                    INSERT INTO applicants (
                        program,
                        comments,
                        date_added,
                        url,
                        status,
                        term,
                        us_or_international,
                        gpa,
                        gre,
                        gre_v,
                        gre_aw,
                        degree,
                        llm_generated_program,
                        llm_generated_university
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    inserts,
                )

        # Commit and save new records
        conn.commit()

    # Return number of new records inserted
    return len(inserts)


def run_pull_pipeline():
    """
    Purpose: Run the full data refresh pipeline
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

        # Run the clean.py script to clean the raw data and then load_data.py to insert into the database
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

    # Store any errors for the website
    except Exception as exc:
        PULL_STATE["status"] = "error"
        PULL_STATE["message"] = f"Pull failed: {exc}"


def fetch_metrics() -> dict:
    """
    Purpose: query the database to answer the questions
    """

    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:

            # Question 1
            cur.execute(
                """
                SELECT COUNT(*)
                FROM applicants
                WHERE term ILIKE %s;
                """,
                ("Fall 2026",),
            )
            fall_2026_count = cur.fetchone()[0]

            # Question 2
            cur.execute(
                """
                SELECT
                    ROUND(
                        100.0 * SUM(CASE WHEN us_or_international = 'International' THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0),
                        2
                    )
                FROM applicants;
                """
            )
            intl_pct = cur.fetchone()[0]

            # Question 3
            cur.execute(
                """
                SELECT
                    ROUND((AVG(gpa) FILTER (WHERE gpa IS NOT NULL AND gpa BETWEEN 0 AND 4.33))::numeric, 2) AS avg_gpa,
                    ROUND((AVG(gre) FILTER (WHERE gre IS NOT NULL AND gre BETWEEN 0 AND 340))::numeric, 2) AS avg_gre,
                    ROUND((AVG(gre_v) FILTER (WHERE gre_v IS NOT NULL AND gre_v BETWEEN 0 AND 170))::numeric, 2) AS avg_gre_v,
                    ROUND((AVG(gre_aw) FILTER (WHERE gre_aw IS NOT NULL AND gre_aw BETWEEN 0 AND 6.0))::numeric, 2) AS avg_gre_aw
                FROM applicants;
                """
            )
            avg_gpa, avg_gre, avg_gre_v, avg_gre_aw = cur.fetchone()

            # Question 4
            cur.execute(
                """
                SELECT
                    ROUND((AVG(gpa) FILTER (WHERE gpa IS NOT NULL))::numeric, 2) AS avg_gpa
                FROM applicants
                WHERE us_or_international = 'American'
                  AND term ILIKE %s
                  AND gpa IS NOT NULL
                  AND gpa <= 4.33;
                """,
                ("%Fall 2026%",),
            )
            avg_gpa_american_fall_2026 = cur.fetchone()[0]

            # Question 5
            cur.execute(
                """
                SELECT
                    ROUND(
                        100.0 * SUM(CASE WHEN status = 'Accepted' THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0),
                        2
                    )
                FROM applicants
                WHERE term ILIKE %s;
                """,
                ("Fall 2026",),
            )
            acceptance_pct_fall_2026 = cur.fetchone()[0]

            # Question 6
            cur.execute(
                """
                SELECT
                    ROUND(AVG(gpa)::numeric, 2) AS avg_gpa
                FROM applicants
                WHERE status = 'Accepted'
                  AND term ILIKE %s
                  AND gpa IS NOT NULL
                  AND gpa <= 4.33;
                """,
                ("Fall 2026",),
            )
            avg_gpa_accepted_fall_2026 = cur.fetchone()[0]

            # Question 7
            cs_patterns = ["%Computer Science%"]
            jhu_patterns = ["%Johns Hopkins%", "%John Hopkins%", "%JHU%"]
            cur.execute(
                """
                SELECT COUNT(*)
                FROM applicants
                WHERE degree = 1.0
                  AND (
                        program ILIKE ANY (%s)
                     OR llm_generated_program ILIKE ANY (%s)
                  )
                  AND (
                        program ILIKE ANY (%s)
                     OR llm_generated_university ILIKE ANY (%s)
                  );
                """,
                (cs_patterns, cs_patterns, jhu_patterns, jhu_patterns),
            )
            jhu_ms_cs_count = cur.fetchone()[0]

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
            cur.execute(
                """
                SELECT COUNT(*)
                FROM applicants
                WHERE status = 'Accepted'
                  AND degree = 2.0
                  AND term in ('Fall 2026', 'Spring 2026')
                  AND program ILIKE ANY (%s)
                  AND program ILIKE ANY (%s);
                """,
                (cs_patterns, uni_patterns),
            )
            cs_phd_accept_2026 = cur.fetchone()[0]

            # Question 9
            cur.execute(
                """
                SELECT COUNT(*)
                FROM applicants
                WHERE status = 'Accepted'
                  AND degree = 2.0
                  AND term in ('Fall 2026', 'Spring 2026')
                  AND llm_generated_program ILIKE ANY (%s)
                  AND llm_generated_university ILIKE ANY (%s);
                """,
                (cs_patterns, uni_patterns),
            )
            cs_phd_accept_2026_llm = cur.fetchone()[0]

            # Question 10
            unc_patterns = [
                "%UNC%",
                "%UNC-CH%",
                "%UNC CH%",
                "%University of North Carolina at Chapel Hill%",
                "%Chapel Hill%",
            ]
            cur.execute(
                """
                SELECT
                    COALESCE(llm_generated_program, program) AS program_name,
                    COUNT(*) AS n
                FROM applicants
                WHERE degree = 1.0
                  AND status = 'Accepted'
                  AND term = 'Fall 2026'
                  AND (
                        program ILIKE ANY (%s)
                     OR llm_generated_university ILIKE ANY (%s)
                  )
                GROUP BY program_name
                ORDER BY n DESC, program_name;
                """,
                (unc_patterns, unc_patterns),
            )
            unc_masters_program_rows = cur.fetchall()

            # Question 11
            unc_patterns = [
                "%UNC%",
                "%UNC-CH%",
                "%UNC CH%",
                "%University of North Carolina%",
                "%Chapel Hill%",
            ]
            program_patterns = ["%Biostat%", "%Epidemiolog%"]
            cur.execute(
                """
                SELECT
                    llm_generated_program,
                    COUNT(*) AS n
                FROM applicants
                WHERE degree = 2.0
                  AND term = 'Fall 2026'
                  AND (
                        llm_generated_program ILIKE ANY (%s)
                  )
                  AND (
                        llm_generated_university ILIKE ANY (%s)
                  )
                GROUP BY llm_generated_program
                ORDER BY n DESC, llm_generated_program;
                """,
                (program_patterns, unc_patterns),
            )
            unc_phd_program_rows = cur.fetchall()

    # Return the answers to the questions
    return {
        "fall_2026_count": fall_2026_count,
        "intl_pct": intl_pct,
        "avg_gpa": avg_gpa,
        "avg_gre": avg_gre,
        "avg_gre_v": avg_gre_v,
        "avg_gre_aw": avg_gre_aw,
        "avg_gpa_american_fall_2026": avg_gpa_american_fall_2026,
        "acceptance_pct_fall_2026": acceptance_pct_fall_2026,
        "avg_gpa_accepted_fall_2026": avg_gpa_accepted_fall_2026,
        "jhu_ms_cs_count": jhu_ms_cs_count,
        "cs_phd_accept_2026": cs_phd_accept_2026,
        "cs_phd_accept_2026_llm": cs_phd_accept_2026_llm,
        "unc_masters_program_rows": unc_masters_program_rows,
        "unc_phd_program_rows": unc_phd_program_rows,
    }


@app.route("/pull-data", methods=["POST"])
def pull_data():
    """
    Purpose: Creates and updates"Pull Data" button
    """

    # If data is running, do not start a second data pull
    if PULL_STATE["status"] == "running":
        return redirect(url_for("index"))

    # Run the pipeline to pull data and update the database
    run_pull_pipeline()

    # Update the website
    return redirect(url_for("index"))


@app.route("/update-analysis", methods=["POST"])
def update_analysis():
    """
    Purpose: Creates the "Update Analysis" button
    """

    # If data is running, do not update analysis
    if PULL_STATE["status"] == "running":
        return redirect(url_for("index"))

    # Refresh data
    return redirect(url_for("index"))


@app.route("/")
def index():
    """
    Purpose: Pull the lastest answers from the database
    """

    metrics = fetch_metrics()

    # Format the SQL questions
    questions = [
        {
            "question": "How many entries do you have in your database who have applied for Fall 2026?",
            "answer": f"Fall 2026 Applicants: {metrics['fall_2026_count']}",
        },
        {
            "question": "What percentage of entries are from international students (not American or Other)?",
            "answer": f"Percentage International: {metrics['intl_pct']}",
        },
        {
            "question": "What is the average GPA, GRE, GRE V, GRE AW of applicants who provide these metrics?",
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
            "answer": f"Fall 2026 Acceptance percent: {metrics['acceptance_pct_fall_2026']}",
        },
        {
            "question": "What is the average GPA of applicants who applied for Fall 2026 who are Acceptances?",
            "answer": f"Average GPA of Fall 2026 Acceptances: {metrics['avg_gpa_accepted_fall_2026']}",
        },
        {
            "question": "How many entries are from applicants who applied to JHU for a "
                        "masters degrees in Computer Science?",
            "answer": f"JHU Masters of Computer Science Applicants: {metrics['jhu_ms_cs_count']}",
        },
        {
            "question": (
                "How many entries from 2026 are acceptances from applicants who applied to Georgetown University, "
                "MIT, Stanford University, or Carnegie Mellon University for a PhD in Computer Science?"
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
            "question": "How many applicants by masters program are from UNC CH in Fall 2026?",
            "answer_lines": [
                f"{program_name or 'Unknown'}: {count}"
                for program_name, count in metrics["unc_masters_program_rows"]
            ] or ["No rows found."],
        },
        {
            "question": (
                "How many PhD Biostatistics or Epidemiology applicants were there from "
                "UNC CH in Fall 2026 by program?"
            ),
            "answer_lines": [
                f"{program_name or 'Unknown'}: {count}"
                for program_name, count in metrics["unc_phd_program_rows"]
            ] or ["No rows found."],
        },
    ]
    return render_template("index.html", questions=questions, pull_state=PULL_STATE)


# Run only if executed directly
if __name__ == "__main__":
    app.run(debug=True)
