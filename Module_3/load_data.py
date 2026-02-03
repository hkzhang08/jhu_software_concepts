"""
Class:         EP 605 2256 81 - Spring 2026
Assignment:    Module_3 - Database Queries Assignment Experiment
Due Date:      February 8th by 11:59AM
Name:          Helen Zhang
Email:         hzhan308@jh.edu
"""

# Load collected data into a PostgreSQL database using psycopg
import json
import re
from datetime import datetime
import psycopg


# LLM cleaned data from Module 2
DATA_PATH = "llm_extend_applicant_data.json"
# Data source name
DSN = "dbname=grad_cafe user=zhang8 host=localhost"


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

    cleaned = str(value).strip()

    # Try different date formats as neede
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
    v = str(value).strip().lower()

    # Update phd/doctorate to 2.0
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


# Create PostgreSQL database
with psycopg.connect(DSN) as conn:

    # Create cursor to run SQL
    with conn.cursor() as cur:

        # Create table for applicant_data and p_id
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

        # Initialize rows
        rows = []

        # Load data from JSON file
        with open(DATA_PATH) as handle:
            for line in handle:

                # Skip empty lines
                if not line.strip():
                    continue

                # Load JSON into dictionary
                row = json.loads(line)

                # Build table to match assignment details
                rows.append(
                    (
                        ftext(row.get("program")),
                        ftext(row.get("comments")),
                        fdate(row.get("date_added")),
                        ftext(row.get("url")),
                        ftext(row.get("applicant_status")),
                        ftext(row.get("semester_year_start")),
                        ftext(row.get("citizenship")),
                        fnum(row.get("gpa")),
                        fnum(row.get("gre")),
                        fnum(row.get("gre_v")),
                        fnum(row.get("gre_aw")),
                        fdegree(row.get("masters_or_phd")),
                        ftext(
                            row.get("llm-generated-program")
                            or row.get("llm_generated_program")
                        ),
                        ftext(
                            row.get("llm-generated-university")
                            or row.get("llm_generated_university")
                        ),
                    )
                )

        # Insert all rows into the database
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
            rows,
        )

    # Commit and save changes
    conn.commit()

# Confirm how many rows were inserted
print(f"Inserted rows: {len(rows)}")
