"""
Load cleaned GradCafe JSONL data into PostgreSQL.

This module is designed to be executed as a script and will:
- Verify the applicants table exists.
- Load rows from the master JSONL file and the new-rows JSONL file.
- Deduplicate by URL before insert.
"""


# Load collected data into a PostgreSQL database using psycopg
import json
import os
import psycopg

try:
    from . import db_builders
except ImportError:  # pragma: no cover - script execution path
    import db_builders


# Path to LLM cleaned JSON data (new rows only)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.environ.get(
    "LLM_NEW_APPLICANT_PATH",
    os.path.join(BASE_DIR, "llm_new_applicant.json"),
)
ORIGINAL_PATH = os.environ.get(
    "LLM_EXTEND_APPLICANT_PATH",
    os.path.join(BASE_DIR, "llm_extend_applicant_data.json"),
)

# Data source name for postgreSQL
DSN = db_builders.get_db_dsn()
fnum = db_builders.fnum
fdate = db_builders.fdate
fdegree = db_builders.fdegree
ftext = db_builders.ftext
QUERY_LIMIT = db_builders.clamp_limit(
    os.environ.get("QUERY_LIMIT", db_builders.MAX_QUERY_LIMIT)
)


def require_applicants_table(db_cursor):
    """
    Verify that the applicants table exists without requiring DDL privileges.

    :param db_cursor: Database cursor.
    :raises RuntimeError: If ``public.applicants`` is missing.
    """

    db_builders.ensure_table_exists(
        db_cursor,
        db_builders.APPLICANTS_REGCLASS,
        (
            "Required table public.applicants is missing. "
            "Create it with a schema owner before running load_data.py."
        ),
    )


# Create / connect to the PostgreSQL database
with psycopg.connect(DSN) as conn:

    # Create cursor to run SQL
    with conn.cursor() as cur:

        # Require existing table to keep runtime DB access least-privilege.
        require_applicants_table(cur)

        # Initialize rows
        rows = []

        # Pull URLs already in the database in bounded pages
        known_url_set = db_builders.fetch_existing_urls(cur, QUERY_LIMIT)
        seen_urls = set(known_url_set)

        def load_from_file(path):
            """
            Load JSONL rows from a file into the pending insert list.

            :param path: JSONL file path.
            """
            if not os.path.exists(path):
                print(f"No file found: {path}")
                return
            with open(path, encoding="utf-8") as handle:
                for line in handle:
                    # Skip empty lines
                    if not line.strip():
                        continue
                    # Load JSON into dictionary
                    row = json.loads(line)
                    url = db_builders.register_unique_url(row, seen_urls)
                    if url is None:
                        continue

                    # Build table / append cleaned values to match assignment details
                    rows.append(
                        db_builders.build_applicant_insert_row(
                            row,
                            url=url,
                            include_llm=True,
                        )
                    )

        # Load original master file first, then new rows file
        load_from_file(ORIGINAL_PATH)
        load_from_file(DATA_PATH)

        # Insert cleaned rows into the database
        if rows:
            insert_stmt = db_builders.applicants_sql(
                """
                INSERT INTO {table} (
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
                """
            )
            cur.executemany(insert_stmt, rows)

    # Commit and save changes
    conn.commit()

# Confirm how many rows were inserted
print(f"Inserted rows: {len(rows)}")
