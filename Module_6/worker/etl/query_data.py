import os

import psycopg

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "app_db")
DB_USER = os.getenv("DB_USER", "app_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "app_password")

CREATE_WATERMARK_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ingestion_watermarks (
    source TEXT PRIMARY KEY,
    last_seen TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
)
"""

SELECT_WATERMARK_SQL = """
SELECT last_seen
FROM ingestion_watermarks
WHERE source = %(source)s
"""

UPSERT_WATERMARK_SQL = """
INSERT INTO ingestion_watermarks (source, last_seen)
VALUES (%(source)s, %(last_seen)s)
ON CONFLICT (source) DO UPDATE
SET
    last_seen = EXCLUDED.last_seen,
    updated_at = now()
"""

UPSERT_SQL = """
INSERT INTO applicants (
    applicant_id,
    name,
    email,
    program,
    university,
    status,
    last_processed_at
)
VALUES (
    %(applicant_id)s,
    %(name)s,
    %(email)s,
    %(program)s,
    %(university)s,
    %(status)s,
    %(last_processed_at)s
)
ON CONFLICT (applicant_id) DO UPDATE
SET
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    program = EXCLUDED.program,
    university = EXCLUDED.university,
    status = EXCLUDED.status,
    last_processed_at = EXCLUDED.last_processed_at
"""


def get_db_dsn() -> str:
    """Return DATABASE_URL when present, otherwise build a conninfo string."""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url
    return (
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD}"
    )


def upsert_applicants(applicants: list[dict], conn=None) -> int:
    """Insert or update a batch of applicants using parameterized SQL."""
    if not applicants:
        return 0

    if conn is None:
        with psycopg.connect(get_db_dsn()) as local_conn:
            with local_conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, applicants)
        return len(applicants)

    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, applicants)
    return len(applicants)


def upsert_applicant(applicant: dict, conn=None) -> None:
    """Insert or update a single applicant row."""
    upsert_applicants([applicant], conn=conn)


def fetch_last_seen(source: str, conn=None) -> str | None:
    """Return the current ingestion watermark for a given source, if any."""
    if conn is None:
        with psycopg.connect(get_db_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_WATERMARK_TABLE_SQL)
                cur.execute(SELECT_WATERMARK_SQL, {"source": source})
                row = cur.fetchone()
        if row is None:
            return None
        return row[0]

    with conn.cursor() as cur:
        cur.execute(CREATE_WATERMARK_TABLE_SQL)
        cur.execute(SELECT_WATERMARK_SQL, {"source": source})
        row = cur.fetchone()

    if row is None:
        return None
    return row[0]


def upsert_last_seen(source: str, last_seen: str, conn=None) -> None:
    """Persist the latest ingestion watermark for a given source."""
    if conn is None:
        with psycopg.connect(get_db_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_WATERMARK_TABLE_SQL)
                cur.execute(
                    UPSERT_WATERMARK_SQL,
                    {"source": source, "last_seen": last_seen},
                )
        return

    with conn.cursor() as cur:
        cur.execute(CREATE_WATERMARK_TABLE_SQL)
        cur.execute(
            UPSERT_WATERMARK_SQL,
            {"source": source, "last_seen": last_seen},
        )
