import json
import os
from pathlib import Path

import psycopg

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "app_db")
DB_USER = os.getenv("DB_USER", "app_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "app_password")

DATA_FILE = Path(os.getenv("DATA_FILE", "/opt/project/data/applicant_data.json"))

CREATE_WATERMARK_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ingestion_watermarks (
    source TEXT PRIMARY KEY,
    last_seen TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
)
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


def load_records(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, list):
        raise ValueError("Input JSON must be a list of applicant objects.")
    return data


def main() -> None:
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")

    records = load_records(DATA_FILE)
    dsn = (
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD}"
    )

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_WATERMARK_TABLE_SQL)
            for record in records:
                cur.execute(UPSERT_SQL, record)
        conn.commit()

    print(f"Loaded {len(records)} applicants from {DATA_FILE}")


if __name__ == "__main__":
    main()
