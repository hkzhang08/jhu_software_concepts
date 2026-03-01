CREATE TABLE IF NOT EXISTS applicants (
    applicant_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    program TEXT,
    university TEXT,
    status TEXT,
    last_processed_at TIMESTAMPTZ
);
