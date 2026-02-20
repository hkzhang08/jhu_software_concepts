-- One-time schema bootstrap for Module_5.
--
-- Run this as a schema owner/admin before using the least-privilege app user.
-- Example:
--   psql -d grad_cafe -f src/sql/bootstrap_applicants_table.sql

\set ON_ERROR_STOP 1

CREATE TABLE IF NOT EXISTS public.applicants (
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
