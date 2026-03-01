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

CREATE TABLE IF NOT EXISTS public.ingestion_watermarks (
    source TEXT PRIMARY KEY,
    last_seen TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);
