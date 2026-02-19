"""
Run SQL queries against the applicants table and print answers.

This module is intended to be executed as a script. It connects to the
database using environment variables and prints the results of each
question.
"""


# Carry out data analysis using SQL queries to answer questions about entries to grad café
import os
import psycopg
from psycopg import sql

try:
    from .db_config import get_db_dsn
except ImportError:  # pragma: no cover - script execution path
    from db_config import get_db_dsn

# Data source name for postgreSQL
DSN = get_db_dsn()
APPLICANTS_TABLE = sql.Identifier("applicants")
MIN_QUERY_LIMIT = 1
MAX_QUERY_LIMIT = 100


def clamp_limit(raw_limit):
    """
    Clamp any requested limit to the safe query window (1..100).

    :param raw_limit: Raw value from configuration/input.
    :returns: Integer limit constrained to [MIN_QUERY_LIMIT, MAX_QUERY_LIMIT].
    """

    try:
        parsed_limit = int(raw_limit)
    except (TypeError, ValueError):
        return MAX_QUERY_LIMIT
    return max(MIN_QUERY_LIMIT, min(parsed_limit, MAX_QUERY_LIMIT))


QUERY_LIMIT = clamp_limit(os.environ.get("QUERY_LIMIT", MAX_QUERY_LIMIT))


def applicants_sql(query_template: str):
    """
    Compose SQL with a safely quoted applicants table identifier.

    :param query_template: SQL template containing ``{table}``.
    :returns: Composed SQL object.
    """

    return sql.SQL(query_template).format(table=APPLICANTS_TABLE)


# Question 1: How many entries have applied for Fall 2026?
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:

        # Count number of records with filters
        stmt = applicants_sql(
            """
            SELECT COUNT(*)
            FROM {table}
            WHERE term ILIKE %s
            LIMIT %s;
            """
        )
        params = ("Fall 2026", QUERY_LIMIT)
        cur.execute(stmt, params)
        fall_2025_count = cur.fetchone()[0]

print(f"Fall 2026 applicants: {fall_2025_count}\n")


# Question 2: Percentage of international entries (two decimals).
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Sum cases matching filters, divide by count, and round to 2 decimals.
        stmt = applicants_sql(
            """
            SELECT
                ROUND(
                    100.0 * SUM(CASE WHEN us_or_international = %s THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0),
                    2
                )
            FROM {table}
            LIMIT %s;
            """
        )
        params = ("International", QUERY_LIMIT)
        cur.execute(stmt, params)
        intl_pct = cur.fetchone()[0]

print(f"Percentage of International Entries: {intl_pct}\n")


# Question 3: Average GPA/GRE metrics among valid values.
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Filter to valid ranges and round to 2 decimals.
        stmt = applicants_sql(
            """
            SELECT
                ROUND(
                    (AVG(gpa) FILTER (WHERE gpa IS NOT NULL AND gpa BETWEEN %s AND %s))::numeric,
                    2
                ) AS avg_gpa,
                ROUND(
                    (AVG(gre) FILTER (WHERE gre IS NOT NULL AND gre BETWEEN %s AND %s))::numeric,
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
        params = (0, 4.33, 0, 340, 0, 170, 0, 6.0, QUERY_LIMIT)
        cur.execute(stmt, params)
        avg_gpa, avg_gre, avg_gre_v, avg_gre_aw = cur.fetchone()

print(
    "Averages Scores of Applicants:\n"
    f"GPA: {avg_gpa}\n"
    f"GRE: {avg_gre}\n"
    f"GRE V: {avg_gre_v}\n"
    f"GRE AW: {avg_gre_aw}\n"
)


# Question 4: Average GPA of American applicants in Fall 2026.
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Exclude likely GPA errors above the 4.33 scale.
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
        params = ("American", "%Fall 2026%", 4.33, QUERY_LIMIT)
        cur.execute(stmt, params)
        avg_gpa_american_fall_2026 = cur.fetchone()[0]

print(f"Average GPA of Fall 2026 American Applicants: {avg_gpa_american_fall_2026}\n")


# Question 5: Acceptance percentage for Fall 2026 (two decimals).
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Sum cases matching filters, divide by count, round to 2 decimals.
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
        params = ("Accepted", "Fall 2026", QUERY_LIMIT)
        cur.execute(stmt, params)
        acceptance_pct_fall_2026 = cur.fetchone()[0]

print(f"Percentage of Fall 2026 Acceptances (to date): {acceptance_pct_fall_2026}\n")


# Question 6: Average GPA of accepted Fall 2026 applicants.
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Restrict to accepted Fall 2026 applicants and valid GPA values.
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
        params = ("Accepted", "Fall 2026", 4.33, QUERY_LIMIT)
        cur.execute(stmt, params)
        avg_gpa_american_fall_2026 = cur.fetchone()[0]

print(f"Average GPA of Fall 2026 Accepted Applicants: {avg_gpa_american_fall_2026}\n")


# Question 7: JHU MS CS applicant count (raw + LLM fields).
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Match on both raw program text and LLM-normalized fields.
        cs_patterns = ["%Computer Science%"]
        jhu_patterns = ["%Johns Hopkins%", "%John Hopkins%", "%JHU%"]

        # Use both raw text fields and LLM-generated fields to improve matching
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
        params = (1.0, cs_patterns, cs_patterns, jhu_patterns, jhu_patterns, QUERY_LIMIT)
        cur.execute(stmt, params)
        jhu_ms_cs_count = cur.fetchone()[0]

print(f"JHU Masters of Computer Science Applicants: {jhu_ms_cs_count}\n")


# Question 8: 2026 PhD CS acceptances at Georgetown/MIT/Stanford/CMU.
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Create patterns to match against program names.
        cs_patterns = ["%Computer Science%"]
        uni_patterns = [
            "%Georgetown%",
            "%Massachusetts Institute of Technology%",
            "%MIT%",
            "%Stanford%",
            "%Carnegie Mellon%",
            "%CMU%",
        ]
        # Match against multiple school name patterns.
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
        params = ("Accepted", 2.0, "Fall 2026", "Spring 2026", cs_patterns, uni_patterns, QUERY_LIMIT)
        cur.execute(stmt, params)
        cs_phd_accept_2026 = cur.fetchone()[0]

print(
    "Accepted 2026 PhD CS applicants at Georgetown/MIT/Stanford/CMU: "
    f"{cs_phd_accept_2026}\n"
)


# Question 9: Same as Q8 using LLM-normalized fields.
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Create patterns to match against raw and LLM fields.
        cs_patterns = ["%Computer Science%"]
        uni_patterns = [
            "%Georgetown%",
            "%Massachusetts Institute of Technology%",
            "%MIT%",
            "%Stanford%",
            "%Carnegie Mellon%",
            "%CMU%",
        ]
        # Check the LLM standardized fields instead of raw text.
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
        params = ("Accepted", 2.0, "Fall 2026", "Spring 2026", cs_patterns, uni_patterns, QUERY_LIMIT)
        cur.execute(stmt, params)
        cs_phd_accept_2026_llm = cur.fetchone()[0]

print(
    "Accepted 2026 PhD CS at Georgetown/MIT/Stanford/CMU (LLM Version): "
    f"{cs_phd_accept_2026_llm}\n"
)


# Question 10: Accepted UNC-CH Masters applicants in Fall 2026 by program.
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Check different patterns for UNC CH.
        unc_patterns = [
            "%UNC%",
            "%UNC-CH%",
            "%UNC CH%",
            "%University of North Carolina at Chapel Hill%",
            "%Chapel Hill%",
        ]
        # Use the first matching program name (LLM if present).
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
        params = (1.0, "Accepted", "Fall 2026", unc_patterns, unc_patterns, QUERY_LIMIT)
        cur.execute(stmt, params)
        rows = cur.fetchall()

print("UNC-CH Masters programs (Fall 2026):")
for program_name, count in rows:
    print(program_name, count)
print("\n")


# Question 11: UNC-CH PhD Biostat/Epidemiology applicants by program.
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Check different patterns for UNC CH.
        unc_patterns = [
            "%UNC%",
            "%UNC-CH%",
            "%UNC CH%",
            "%University of North Carolina%",
            "%Chapel Hill%",
        ]
        # Check different patterns for biostat/epidemiology.
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
        params = (2.0, "Fall 2026", program_patterns, unc_patterns, QUERY_LIMIT)
        cur.execute(stmt, params)
        rows = cur.fetchall()

print("UNC-CH PhD Biostat/Epidemiology (Fall 2026) by program:")
for llm_generated_program, count in rows:
    print(llm_generated_program, count)
