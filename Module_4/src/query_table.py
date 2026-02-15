"""
Class:         EP 605 2256 81 - Spring 2026
Assignment:    Module_3 - Database Queries Assignment Experiment
Due Date:      February 8th by 11:59AM
Name:          Helen Zhang
Email:         hzhan308@jh.edu

Run SQL queries against the applicants table and print answers.

This module is intended to be executed as a script. It connects to the
database using ``DATABASE_URL`` and prints the results of each question.
"""


# Carry out data analysis using SQL queries to answer questions about submission entries to grad café
import os
import psycopg

# Data source name for postgreSQL
DSN = os.environ["DATABASE_URL"]


# Question 1: How many entries have applied for Fall 2026?
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:

        # Count number of records with filters
        cur.execute(
            """
            SELECT COUNT(*)
            FROM applicants
            WHERE term ILIKE %s;
            """,
            ("Fall 2026",),
        )
        fall_2025_count = cur.fetchone()[0]

print(f"Fall 2026 applicants: {fall_2025_count}\n")


# Question 2: Percentage of international entries (two decimals).
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Sum cases matching filters, divide by count, and round to 2 decimals.
        cur.execute(
            """
            SELECT
                ROUND(
                    100.0 * SUM(CASE WHEN us_or_international = 'International' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0),
                    2
                )
            FROM applicants
            """
        )
        intl_pct = cur.fetchone()[0]

print(f"Percentage of International Entries: {intl_pct}\n")


# Question 3: Average GPA/GRE metrics among valid values.
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Filter to valid ranges and round to 2 decimals.
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

print(f"Average GPA of Fall 2026 American Applicants: {avg_gpa_american_fall_2026}\n")


# Question 5: Acceptance percentage for Fall 2026 (two decimals).
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Sum cases matching filters, divide by count, round to 2 decimals.
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

print(f"Percentage of Fall 2026 Acceptances (to date): {acceptance_pct_fall_2026}\n")


# Question 6: Average GPA of accepted Fall 2026 applicants.
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Restrict to accepted Fall 2026 applicants and valid GPA values.
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
        avg_gpa_american_fall_2026 = cur.fetchone()[0]

print(f"Average GPA of Fall 2026 Accepted Applicants: {avg_gpa_american_fall_2026}\n")


# Question 7: JHU MS CS applicant count (raw + LLM fields).
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        # Match on both raw program text and LLM-normalized fields.
        cs_patterns = ["%Computer Science%"]
        jhu_patterns = ["%Johns Hopkins%", "%John Hopkins%", "%JHU%"]

        # Use both raw text fields and LLM-generated fields to improve matching
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
        rows = cur.fetchall()

print("UNC-CH PhD Biostat/Epidemiology (Fall 2026) by program:")
for llm_generated_program, count in rows:
    print(llm_generated_program, count)
