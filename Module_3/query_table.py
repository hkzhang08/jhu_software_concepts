"""
Class:         EP 605 2256 81 - Spring 2026
Assignment:    Module_3 - Database Queries Assignment Experiment
Due Date:      February 8th by 11:59AM
Name:          Helen Zhang
Email:         hzhan308@jh.edu
"""


# Carry out data analysis using SQL queries to answer questions about submission entries to grad café
import psycopg

DSN = "dbname=grad_cafe user=zhang8 host=localhost"


# Question 1
# How many entries do you have in your database who have applied for Fall 2026?
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
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


# Question 2
# What percentage of entries are from international students (to two decimal places)?
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
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


# Question 3
# What is the average GPA, GRE, GRE V, GRE AW of applicants who provide these metrics?
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
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

# Remove gpa greater than 4.33 (the US max)
# Remove gre greater than 340 (the max)
# Remove gre verbal greater than 170 (the max)
# Remove gre writing greater than 6.0 (the max)

print(
    "Averages Scores of Applicants:\n"
    f"GPA: {avg_gpa}\n"
    f"GRE: {avg_gre}\n"
    f"GRE V: {avg_gre_v}\n"
    f"GRE AW: {avg_gre_aw}\n"
)


# Question 4
# What is the average GPA of American students in Fall 2026?
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
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

# Needed to exclude likely gpa errors where gpa > 4.33 (ex. case where gpa = 389.0)
print(f"Average GPA of Fall 2026 American Applicants: {avg_gpa_american_fall_2026}\n")


# Question 5
# What percent of entries for Fall 2026 are Acceptances (to two decimal places)?
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
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


# Question 6
# What is the average GPA of applicants who applied for Fall 2026 who are Acceptances?
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
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

# Needed to exclude likely gpa errors where gpa > 4.33 (ex. case where gpa = 389.0)
print(f"Average GPA of Fall 2026 Accepted Applicants: {avg_gpa_american_fall_2026}\n")


# Question 7
# How many entries are from applicants who applied to JHU for a masters degrees in Computer Science?
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
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

print(f"JHU Masters of Computer Science Applicants: {jhu_ms_cs_count}\n")


# Question 8
# How many entries from 2026 are acceptances from applicants who applied to
# Georgetown, MIT, Stanford, or CMU for a PhD in CS?
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
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

print(
    "Accepted 2026 PhD CS applicants at Georgetown/MIT/Stanford/CMU: "
    f"{cs_phd_accept_2026}\n"
)



# Question 9
# Do your numbers for Q8 change if you use LLM Fields?
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
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


# Next, come up with 2 additional questions that you are curious to answer
# Question 10
# How many entries by masters program are from UNC CH in Fall 2026
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
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
        rows = cur.fetchall()

print("UNC-CH Masters programs (Fall 2026):")
for program_name, count in rows:
    print(program_name, count)
print("\n")


# Question 11
# How many phd biostatics or epidemiology applicants were there from UNC CH in Fall 2026 by program
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
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
        rows = cur.fetchall()

print("UNC-CH PhD Biostat/Epidemiology (Fall 2026) by program:")
for llm_generated_program, count in rows:
    print(llm_generated_program, count)
