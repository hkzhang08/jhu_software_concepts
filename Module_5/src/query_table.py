"""
Run SQL queries against the applicants table and expose reusable metrics.

This module is shared by the web app and can also be executed as a script
to print analysis answers to stdout.
"""

import os

import psycopg

try:
    from . import db_builders
except ImportError:  # pragma: no cover - script execution path
    import db_builders

DSN = db_builders.get_db_dsn()


QUERY_LIMIT = db_builders.clamp_limit(
    os.environ.get("QUERY_LIMIT", db_builders.MAX_QUERY_LIMIT)
)


def fetch_scalar_value(cur, query_template: str, params: tuple):
    """
    Execute a query and return the first column from the first row.

    :param cur: Database cursor.
    :param query_template: SQL template containing ``{table}``.
    :param params: Execute parameters for placeholders.
    :returns: Scalar value or None when no row is returned.
    """

    stmt = db_builders.applicants_sql(query_template)
    cur.execute(stmt, params)
    row = cur.fetchone()
    if row is None:
        return None
    return row[0]


def fetch_single_row(cur, query_template: str, params: tuple):
    """
    Execute a query and return exactly one row (or None).

    :param cur: Database cursor.
    :param query_template: SQL template containing ``{table}``.
    :param params: Execute parameters for placeholders.
    :returns: First row returned by the query.
    """

    stmt = db_builders.applicants_sql(query_template)
    cur.execute(stmt, params)
    return cur.fetchone()


def fetch_all_rows(cur, query_template: str, params: tuple):
    """
    Execute a query and return all rows.

    :param cur: Database cursor.
    :param query_template: SQL template containing ``{table}``.
    :param params: Execute parameters for placeholders.
    :returns: List of result rows.
    """

    stmt = db_builders.applicants_sql(query_template)
    cur.execute(stmt, params)
    return cur.fetchall()


def fetch_metrics(query_limit=None, connect_fn=None) -> dict:
    """
    Query the database and return metrics used by analysis views.

    :param query_limit: Optional per-query LIMIT value.
    :param connect_fn: Optional DB connector for dependency injection.
    :returns: Dict of computed metrics.
    """

    if query_limit is None:
        query_limit = QUERY_LIMIT
    query_limit = db_builders.clamp_limit(query_limit)
    if connect_fn is None:
        connect_fn = psycopg.connect

    with connect_fn(DSN) as conn:
        with conn.cursor() as cur:
            metrics = {}

            metrics["fall_2026_count"] = fetch_scalar_value(
                cur,
                """
                SELECT COUNT(*)
                FROM {table}
                WHERE term ILIKE %s
                LIMIT %s;
                """,
                ("Fall 2026", query_limit),
            )

            metrics["intl_pct"] = fetch_scalar_value(
                cur,
                """
                SELECT
                    ROUND(
                        100.0 * SUM(
                            CASE
                                WHEN us_or_international = %s THEN 1
                                ELSE 0
                            END
                        )
                        / NULLIF(COUNT(*), 0),
                        2
                    )
                FROM {table}
                LIMIT %s;
                """,
                ("International", query_limit),
            )

            avg_row = fetch_single_row(
                cur,
                """
                SELECT
                    ROUND(
                        (AVG(gpa) FILTER (
                            WHERE gpa IS NOT NULL
                              AND gpa BETWEEN %s AND %s
                        ))::numeric,
                        2
                    ) AS avg_gpa,
                    ROUND(
                        (AVG(gre) FILTER (
                            WHERE gre IS NOT NULL
                              AND gre BETWEEN %s AND %s
                        ))::numeric,
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
                """,
                (0, 4.33, 0, 340, 0, 170, 0, 6.0, query_limit),
            )
            metrics["avg_gpa"] = avg_row[0]
            metrics["avg_gre"] = avg_row[1]
            metrics["avg_gre_v"] = avg_row[2]
            metrics["avg_gre_aw"] = avg_row[3]

            metrics["avg_gpa_american_fall_2026"] = fetch_scalar_value(
                cur,
                """
                SELECT
                    ROUND((AVG(gpa) FILTER (WHERE gpa IS NOT NULL))::numeric, 2) AS avg_gpa
                FROM {table}
                WHERE us_or_international = %s
                  AND term ILIKE %s
                  AND gpa IS NOT NULL
                  AND gpa <= %s
                LIMIT %s;
                """,
                ("American", "%Fall 2026%", 4.33, query_limit),
            )

            metrics["acceptance_pct_fall_2026"] = fetch_scalar_value(
                cur,
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
                """,
                ("Accepted", "Fall 2026", query_limit),
            )

            metrics["avg_gpa_accepted_fall_2026"] = fetch_scalar_value(
                cur,
                """
                SELECT
                    ROUND(AVG(gpa)::numeric, 2) AS avg_gpa
                FROM {table}
                WHERE status = %s
                  AND term ILIKE %s
                  AND gpa IS NOT NULL
                  AND gpa <= %s
                LIMIT %s;
                """,
                ("Accepted", "Fall 2026", 4.33, query_limit),
            )

            cs_patterns = ["%Computer Science%"]
            jhu_patterns = ["%Johns Hopkins%", "%John Hopkins%", "%JHU%"]
            metrics["jhu_ms_cs_count"] = fetch_scalar_value(
                cur,
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
                """,
                (1.0, cs_patterns, cs_patterns, jhu_patterns, jhu_patterns, query_limit),
            )

            uni_patterns = [
                "%Georgetown%",
                "%Massachusetts Institute of Technology%",
                "%MIT%",
                "%Stanford%",
                "%Carnegie Mellon%",
                "%CMU%",
            ]
            phd_2026_params = (
                "Accepted",
                2.0,
                "Fall 2026",
                "Spring 2026",
                cs_patterns,
                uni_patterns,
                query_limit,
            )
            metrics["cs_phd_accept_2026"] = fetch_scalar_value(
                cur,
                """
                SELECT COUNT(*)
                FROM {table}
                WHERE status = %s
                  AND degree = %s
                  AND term in (%s, %s)
                  AND program ILIKE ANY (%s)
                  AND program ILIKE ANY (%s)
                LIMIT %s;
                """,
                phd_2026_params,
            )
            metrics["cs_phd_accept_2026_llm"] = fetch_scalar_value(
                cur,
                """
                SELECT COUNT(*)
                FROM {table}
                WHERE status = %s
                  AND degree = %s
                  AND term in (%s, %s)
                  AND llm_generated_program ILIKE ANY (%s)
                  AND llm_generated_university ILIKE ANY (%s)
                LIMIT %s;
                """,
                phd_2026_params,
            )

            unc_masters_patterns = [
                "%UNC%",
                "%UNC-CH%",
                "%UNC CH%",
                "%University of North Carolina at Chapel Hill%",
                "%Chapel Hill%",
            ]
            metrics["unc_masters_program_rows"] = fetch_all_rows(
                cur,
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
                """,
                (
                    1.0,
                    "Accepted",
                    "Fall 2026",
                    unc_masters_patterns,
                    unc_masters_patterns,
                    query_limit,
                ),
            )

            unc_phd_patterns = [
                "%UNC%",
                "%UNC-CH%",
                "%UNC CH%",
                "%University of North Carolina%",
                "%Chapel Hill%",
            ]
            program_patterns = ["%Biostat%", "%Epidemiolog%"]
            metrics["unc_phd_program_rows"] = fetch_all_rows(
                cur,
                """
                SELECT
                    llm_generated_program,
                    COUNT(*) AS n
                FROM {table}
                WHERE degree = %s
                  AND term = %s
                  AND llm_generated_program ILIKE ANY (%s)
                  AND llm_generated_university ILIKE ANY (%s)
                GROUP BY llm_generated_program
                ORDER BY n DESC, llm_generated_program
                LIMIT %s;
                """,
                (2.0, "Fall 2026", program_patterns, unc_phd_patterns, query_limit),
            )

    return metrics


def print_metrics(metrics: dict):
    """
    Print formatted metric output for command-line execution.

    :param metrics: Metrics dictionary returned by ``fetch_metrics``.
    """

    print(f"Fall 2026 applicants: {metrics['fall_2026_count']}\n")
    print(f"Percentage of International Entries: {metrics['intl_pct']}\n")
    print(
        "Averages Scores of Applicants:\n"
        f"GPA: {metrics['avg_gpa']}\n"
        f"GRE: {metrics['avg_gre']}\n"
        f"GRE V: {metrics['avg_gre_v']}\n"
        f"GRE AW: {metrics['avg_gre_aw']}\n"
    )
    print(
        "Average GPA of Fall 2026 American Applicants: "
        f"{metrics['avg_gpa_american_fall_2026']}\n"
    )
    print(
        "Percentage of Fall 2026 Acceptances (to date): "
        f"{metrics['acceptance_pct_fall_2026']}\n"
    )
    print(
        "Average GPA of Fall 2026 Accepted Applicants: "
        f"{metrics['avg_gpa_accepted_fall_2026']}\n"
    )
    print(f"JHU Masters of Computer Science Applicants: {metrics['jhu_ms_cs_count']}\n")
    print(
        "Accepted 2026 PhD CS applicants at Georgetown/MIT/Stanford/CMU: "
        f"{metrics['cs_phd_accept_2026']}\n"
    )
    print(
        "Accepted 2026 PhD CS at Georgetown/MIT/Stanford/CMU (LLM Version): "
        f"{metrics['cs_phd_accept_2026_llm']}\n"
    )
    print("UNC-CH Masters programs (Fall 2026):")
    for program_name, count in metrics["unc_masters_program_rows"]:
        print(program_name, count)
    print("\n")
    print("UNC-CH PhD Biostat/Epidemiology (Fall 2026) by program:")
    for program_name, count in metrics["unc_phd_program_rows"]:
        print(program_name, count)


def main():
    """Run query metrics and print formatted answers."""

    metrics = fetch_metrics(query_limit=QUERY_LIMIT)
    print_metrics(metrics)


if __name__ == "__main__":
    main()
