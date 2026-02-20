"""
Shared DB/query/value helper functions for Module 5.
"""

import re
from datetime import datetime

from psycopg import sql

try:
    from .db_config import get_db_dsn
except ImportError:  # pragma: no cover - script execution path
    from db_config import get_db_dsn


APPLICANTS_TABLE = sql.Identifier("applicants")
APPLICANTS_REGCLASS = "public.applicants"
URL_COLUMN = sql.Identifier("url")
P_ID_COLUMN = sql.Identifier("p_id")
MIN_QUERY_LIMIT = 1
MAX_QUERY_LIMIT = 100
DEFAULT_QUERY_LIMIT = MAX_QUERY_LIMIT


def fnum(value):
    """
    Convert numeric-like values to float.

    :param value: Any numeric-like value (str/int/float).
    :returns: Float if parsable, otherwise None.
    """

    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"[-+]?\d*\.?\d+", str(value))
    return float(match.group(0)) if match else None


def fdate(value):
    """
    Convert date strings to ``datetime.date``.

    :param value: Date string.
    :returns: ``datetime.date`` or None if parsing fails.
    """

    if not value:
        return None
    cleaned = str(value).strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def fdegree(value):
    """
    Normalize degree labels to numeric categories.

    :param value: Degree label (e.g., "PhD", "Masters").
    :returns: 2.0 for PhD/doctorate, 1.0 for masters, else None.
    """

    if not value:
        return None
    normalized = str(value).strip().lower()
    if "phd" in normalized or "doctor" in normalized:
        return 2.0
    if "master" in normalized or normalized in {"ms", "ma", "msc", "mba"}:
        return 1.0
    return None


def ftext(value):
    """
    Normalize text values to strings without NULL bytes.

    :param value: Input value.
    :returns: Cleaned string or None.
    """

    if value is None:
        return None
    return str(value).replace("\x00", "")


def clamp_limit(raw_limit):
    """
    Clamp any requested limit to the safe query window (1..100).

    :param raw_limit: Raw value from configuration/input.
    :returns: Integer limit constrained to [MIN_QUERY_LIMIT, MAX_QUERY_LIMIT].
    """

    try:
        parsed_limit = int(raw_limit)
    except (TypeError, ValueError):
        return DEFAULT_QUERY_LIMIT
    return max(MIN_QUERY_LIMIT, min(parsed_limit, MAX_QUERY_LIMIT))


def applicants_sql(query_template: str, *, table_identifier=APPLICANTS_TABLE, **identifiers):
    """
    Compose SQL with safely quoted identifier placeholders.

    :param query_template: SQL template containing ``{table}``.
    :param table_identifier: Identifier for the target table.
    :param identifiers: Extra SQL identifiers used in ``format``.
    :returns: Composed SQL object.
    """

    format_args = {"table": table_identifier}
    format_args.update(identifiers)
    return sql.SQL(query_template).format(**format_args)


def ensure_table_exists(db_cursor, regclass_name: str, missing_message: str):
    """
    Verify that the required table exists without requiring DDL privileges.

    :param db_cursor: Database cursor.
    :param regclass_name: ``schema.table`` name for ``to_regclass`` lookup.
    :param missing_message: RuntimeError message when the table is missing.
    :raises RuntimeError: If the table does not exist.
    """

    stmt = sql.SQL(
        """
        SELECT to_regclass(%s)
        LIMIT %s;
        """
    )
    params = (regclass_name, 1)
    db_cursor.execute(stmt, params)
    row = db_cursor.fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(missing_message)


def fetch_existing_urls(
    db_cursor,
    batch_limit: int,
    *,
    table_identifier=APPLICANTS_TABLE,
    url_identifier=URL_COLUMN,
    order_identifier=P_ID_COLUMN,
):
    """
    Read existing URLs in bounded pages so each query has an inherent LIMIT.

    :param db_cursor: Database cursor.
    :param batch_limit: Page size for URL fetches.
    :param table_identifier: Target table identifier.
    :param url_identifier: URL column identifier.
    :param order_identifier: Column used for stable paging.
    :returns: Set of non-null existing URL strings.
    """

    stmt = applicants_sql(
        """
        SELECT {url_col}
        FROM {table}
        WHERE {url_col} IS NOT NULL
        ORDER BY {order_col}
        LIMIT %s OFFSET %s;
        """,
        table_identifier=table_identifier,
        url_col=url_identifier,
        order_col=order_identifier,
    )
    existing_urls = set()
    offset = 0

    while True:
        params = (batch_limit, offset)
        db_cursor.execute(stmt, params)
        batch = db_cursor.fetchall()
        if not batch:
            break
        existing_urls.update(url for (url,) in batch if url is not None)
        if len(batch) < batch_limit:
            break
        offset += batch_limit

    return existing_urls


def build_applicant_insert_row(row: dict, *, url=None, include_llm=True):
    """
    Build one applicants INSERT tuple from a raw row.

    :param row: Raw applicant row dictionary.
    :param url: Optional pre-normalized URL. Defaults to cleaned row URL.
    :param include_llm: Whether to load LLM program/university fields from row.
    :returns: Tuple matching applicants INSERT column order.
    """

    normalized_url = ftext(row.get("url")) if url is None else url
    llm_program = None
    llm_university = None
    if include_llm:
        llm_program = ftext(
            row.get("llm-generated-program") or row.get("llm_generated_program")
        )
        llm_university = ftext(
            row.get("llm-generated-university") or row.get("llm_generated_university")
        )

    return (
        ftext(row.get("program")),
        ftext(row.get("comments")),
        fdate(row.get("date_added")),
        normalized_url,
        ftext(row.get("applicant_status")),
        ftext(row.get("semester_year_start")),
        ftext(row.get("citizenship")),
        fnum(row.get("gpa")),
        fnum(row.get("gre")),
        fnum(row.get("gre_v")),
        fnum(row.get("gre_aw")),
        fdegree(row.get("masters_or_phd")),
        llm_program,
        llm_university,
    )


def register_unique_url(row: dict, seen_urls: set):
    """
    Normalize a row URL and update a seen set for deduplication.

    :param row: Raw applicant row dictionary.
    :param seen_urls: Set of URL strings already observed.
    :returns: URL string when unique/empty, otherwise None if duplicate.
    """

    url = ftext(row.get("url"))
    if url and url in seen_urls:
        return None
    if url:
        seen_urls.add(url)
    return url


__all__ = [
    "get_db_dsn",
    "APPLICANTS_TABLE",
    "APPLICANTS_REGCLASS",
    "MIN_QUERY_LIMIT",
    "MAX_QUERY_LIMIT",
    "DEFAULT_QUERY_LIMIT",
    "fnum",
    "fdate",
    "fdegree",
    "ftext",
    "clamp_limit",
    "applicants_sql",
    "ensure_table_exists",
    "fetch_existing_urls",
    "build_applicant_insert_row",
    "register_unique_url",
]
