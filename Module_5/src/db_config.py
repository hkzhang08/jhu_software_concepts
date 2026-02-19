"""
Database connection configuration helpers.

This module centralizes database connection loading from environment
variables and avoids hard-coded credentials in application code.
"""

import os


DB_ENV_KEYS = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")


def _quote_conninfo_value(value: str) -> str:
    """
    Quote and escape a libpq conninfo value.

    :param value: Raw connection value.
    :returns: Safely quoted value for a conninfo string.
    """

    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def get_db_dsn() -> str:
    """
    Build a database DSN from environment variables.

    Resolution order:
    1) ``DATABASE_URL`` if provided.
    2) Compose from ``DB_HOST``, ``DB_PORT``, ``DB_NAME``, ``DB_USER``,
       ``DB_PASSWORD``.

    :raises RuntimeError: If required DB_* variables are missing.
    :returns: Connection string compatible with ``psycopg.connect``.
    """

    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        return database_url

    missing = [key for key in DB_ENV_KEYS if not os.environ.get(key)]
    if missing:
        missing_vars = ", ".join(missing)
        raise RuntimeError(
            "Database configuration missing. Set DATABASE_URL or all of "
            "DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD "
            f"(missing: {missing_vars})."
        )

    conn_values = {
        "host": os.environ["DB_HOST"],
        "port": os.environ["DB_PORT"],
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
    }
    return " ".join(
        f"{key}={_quote_conninfo_value(value)}"
        for key, value in conn_values.items()
    )
