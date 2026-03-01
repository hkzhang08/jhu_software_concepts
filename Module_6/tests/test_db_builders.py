"""Unit tests for db_builders branch coverage."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import db_builders

pytestmark = pytest.mark.db


def test_clamp_limit_invalid_values_use_default():
    """Non-integer limits fall back to the default query limit."""
    assert db_builders.clamp_limit("not-a-number") == db_builders.DEFAULT_QUERY_LIMIT
    assert db_builders.clamp_limit(None) == db_builders.DEFAULT_QUERY_LIMIT


def test_ensure_table_exists_uses_fetchall_when_no_fetchone():
    """Table check supports cursors that expose only fetchall()."""
    class CursorWithoutFetchone:
        def __init__(self):
            self.executed = []

        def execute(self, stmt, params):
            self.executed.append((stmt, params))

        def fetchall(self):
            return [("public.applicants",)]

    cursor = CursorWithoutFetchone()
    db_builders.ensure_table_exists(
        cursor,
        "public.applicants",
        "missing table",
    )
    assert cursor.executed


def test_ensure_table_exists_raises_when_table_missing():
    """Missing table check raises RuntimeError."""
    class CursorWithFetchone:
        def execute(self, _stmt, _params):
            return None

        def fetchone(self):
            return (None,)

    with pytest.raises(RuntimeError, match="missing table"):
        db_builders.ensure_table_exists(
            CursorWithFetchone(),
            "public.applicants",
            "missing table",
        )


def test_fetch_existing_urls_pages_until_empty_batch():
    """URL fetch paging advances offset for full-size batches."""
    class PagingCursor:
        def __init__(self):
            self.offset = 0
            self.calls = []

        def execute(self, _stmt, params):
            limit, offset = params
            self.calls.append((limit, offset))
            self.offset = offset

        def fetchall(self):
            if self.offset == 0:
                return [("url-1",), ("url-2",)]
            if self.offset == 2:
                return [("url-3",), (None,)]
            return []

    cursor = PagingCursor()
    urls = db_builders.fetch_existing_urls(cursor, batch_limit=2)
    assert urls == {"url-1", "url-2", "url-3"}
    assert cursor.calls == [(2, 0), (2, 2), (2, 4)]
