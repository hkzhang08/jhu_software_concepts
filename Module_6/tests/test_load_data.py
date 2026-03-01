"""Tests for the load_data.py module and helpers."""

import importlib
import io
import json
import os
import sys
from pathlib import Path

import pytest
import psycopg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytestmark = pytest.mark.db


def _import_load_data(monkeypatch, fake_conn):
    """Import/reload load_data with a fake database connection."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/grad_cafe")
    monkeypatch.setenv(
        "LLM_NEW_APPLICANT_PATH",
        str(Path.cwd() / "llm_new_applicant.json"),
    )
    monkeypatch.setenv(
        "LLM_EXTEND_APPLICANT_PATH",
        str(Path.cwd() / "llm_extend_applicant_data.json"),
    )
    monkeypatch.setattr(psycopg, "connect", lambda _dsn: fake_conn)
    if "src.load_data" in sys.modules:
        del sys.modules["src.load_data"]
    import src.load_data as load_data
    return importlib.reload(load_data)


def test_load_data_top_level_no_files(monkeypatch, tmp_path):
    """Top-level load handles missing files and inserts zero rows."""
    class FakeCursor:
        def __init__(self):
            self.executemany_called = False
            self._result = []
            self._existing_urls = []

        def execute(self, _query, _params=None):
            if _params and len(_params) == 2 and isinstance(_params[0], str):
                self._result = [(_params[0],)]
                return
            if _params and len(_params) == 2 and all(isinstance(v, int) for v in _params):
                limit, offset = _params
                self._result = self._existing_urls[offset: offset + limit]
                return
            self._result = []

        def executemany(self, _query, _rows):
            self.executemany_called = True

        def fetchone(self):
            if not self._result:
                return None
            return self._result[0]

        def fetchall(self):
            return list(self._result)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def __init__(self):
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_conn = FakeConnection()
    monkeypatch.chdir(tmp_path)

    load_data = _import_load_data(monkeypatch, fake_conn)
    captured = io.StringIO()
    monkeypatch.setattr(sys, "stdout", captured)

    importlib.reload(load_data)

    assert fake_conn.cursor_obj.executemany_called is False
    assert "Inserted rows: 0" in captured.getvalue()


def test_load_data_happy_path_jsonl_inserts(monkeypatch, tmp_path):
    """Happy path loads JSONL from both files and inserts rows."""
    class FakeCursor:
        def __init__(self):
            self.executemany_called = False
            self.inserted_rows = []
            self._result = []
            self._existing_urls = []

        def execute(self, _query, _params=None):
            if _params and len(_params) == 2 and isinstance(_params[0], str):
                self._result = [(_params[0],)]
                return
            if _params and len(_params) == 2 and all(isinstance(v, int) for v in _params):
                limit, offset = _params
                self._result = self._existing_urls[offset: offset + limit]
                return
            self._result = []

        def executemany(self, _query, rows):
            self.executemany_called = True
            self.inserted_rows = list(rows)

        def fetchone(self):
            if not self._result:
                return None
            return self._result[0]

        def fetchall(self):
            return list(self._result)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def __init__(self):
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_conn = FakeConnection()
    monkeypatch.chdir(tmp_path)

    # Create JSONL files with one empty line
    original_path = tmp_path / "llm_extend_applicant_data.json"
    new_path = tmp_path / "llm_new_applicant.json"
    row1 = {
        "program": "Test Program",
        "comments": "Row 1",
        "date_added": "February 01, 2026",
        "url": "https://example.com/result/1",
        "applicant_status": "Accepted",
        "semester_year_start": "Fall 2026",
        "citizenship": "American",
        "gpa": "GPA 4.0",
        "gre": "GRE 320",
        "gre_v": "GRE V 160",
        "gre_aw": "GRE AW 4.0",
        "masters_or_phd": "Masters",
    }
    row2 = {
        "program": "Test Program 2",
        "comments": "Row 2",
        "date_added": "February 02, 2026",
        "url": "https://example.com/result/2",
        "applicant_status": "Rejected",
        "semester_year_start": "Fall 2026",
        "citizenship": "International",
        "gpa": "GPA 3.5",
        "gre": "GRE 310",
        "gre_v": "GRE V 155",
        "gre_aw": "GRE AW 3.5",
        "masters_or_phd": "Masters",
    }
    with open(original_path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(row1) + "\n\n")
    with open(new_path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(row2) + "\n")

    load_data = _import_load_data(monkeypatch, fake_conn)

    assert fake_conn.cursor_obj.executemany_called is True
    assert len(fake_conn.cursor_obj.inserted_rows) == 2


def test_load_data_skips_duplicate_urls(monkeypatch, tmp_path):
    """Duplicate URLs are skipped before insert."""
    class FakeCursor:
        def __init__(self):
            self.executemany_called = False
            self.inserted_rows = []
            self._result = []
            self._existing_urls = [("https://example.com/result/1",)]

        def execute(self, _query, _params=None):
            if _params and len(_params) == 2 and isinstance(_params[0], str):
                self._result = [(_params[0],)]
                return
            if _params and len(_params) == 2 and all(isinstance(v, int) for v in _params):
                limit, offset = _params
                self._result = self._existing_urls[offset: offset + limit]
                return
            self._result = []

        def executemany(self, _query, rows):
            self.executemany_called = True
            self.inserted_rows = list(rows)

        def fetchone(self):
            if not self._result:
                return None
            return self._result[0]

        def fetchall(self):
            return list(self._result)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def __init__(self):
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_conn = FakeConnection()
    monkeypatch.chdir(tmp_path)

    original_path = tmp_path / "llm_extend_applicant_data.json"
    new_path = tmp_path / "llm_new_applicant.json"
    row1 = {"url": "https://example.com/result/1", "program": "P1"}
    row2 = {"url": "https://example.com/result/1", "program": "P1-dup"}
    row3 = {"url": "https://example.com/result/2", "program": "P2"}
    with open(original_path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(row1) + "\n")
        handle.write(json.dumps(row2) + "\n")
    with open(new_path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(row3) + "\n")

    load_data = _import_load_data(monkeypatch, fake_conn)

    assert fake_conn.cursor_obj.executemany_called is True
    assert len(fake_conn.cursor_obj.inserted_rows) == 1


def test_load_data_llm_generated_fallback(monkeypatch, tmp_path):
    """LLM-generated fields are used when present in input."""
    class FakeCursor:
        def __init__(self):
            self.executemany_called = False
            self.inserted_rows = []
            self._result = []
            self._existing_urls = []

        def execute(self, _query, _params=None):
            if _params and len(_params) == 2 and isinstance(_params[0], str):
                self._result = [(_params[0],)]
                return
            if _params and len(_params) == 2 and all(isinstance(v, int) for v in _params):
                limit, offset = _params
                self._result = self._existing_urls[offset: offset + limit]
                return
            self._result = []

        def executemany(self, _query, rows):
            self.executemany_called = True
            self.inserted_rows = list(rows)

        def fetchone(self):
            if not self._result:
                return None
            return self._result[0]

        def fetchall(self):
            return list(self._result)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def __init__(self):
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_conn = FakeConnection()
    monkeypatch.chdir(tmp_path)

    original_path = tmp_path / "llm_extend_applicant_data.json"
    new_path = tmp_path / "llm_new_applicant.json"
    row = {
        "program": "Test Program",
        "comments": "Row 1",
        "date_added": "February 01, 2026",
        "url": "https://example.com/result/1",
        "applicant_status": "Accepted",
        "semester_year_start": "Fall 2026",
        "citizenship": "American",
        "gpa": "GPA 4.0",
        "gre": "GRE 320",
        "gre_v": "GRE V 160",
        "gre_aw": "GRE AW 4.0",
        "masters_or_phd": "Masters",
        "llm-generated-program": "LLM Program",
        "llm-generated-university": "LLM University",
    }
    with open(original_path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")
    with open(new_path, "w", encoding="utf-8") as handle:
        handle.write("")

    load_data = _import_load_data(monkeypatch, fake_conn)

    assert fake_conn.cursor_obj.executemany_called is True
    inserted = fake_conn.cursor_obj.inserted_rows[0]
    assert inserted[-2] == "LLM Program"
    assert inserted[-1] == "LLM University"


def test_load_data_helper_functions():
    """Helper functions handle edge cases for numbers, dates, degrees, and text."""
    class FakeCursor:
        def execute(self, _query, _params=None):
            self._result = [(_params[0],)] if _params and isinstance(_params[0], str) else []
            return None

        def executemany(self, _query, _rows):
            return None

        def fetchone(self):
            if not getattr(self, "_result", []):
                return None
            return self._result[0]

        def fetchall(self):
            return list(getattr(self, "_result", []))

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    mp = pytest.MonkeyPatch()
    load_data = _import_load_data(mp, FakeConnection())

    assert load_data.fnum(None) is None
    assert load_data.fnum(3) == 3.0
    assert load_data.fnum("GPA 4.5") == 4.5
    assert load_data.fnum("no number") is None

    assert str(load_data.fdate("February 01, 2026")) == "2026-02-01"
    assert str(load_data.fdate("2026-02-01")) == "2026-02-01"
    assert load_data.fdate("bad") is None

    assert load_data.fdegree("PhD") == 2.0
    assert load_data.fdegree("Masters") == 1.0
    assert load_data.fdegree("Other") is None

    assert load_data.ftext(None) is None
    assert load_data.ftext("a\x00b") == "ab"
    mp.undo()
