"""Tests for the query_table module metric queries."""

import importlib
import runpy
import sys
from pathlib import Path

import pytest
import psycopg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytestmark = pytest.mark.db


def test_query_table_fetch_metrics_with_fake_db(monkeypatch):
    """fetch_metrics executes all expected queries against a fake DB."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/grad_cafe")
    class State:
        def __init__(self):
            self.execute_count = 0
            self.fetchone_calls = 0
            self.fetchall_calls = 0
            self.fetchone_values = [
                (10,),
                (12.34,),
                (3.5, 300.0, 150.0, 4.0),
                (3.7,),
                (45.67,),
                (3.9,),
                (5,),
                (2,),
                (1,),
            ]
            self.fetchall_values = [
                [("Program A", 1)],
                [("Program B", 2)],
            ]

    class FakeCursor:
        def __init__(self, state):
            self.state = state

        def execute(self, _query, _params=None):
            self.state.execute_count += 1

        def fetchone(self):
            self.state.fetchone_calls += 1
            return self.state.fetchone_values.pop(0)

        def fetchall(self):
            self.state.fetchall_calls += 1
            return self.state.fetchall_values.pop(0)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def __init__(self, state):
            self.state = state

        def cursor(self):
            return FakeCursor(self.state)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    state = State()
    monkeypatch.setattr(psycopg, "connect", lambda _dsn: FakeConnection(state))

    if "src.query_table" in sys.modules:
        del sys.modules["src.query_table"]
    query_table = importlib.import_module("src.query_table")
    metrics = query_table.fetch_metrics()

    expected_keys = {
        "fall_2026_count",
        "intl_pct",
        "avg_gpa",
        "avg_gre",
        "avg_gre_v",
        "avg_gre_aw",
        "avg_gpa_american_fall_2026",
        "acceptance_pct_fall_2026",
        "avg_gpa_accepted_fall_2026",
        "jhu_ms_cs_count",
        "cs_phd_accept_2026",
        "cs_phd_accept_2026_llm",
        "unc_masters_program_rows",
        "unc_phd_program_rows",
    }

    assert state.execute_count == 11
    assert state.fetchone_calls == 9
    assert state.fetchall_calls == 2
    assert state.fetchone_values == []
    assert state.fetchall_values == []
    assert set(metrics.keys()) == expected_keys


def test_fetch_scalar_value_returns_none_when_no_row(monkeypatch):
    """fetch_scalar_value returns None when the cursor has no row."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/grad_cafe")
    if "src.query_table" in sys.modules:
        del sys.modules["src.query_table"]
    query_table = importlib.import_module("src.query_table")

    class EmptyCursor:
        def execute(self, _query, _params=None):
            return None

        def fetchone(self):
            return None

    result = query_table.fetch_scalar_value(
        EmptyCursor(),
        "SELECT 1 FROM {table} LIMIT %s;",
        (1,),
    )
    assert result is None


def test_print_metrics_formats_all_sections(capsys, monkeypatch):
    """print_metrics prints every section including grouped rows."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/grad_cafe")
    if "src.query_table" in sys.modules:
        del sys.modules["src.query_table"]
    query_table = importlib.import_module("src.query_table")

    metrics = {
        "fall_2026_count": 10,
        "intl_pct": 12.34,
        "avg_gpa": 3.5,
        "avg_gre": 300.0,
        "avg_gre_v": 150.0,
        "avg_gre_aw": 4.0,
        "avg_gpa_american_fall_2026": 3.7,
        "acceptance_pct_fall_2026": 45.67,
        "avg_gpa_accepted_fall_2026": 3.9,
        "jhu_ms_cs_count": 5,
        "cs_phd_accept_2026": 2,
        "cs_phd_accept_2026_llm": 1,
        "unc_masters_program_rows": [("Program A", 1)],
        "unc_phd_program_rows": [("Program B", 2)],
    }

    query_table.print_metrics(metrics)
    out = capsys.readouterr().out
    assert "Fall 2026 applicants: 10" in out
    assert "Averages Scores of Applicants:" in out
    assert "GRE AW: 4.0" in out
    assert "JHU Masters of Computer Science Applicants: 5" in out
    assert "UNC-CH Masters programs (Fall 2026):" in out
    assert "Program A 1" in out
    assert "UNC-CH PhD Biostat/Epidemiology (Fall 2026) by program:" in out
    assert "Program B 2" in out


def test_main_guard_runs_with_fake_db(monkeypatch, capsys):
    """Running query_table as __main__ executes main() and prints output."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/grad_cafe")

    class State:
        def __init__(self):
            self.fetchone_values = [
                (10,),
                (12.34,),
                (3.5, 300.0, 150.0, 4.0),
                (3.7,),
                (45.67,),
                (3.9,),
                (5,),
                (2,),
                (1,),
            ]
            self.fetchall_values = [
                [("Program A", 1)],
                [("Program B", 2)],
            ]

    class FakeCursor:
        def __init__(self, state):
            self.state = state

        def execute(self, _query, _params=None):
            return None

        def fetchone(self):
            return self.state.fetchone_values.pop(0)

        def fetchall(self):
            return self.state.fetchall_values.pop(0)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def __init__(self, state):
            self.state = state

        def cursor(self):
            return FakeCursor(self.state)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    state = State()
    monkeypatch.setattr(psycopg, "connect", lambda _dsn: FakeConnection(state))

    if "src.query_table" in sys.modules:
        del sys.modules["src.query_table"]
    runpy.run_module("src.query_table", run_name="__main__")
    out = capsys.readouterr().out
    assert "Fall 2026 applicants: 10" in out
