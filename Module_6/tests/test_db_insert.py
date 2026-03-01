"""Tests for database insert logic and metric queries in website.py."""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://localhost/grad_cafe")

import json

import pytest
import psycopg
from src import website as website

pytestmark = pytest.mark.db


class FakeCursor:
    """Minimal cursor stub for insert/dedup tests."""
    def __init__(self, table):
        self.table = table
        self._result = []

    def execute(self, query, params=None):
        # ensure_table_exists(...): params = (regclass_name, 1)
        if params and len(params) == 2 and isinstance(params[0], str):
            self._result = [(params[0],)]
            return

        # fetch_existing_urls(...): params = (limit, offset)
        if params and len(params) == 2 and all(isinstance(v, int) for v in params):
            limit, offset = params
            urls = [(row[3],) for row in self.table if row[3] is not None]
            self._result = urls[offset: offset + limit]
            return

        self._result = []

    def executemany(self, query, rows):
        self.table.extend(rows)

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
    """Connection stub that returns the fake cursor."""
    def __init__(self, table):
        self.table = table

    def cursor(self):
        return FakeCursor(self.table)

    def commit(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_pull_inserts_rows_into_empty_table(monkeypatch, tmp_path):
    """POST /pull-data inserts rows and required fields are non-null."""
    table = []

    def fake_connect(_dsn):
        return FakeConnection(table)

    monkeypatch.setattr(website.psycopg, "connect", fake_connect)

    sample_rows = [
        {
            "program": "Test Program",
            "comments": "Test comment",
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
    ]
    json_path = tmp_path / "applicant_data.json"
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(sample_rows, handle)

    def fake_run_pull_pipeline():
        website.load_cleaned_data_to_db(str(json_path))

    monkeypatch.setattr(website, "run_pull_pipeline", fake_run_pull_pipeline)
    website.PULL_STATE["status"] = "idle"

    assert len(table) == 0

    app = website.create_app(run_pull_pipeline_fn=website.run_pull_pipeline)
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.post("/pull-data")
    assert resp.status_code == 202
    assert resp.get_json() == {"ok": True}

    assert len(table) == 1
    inserted = table[0]
    assert inserted[0] is not None  # program
    assert inserted[3] is not None  # url
    assert inserted[4] is not None  # status
    assert inserted[5] is not None  # term


def test_load_cleaned_data_missing_file_returns_zero(tmp_path):
    """Missing input file yields zero inserts."""
    missing_path = tmp_path / "missing.json"
    assert website.load_cleaned_data_to_db(str(missing_path)) == 0


def test_duplicate_rows_do_not_insert_twice(monkeypatch, tmp_path):
    """Duplicate pulls do not insert duplicates by URL."""
    table = []

    def fake_connect(_dsn):
        return FakeConnection(table)

    monkeypatch.setattr(website.psycopg, "connect", fake_connect)

    sample_rows = [
        {
            "program": "Test Program",
            "comments": "Test comment",
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
    ]
    json_path = tmp_path / "applicant_data.json"
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(sample_rows, handle)

    first_inserted = website.load_cleaned_data_to_db(str(json_path))
    second_inserted = website.load_cleaned_data_to_db(str(json_path))

    assert first_inserted == 1
    assert second_inserted == 0
    assert len(table) == 1


def test_duplicate_rows_within_single_file(monkeypatch, tmp_path):
    """Duplicate URLs within a single file insert only once."""
    table = []

    def fake_connect(_dsn):
        return FakeConnection(table)

    monkeypatch.setattr(website.psycopg, "connect", fake_connect)

    sample_rows = [
        {
            "program": "Test Program",
            "comments": "Test comment",
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
        },
        {
            "program": "Test Program",
            "comments": "Test comment duplicate",
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
        },
    ]
    json_path = tmp_path / "applicant_data.json"
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(sample_rows, handle)

    inserted = website.load_cleaned_data_to_db(str(json_path))

    assert inserted == 1
    assert len(table) == 1


def test_fetch_metrics_returns_expected_keys(monkeypatch):
    """fetch_metrics() returns the expected key set for the template."""
    class MetricsCursor:
        def __init__(self):
            self.results = [
                ("one", (10,)),  # fall_2026_count
                ("one", (12.34,)),  # intl_pct
                ("one", (3.5, 300.0, 150.0, 4.0)),  # avg_gpa, avg_gre, avg_gre_v, avg_gre_aw
                ("one", (3.7,)),  # avg_gpa_american_fall_2026
                ("one", (45.67,)),  # acceptance_pct_fall_2026
                ("one", (3.9,)),  # avg_gpa_accepted_fall_2026
                ("one", (5,)),  # jhu_ms_cs_count
                ("one", (2,)),  # cs_phd_accept_2026
                ("one", (1,)),  # cs_phd_accept_2026_llm
                ("all", [("Program A", 1)]),  # unc_masters_program_rows
                ("all", [("Program B", 2)]),  # unc_phd_program_rows
            ]

        def execute(self, _query, _params=None):
            return None

        def fetchone(self):
            kind, value = self.results.pop(0)
            assert kind == "one"
            return value

        def fetchall(self):
            kind, value = self.results.pop(0)
            assert kind == "all"
            return value

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class MetricsConnection:
        def cursor(self):
            return MetricsCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(website.psycopg, "connect", lambda _dsn: MetricsConnection())
    metrics = website.fetch_metrics()

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

    assert set(metrics.keys()) == expected_keys


def test_fetch_metrics_falls_back_to_simplified_schema(monkeypatch):
    """fetch_metrics() should derive compatibility values when legacy columns are absent."""

    class FallbackCursor:
        def __init__(self):
            self.results = [
                (3,),      # total applicants
                (0.0,),    # acceptance percentage
                (1,),      # JHU MS CS style count from simplified rows
            ]

        def execute(self, _query, _params=None):
            return None

        def fetchone(self):
            if not self.results:
                return None
            return self.results.pop(0)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FallbackConnection:
        def cursor(self):
            return FallbackCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def boom(*_args, **_kwargs):
        raise psycopg.errors.UndefinedColumn("missing legacy analytics columns")

    monkeypatch.setattr(website, "query_table_fetch_metrics", boom)
    monkeypatch.setattr(website.psycopg, "connect", lambda _dsn: FallbackConnection())

    metrics = website.fetch_metrics()

    assert metrics["fall_2026_count"] == 3
    assert metrics["acceptance_pct_fall_2026"] == 0.0
    assert metrics["jhu_ms_cs_count"] == 1
    assert metrics["unc_masters_program_rows"] == []
    assert metrics["avg_gpa"] is None
