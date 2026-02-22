"""Integration tests for pull → update → render flows."""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://localhost/grad_cafe")

import pytest
from src import website as website

pytestmark = pytest.mark.integration


def test_end_to_end_pull_update_render_with_fake_scraper():
    """End-to-end flow with a fake scraper and in-memory state."""
    state = {"rows": [], "db_rows": []}

    def fake_scraper():
        return [
            {
                "program": "Test Program A",
                "comments": "Row A",
                "date_added": "February 01, 2026",
                "url": "https://example.com/result/1",
                "applicant_status": "Accepted",
                "semester_year_start": "Fall 2026",
                "citizenship": "International",
                "gpa": "GPA 4.0",
                "gre": "GRE 320",
                "gre_v": "GRE V 160",
                "gre_aw": "GRE AW 4.0",
                "masters_or_phd": "Masters",
            },
            {
                "program": "Test Program B",
                "comments": "Row B",
                "date_added": "February 01, 2026",
                "url": "https://example.com/result/2",
                "applicant_status": "Rejected",
                "semester_year_start": "Fall 2026",
                "citizenship": "American",
                "gpa": "GPA 3.5",
                "gre": "GRE 310",
                "gre_v": "GRE V 155",
                "gre_aw": "GRE AW 3.5",
                "masters_or_phd": "Masters",
            },
        ]

    def fake_insert_rows(rows):
        existing_urls = {row.get("url") for row in state["db_rows"]}
        inserted = 0
        for row in rows:
            url = row.get("url")
            if url and url in existing_urls:
                continue
            state["db_rows"].append(row)
            if url:
                existing_urls.add(url)
            inserted += 1
        return inserted

    def fake_run_pull_pipeline():
        website.PULL_STATE["status"] = "running"
        state["rows"] = fake_scraper()
        inserted = fake_insert_rows(state["rows"])
        website.PULL_STATE["status"] = "done"
        website.PULL_STATE["message"] = f"Pull complete. Inserted {inserted} new rows."

    def fake_fetch_metrics():
        rows = state["db_rows"]
        fall_2026_count = sum(1 for row in rows if row.get("semester_year_start") == "Fall 2026")
        intl_count = sum(1 for row in rows if row.get("citizenship") == "International")
        accepted_count = sum(1 for row in rows if row.get("applicant_status") == "Accepted")
        intl_pct = (100.0 * intl_count / fall_2026_count) if fall_2026_count else 0.0
        acceptance_pct = (100.0 * accepted_count / fall_2026_count) if fall_2026_count else 0.0
        return {
            "fall_2026_count": fall_2026_count,
            "intl_pct": intl_pct,
            "avg_gpa": 0.0,
            "avg_gre": 0.0,
            "avg_gre_v": 0.0,
            "avg_gre_aw": 0.0,
            "avg_gpa_american_fall_2026": 0.0,
            "acceptance_pct_fall_2026": acceptance_pct,
            "avg_gpa_accepted_fall_2026": 0.0,
            "jhu_ms_cs_count": 0,
            "cs_phd_accept_2026": 0,
            "cs_phd_accept_2026_llm": 0,
            "unc_masters_program_rows": [],
            "unc_phd_program_rows": [],
        }

    website.PULL_STATE["status"] = "idle"

    app = website.create_app(
        run_pull_pipeline_fn=fake_run_pull_pipeline,
        fetch_metrics_fn=fake_fetch_metrics,
    )
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data")
    assert resp.status_code == 202
    assert resp.get_json() == {"ok": True}
    assert len(state["db_rows"]) == 2

    resp = client.post("/update-analysis")
    assert resp.status_code == 303
    assert resp.headers["Location"].endswith("/")

    resp = client.get("/analysis")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Fall 2026 Applicants: 2" in body
    assert "Percentage International: 50.00" in body
    assert "Fall 2026 Acceptance percent: 50.00" in body


def test_update_analysis_redirects_when_not_busy():
    """Update-analysis redirects to / when the system is not busy."""
    def fake_metrics():
        return {
            "fall_2026_count": 0,
            "intl_pct": 0.0,
            "avg_gpa": 0.0,
            "avg_gre": 0.0,
            "avg_gre_v": 0.0,
            "avg_gre_aw": 0.0,
            "avg_gpa_american_fall_2026": 0.0,
            "acceptance_pct_fall_2026": 0.0,
            "avg_gpa_accepted_fall_2026": 0.0,
            "jhu_ms_cs_count": 0,
            "cs_phd_accept_2026": 0,
            "cs_phd_accept_2026_llm": 0,
            "unc_masters_program_rows": [],
            "unc_phd_program_rows": [],
        }

    website.PULL_STATE["status"] = "idle"
    app = website.create_app(fetch_metrics_fn=fake_metrics)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/update-analysis")
    assert resp.status_code == 303
    assert resp.headers["Location"].endswith("/")


def test_analysis_shows_updated_metrics_with_formatted_values():
    """Rendered analysis includes formatted metrics."""
    state = {
        "db_rows": [
            {
                "program": "Test Program A",
                "comments": "Row A",
                "date_added": "February 01, 2026",
                "url": "https://example.com/result/1",
                "applicant_status": "Accepted",
                "semester_year_start": "Fall 2026",
                "citizenship": "International",
                "gpa": "GPA 4.0",
                "gre": "GRE 320",
                "gre_v": "GRE V 160",
                "gre_aw": "GRE AW 4.0",
                "masters_or_phd": "Masters",
            },
            {
                "program": "Test Program B",
                "comments": "Row B",
                "date_added": "February 01, 2026",
                "url": "https://example.com/result/2",
                "applicant_status": "Rejected",
                "semester_year_start": "Fall 2026",
                "citizenship": "American",
                "gpa": "GPA 3.5",
                "gre": "GRE 310",
                "gre_v": "GRE V 155",
                "gre_aw": "GRE AW 3.5",
                "masters_or_phd": "Masters",
            },
        ]
    }

    def fake_fetch_metrics():
        rows = state["db_rows"]
        fall_2026_count = sum(1 for row in rows if row.get("semester_year_start") == "Fall 2026")
        intl_count = sum(1 for row in rows if row.get("citizenship") == "International")
        accepted_count = sum(1 for row in rows if row.get("applicant_status") == "Accepted")
        intl_pct = (100.0 * intl_count / fall_2026_count) if fall_2026_count else 0.0
        acceptance_pct = (100.0 * accepted_count / fall_2026_count) if fall_2026_count else 0.0
        return {
            "fall_2026_count": fall_2026_count,
            "intl_pct": intl_pct,
            "avg_gpa": 3.75,
            "avg_gre": 315.0,
            "avg_gre_v": 157.5,
            "avg_gre_aw": 3.75,
            "avg_gpa_american_fall_2026": 3.5,
            "acceptance_pct_fall_2026": acceptance_pct,
            "avg_gpa_accepted_fall_2026": 4.0,
            "jhu_ms_cs_count": 0,
            "cs_phd_accept_2026": 0,
            "cs_phd_accept_2026_llm": 0,
            "unc_masters_program_rows": [],
            "unc_phd_program_rows": [],
        }

    website.PULL_STATE["status"] = "idle"

    app = website.create_app(fetch_metrics_fn=fake_fetch_metrics)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.get("/analysis")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Fall 2026 Applicants: 2" in body
    assert "Percentage International: 50.00" in body
    assert "Fall 2026 Acceptance percent: 50.00" in body
    assert "Average GPA: 3.75" in body


def test_multiple_pulls_with_overlap_respect_uniqueness():
    """Multiple pulls with overlapping URLs dedupe correctly."""
    state = {"db_rows": []}

    def fake_insert_rows(rows):
        existing_urls = {row.get("url") for row in state["db_rows"]}
        for row in rows:
            url = row.get("url")
            if url and url in existing_urls:
                continue
            state["db_rows"].append(row)
            if url:
                existing_urls.add(url)

    pulls = [
        [
            {"program": "P1", "url": "https://example.com/result/1"},
            {"program": "P2", "url": "https://example.com/result/2"},
        ],
        [
            {"program": "P1-dup", "url": "https://example.com/result/1"},
            {"program": "P3", "url": "https://example.com/result/3"},
        ],
    ]
    pull_idx = {"value": 0}

    def fake_run_pull_pipeline():
        rows = pulls[pull_idx["value"]]
        pull_idx["value"] += 1
        fake_insert_rows(rows)

    website.PULL_STATE["status"] = "idle"

    app = website.create_app(
        run_pull_pipeline_fn=fake_run_pull_pipeline,
        fetch_metrics_fn=lambda: {
            "fall_2026_count": 0,
            "intl_pct": 0.0,
            "avg_gpa": 0.0,
            "avg_gre": 0.0,
            "avg_gre_v": 0.0,
            "avg_gre_aw": 0.0,
            "avg_gpa_american_fall_2026": 0.0,
            "acceptance_pct_fall_2026": 0.0,
            "avg_gpa_accepted_fall_2026": 0.0,
            "jhu_ms_cs_count": 0,
            "cs_phd_accept_2026": 0,
            "cs_phd_accept_2026_llm": 0,
            "unc_masters_program_rows": [],
            "unc_phd_program_rows": [],
        },
    )
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data")
    assert resp.status_code == 202
    assert resp.get_json() == {"ok": True}
    assert len(state["db_rows"]) == 2

    resp = client.post("/pull-data")
    assert resp.status_code == 202
    assert resp.get_json() == {"ok": True}
    assert len(state["db_rows"]) == 3
    urls = {row.get("url") for row in state["db_rows"]}
    assert urls == {
        "https://example.com/result/1",
        "https://example.com/result/2",
        "https://example.com/result/3",
    }
