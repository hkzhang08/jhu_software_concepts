"""Web route and template smoke tests for the Flask app."""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://localhost/grad_cafe")

import pytest
from flask import Flask
from src import website as website

pytestmark = pytest.mark.web


def test_create_app_returns_flask():
    """create_app() returns a Flask instance."""
    app = website.create_app()
    assert isinstance(app, Flask)


def test_init_main_runs_app(monkeypatch):
    """__init__ main guard invokes Flask.run()."""
    called = {}

    def fake_run(self, *args, **kwargs):
        called["kwargs"] = kwargs

    monkeypatch.setattr(Flask, "run", fake_run)
    init_path = ROOT / "src" / "__init__.py"
    with open(init_path, "r", encoding="utf-8") as handle:
        code = handle.read()
    globals_dict = {"__file__": str(init_path), "__name__": "__main__", "__package__": "src"}
    exec(compile(code, str(init_path), "exec"), globals_dict)
    assert called["kwargs"].get("debug") is True


def test_routes_registered():
    """Expected routes are registered with correct methods."""
    app = website.create_app()
    rules = {rule.rule: rule.methods for rule in app.url_map.iter_rules()}
    assert "/" in rules and "GET" in rules["/"]
    assert "/analysis" in rules and "GET" in rules["/analysis"]
    assert "/pull-data" in rules and "POST" in rules["/pull-data"]
    assert "/pull-status" in rules and "GET" in rules["/pull-status"]
    assert "/update-analysis" in rules and "POST" in rules["/update-analysis"]


def test_index_route_renders():
    """GET / renders successfully with injected metrics."""
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
            "unc_masters_program_rows": [("Test Program", 1)],
            "unc_phd_program_rows": [("Test Program", 1)],
        }

    app = website.create_app(fetch_metrics_fn=fake_metrics)
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200


def test_analysis_page_contains_buttons_and_text():
    """GET /analysis includes buttons, title text, and Answer labels."""
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
            "unc_masters_program_rows": [("Test Program", 1)],
            "unc_phd_program_rows": [("Test Program", 1)],
        }

    app = website.create_app(fetch_metrics_fn=fake_metrics)
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.get("/analysis")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "<button" in body and "Pull Data" in body
    assert "<button" in body and "Update Analysis" in body
    assert "Analysis" in body
    assert "Answer" in body


def test_index_route_handles_fetch_failure_without_leaking_error_text():
    """GET /analysis returns a safe fallback response when metrics fail."""
    def fake_metrics():
        raise RuntimeError("secret token should not leak")

    app = website.create_app(fetch_metrics_fn=fake_metrics)
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.get("/analysis")
    assert resp.status_code == 503
    body = resp.get_data(as_text=True)
    assert "Analysis is temporarily unavailable" in body
    assert "secret token should not leak" not in body


def test_pull_data_route_returns_ok_json(monkeypatch):
    """POST /pull-data returns ok JSON when idle."""
    monkeypatch.setattr(website, "run_pull_pipeline", lambda: None)
    website.PULL_STATE["status"] = "idle"
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.post("/pull-data")
    assert resp.status_code == 202
    assert resp.get_json() == {"ok": True}


def test_update_analysis_route_redirects_to_index():
    """POST /update-analysis redirects to / when idle."""
    website.PULL_STATE["status"] = "idle"
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.post("/update-analysis")
    assert resp.status_code == 303
    assert resp.headers["Location"].endswith("/")


def test_pull_status_route_returns_current_state():
    """GET /pull-status returns the in-memory pull status payload."""
    website.PULL_STATE["status"] = "running"
    website.PULL_STATE["message"] = website.PULL_RUNNING_MESSAGE
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.get("/pull-status")
    assert resp.status_code == 200
    assert resp.get_json() == {
        "status": "running",
        "message": website.PULL_RUNNING_MESSAGE,
    }


def test_index_shows_pull_status_message():
    """GET / renders the pull status message when present."""
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

    website.PULL_STATE["status"] = "done"
    website.PULL_STATE["message"] = "Data Pull Complete. Inserted 5 new rows."
    app = website.create_app(fetch_metrics_fn=fake_metrics)
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "data-testid=\"pull-status\"" in body
    assert "Data Pull Complete. Inserted 5 new rows." in body


def test_index_shows_and_clears_analysis_refresh_message():
    """GET / renders the analysis refresh note once, then clears it."""
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

    website.ANALYSIS_STATE["message"] = website.ANALYSIS_REFRESHED_MESSAGE
    app = website.create_app(fetch_metrics_fn=fake_metrics)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "data-testid=\"analysis-status\"" in body
    assert website.ANALYSIS_REFRESHED_MESSAGE in body
    assert website.ANALYSIS_STATE["message"] == ""

    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "data-testid=\"analysis-status\"" not in body
