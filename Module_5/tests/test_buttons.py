"""Tests for button endpoints and busy-state behavior."""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://localhost/grad_cafe")

import json
from types import SimpleNamespace

import pytest
from src import website as website

pytestmark = pytest.mark.buttons


def _fake_metrics():
    """Return a minimal metrics payload for analysis rendering."""
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


def test_post_pull_data_triggers_loader_with_scraped_rows(monkeypatch, tmp_path):
    """POST /pull-data runs the pipeline and returns ok JSON by default."""
    fake_rows = [{"program": "Test Program", "url": "https://example.com/result/1"}]
    calls = []

    def fake_subprocess_run(args, cwd=None, check=None, capture_output=False, text=False):
        if len(args) >= 2 and args[1] == "-c":
            calls.append("scrape")
            os.makedirs(cwd, exist_ok=True)
            with open(os.path.join(cwd, "applicant_data.json"), "w", encoding="utf-8") as handle:
                json.dump(fake_rows, handle)
            return SimpleNamespace(stdout="")
        if len(args) >= 2 and os.path.basename(args[1]) == "clean.py":
            calls.append("clean")
            return SimpleNamespace(stdout="")
        if len(args) >= 2 and os.path.basename(args[1]) == "load_data.py":
            calls.append("load")
            with open(os.path.join(cwd, "applicant_data.json"), "r", encoding="utf-8") as handle:
                loaded_rows = json.load(handle)
            assert loaded_rows == fake_rows
            return SimpleNamespace(stdout="Inserted rows: 1")
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(website, "BASE_DIR", str(tmp_path))
    monkeypatch.setattr(website, "RAW_FILE", str(tmp_path / "applicant_data.json"))
    monkeypatch.setattr(website.subprocess, "run", fake_subprocess_run)
    website.PULL_STATE["status"] = "idle"
    app = website.create_app(fetch_metrics_fn=_fake_metrics)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data")
    assert resp.status_code == 202
    assert resp.get_json() == {"ok": True}
    assert calls == ["scrape", "clean", "load"]


def test_post_pull_data_with_explicit_json_accept(monkeypatch):
    """Explicit JSON Accept header follows the JSON response branch."""
    monkeypatch.setattr(website, "run_pull_pipeline", lambda: None)
    website.PULL_STATE["status"] = "idle"
    app = website.create_app(fetch_metrics_fn=_fake_metrics)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data", headers={"Accept": "application/json"})
    assert resp.status_code == 202
    assert resp.get_json() == {"ok": True}


def test_post_update_analysis_redirects_when_not_busy():
    """POST /update-analysis redirects to / when not busy."""
    website.PULL_STATE["status"] = "idle"
    app = website.create_app(fetch_metrics_fn=_fake_metrics)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/update-analysis")
    assert resp.status_code == 303
    assert resp.headers["Location"].endswith("/")


def test_run_pull_pipeline_sets_default_message(monkeypatch, tmp_path):
    """Pipeline sets a default message when inserted row count is unknown."""
    def fake_subprocess_run(args, cwd=None, check=None, capture_output=False, text=False):
        if len(args) >= 2 and os.path.basename(args[1]) == "load_data.py":
            return SimpleNamespace(stdout="no insert info")
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(website, "RAW_FILE", str(tmp_path / "applicant_data.json"))
    monkeypatch.setattr(website.subprocess, "run", fake_subprocess_run)

    website.run_pull_pipeline()
    assert website.PULL_STATE["status"] == "done"
    assert website.PULL_STATE["message"] == "Data Pull Complete. Database updated."


def test_run_pull_pipeline_error_sets_status(monkeypatch, tmp_path):
    """Pipeline exceptions set error status and message."""
    def boom(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(website, "RAW_FILE", str(tmp_path / "applicant_data.json"))
    monkeypatch.setattr(website.subprocess, "run", boom)

    website.run_pull_pipeline()
    assert website.PULL_STATE["status"] == "error"
    assert website.PULL_STATE["message"] == website.PULL_ERROR_MESSAGE


def test_post_pull_data_returns_error_when_pipeline_fails():
    """POST /pull-data returns 500 when pipeline reports failure."""
    def fake_run_pull_pipeline():
        website.PULL_STATE["status"] = "error"
        website.PULL_STATE["message"] = "Pull failed: boom"
        return False

    website.PULL_STATE["status"] = "idle"
    app = website.create_app(run_pull_pipeline_fn=fake_run_pull_pipeline)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data")
    assert resp.status_code == 500
    assert resp.get_json() == {"ok": False, "error": website.PULL_ERROR_MESSAGE}


def test_post_pull_data_returns_error_when_pipeline_raises():
    """POST /pull-data returns 500 when pipeline raises."""
    def boom():
        raise RuntimeError("kaboom")

    website.PULL_STATE["status"] = "idle"
    app = website.create_app(run_pull_pipeline_fn=boom)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data")
    assert resp.status_code == 500
    assert resp.get_json() == {"ok": False, "error": website.PULL_ERROR_MESSAGE}


def test_busy_gating_update_analysis_returns_409():
    """Busy state blocks update-analysis with 409."""
    website.PULL_STATE["status"] = "running"
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/update-analysis")
    assert resp.status_code == 409
    assert resp.get_json() == {"busy": True}


def test_busy_gating_pull_data_returns_409(monkeypatch):
    """Busy state blocks pull-data and prevents pipeline execution."""
    def should_not_run():
        raise AssertionError("run_pull_pipeline should not be called while busy")

    monkeypatch.setattr(website, "run_pull_pipeline", should_not_run)
    website.PULL_STATE["status"] = "running"
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data")
    assert resp.status_code == 409
    assert resp.get_json() == {"busy": True}


def test_busy_gating_pull_data_redirects_for_html(monkeypatch):
    """Busy state redirects browser form posts and updates pull message."""
    def should_not_run():
        raise AssertionError("run_pull_pipeline should not be called while busy")

    monkeypatch.setattr(website, "run_pull_pipeline", should_not_run)
    website.PULL_STATE["status"] = "running"
    website.PULL_STATE["message"] = ""
    app = website.create_app(fetch_metrics_fn=_fake_metrics)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data", headers={"Accept": "text/html"})
    assert resp.status_code == 303
    assert resp.headers["Location"].endswith("/")
    assert website.PULL_STATE["message"] == "Pull in progress..."


def test_pull_data_failure_redirects_for_html():
    """Failed pipeline redirects for HTML form callers."""
    def fake_run_pull_pipeline():
        website.PULL_STATE["status"] = "error"
        website.PULL_STATE["message"] = "Pull failed: boom"
        return False

    website.PULL_STATE["status"] = "idle"
    app = website.create_app(run_pull_pipeline_fn=fake_run_pull_pipeline)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data", headers={"Accept": "text/html"})
    assert resp.status_code == 303
    assert resp.headers["Location"].endswith("/")


def test_pull_data_redirects_for_html_form_posts(monkeypatch):
    """Browser-style HTML form submits are redirected back to index."""
    monkeypatch.setattr(website, "run_pull_pipeline", lambda: None)
    website.PULL_STATE["status"] = "idle"
    app = website.create_app(fetch_metrics_fn=_fake_metrics)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data", headers={"Accept": "text/html"})
    assert resp.status_code == 303
    assert resp.headers["Location"].endswith("/")
