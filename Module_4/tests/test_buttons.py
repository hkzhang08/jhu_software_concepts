import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import json
import os
from types import SimpleNamespace

import pytest
from src import website as website

pytestmark = pytest.mark.buttons


def test_post_pull_data_triggers_loader_with_scraped_rows(monkeypatch, tmp_path):
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
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    assert calls == ["scrape", "clean", "load"]


def test_post_update_analysis_returns_200_when_not_busy():
    website.PULL_STATE["status"] = "idle"
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/update-analysis", follow_redirects=True)
    assert resp.status_code == 200


def test_run_pull_pipeline_sets_default_message(monkeypatch, tmp_path):
    def fake_subprocess_run(args, cwd=None, check=None, capture_output=False, text=False):
        if len(args) >= 2 and os.path.basename(args[1]) == "load_data.py":
            return SimpleNamespace(stdout="no insert info")
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(website, "RAW_FILE", str(tmp_path / "applicant_data.json"))
    monkeypatch.setattr(website.subprocess, "run", fake_subprocess_run)

    website.run_pull_pipeline()
    assert website.PULL_STATE["status"] == "done"
    assert website.PULL_STATE["message"] == "Pull complete. Database updated."


def test_run_pull_pipeline_error_sets_status(monkeypatch, tmp_path):
    def boom(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(website, "RAW_FILE", str(tmp_path / "applicant_data.json"))
    monkeypatch.setattr(website.subprocess, "run", boom)

    website.run_pull_pipeline()
    assert website.PULL_STATE["status"] == "error"
    assert "Pull failed: boom" in website.PULL_STATE["message"]


def test_busy_gating_update_analysis_returns_409():
    website.PULL_STATE["status"] = "running"
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/update-analysis")
    assert resp.status_code == 409


def test_busy_gating_pull_data_returns_409(monkeypatch):
    def should_not_run():
        raise AssertionError("run_pull_pipeline should not be called while busy")

    monkeypatch.setattr(website, "run_pull_pipeline", should_not_run)
    website.PULL_STATE["status"] = "running"
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post("/pull-data")
    assert resp.status_code == 409
