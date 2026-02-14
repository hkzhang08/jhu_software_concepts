"""
Class:         EP 605 2256 81 - Spring 2026
Assignment:    Module_4 - Testing and Documentation Assignment
Due Date:      February 15th by 11:59AM
Name:          Helen Zhang
Email:         hzhan308@jh.edu
"""


from flask import Flask
from src import website as website
def test_create_app_returns_flask():
    app = website.create_app()
    assert isinstance(app, Flask)


def test_routes_registered():
    app = website.create_app()
    rules = {rule.rule: rule.methods for rule in app.url_map.iter_rules()}
    assert "/" in rules and "GET" in rules["/"]
    assert "/analysis" in rules and "GET" in rules["/analysis"]
    assert "/pull-data" in rules and "POST" in rules["/pull-data"]
    assert "/update-analysis" in rules and "POST" in rules["/update-analysis"]


def test_index_route_renders(monkeypatch):
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

    monkeypatch.setattr(website, "fetch_metrics", fake_metrics)
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200


def test_analysis_page_contains_buttons_and_text(monkeypatch):
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

    monkeypatch.setattr(website, "fetch_metrics", fake_metrics)
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.get("/analysis")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "<button" in body and "Pull Data" in body
    assert "<button" in body and "Update Analysis" in body
    assert "Analysis" in body
    assert "Answer" in body


def test_pull_data_route_redirects(monkeypatch):
    monkeypatch.setattr(website, "run_pull_pipeline", lambda: None)
    website.PULL_STATE["status"] = "idle"
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.post("/pull-data")
    assert resp.status_code in (302, 303)


def test_update_analysis_route_redirects():
    website.PULL_STATE["status"] = "idle"
    app = website.create_app()
    app.config["TESTING"] = True
    client = app.test_client()
    resp = client.post("/update-analysis")
    assert resp.status_code in (302, 303)
