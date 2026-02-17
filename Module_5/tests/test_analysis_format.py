"""Tests for analysis page formatting and helper utilities.

These tests ensure analysis rendering is labeled and percentages are shown
with two decimals, and they validate helper formatting behavior.
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://localhost/grad_cafe")

import re

import pytest
from src import website as website

pytestmark = pytest.mark.analysis


def test_answer_labels_per_analysis():
    """Render /analysis and verify labels and two-decimal percentages."""
    def fake_metrics():
        return {
            "fall_2026_count": 0,
            "intl_pct": 12.3,
            "avg_gpa": 0.0,
            "avg_gre": 0.0,
            "avg_gre_v": 0.0,
            "avg_gre_aw": 0.0,
            "avg_gpa_american_fall_2026": 0.0,
            "acceptance_pct_fall_2026": 45.678,
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
    answer_count = body.count("Answer:")
    question_count = body.count("card-question")
    assert question_count > 0
    assert answer_count == question_count
    assert re.search(r"Percentage International:\s*\d+\.\d{2}", body)
    assert re.search(r"Acceptance percent:\s*\d+\.\d{2}", body)


def test_helper_formatters_cover_edge_cases():
    """Cover helper functions used by analysis rendering."""
    assert website.fnum(None) is None
    assert website.fnum(3) == 3.0
    assert website.fnum("GPA 4.5") == 4.5

    assert website.fdate("") is None
    assert str(website.fdate("February 01, 2026")) == "2026-02-01"
    assert website.fdate("not a date") is None

    assert website.fdegree(None) is None
    assert website.fdegree("PhD") == 2.0
    assert website.fdegree("Masters") == 1.0
    assert website.fdegree("Bachelor") is None

    assert website.ftext(None) is None
    assert website.ftext("a\x00b") == "ab"

    assert website.fmt_pct(None) == "N/A"
    assert website.fmt_pct("abc") == "abc"
    assert website.fmt_pct(12.3) == "12.30"
