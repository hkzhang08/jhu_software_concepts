from src import website as website


def test_answer_labels_per_analysis(monkeypatch):
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
    answer_count = body.count("Answer:")
    question_count = body.count("card-question")
    assert question_count > 0
    assert answer_count == question_count
