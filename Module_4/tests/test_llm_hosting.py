import importlib
import io
import json
import runpy
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytestmark = pytest.mark.db


def import_app():
    fake_hf = types.SimpleNamespace(hf_hub_download=lambda **_kw: "/tmp/model.gguf")

    class FakeLlama:
        def __init__(self, *args, **kwargs):
            pass

        def create_chat_completion(self, messages, **kwargs):
            return {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"standardized_program":"X",'
                                '"standardized_university":"Y"}'
                            )
                        }
                    }
                ]
            }

    sys.modules["huggingface_hub"] = fake_hf
    sys.modules["llama_cpp"] = types.SimpleNamespace(Llama=FakeLlama)

    if "src.llm_hosting.app" in sys.modules:
        del sys.modules["src.llm_hosting.app"]
    import src.llm_hosting.app as app

    return importlib.reload(app)


def test_normalize_input_variants():
    app = import_app()
    assert app._normalize_input([{"a": 1}]) == [{"a": 1}]
    assert app._normalize_input({"rows": [{"b": 2}]}) == [{"b": 2}]
    assert app._normalize_input({"not_rows": []}) == []


def test_read_lines_exists_and_missing(tmp_path):
    app = import_app()
    path = tmp_path / "lines.txt"
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("line1\n\nline2\n")
    assert app._read_lines(str(path)) == ["line1", "line2"]
    assert app._read_lines(str(tmp_path / "missing.txt")) == []


def test_split_fallback_variants():
    app = import_app()

    prog, uni = app._split_fallback("Mathematics, University of X")
    assert prog == "Mathematics"
    assert uni == "University of X"

    prog, uni = app._split_fallback("Physics at Test U")
    assert prog == "Physics"
    assert uni == "Test U"

    prog, uni = app._split_fallback("Chemistry @ Test U")
    assert prog == "Chemistry"
    assert uni == "Test U"

    prog, uni = app._split_fallback("Information, McG")
    assert uni == "Mcgill University"

    prog, uni = app._split_fallback("Math, UBC")
    assert uni == "University of British Columbia"

    prog, uni = app._split_fallback("Standalone Program")
    assert uni == "Unknown"


def test_best_match_variants():
    app = import_app()
    assert app._best_match("", ["A"]) is None
    assert app._best_match("Test", []) is None
    assert app._best_match("McGill Universty", ["McGill University"]) == "McGill University"


def test_post_normalize_program_paths(monkeypatch):
    app = import_app()
    monkeypatch.setattr(app, "CANON_PROGS", ["Computer Science"])

    assert app._post_normalize_program("Mathematic") == "Mathematics"
    assert app._post_normalize_program("computer science") == "Computer Science"
    monkeypatch.setattr(
        app,
        "_best_match",
        lambda name, candidates, cutoff=0.84: "Computer Science"
        if name == "Comp Science"
        else None,
    )
    assert app._post_normalize_program("Comp Science") == "Computer Science"
    assert app._post_normalize_program("random field") == "Random Field"


def test_post_normalize_university_paths(monkeypatch):
    app = import_app()
    monkeypatch.setattr(app, "CANON_UNIS", ["University of Toronto"])

    assert app._post_normalize_university("mcg") == "Mcgill University"
    assert app._post_normalize_university("McGiill University") == "Mcgill University"
    assert app._post_normalize_university("University Of Toronto") == "University of Toronto"
    assert app._post_normalize_university("university of toronto") == "University of Toronto"

    monkeypatch.setattr(
        app,
        "_best_match",
        lambda name, candidates, cutoff=0.86: "University of Toronto"
        if name == "Universty of Toronto"
        else None,
    )
    assert app._post_normalize_university("Universty of Toronto") == "University of Toronto"
    assert app._post_normalize_university("") == "Unknown"


def test_call_llm_json_success(monkeypatch):
    app = import_app()

    class FakeLlama:
        def create_chat_completion(self, *args, **kwargs):
            return {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"standardized_program":"math","standardized_university":"test university"}'
                            )
                        }
                    }
                ]
            }

    monkeypatch.setattr(app, "_load_llm", lambda: FakeLlama())
    monkeypatch.setattr(app, "_post_normalize_program", lambda p: f"NP:{p}")
    monkeypatch.setattr(app, "_post_normalize_university", lambda u: f"NU:{u}")

    result = app._call_llm("ignored")
    assert result["standardized_program"] == "NP:math"
    assert result["standardized_university"] == "NU:test university"


def test_call_llm_json_failure_triggers_fallback(monkeypatch):
    app = import_app()

    class FakeLlama:
        def create_chat_completion(self, *args, **kwargs):
            return {"choices": [{"message": {"content": "not json"}}]}

    monkeypatch.setattr(app, "_load_llm", lambda: FakeLlama())
    monkeypatch.setattr(app, "_split_fallback", lambda _text: ("prog", "uni"))
    monkeypatch.setattr(app, "_post_normalize_program", lambda p: f"NP:{p}")
    monkeypatch.setattr(app, "_post_normalize_university", lambda u: f"NU:{u}")

    result = app._call_llm("input")
    assert result["standardized_program"] == "NP:prog"
    assert result["standardized_university"] == "NU:uni"


def test_health_endpoint():
    app = import_app()
    client = app.app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}


def test_standardize_endpoint_appends_fields(monkeypatch):
    app = import_app()

    def fake_call_llm(_text):
        return {
            "standardized_program": "Program",
            "standardized_university": "University",
        }

    monkeypatch.setattr(app, "_call_llm", fake_call_llm)
    client = app.app.test_client()
    resp = client.post(
        "/standardize",
        json={"rows": [{"program": "CS, Test U"}]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["rows"][0]["llm-generated-program"] == "Program"
    assert data["rows"][0]["llm-generated-university"] == "University"


def test_standardize_endpoint_accepts_list_payload(monkeypatch):
    app = import_app()

    def fake_call_llm(_text):
        return {
            "standardized_program": "Program",
            "standardized_university": "University",
        }

    monkeypatch.setattr(app, "_call_llm", fake_call_llm)
    client = app.app.test_client()
    resp = client.post(
        "/standardize",
        json=[{"program": "CS, Test U"}],
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["rows"][0]["llm-generated-program"] == "Program"
    assert data["rows"][0]["llm-generated-university"] == "University"


def test_cli_process_file_stdout_and_file(monkeypatch, tmp_path):
    app = import_app()

    def fake_call_llm(_text):
        return {
            "standardized_program": "Program",
            "standardized_university": "University",
        }

    monkeypatch.setattr(app, "_call_llm", fake_call_llm)

    input_path = tmp_path / "in.json"
    with open(input_path, "w", encoding="utf-8") as handle:
        json.dump([{"program": "CS, Test U"}], handle)

    # stdout path
    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)
    app._cli_process_file(str(input_path), out_path=None, append=False, to_stdout=True)
    out_lines = [line for line in buf.getvalue().splitlines() if line.strip()]
    assert len(out_lines) == 1

    # file path
    out_path = tmp_path / "out.jsonl"
    app._cli_process_file(str(input_path), out_path=str(out_path), append=False, to_stdout=False)
    with open(out_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    assert len(lines) == 1


def test_cli_process_file_append(monkeypatch, tmp_path):
    app = import_app()

    def fake_call_llm(_text):
        return {
            "standardized_program": "Program",
            "standardized_university": "University",
        }

    monkeypatch.setattr(app, "_call_llm", fake_call_llm)

    input_path = tmp_path / "in.json"
    with open(input_path, "w", encoding="utf-8") as handle:
        json.dump([{"program": "CS, Test U"}], handle)

    out_path = tmp_path / "out.jsonl"
    app._cli_process_file(str(input_path), out_path=str(out_path), append=False, to_stdout=False)
    app._cli_process_file(str(input_path), out_path=str(out_path), append=True, to_stdout=False)

    with open(out_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    assert len(lines) == 2


def test_main_guard_serve_path(monkeypatch, tmp_path):
    app = import_app()
    target_path = app.__file__

    called = {}

    def fake_run(self, *args, **kwargs):
        called["run"] = kwargs

    monkeypatch.setattr("flask.Flask.run", fake_run)
    monkeypatch.setattr(sys, "argv", ["app.py", "--serve"])

    runpy.run_path(target_path, run_name="__main__")

    assert "run" in called
    assert called["run"]["host"] == "0.0.0.0"


def test_main_guard_file_path(monkeypatch, tmp_path):
    app = import_app()
    target_path = app.__file__

    input_path = tmp_path / "in.json"
    out_path = tmp_path / "out.jsonl"
    with open(input_path, "w", encoding="utf-8") as handle:
        json.dump([{"program": "CS, Test U"}], handle)

    monkeypatch.setattr(
        sys,
        "argv",
        ["app.py", "--file", str(input_path), "--out", str(out_path)],
    )

    runpy.run_path(target_path, run_name="__main__")

    with open(out_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    assert len(lines) == 1


def test_load_llm_returns_cached_instance(monkeypatch):
    app = import_app()
    sentinel = object()
    monkeypatch.setattr(app, "_LLM", sentinel)
    monkeypatch.setattr(app, "hf_hub_download", lambda **_kw: (_ for _ in ()).throw(RuntimeError("should not download")))
    result = app._load_llm()
    assert result is sentinel
