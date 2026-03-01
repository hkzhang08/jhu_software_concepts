"""Tests for the LLM standardizer API and CLI helpers."""

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
    """Import the app module with stubbed LLM and hub dependencies."""
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
    """_normalize_input accepts list or {'rows': [...]} payloads."""
    app = import_app()
    assert app._normalize_input([{"a": 1}]) == [{"a": 1}]
    assert app._normalize_input({"rows": [{"b": 2}]}) == [{"b": 2}]
    assert app._normalize_input({"not_rows": []}) == []


def test_read_lines_exists_and_missing(tmp_path):
    """_read_lines handles existing and missing files."""
    app = import_app()
    path = tmp_path / "lines.txt"
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("line1\n\nline2\n")
    assert app._read_lines(str(path)) == ["line1", "line2"]
    assert app._read_lines(str(tmp_path / "missing.txt")) == []


def test_split_fallback_variants():
    """_split_fallback handles commas, 'at', abbreviations, and unknowns."""
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
    """_best_match returns None or the closest candidate."""
    app = import_app()
    assert app._best_match("", ["A"]) is None
    assert app._best_match("Test", []) is None
    assert app._best_match("McGill Universty", ["McGill University"]) == "McGill University"


def test_post_normalize_program_paths(monkeypatch):
    """_post_normalize_program follows fixes, canonical, and fuzzy paths."""
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
    """_post_normalize_university follows abbrev/fixes/canonical/fuzzy paths."""
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
    """_call_llm parses JSON response and post-normalizes."""
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
    """_call_llm falls back to rule-based parsing on invalid JSON."""
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


def test_call_llm_uses_rules_when_llm_unavailable(monkeypatch):
    """_call_llm uses rules-first fallback when optional deps are unavailable."""
    app = import_app()

    monkeypatch.setattr(app, "_load_llm", lambda: None)
    monkeypatch.setattr(app, "_split_fallback", lambda _text: ("prog", "uni"))
    monkeypatch.setattr(app, "_post_normalize_program", lambda p: f"NP:{p}")
    monkeypatch.setattr(app, "_post_normalize_university", lambda u: f"NU:{u}")

    result = app._call_llm("input")
    assert result["standardized_program"] == "NP:prog"
    assert result["standardized_university"] == "NU:uni"


def test_health_endpoint():
    """Health endpoint returns ok JSON."""
    app = import_app()
    client = app.app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}


def test_standardize_endpoint_appends_fields(monkeypatch):
    """POST /standardize appends LLM fields for row payload."""
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
    """POST /standardize accepts a list payload."""
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


def test_standardize_endpoint_reports_invalid_rows_skipped(monkeypatch):
    """Invalid non-dict rows are skipped and counted in response metadata."""
    app = import_app()

    monkeypatch.setattr(
        app,
        "_call_llm",
        lambda _text: {
            "standardized_program": "Program",
            "standardized_university": "University",
        },
    )
    client = app.app.test_client()
    resp = client.post(
        "/standardize",
        json={"rows": ["bad-row", {"program": "CS, Test U"}]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["invalid_rows_skipped"] == 1
    assert len(data["rows"]) == 1


def test_standardize_endpoint_sets_truncated_flag(monkeypatch):
    """Response includes truncated metadata when rows exceed configured limit."""
    app = import_app()
    monkeypatch.setattr(app, "STANDARDIZE_MAX_ROWS", 1)
    monkeypatch.setattr(
        app,
        "_call_llm",
        lambda _text: {
            "standardized_program": "Program",
            "standardized_university": "University",
        },
    )

    client = app.app.test_client()
    resp = client.post(
        "/standardize",
        json={"rows": [{"program": "A"}, {"program": "B"}]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["truncated"] is True
    assert len(data["rows"]) == 1


def test_standardize_endpoint_fallback_on_row_error_and_program_cap(monkeypatch):
    """Row-level failures fall back to rules and enforce max program length."""
    app = import_app()
    monkeypatch.setattr(app, "STANDARDIZE_MAX_PROGRAM_CHARS", 5)

    seen = {}

    def fake_call_llm(program_text):
        seen["program_text"] = program_text
        raise ValueError("boom")

    monkeypatch.setattr(app, "_call_llm", fake_call_llm)
    monkeypatch.setattr(app, "_split_fallback", lambda _text: ("prog", "uni"))
    monkeypatch.setattr(app, "_post_normalize_program", lambda p: f"P:{p}")
    monkeypatch.setattr(app, "_post_normalize_university", lambda u: f"U:{u}")

    client = app.app.test_client()
    resp = client.post(
        "/standardize",
        json={"rows": [{"program": "abcdefghijklmnopqrstuvwxyz"}]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert seen["program_text"] == "abcde"
    assert data["rows"][0]["llm-generated-program"] == "P:prog"
    assert data["rows"][0]["llm-generated-university"] == "U:uni"


def test_cli_process_file_stdout_and_file(monkeypatch, tmp_path):
    """CLI writes JSONL to stdout and to a file."""
    app = import_app()
    monkeypatch.setenv("LLM_IO_BASE_DIR", str(tmp_path))

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
    app._cli_process_file("in.json", out_path=None, append=False, to_stdout=True)
    out_lines = [line for line in buf.getvalue().splitlines() if line.strip()]
    assert len(out_lines) == 1

    # file path
    out_path = tmp_path / "out.jsonl"
    app._cli_process_file("in.json", out_path="out.jsonl", append=False, to_stdout=False)
    with open(out_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    assert len(lines) == 1


def test_cli_process_file_append(monkeypatch, tmp_path):
    """CLI append mode adds new rows to existing JSONL."""
    app = import_app()
    monkeypatch.setenv("LLM_IO_BASE_DIR", str(tmp_path))

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
    app._cli_process_file("in.json", out_path="out.jsonl", append=False, to_stdout=False)
    app._cli_process_file("in.json", out_path="out.jsonl", append=True, to_stdout=False)

    with open(out_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    assert len(lines) == 2


def test_main_guard_serve_path(monkeypatch, tmp_path):
    """__main__ guard runs server when --serve is used."""
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
    """__main__ guard processes a file when --file is provided."""
    app = import_app()
    target_path = app.__file__
    monkeypatch.setenv("LLM_IO_BASE_DIR", str(tmp_path))

    input_path = tmp_path / "in.json"
    out_path = tmp_path / "out.jsonl"
    with open(input_path, "w", encoding="utf-8") as handle:
        json.dump([{"program": "CS, Test U"}], handle)

    monkeypatch.setattr(
        sys,
        "argv",
        ["app.py", "--file", "in.json", "--out", "out.jsonl"],
    )

    runpy.run_path(target_path, run_name="__main__")

    with open(out_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    assert len(lines) == 1


def test_cli_process_file_rejects_path_outside_base(monkeypatch, tmp_path):
    """CLI path guard rejects input files outside the allowed base directory."""
    app = import_app()
    monkeypatch.setenv("LLM_IO_BASE_DIR", str(tmp_path))

    outside_path = tmp_path.parent / "outside.json"
    with open(outside_path, "w", encoding="utf-8") as handle:
        json.dump([{"program": "CS, Test U"}], handle)

    with pytest.raises(ValueError, match="filename without directories"):
        app._cli_process_file(str(outside_path), out_path=None, append=False, to_stdout=True)


def test_cli_process_file_rejects_empty_input_filename(monkeypatch, tmp_path):
    """CLI requires a non-empty input filename."""
    app = import_app()
    monkeypatch.setenv("LLM_IO_BASE_DIR", str(tmp_path))

    with pytest.raises(ValueError, match="Input filename is required"):
        app._cli_process_file("", out_path=None, append=False, to_stdout=True)


def test_cli_process_file_rejects_invalid_input_filename_chars(monkeypatch, tmp_path):
    """CLI blocks unsafe characters in input filename."""
    app = import_app()
    monkeypatch.setenv("LLM_IO_BASE_DIR", str(tmp_path))

    with pytest.raises(ValueError, match="Input filename contains invalid characters"):
        app._cli_process_file("bad*name.json", out_path=None, append=False, to_stdout=True)


def test_cli_process_file_rejects_missing_input_file(monkeypatch, tmp_path):
    """CLI fails fast when the input file does not exist under base dir."""
    app = import_app()
    monkeypatch.setenv("LLM_IO_BASE_DIR", str(tmp_path))

    with pytest.raises(FileNotFoundError, match="Input file not found"):
        app._cli_process_file("missing.json", out_path=None, append=False, to_stdout=True)


def test_cli_process_file_rejects_output_with_directories(monkeypatch, tmp_path):
    """CLI blocks output paths that include directories."""
    app = import_app()
    monkeypatch.setenv("LLM_IO_BASE_DIR", str(tmp_path))
    monkeypatch.setattr(
        app,
        "_call_llm",
        lambda _text: {
            "standardized_program": "Program",
            "standardized_university": "University",
        },
    )
    with open(tmp_path / "in.json", "w", encoding="utf-8") as handle:
        json.dump([{"program": "CS, Test U"}], handle)

    with pytest.raises(ValueError, match="Output path must be a filename without directories"):
        app._cli_process_file("in.json", out_path="../out.jsonl", append=False, to_stdout=False)


def test_cli_process_file_rejects_invalid_output_filename_chars(monkeypatch, tmp_path):
    """CLI blocks unsafe characters in output filename."""
    app = import_app()
    monkeypatch.setenv("LLM_IO_BASE_DIR", str(tmp_path))
    monkeypatch.setattr(
        app,
        "_call_llm",
        lambda _text: {
            "standardized_program": "Program",
            "standardized_university": "University",
        },
    )
    with open(tmp_path / "in.json", "w", encoding="utf-8") as handle:
        json.dump([{"program": "CS, Test U"}], handle)

    with pytest.raises(ValueError, match="Output filename contains invalid characters"):
        app._cli_process_file("in.json", out_path="bad*out.jsonl", append=False, to_stdout=False)


def test_main_guard_file_path_reports_cli_error(monkeypatch, tmp_path):
    """__main__ guard returns parser error for invalid CLI file inputs."""
    app = import_app()
    target_path = app.__file__
    monkeypatch.setenv("LLM_IO_BASE_DIR", str(tmp_path))
    monkeypatch.setattr(
        sys,
        "argv",
        ["app.py", "--file", "missing.json", "--out", "out.jsonl"],
    )

    with pytest.raises(SystemExit) as exc_info:
        runpy.run_path(target_path, run_name="__main__")
    assert exc_info.value.code == 2


def test_load_llm_returns_cached_instance(monkeypatch):
    """_load_llm returns cached instance without download."""
    app = import_app()
    sentinel = object()
    monkeypatch.setitem(app._LLM_CACHE, "instance", sentinel)
    monkeypatch.setattr(app, "hf_hub_download", lambda **_kw: (_ for _ in ()).throw(RuntimeError("should not download")))
    result = app._load_llm()
    assert result is sentinel


def test_load_llm_returns_none_when_optional_deps_missing(monkeypatch):
    """_load_llm returns None when llama or huggingface runtime is unavailable."""
    app = import_app()
    monkeypatch.setitem(app._LLM_CACHE, "instance", None)
    monkeypatch.setattr(app, "Llama", None)
    assert app._load_llm() is None
