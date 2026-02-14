import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from src import clean

pytestmark = pytest.mark.clean


def test_load_data_reads_json_file(tmp_path):
    data = [{"a": 1}, {"b": 2}]
    file_path = tmp_path / "data.json"
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(data, handle)

    result = clean.load_data(str(file_path))
    assert result == data


def test_load_rows_missing_file_returns_empty(tmp_path):
    missing = tmp_path / "missing.json"
    assert clean.load_rows(str(missing)) == []


def test_load_rows_json_array(tmp_path):
    rows = [{"x": 1}, {"y": 2}]
    file_path = tmp_path / "array.json"
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(rows, handle)

    result = clean.load_rows(str(file_path))
    assert result == rows


def test_load_rows_jsonl(tmp_path):
    rows = [{"x": 1}, {"y": 2}]
    file_path = tmp_path / "data.jsonl"
    with open(file_path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
        handle.write("\n")

    result = clean.load_rows(str(file_path))
    assert result == rows


def test_load_rows_empty_or_whitespace(tmp_path):
    file_path = tmp_path / "empty.jsonl"
    with open(file_path, "w", encoding="utf-8") as handle:
        handle.write("   \n\n  ")

    result = clean.load_rows(str(file_path))
    assert result == []


def test_is_missing_variants():
    assert clean.is_missing(None) is True
    assert clean.is_missing("n/a") is True
    assert clean.is_missing("NA") is True
    assert clean.is_missing(" null ") is True
    assert clean.is_missing("valid value") is False
    assert clean.is_missing(123) is False


def test_clean_data_filters_and_composes_program(capsys):
    rows = [
        {"program_name": None, "university": "Test U"},
        {"program_name": "CS", "university": None},
        {"program_name": "CS", "university": "Test U"},
    ]
    cleaned = clean.clean_data(rows)
    captured = capsys.readouterr()

    assert len(cleaned) == 1
    assert cleaned[0]["program"] == "CS, Test U"
    assert "Records removed due to missing program/university: 2" in captured.out


def test_save_data_writes_json_file(tmp_path):
    rows = [{"a": 1}, {"b": 2}]
    file_path = tmp_path / "out.json"
    clean.save_data(rows, str(file_path))

    with open(file_path, "r", encoding="utf-8") as handle:
        loaded = json.load(handle)
    assert loaded == rows


def test_save_json_list_writes_json_array(tmp_path):
    rows = [{"a": 1}, {"b": 2}]
    file_path = tmp_path / "out_list.json"
    clean.save_json_list(rows, str(file_path))

    with open(file_path, "r", encoding="utf-8") as handle:
        loaded = json.load(handle)
    assert loaded == rows


def test_reorder_data_returns_expected_keys():
    row = {
        "program": "CS, Test U",
        "masters_or_phd": "Masters",
        "comments": "Hello",
        "date_added": "February 01, 2026",
        "url": "https://example.com/result/1",
        "applicant_status": "Accepted",
        "decision_date": "01 Feb",
        "semester_year_start": "Fall 2026",
        "citizenship": "American",
        "gpa": "GPA 4.0",
        "gre": "GRE 320",
        "gre_v": "GRE V 160",
        "gre_aw": "GRE AW 4.0",
    }
    reordered = clean.reorder_data(row)
    keys = list(reordered.keys())
    assert keys[0] == "program"
    assert reordered["program"] == "CS, Test U"
    assert reordered["masters_or_phd"] == "Masters"
    assert reordered["comments"] == "Hello"


def test_append_jsonl_missing_source_is_noop(tmp_path):
    src = tmp_path / "missing.jsonl"
    dest = tmp_path / "dest.jsonl"
    clean.append_jsonl(str(src), str(dest))
    assert not dest.exists()


def test_append_jsonl_creates_new_dest(tmp_path):
    src = tmp_path / "src.jsonl"
    dest = tmp_path / "dest.jsonl"
    with open(src, "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"a": 1}) + "\n")
        handle.write("\n")
        handle.write(json.dumps({"b": 2}) + "\n")

    clean.append_jsonl(str(src), str(dest))
    with open(dest, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    assert lines == [json.dumps({"a": 1}), json.dumps({"b": 2})]


def test_append_jsonl_appends_to_existing_dest(tmp_path):
    src = tmp_path / "src.jsonl"
    dest = tmp_path / "dest.jsonl"
    with open(dest, "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"a": 1}) + "\n")
    with open(src, "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"b": 2}) + "\n")

    clean.append_jsonl(str(src), str(dest))
    with open(dest, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    assert lines == [json.dumps({"a": 1}), json.dumps({"b": 2})]


def test_run_llm_standardizer_invokes_subprocess(monkeypatch, tmp_path):
    captured = {}

    def fake_run(args, cwd=None, check=None):
        captured["args"] = args
        captured["cwd"] = cwd
        captured["check"] = check

    monkeypatch.setattr(clean.subprocess, "run", fake_run)
    monkeypatch.setattr(clean, "LLM_DIR", str(tmp_path))
    monkeypatch.setattr(clean, "LLM_APP", str(tmp_path / "app.py"))

    input_file = tmp_path / "input.json"
    output_file = tmp_path / "output.json"
    clean.run_llm_standardizer(str(input_file), str(output_file))

    assert captured["cwd"] == str(tmp_path)
    assert captured["check"] is True
    assert captured["args"][:2] == [sys.executable, str(tmp_path / "app.py")]
    assert "--file" in captured["args"]
    assert "--out" in captured["args"]
    assert str(input_file.resolve()) in captured["args"]
    assert str(output_file.resolve()) in captured["args"]


def test_main_no_new_rows(monkeypatch, capsys):
    monkeypatch.setattr(clean, "load_data", lambda _file: [{"program_name": "CS", "university": "Test U"}])
    monkeypatch.setattr(clean, "load_rows", lambda _file: [{"url": "u1"}])
    monkeypatch.setattr(clean, "save_data", lambda _rows, _file: None)
    monkeypatch.setattr(clean, "save_json_list", lambda _rows, _file: None)
    monkeypatch.setattr(clean, "run_llm_standardizer", lambda _in, _out: None)
    monkeypatch.setattr(clean, "append_jsonl", lambda _src, _dst: None)

    clean.main()
    captured = capsys.readouterr()
    assert "No new rows to add to LLM output." in captured.out


def test_main_with_new_rows_triggers_pipeline(monkeypatch):
    called = {"save_json_list": False, "run_llm": False, "append": False}

    monkeypatch.setattr(
        clean,
        "load_data",
        lambda _file: [
            {"program_name": "CS", "university": "Test U", "url": "u1"},
            {"program_name": "CS", "university": "Test U", "url": "u2"},
        ],
    )
    monkeypatch.setattr(clean, "load_rows", lambda _file: [{"url": "u1"}])
    monkeypatch.setattr(clean, "save_data", lambda _rows, _file: None)

    def fake_save_json_list(_rows, _file):
        called["save_json_list"] = True

    def fake_run_llm(_in, _out):
        called["run_llm"] = True

    def fake_append(_src, _dst):
        called["append"] = True

    monkeypatch.setattr(clean, "save_json_list", fake_save_json_list)
    monkeypatch.setattr(clean, "run_llm_standardizer", fake_run_llm)
    monkeypatch.setattr(clean, "append_jsonl", fake_append)

    clean.main()
    assert called["save_json_list"] is True
    assert called["run_llm"] is True
    assert called["append"] is True


def test_main_guard_executes(monkeypatch):
    called = {"ran": False}

    def stub_main():
        called["ran"] = True

    target_path = clean.__file__
    with open(target_path, "r", encoding="utf-8") as handle:
        lines = handle.readlines()
    guard_line = next(
        i for i, line in enumerate(lines, 1) if 'if __name__ == "__main__":' in line
    )
    call_line = guard_line + 1
    code = "\n" * (call_line - 1) + "main()\n"
    exec(compile(code, target_path, "exec"), {"main": stub_main})

    assert called["ran"] is True
