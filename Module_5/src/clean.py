"""
Clean and enrich scraped GradCafe data, then run LLM standardization.

This module prepares scraped rows for analysis and optional LLM-based
standardization, producing both cleaned JSON and JSONL outputs.
"""

import json
import os
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Save file name for easy reference
SCRAPED_FILE = os.path.join(BASE_DIR, "applicant_data.json")
MASTER_FILE = os.path.join(BASE_DIR, "llm_extend_applicant_data.json")
NEW_INPUT_FILE = os.path.join(BASE_DIR, "new_applicant_data.json")
NEW_LLM_FILE = os.path.join(BASE_DIR, "llm_new_applicant.json")
LLM_DIR = os.path.join(BASE_DIR, "llm_hosting")
LLM_APP = os.path.join(LLM_DIR, "app.py")


# Options for missing data
UNAVAILABLE = {"", "n/a", "na", "none", "null", "nan"}


def load_data(file):
    """
    Load a JSON array file and return its parsed rows.

    :param file: Path to a JSON file containing a list of records.
    :returns: A list of dicts parsed from the JSON file.
    """
    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)


def load_rows(file_path):
    """
    Load rows from a JSON array file or JSONL file.

    :param file_path: Path to a JSON array or JSONL file.
    :returns: A list of dicts. Returns [] if the file is missing or empty.
    """
    if not os.path.exists(file_path):
        return []

    with open(file_path, "r", encoding="utf-8") as f:
        first_non_ws = ""
        while True:
            ch = f.read(1)
            if ch == "":
                return []
            if not ch.isspace():
                first_non_ws = ch
                break
        f.seek(0)
        if first_non_ws == "[":
            return json.load(f)

        rows = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
        return rows


def is_missing(value):
    """
    Return True if the value is missing or one of the unavailable sentinels.

    :param value: Any value to test.
    :returns: True if missing, False otherwise.
    """

    # Check if value is none
    if value is None:
        return True

    # Check if value is not none but options similar to none
    if isinstance(value, str):
        return value.strip().lower() in UNAVAILABLE

    # Cases where value is not missing
    return False


def clean_data(rows):
    """
    Build a combined "program" field and drop invalid rows.

    :param rows: Iterable of raw records.
    :returns: A new list of records with a "program" field added. Rows missing
        program_name or university are removed.
    """

    # Initialize list
    cleaned = []
    remove = 0

    # Loop through each record
    for row in rows:

        # Pull program name and university from data
        program_name = (row.get("program_name") or "").strip()
        university = (row.get("university") or "").strip()

        # remove cases where program name or university is missing (not valid)
        if is_missing(program_name) or is_missing(university):
            remove += 1
            continue

        # Combine into a program field
        program = f"{program_name}, {university}".strip(", ").strip()

        # Add the new field into the record
        new_row = dict(row)
        new_row["program"] = program
        cleaned.append(new_row)

    print("Records removed due to missing program/university:", remove)
    # Return the list of cleaned records
    return cleaned


def save_data(rows, file):
    """
    Write rows to a JSON file.

    :param rows: List of dicts to serialize.
    :param file: Destination path.
    """
    with open(file, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


def save_json_list(rows, file):
    """
    Write rows as a JSON array (used as LLM input).

    :param rows: List of dicts to serialize.
    :param file: Destination path.
    """
    with open(file, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


def reorder_data(row):
    """
    Reorder keys so "program" appears first and output matches the example format.

    :param row: A cleaned record dict.
    :returns: A new dict with the expected key order and without program_name/university.
    """

    return {
        "program": row.get("program", ""),
        "masters_or_phd": row.get("masters_or_phd"),
        "comments": row.get("comments"),
        "date_added": row.get("date_added"),
        "url": row.get("url"),
        "applicant_status": row.get("applicant_status"),
        "decision_date": row.get("decision_date"),
        "semester_year_start": row.get("semester_year_start"),
        "citizenship": row.get("citizenship"),
        "gpa": row.get("gpa"),
        "gre": row.get("gre"),
        "gre_v": row.get("gre_v"),
        "gre_aw": row.get("gre_aw"),
    }


def run_llm_standardizer(input_file, output_file):
    """
    Run the LLM standardizer CLI to produce JSONL output.

    :param input_file: Path to JSON array input for the LLM.
    :param output_file: Destination JSONL output path.
    """
    input_path = os.path.abspath(input_file)
    output_path = os.path.abspath(output_file)
    subprocess.run(
        [
            sys.executable,
            LLM_APP,
            "--file",
            input_path,
            "--out",
            output_path,
        ],
        cwd=LLM_DIR,
        check=True,
    )


def append_jsonl(src_file, dest_file):
    """
    Append JSONL rows to a destination file.

    :param src_file: JSONL source file.
    :param dest_file: JSONL destination file (created if missing).
    """
    if not os.path.exists(src_file):
        return

    mode = "a" if os.path.exists(dest_file) else "w"
    with open(src_file, "r", encoding="utf-8") as src, open(
        dest_file, mode, encoding="utf-8"
    ) as dest:
        for line in src:
            line = line.strip()
            if not line:
                continue
            dest.write(line + "\n")


def main():
    """
    Orchestrate the clean + LLM pipeline.

    Steps:
        1. Load scraped rows.
        2. Clean and add the "program" field.
        3. Reorder fields and overwrite the scraped JSON file.
        4. Deduplicate by URL against the LLM master file.
        5. Run the LLM standardizer for new rows only.
        6. Append new JSONL rows to the master file.
    """

    # Load the original JSON file
    raw_rows = load_data(SCRAPED_FILE)
    # Clean and add new program field
    cleaned_rows = clean_data(raw_rows)
    # Match format of example output
    reordered_rows = [reorder_data(row) for row in cleaned_rows]
    # Save as a new JSON file
    save_data(reordered_rows, SCRAPED_FILE)

    existing_urls = {
        row.get("url")
        for row in load_rows(MASTER_FILE)
        if row.get("url")
    }
    new_rows = [
        row
        for row in reordered_rows
        if row.get("url") and row.get("url") not in existing_urls
    ]
    if not new_rows:
        print("No new rows to add to LLM output.")
        return

    save_json_list(new_rows, NEW_INPUT_FILE)
    run_llm_standardizer(NEW_INPUT_FILE, NEW_LLM_FILE)
    append_jsonl(NEW_LLM_FILE, MASTER_FILE)


# Run only if executed directly
if __name__ == "__main__":
    main()
