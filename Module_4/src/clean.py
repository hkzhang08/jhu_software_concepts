"""
Class:         EP 605 2256 81 - Spring 2026
Assignment:    Module_2 - Web Scraping
Due Date:      February 1st by 11:59AM
Name:          Helen Zhang
Email:         hzhan308@jh.edu
"""

import json
import os
import subprocess
import sys

# Save file name for easy reference
SCRAPED_FILE = "applicant_data.json"
MASTER_FILE = "llm_extend_applicant_data.json"
NEW_INPUT_FILE = "new_applicant_data.json"
NEW_LLM_FILE = "llm_new_applicant.json"
LLM_DIR = "llm_hosting"
LLM_APP = "app.py"


# Options for missing data
UNAVAILABLE = {"", "n/a", "na", "none", "null", "nan"}


def load_data(file):
    """
    Purpose: Load JSON data from a file and return as object
    """
    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)


def load_rows(file_path):
    """
    Purpose: load rows from JSON array or JSONL file
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
    Purpose: flag program name or university if missing
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
    Purpose: create a "program" field for each record from program name and university
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
    Purpose: Save cleaned data as a new JSON file
    """
    with open(file, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


def save_json_list(rows, file):
    """
    Purpose: save rows as a JSON array (for LLM input)
    """
    with open(file, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)

def reorder_data(row):
    """
    Purpose: remove program_name and university and move program to the top to
    match assignment example
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
    Purpose: run the llm_hosting app to create LLM output JSONL
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
    Purpose: append JSONL rows to a master file
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
    Purpose: main function to add program as a new field in data
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
