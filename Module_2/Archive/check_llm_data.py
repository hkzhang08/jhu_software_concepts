
import json

# Save file names for easy reference
SCRAPED_FILE = "Module_2/llm_extend_applicant_data.json"
CLEANED_FILE = "Module_2/llm_extend_applicant_data2.json"

# Options for missing data
UNAVAILABLE = {"", "n/a", "na", "none", "null", "nan"}


def load_data(file):
    """
    Purpose: Load JSON Lines data (one JSON object per line) and return as a list
    """
    rows = []
    with open(file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return rows



def is_missing(value):
    """
    Purpose: flag program name or university if missing
    """
    if value is None:
        return True

    if isinstance(value, str):
        return value.strip().lower() in UNAVAILABLE

    return False


def clean_data(rows):
    """
    Purpose: remove records where program_name or university is missing
    """

    cleaned = []
    removed = 0

    for row in rows:
        program_name = row.get("program_name")
        university = row.get("university")

        if is_missing(program_name) or is_missing(university):
            removed += 1
            continue

        cleaned.append(row)

    print("Records removed due to missing program_name or university:", removed)
    return cleaned


def save_data(rows, file):
    """
    Purpose: Save cleaned data as a new JSON file
    """
    with open(file, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


def main():
    """
    Purpose: main function to remove invalid records
    """
    raw_rows = load_data(SCRAPED_FILE)
    cleaned_rows = clean_data(raw_rows)
    save_data(cleaned_rows, CLEANED_FILE)

    print(f"Cleaned data saved to: {CLEANED_FILE}")
    if cleaned_rows:
        print("Example valid record:", cleaned_rows[0])


if __name__ == "__main__":
    main()

# import json
# import pandas as pd
#
# FILE = "Module_2/llm_extend_applicant_data.json"
#
# records = []
# with open(FILE, "r", encoding="utf-8") as f:
#     for line in f:
#         if line.strip():
#             records.append(json.loads(line))
#
# df = pd.DataFrame(records)
#
# # Normalize program_name
# df["gpa_clean"] = (
#     df["gpa"]
#     .fillna("")          # None → ""
#     .astype(str)
#     .str.strip()
# )
#
# # Define unavailable values
# UNAVAILABLE = {"", "N/A", "NA", "None", "null", "unknown", "Unavailable"}
#
# # Count unavailable entries
# unavailable_count = df["gpa_clean"].isin(UNAVAILABLE).sum()
#
# # Frequency table INCLUDING unavailable
# freq = df["gpa_clean"].value_counts(dropna=False)
#
# unavailable_values = (
#     df.loc[df["gpa_clean"].isin(UNAVAILABLE), "gpa"]
#     .value_counts(dropna=False)
# )
#
# print("Distinct unavailable university values:")
# print(unavailable_values)
# print("Unavailable university:", unavailable_count)

#
# unavailable_df = df[df["program_name_clean"].isin(UNAVAILABLE)]
#
# print("\nReviewing unavailable records:\n")
#
# for i, row in unavailable_df.iterrows():
#     print("=" * 80)
#     # drop helper column so output is clean
#     record_dict = row.drop(labels=["program_name_clean"]).to_dict()
#     print(json.dumps(record_dict, indent=2, ensure_ascii=False))
#



# missing_program = 0
# missing_university = 0
# total = 0
#
# with open(FILE, "r", encoding="utf-8") as f:
#     for line_num, line in enumerate(f, start=1):
#         if not line.strip():
#             continue
#
#         total += 1
#         record = json.loads(line)
#
#         if not record.get("llm-generated-program"):
#             missing_program += 1
#             print(f"Missing llm-generated-program at line {line_num}")
#
#         if not record.get("llm-generated-university"):
#             missing_university += 1
#             print(f"Missing llm-generated-university at line {line_num}")
#
# print("\nSummary")
# print("------")
# print("Total records:", total)
# print("Missing llm-generated-program:", missing_program)
# print("Missing llm-generated-university:", missing_university)
