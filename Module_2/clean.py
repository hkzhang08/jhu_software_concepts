
import json

scraped_file = "Module_2/applicant_data.json"
program_file = "Module_2/applicant_data2.json"

def load_data(file):
    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)

def clean_data(rows):
    cleaned = []

    for row in rows:
        program_name = (row.get("program_name") or "").strip()
        university = (row.get("university") or "").strip()

        program = f"{program_name}, {university}".strip(", ").strip()

        new_row = dict(row)
        new_row["program"] = program
        cleaned.append(new_row)

    return cleaned

def save_data(rows, file):
    with open(file, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


def main():
    raw_rows = load_data(scraped_file)
    cleaned_rows = clean_data(raw_rows)
    save_data(cleaned_rows, program_file)

    print(f"Cleaned data saved to: {program_file}")
    print("Example program field:", cleaned_rows[0]["program"])


if __name__ == "__main__":
    main()


# D.	Clean the data using an LLM
# E.	Structure the data as a clean, formatted JSON data object.
#
# clean_data(): converts data into a structured format
# save_data(): saves cleaned data into a JSON file
# load_data(): loads cleaned data from a JSON file
