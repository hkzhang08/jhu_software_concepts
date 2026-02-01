"""
Class:         EP 605 2256 81 - Spring 2026
Assignment:    Module_2 - Web Scraping
Due Date:      February 1st by 11:59AM
Name:          Helen Zhang
Email:         hzhan308@jh.edu
"""



import json

# Save file names for easy reference
SCRAPED_FILE = "Module_2/applicant_data.json"
PROGRAM_FILE = "Module_2/applicant_data2.json"


def load_data(file):
    """
    Purpose: Load JSON data from a file and return as object
    """
    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)


def clean_data(rows):
    """
    Purpose: create a "program" field for each record from program name and university
    """

    # Initialize list
    cleaned = []

    # Loop through each record
    for row in rows:

        # Pull program name and university from data
        program_name = (row.get("program_name") or "").strip()
        university = (row.get("university") or "").strip()

        # Combine into a program field
        program = f"{program_name}, {university}".strip(", ").strip()

        # Add the new field into the record
        new_row = dict(row)
        new_row["program"] = program
        cleaned.append(new_row)

    # Return the list of cleaned records
    return cleaned


def save_data(rows, file):
    """
    Purpose: Save cleaned data as a new JSON file
    """
    with open(file, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


def main():
    """
    Purpose: main function to add program as a new field in data
    """

    # Load the original JSON file
    raw_rows = load_data(SCRAPED_FILE)
    # Clean and add new program field
    cleaned_rows = clean_data(raw_rows)
    # Save as a new JSON file
    save_data(cleaned_rows, PROGRAM_FILE)

    # Print confirmation and example that it worked
    print(f"Cleaned data saved to: {PROGRAM_FILE}")
    print("Example program field:", cleaned_rows[0]["program"])


# Run only if executed directly
if __name__ == "__main__":
    main()
