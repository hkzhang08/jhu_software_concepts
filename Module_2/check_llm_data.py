import json

FILE = "Module_2/llm_extend_applicant_data.json"

missing_program = 0
missing_university = 0
total = 0

with open(FILE, "r", encoding="utf-8") as f:
    for line_num, line in enumerate(f, start=1):
        if not line.strip():
            continue

        total += 1
        record = json.loads(line)

        if not record.get("llm-generated-program"):
            missing_program += 1
            print(f"Missing llm-generated-program at line {line_num}")

        if not record.get("llm-generated-university"):
            missing_university += 1
            print(f"Missing llm-generated-university at line {line_num}")

print("\nSummary")
print("------")
print("Total records:", total)
print("Missing llm-generated-program:", missing_program)
print("Missing llm-generated-university:", missing_university)
