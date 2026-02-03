
import json, csv
from pathlib import Path

inp = Path("module_3/llm_extend_applicant_data.json")
out = Path("module_3/llm_extend_applicant_data.csv")

rows = []
with inp.open() as f:
    for line in f:
        if line.strip():
            rows.append(json.loads(line))

fieldnames = sorted({k for r in rows for k in r.keys()})

with out.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print("Wrote", out)
