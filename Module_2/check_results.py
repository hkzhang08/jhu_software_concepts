import json

with open("gradcafe_results.jsonl") as f:
    records = [json.loads(line) for line in f]

print("Total records:", len(records))
print("First record:")
print(records[0])