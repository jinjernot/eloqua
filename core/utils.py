import json
import csv
import os
from config import SAVE_JSON_FILES 

def save_json(data, filename):
    if not SAVE_JSON_FILES:
        return 

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

def sanitize_field(value):
    if isinstance(value, str):
        return value.replace('\n', ' ').replace('\r', ' ').strip()
    if isinstance(value, float):
        return int(value)  # remove decimal by converting to int
    return value

def save_csv(data, filename):
    if not data:
        keys = ["No Data"]
    else:
        keys = data[0].keys()

    # Sanitize all rows
    sanitized_data = [
        {k: sanitize_field(v) for k, v in row.items()}
        for row in data
    ]

    with open(filename, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=keys, delimiter="\t")
        writer.writeheader()
        writer.writerows(sanitized_data)

    return filename