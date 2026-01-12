import json
import csv
import os

def save_json(data, filename):
    """Save JSON data to file. Currently disabled as JSON files are not needed."""
    return

def sanitize_field(value):
    if isinstance(value, str):
        return value.replace('\n', ' ').replace('\r', ' ').strip()
    if isinstance(value, float):
        return int(value)
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