import json
import csv
import os

def save_json(data, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)  # Ensure directory exists
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

def save_csv(data, filename):
    keys = data[0].keys() if data else ["No Data"]

    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=keys) #, delimiter="\t"
        writer.writeheader()
        if data:
            writer.writerows(data)

    return filename