import json
import csv

def save_json(data, filename):

    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

def save_csv(data, filename):

    if not data:
        return filename

    keys = data[0].keys()
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

    return filename