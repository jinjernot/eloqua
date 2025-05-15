from flask import Flask, request, jsonify
from auth import get_access_token, get_valid_access_token
from config import *
from core.bulk.process_data_bulk import generate_daily_report
from datetime import datetime, timedelta
import os

app = Flask(__name__)

@app.route("/")
def home():
    return f'<a href="{AUTH_URL}">Authorize Eloqua</a>'

@app.route("/callback")
def callback():

    code = request.args.get("code")
    if not code:
        return jsonify({"error": "Authorization failed."})

    token_info, error = get_access_token(code)
    if error:
        return jsonify({"error": "Failed to retrieve token", "details": error})

    return jsonify(token_info)

@app.route("/daily", methods=["GET"])
def generate_batch():
    # How many past days you want to generate reports for, including yesterday
    days_back = 5
    
    # Yesterday date (UTC)
    end_date = datetime.utcnow().date() - timedelta(days=1)
    
    generated_files = []

    for i in range(days_back):
        date = end_date - timedelta(days=i)  # Move backwards from yesterday
        date_str = date.strftime("%Y-%m-%d")
        output_file = f"data/{date_str}.csv"

        if os.path.exists(output_file):
            print(f"[INFO] Skipping {date_str}, report already exists.")
            continue

        print(f"[INFO] Generating report for {date_str}")
        path = generate_daily_report(date_str)
        if path:
            generated_files.append(path)

    return jsonify({
        "message": "Batch report generation complete",
        "files": generated_files
    })
    
if __name__ == "__main__":
    app.run(port=5000, debug=True)
