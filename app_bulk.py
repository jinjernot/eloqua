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
    date_str = request.args.get("date")

    if date_str:
        try:
            # Validate and parse the input date
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

        dates_to_process = [target_date]
    else:
        # Default: run for yesterday only
        days_back = 1
        end_date = datetime.utcnow().date() - timedelta(days=1)
        dates_to_process = [end_date - timedelta(days=i) for i in range(days_back)]

    generated_files = []

    for date in dates_to_process:
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
        "message": "Report generation complete",
        "files": generated_files
    })
        
if __name__ == "__main__":
        app.run(port=5000, debug=True)