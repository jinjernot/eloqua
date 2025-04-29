from flask import Flask, request, jsonify, send_file
from auth import get_access_token
from core.fetch_data import fetch_account_activity
from config import *
from core.process_data import generate_daily_report
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

@app.route("/test", methods=["GET"])
def save_account_activity():

    filename = fetch_account_activity()
    return jsonify({"message": "Data saved", "filename": "test.json"})


@app.route("/daily", methods=["GET"])
def generate_batch():
    start_date = datetime.utcnow() - timedelta(days=4)
    generated_files = []

    for i in range(5):
        date = start_date + timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        output_file = f"data/{date_str}.csv"

        if os.path.exists(output_file):
            print(f"[INFO] Skipping {date_str}, report already exists.")
            continue

        print(f"[INFO] Generating report for {date_str}")
        path = generate_daily_report(date_str)
        generated_files.append(path)

    return jsonify({
        "message": "Batch report generation complete",
        "files": generated_files
    })



if __name__ == "__main__":
    app.run(port=5000, debug=True)
