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

@app.route("/test-auth")
def test_auth():
    token = get_valid_access_token()
    if token:
        return f"Valid token acquired: {token[:10]}..."
    else:
        return "Token missing or refresh failed. Please re-authenticate.", 401

if __name__ == "__main__":
    app.run(port=5000, debug=True)
