# app_bulk.py

from flask import Flask, request, jsonify, g
from auth import get_access_token
from config import *
from core.bulk.process_data_bulk import generate_daily_report
from datetime import datetime, timedelta
import os
import logging
import uuid
from core.aws.s3_utils import upload_to_s3, ping_s3_bucket

app = Flask(__name__)

# Configure logging
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(request_id)s - %(message)s')
handler.setFormatter(formatter)
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)

class RequestIdFilter(logging.Filter):
    def filter(self, record):
        record.request_id = g.get('request_id', 'N/A')
        return True

app.logger.addFilter(RequestIdFilter())

@app.before_request
def before_request():
    g.request_id = str(uuid.uuid4())

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
    app.logger.info("Daily report generation request received.")

    if not SAVE_LOCALLY:
        is_s3_ok, s3_message = ping_s3_bucket(S3_BUCKET_NAME)
        if not is_s3_ok:
            app.logger.error("S3 bucket check failed: %s", s3_message)
            return jsonify({"error": "S3 bucket check failed", "details": s3_message}), 503


    date_str = request.args.get("date")

    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            app.logger.error("Invalid date format: %s", date_str)
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
        dates_to_process = [target_date]
    else:
        days_back = 1
        end_date = datetime.utcnow().date() - timedelta(days=1)
        dates_to_process = [end_date - timedelta(days=i) for i in range(days_back)]

    generated_files = []
    statuses = []

    for date in dates_to_process:
        date_str = date.strftime("%Y-%m-%d")
        output_file = f"data/{date_str}.csv"

        if os.path.exists(output_file):
            app.logger.info("Skipping %s, report already exists.", date_str)
            continue

        app.logger.info("Generating report for %s", date_str)
        path = generate_daily_report(date_str)
        if path:
            generated_files.append(path)
            

            if SAVE_LOCALLY:
                app.logger.info("File saved locally at: %s. Skipping S3 upload.", path)
                statuses.append({
                    "file": path,
                    "status": "Saved locally"
                })
            else:
                app.logger.info("Uploading %s to S3 bucket: %s", path, S3_BUCKET_NAME)
                upload_success = upload_to_s3(path, S3_BUCKET_NAME, S3_FOLDER_PATH)
                statuses.append({
                    "file": path,
                    "uploaded_to_s3": upload_success
                })

    app.logger.info("Report generation complete.")
    return jsonify({
        "message": "Report generation complete",
        "mode": "local_save" if SAVE_LOCALLY else "s3_upload",
        "files_generated": generated_files,
        "statuses": statuses
    })
        
if __name__ == "__main__":
        app.run(port=5000, debug=True)