from flask import Flask, request, jsonify
from auth import get_access_token
from config import *
from core.bulk.process_data_bulk import generate_daily_report
from datetime import datetime, timedelta
import os
# Import both S3 utility functions
from core.aws.s3_utils import upload_to_s3, ping_s3_bucket 

app = Flask(__name__)

# ... (home() and callback() routes remain the same) ...
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

    # 2. Ping S3 Bucket
    is_s3_ok, s3_message = ping_s3_bucket(S3_BUCKET_NAME)
    if not is_s3_ok:
        return jsonify({"error": "S3 bucket check failed", "details": s3_message}), 503
    # -------------------------

    date_str = request.args.get("date")

    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
        dates_to_process = [target_date]
    else:
        # Default: run for yesterday only
        days_back = 10
        end_date = datetime.utcnow().date() - timedelta(days=1)
        dates_to_process = [end_date - timedelta(days=i) for i in range(days_back)]

    generated_files = []
    upload_statuses = []

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
            
            print(f"[INFO] Uploading {path} to S3 bucket: {S3_BUCKET_NAME}")
            upload_success = upload_to_s3(path, S3_BUCKET_NAME, S3_FOLDER_PATH)
            upload_statuses.append({
                "file": path,
                "uploaded_to_s3": upload_success
            })

    return jsonify({
        "message": "Report generation complete",
        "files_generated": generated_files,
        "s3_upload_status": upload_statuses
    })
        
if __name__ == "__main__":
        app.run(port=5000, debug=True)