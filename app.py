from flask import Flask, request, jsonify, send_file
from auth import get_access_token
from core.fetch_data import fetch_account_activity
from config import *
from core.process_data import generate_monthly_report

app = Flask(__name__)

@app.route("/")
def home():
    return f'<a href="{AUTH_URL}">Authorize Eloqua</a>'

@app.route("/callback")
def callback():
    """Handles OAuth callback and retrieves access token."""
    code = request.args.get("code")
    if not code:
        return jsonify({"error": "Authorization failed."})

    token_info, error = get_access_token(code)
    if error:
        return jsonify({"error": "Failed to retrieve token", "details": error})

    return jsonify(token_info)



@app.route("/test", methods=["GET"])
def save_account_activity():
    """Fetch and save Account Activity data from Eloqua API."""
    filename = fetch_account_activity()
    return jsonify({"message": "Data saved", "filename": "test.json"})



@app.route("/monthly-report", methods=["GET"])
def get_monthly_report():
    """Fetch, process, and generate a monthly report as a downloadable CSV file."""
    filename = generate_monthly_report()
    return send_file(filename, as_attachment=True)

if __name__ == "__main__":
    app.run(port=5000, debug=True)
