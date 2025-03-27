import requests
import csv
from flask import Flask, request, jsonify, send_file
from auth import get_valid_access_token, get_access_token  # Import from auth.py
from config import EMAIL_SEND_ENDPOINT, AUTH_URL

app = Flask(__name__)

@app.route("/")
def home():
    
    return f'<a href="{AUTH_URL}">Authorize Eloqua</a>'

@app.route("/callback")
def callback():
    """Handles OAuth callback and retrieves access token."""
    code = request.args.get("code")
    if not code:
        return "Authorization failed."

    token_info, error = get_access_token(code)
    if error:
        return jsonify({"error": "Failed to retrieve token", "details": error})

    return jsonify(token_info)

def fetch_email_sends():
    """Fetch Email Send data from Eloqua"""

    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}


    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    response = requests.get(EMAIL_SEND_ENDPOINT, headers=headers)

    if response.status_code == 200:
        return response.json()  # Return the raw data
    else:
        return {"error": "Failed to fetch email sends", "details": response.text}

def process_email_sends(data):
    """Extract relevant fields from Email Sends response"""

    if "error" in data:
        return data  # Return error if the API call failed

    report_data = []
    for record in data.get("value", []):
        report_data.append({
            "Email Send ID": record.get("emailSendID"),
            "Campaign ID": record.get("eloquaCampaignID"),
            "Account ID": record.get("accountID"),
            "Contact ID": record.get("contactID"),
            "Email ID": record.get("emailID"),
            "Segment ID": record.get("segmentID"),
            "Sent Date Hour": record.get("sentDateHour"),
        })

    return report_data

def save_data_as_csv(data, filename="email_sends.csv"):
    """Save the processed email sends data to a CSV file"""
    keys = data[0].keys() if data else []
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    return filename

@app.route("/email-sends", methods=["GET"])
def get_email_sends():
    """API endpoint to fetch, process and return formatted Email Sends data as CSV"""
    
    raw_data = fetch_email_sends()
    processed_data = process_email_sends(raw_data)

    # Save the processed data as CSV
    filename = save_data_as_csv(processed_data)

    return send_file(filename, as_attachment=True)

if __name__ == "__main__":
    app.run(port=5000)
