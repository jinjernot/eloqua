import requests
import csv
from flask import Flask, request, jsonify, send_file
from auth import get_valid_access_token, get_access_token  # Import from auth.py
from config import EMAIL_SEND_ENDPOINT, EMAIL_ASSET_ENDPOINT, EMAIL_ACTIVITY_ENDPOINT, AUTH_URL

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

def fetch_data(endpoint):
    """Fetch data from Eloqua API given an endpoint."""
    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        return response.json()  # Return raw data
    else:
        return {"error": f"Failed to fetch data from {endpoint}", "details": response.text}

def process_email_sends(data):
    """Extract relevant fields from Email Sends response."""
    if "error" in data:
        return data

    return [
        {
            "Email Send ID": record.get("emailSendID"),
            "Campaign ID": record.get("eloquaCampaignID"),
            "Account ID": record.get("accountID"),
            "Contact ID": record.get("contactID"),
            "Email ID": record.get("emailID"),
            "Segment ID": record.get("segmentID"),
            "Sent Date Hour": record.get("sentDateHour"),
        }
        for record in data.get("value", [])
    ]

def process_email_assets(data):
    """Extract relevant fields from Email Assets response."""
    if "error" in data:
        return data

    return [
        {
            "Email ID": record.get("emailID"),
            "Email Name": record.get("emailName"),
            "Email Subject Line": record.get("subjectLine"),
            "Last Modified Date": record.get("lastModifiedDate"),
            "Created By User ID": record.get("emailCreatedByUserID"),
            "Email Group": record.get("emailGroup"),
            "Email Group ID": record.get("emailGroupID"),
            "Email Group Description": record.get("emailGroupDescription"),
            "Email Group Is Deleted": record.get("emailGroupIsDeleted"),
            "Is Archived": record.get("isArchived"),
            "Is Deleted": record.get("isDeleted"),
        }
        for record in data.get("value", [])
    ]

def process_email_activities(data):
    """Extract relevant fields from Email Activities response."""
    if "error" in data:
        return data

    return [
        {
            "openRate": float(record.get("openRate") or 0.0),
            "clickthroughRate": float(record.get("clickthroughRate") or 0.0),
            "clickToOpenRate": float(record.get("clickToOpenRate") or 0.0),
            "emailSentAggKey": int(record.get("emailSentAggKey") or 0),
            "eloquaCampaignId": int(record.get("eloquaCampaignId") or 0),
            "emailId": int(record.get("emailId") or 0),
            "segmentId": int(record.get("segmentId") or 0),
            "dateHour": str(record.get("dateHour") or ""),
            "lastModifiedDate": str(record.get("lastModifiedDate") or ""),
            "totalSends": int(record.get("totalSends") or 0),
            "totalDelivered": int(record.get("totalDelivered") or 0),
            "totalHardBouncebacks": int(record.get("totalHardBouncebacks") or 0),
            "totalSoftBouncebacks": int(record.get("totalSoftBouncebacks") or 0),
            "totalOpens": int(record.get("totalOpens") or 0),
            "totalClickthroughs": int(record.get("totalClickthroughs") or 0),
            "totalPossibleForwarders": int(record.get("totalPossibleForwarders") or 0),
            "totalUnsubscribesbyEmail": int(record.get("totalUnsubscribesbyEmail") or 0),
            "totalBouncebacks": int(record.get("totalBouncebacks") or 0),
            "totalSpamUnsubscribersByEmail": int(record.get("totalSpamUnsubscribersByEmail") or 0),
            "existingVisitorClickthroughs": int(record.get("existingVisitorClickthroughs") or 0),
            "newVisitorClickthroughs": int(record.get("newVisitorClickthroughs") or 0),
            "isOpened": int(record.get("isOpened") or 0),
            "isClickThroughed": int(record.get("isClickThroughed") or 0),
            "segment": record.get("segment") or {},
            "campaign": record.get("campaign") or {},
            "calendar": record.get("calendar") or {},
            "emailAsset": record.get("emailAsset") or {},
        }
        for record in data.get("value", [])
    ]

def save_data_as_csv(data, filename="report.csv"):
    """Save processed data to a CSV file."""
    keys = data[0].keys() if data else []
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    return filename

@app.route("/email-sends", methods=["GET"])
def get_email_sends():
    """API endpoint to fetch, process, and return Email Sends data as CSV."""
    raw_data = fetch_data(EMAIL_SEND_ENDPOINT)
    processed_data = process_email_sends(raw_data)
    filename = save_data_as_csv(processed_data, "email_sends.csv")
    return send_file(filename, as_attachment=True)

@app.route("/email-assets", methods=["GET"])
def get_email_assets():
    """API endpoint to fetch, process, and return Email Assets data as CSV."""
    raw_data = fetch_data(EMAIL_ASSET_ENDPOINT)
    processed_data = process_email_assets(raw_data)
    filename = save_data_as_csv(processed_data, "email_assets.csv")
    return send_file(filename, as_attachment=True)

@app.route("/email-activities", methods=["GET"])
def get_email_activities():
    """API endpoint to fetch, process, and return Email Activities data as CSV."""
    raw_data = fetch_data(EMAIL_ACTIVITY_ENDPOINT)
    processed_data = process_email_activities(raw_data)
    filename = save_data_as_csv(processed_data, "email_activities.csv")
    return send_file(filename, as_attachment=True)

if __name__ == "__main__":
    app.run(port=5000)
