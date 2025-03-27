import requests
import csv
from flask import Flask, request, jsonify, send_file
from auth import get_valid_access_token, get_access_token
from config import EMAIL_SEND_ENDPOINT, AUTH_URL, EMAIL_ASSET_ENDPOINT

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
    """Helper function to fetch data from Eloqua API"""
    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        return {"error": "Failed to fetch data", "details": response.text}

def process_email_sends(raw_data):
    """Processes email send data into a structured format"""
    report_data = []
    for record in raw_data.get("value", []):
        report_data.append({
            "Email Name": record.get("emailName", ""),
            "Email ID": record.get("emailID", ""),
            "Email Subject Line": record.get("subjectLine", ""),
            "Last Activated by User": record.get("lastModifiedByUser", ""),
            "Total Delivered": record.get("totalDelivered", 0),
            "Total Hard Bouncebacks": record.get("totalHardBouncebacks", 0),
            "Total Sends": record.get("totalSends", 0),
            "Total Soft Bouncebacks": record.get("totalSoftBouncebacks", 0),
            "Total Bouncebacks": record.get("totalBouncebacks", 0),
            "Unique Opens": record.get("totalUniqueOpens", 0),
            "Hard Bounceback Rate": record.get("hardBouncebackRate", 0.0),
            "Soft Bounceback Rate": record.get("softBouncebackRate", 0.0),
            "Bounceback Rate": record.get("bouncebackRate", 0.0),
            "Clickthrough Rate": record.get("clickthroughRate", 0.0),
            "Unique Clickthrough Rate": record.get("uniqueClickthroughRate", 0.0),
            "Delivered Rate": record.get("deliveredRate", 0.0),
            "Unique Open Rate": record.get("uniqueOpenRate", 0.0),
            "Email Group": record.get("emailGroup", ""),
            "Email Send Date": record.get("emailSendDate", ""),
            "Email Address": record.get("emailAddress", ""),
            "Contact Country": record.get("contactCountry", ""),
            "HP Role": record.get("hpRole", ""),
            "HP Partner Id": record.get("hpPartnerId", ""),
            "Partner Name": record.get("partnerName", ""),
            "Market": record.get("market", ""),
        })
    return report_data

@app.route("/email-sends", methods=["GET"])
def get_email_sends():
    """Fetch Email Sends data and return as CSV"""
    raw_data = fetch_data(EMAIL_SEND_ENDPOINT)
    processed_data = process_email_sends(raw_data)
    filename = save_data_as_csv(processed_data, "email_sends.csv")
    return send_file(filename, as_attachment=True)

@app.route("/monthly-report", methods=["GET"])
def generate_monthly_report():
    """Generate a monthly report combining all email-related data"""
    
    # Fetch data from all endpoints
    email_sends = fetch_data(EMAIL_SEND_ENDPOINT)
    email_assets = fetch_data(EMAIL_ASSET_ENDPOINT)
    email_activities = fetch_data("https://secure.p06.eloqua.com/API/OData/CampaignAnalysis/1/EmailActivities")

    report_data = []
    
    # Process data and merge into the final report format
    for send in email_sends.get("value", []):
        email_id = send.get("emailID")
        email_asset = next((ea for ea in email_assets.get("value", []) if ea.get("emailID") == email_id), {})
        email_activity = next((ea for ea in email_activities.get("value", []) if ea.get("emailId") == email_id), {})

        report_data.append({
            "Email Name": email_asset.get("emailName", ""),
            "Email ID": email_id,
            "Email Subject Line": email_asset.get("subjectLine", ""),
            "Last Activated by User": email_asset.get("emailCreatedByUserID", ""),
            "Total Delivered": email_activity.get("totalDelivered", 0),
            "Total Hard Bouncebacks": email_activity.get("totalHardBouncebacks", 0),
            "Total Sends": email_activity.get("totalSends", 0),
            "Total Soft Bouncebacks": email_activity.get("totalSoftBouncebacks", 0),
            "Total Bouncebacks": email_activity.get("totalBouncebacks", 0),
            "Unique Opens": email_activity.get("totalOpens", 0),
            "Hard Bounceback Rate": email_activity.get("openRate", 0.0),
            "Soft Bounceback Rate": email_activity.get("clickthroughRate", 0.0),
            "Bounceback Rate": email_activity.get("clickToOpenRate", 0.0),
            "Clickthrough Rate": email_activity.get("clickthroughRate", 0.0),
            "Unique Clickthrough Rate": email_activity.get("clickToOpenRate", 0.0),
            "Delivered Rate": email_activity.get("totalDelivered", 0),
            "Unique Open Rate": email_activity.get("totalOpens", 0),
            "Email Group": email_asset.get("emailGroup", ""),
            "Email Send Date": send.get("sentDateHour", ""),
            "Email Address": send.get("emailAddress", ""),
            "Contact Country": send.get("contactCountry", ""),
            "HP Role": send.get("hpRole", ""),
            "HP Partner Id": send.get("hpPartnerId", ""),
            "Partner Name": send.get("partnerName", ""),
            "Market": send.get("market", ""),
        })

    # Save report as CSV
    filename = save_data_as_csv(report_data, "monthly_report.csv")
    return send_file(filename, as_attachment=True)

def save_data_as_csv(data, filename):
    """Save data to CSV file"""
    keys = data[0].keys() if data else []
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    return filename

if __name__ == "__main__":
    app.run(port=5000, debug=True)
