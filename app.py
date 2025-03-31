import requests
import csv
import json
from flask import Flask, request, jsonify, send_file
from auth import get_valid_access_token, get_access_token
from config import EMAIL_SEND_ENDPOINT, EMAIL_ASSET_ENDPOINT, CONTACTS_ENDPOINT, AUTH_URL,EMAIL_ACTIVITY_ENDPOINT

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

def fetch_data(endpoint, filename):
    """Helper function to fetch data from Eloqua API and save as JSON."""
    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        data = response.json()
        # Save raw response to JSON
        with open(filename, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        return data
    else:
        return {"error": "Failed to fetch data", "details": response.text}

def fetch_contacts():
    """Fetch contacts from Eloqua and save the response as a JSON file."""
    raw_contacts = fetch_data(CONTACTS_ENDPOINT, "contacts_response.json")
    
    contacts_map = {}
    for contact in raw_contacts.get("elements", []):  
        contact_id = contact.get("id", "")
        contacts_map[contact_id] = {
            "Email Address": contact.get("emailAddress", ""),
            "Contact Country": contact.get("country", ""),
            "HP Role": contact.get("hpRole", ""),
            "HP Partner Id": contact.get("hpPartnerId", ""),
            "Partner Name": contact.get("partnerName", ""),
            "Market": contact.get("market", ""),
        }

    return contacts_map

@app.route("/monthly-report", methods=["GET"])
def generate_monthly_report():
    """Generate a monthly report combining all email-related data."""
    
    # Fetch and save API data
    email_sends = fetch_data(EMAIL_SEND_ENDPOINT, "email_sends.json")
    email_assets = fetch_data(EMAIL_ASSET_ENDPOINT, "email_assets.json")
    email_activities = fetch_data(EMAIL_ACTIVITY_ENDPOINT, "email_activities.json")
    contacts_data = fetch_contacts()  # Fetch contacts and save separately

    report_data = []
    
    for send in email_sends.get("value", []):
        email_id = send.get("emailID")
        contact_id = send.get("contactID")  # Assuming the contact ID is provided in the send data

        email_asset = next((ea for ea in email_assets.get("value", []) if ea.get("emailID") == email_id), {})
        email_activity = next((ea for ea in email_activities.get("value", []) if ea.get("emailId") == email_id), {})

        contact_info = contacts_data.get(contact_id, {
            "Email Address": "",
            "Contact Country": "",
            "HP Role": "",
            "HP Partner Id": "",
            "Partner Name": "",
            "Market": "",
        })

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
            **contact_info  # Merge contact details
        })

    # Save report as CSV
    filename = save_data_as_csv(report_data, "monthly_report.csv")
    return send_file(filename, as_attachment=True)

def save_data_as_csv(data, filename):
    """Save data to CSV file."""
    keys = data[0].keys() if data else []
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    return filename

if __name__ == "__main__":
    app.run(port=5000, debug=True)
