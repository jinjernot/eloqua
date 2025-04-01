import requests
from auth import get_valid_access_token
from config import EMAIL_SEND_ENDPOINT, EMAIL_ASSET_ENDPOINT, EMAIL_ACTIVITY_ENDPOINT, CONTACT_ACTIVITY_ENDPOINT, TEST_ENDPOINT
from core.utils import save_json

def fetch_data(endpoint, filename):
    """Fetch data from Eloqua API and save as JSON."""
    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        data = response.json()
        save_json(data, filename)
        return data
    else:
        return {"error": "Failed to fetch data", "details": response.text}

def fetch_and_save_data():
    """Fetch and save all required data from Eloqua."""
    email_sends = fetch_data(EMAIL_SEND_ENDPOINT, "email_sends.json")
    email_assets = fetch_data(EMAIL_ASSET_ENDPOINT, "email_assets.json")
    email_activities = fetch_data(EMAIL_ACTIVITY_ENDPOINT, "email_activities.json")
    contact_activities = fetch_data(CONTACT_ACTIVITY_ENDPOINT, "contact_activities.json") 

    return email_sends, email_assets, email_activities, contact_activities

def fetch_account_activity():
    """Fetch and save Account Activity data separately for testing."""
    return fetch_data(TEST_ENDPOINT, "test.json")
