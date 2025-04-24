import requests
from auth import get_valid_access_token
from config import (
    EMAIL_SEND_ENDPOINT, EMAIL_ASSET_ENDPOINT, EMAIL_ACTIVITY_ENDPOINT,
    CAMPAING_ANALYSIS_ENDPOINT, CAMPAING_USERS_ENDPOINT, TEST_ENDPOINT
)
from core.utils import save_json
from core.bulk_contacts import fetch_contacts_bulk, batch_fetch_contacts_bulk
from datetime import datetime, timedelta


def fetch_data(endpoint, filename, extra_params=None):
    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    params = {"depth": "complete"}
    if extra_params:
        params.update(extra_params)

    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        save_json(data, filename)
        return data
    else:
        return {"error": "Failed to fetch data", "details": response.text}


def fetch_and_save_data():
    
    one_hundred_days_ago = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    email_send_params = {
        "$orderby": "sentDateHour desc",
        "$filter": f"sentDateHour ge {one_hundred_days_ago}"
    }

    email_sends = fetch_data(EMAIL_SEND_ENDPOINT, "email_sends.json", extra_params=email_send_params)
    email_assets = fetch_data(EMAIL_ASSET_ENDPOINT, "email_assets.json")
    email_activities = fetch_data(EMAIL_ACTIVITY_ENDPOINT, "email_activities.json")
    campaign_analysis = fetch_data(CAMPAING_ANALYSIS_ENDPOINT, "campaign.json")
    campaign_users = fetch_data(CAMPAING_USERS_ENDPOINT, "campaign_users.json")

    # Extract contact IDs for filtering
    active_contact_ids = {
        str(send.get("contactID")) for send in email_sends.get("value", [])
        if send.get("contactID")
    }

    contact_activities = batch_fetch_contacts_bulk(contact_ids=list(active_contact_ids), batch_size=30)
    
    return email_sends, email_assets, email_activities, contact_activities, campaign_analysis, campaign_users


def fetch_account_activity():
    return fetch_data(TEST_ENDPOINT, "test.json")
