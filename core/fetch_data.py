import os
import requests
from auth import get_valid_access_token
from config import (
    EMAIL_SEND_ENDPOINT, EMAIL_ASSET_ENDPOINT, EMAIL_ACTIVITY_ENDPOINT,
    CAMPAING_ANALYSIS_ENDPOINT, CAMPAING_USERS_ENDPOINT, TEST_ENDPOINT
)
from core.utils import save_json
from core.bulk_contacts import batch_fetch_contacts_bulk
from datetime import datetime, timedelta

DATA_DIR = "data"

def fetch_data(endpoint, filename, extra_params=None):
    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    params = {"depth": "complete"}
    if extra_params:
        params.update(extra_params)

    full_data = {"value": []}
    url = endpoint

    while url:
        response = requests.get(url, headers=headers, params=params if url == endpoint else None)

        if response.status_code != 200:
            return {"error": "Failed to fetch data", "details": response.text}

        data = response.json()
        full_data["value"].extend(data.get("value", []))
        
        url = data.get("@odata.nextLink")  # Follow pagination if exists
        params = None  # After first request, nextLink already contains params

    filepath = os.path.join(DATA_DIR, filename)
    save_json(full_data, filepath)
    return full_data


def fetch_and_save_data():
    fourteen_days_ago = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    email_send_params = {
        "$orderby": "sentDateHour desc",
        "$filter": f"sentDateHour ge {fourteen_days_ago}"
    }
    
    email_activity_params = {
        "$orderby": "dateHour desc",
        "$filter": f"dateHour ge {fourteen_days_ago}"
    }

    email_sends = fetch_data(EMAIL_SEND_ENDPOINT, "email_sends.json", extra_params=email_send_params)
    email_assets = fetch_data(EMAIL_ASSET_ENDPOINT, "email_assets.json")
    email_activities = fetch_data(EMAIL_ACTIVITY_ENDPOINT, "email_activities.json", extra_params=email_activity_params)
    campaign_analysis = fetch_data(CAMPAING_ANALYSIS_ENDPOINT, "campaign.json")
    campaign_users = fetch_data(CAMPAING_USERS_ENDPOINT, "campaign_users.json")

    active_contact_ids = {
        str(send.get("contactID")) for send in email_sends.get("value", [])
        if send.get("contactID")
    }

    contact_activities = batch_fetch_contacts_bulk(contact_ids=list(active_contact_ids), batch_size=30)

    return email_sends, email_assets, email_activities, contact_activities, campaign_analysis, campaign_users


def fetch_account_activity():
    return fetch_data(TEST_ENDPOINT, "test.json")
