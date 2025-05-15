import os
import requests
from auth import get_valid_access_token
from config import (
    EMAIL_ASSET_ENDPOINT,
    CAMPAING_ANALYSIS_ENDPOINT, CAMPAING_USERS_ENDPOINT
)
from core.utils import save_json
from core.bulk.bulk_contacts import batch_fetch_contacts_bulk
from core.bulk.bulk_activities import batch_fetch_activities_bulk
from core.bulk.bulk_email_send import fetch_email_sends_bulk
from datetime import datetime, timedelta

data_dir = "data"

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
        url = data.get("@odata.nextLink")
        params = None

    filepath = os.path.join(data_dir, filename)
    save_json(full_data, filepath)
    return full_data

def fetch_and_save_data(target_date=None):
    if target_date:
        start = datetime.strptime(target_date, "%Y-%m-%d")
    else:
        start = datetime.utcnow() - timedelta(days=1)

    start_str = start.strftime("%Y-%m-%dT00:00:00Z")
    end_str = (start + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

    # Fetch all data via Bulk API
    email_sends = fetch_email_sends_bulk(start_str, end_str)
    save_json(email_sends, os.path.join(data_dir, "email_sends.json"))

    email_assets = fetch_data(EMAIL_ASSET_ENDPOINT, "email_assets.json")
    campaign_analysis = fetch_data(CAMPAING_ANALYSIS_ENDPOINT, "campaign.json")
    campaign_users = fetch_data(CAMPAING_USERS_ENDPOINT, "campaign_users.json")

    # Extract contact IDs from email sends
    active_contact_ids = {
        str(send.get("contactID")) for send in email_sends if send.get("contactID")
    }

    # Fetch EmailSend activities using Bulk API
    bulk_email_activities = batch_fetch_activities_bulk(contact_ids=list(active_contact_ids), max_workers=15)

    # Fetch contact metadata using Bulk API
    contact_activities = batch_fetch_contacts_bulk(contact_ids=list(active_contact_ids), batch_size=20, max_workers=15)

    return email_sends, email_assets, bulk_email_activities, contact_activities, campaign_analysis, campaign_users
