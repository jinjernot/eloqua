import os
import requests
from auth import get_valid_access_token
from config import (
    EMAIL_SEND_ENDPOINT, EMAIL_ASSET_ENDPOINT,
    CAMPAING_ANALYSIS_ENDPOINT, CAMPAING_USERS_ENDPOINT, TEST_ENDPOINT
)
from core.utils import save_json
from core.bulk.bulk_contacts import batch_fetch_contacts_bulk
from core.bulk.bulk_activities import batch_fetch_activities_bulk
from datetime import datetime, timedelta

from data.fetch_fields import fetch_field_definitions

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


def fetch_and_save_data(target_date=None):
    if target_date:
        start = datetime.strptime(target_date, "%Y-%m-%d")
    else:
        start = datetime.utcnow() - timedelta(days=1)

    start_str = start.strftime("%Y-%m-%dT00:00:00Z")
    end_str = (start + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

    email_send_params = {
        "$filter": f"sentDateHour ge {start_str} and sentDateHour lt {end_str}"
    }

    # Fetch OData email sends (used for gathering contact IDs)
    email_sends = fetch_data(EMAIL_SEND_ENDPOINT, "email_sends.json", extra_params=email_send_params)
    email_assets = fetch_data(EMAIL_ASSET_ENDPOINT, "email_assets.json")
    campaign_analysis = fetch_data(CAMPAING_ANALYSIS_ENDPOINT, "campaign.json")
    campaign_users = fetch_data(CAMPAING_USERS_ENDPOINT, "campaign_users.json")

    # Extract contact IDs from email sends
    active_contact_ids = {
        str(send.get("contactID")) for send in email_sends.get("value", [])
        if send.get("contactID")
    }

    # Fetch EmailSend activities using Bulk API
    bulk_email_activities = batch_fetch_activities_bulk(activity_ids=list(active_contact_ids), max_workers=15)

    # Fetch contact metadata using bulk
    contact_activities = batch_fetch_contacts_bulk(contact_ids=list(active_contact_ids), batch_size=20, max_workers=15)

    return email_sends, email_assets, bulk_email_activities, contact_activities, campaign_analysis, campaign_users