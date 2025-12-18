import requests
import logging
import time
import os
import json
from auth import get_valid_access_token
from core.utils import save_json
from config import *

DEBUG_MODE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_debug_payload(payload, filename, debug_dir="debug_payloads"):
    if not DEBUG_MODE:
        return

    os.makedirs(debug_dir, exist_ok=True)
    path = os.path.join(debug_dir, filename)
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
        logging.info(f"Saved debug payload to: {path}")
    except Exception as e:
        logging.error("Failed to save debug payload: %s", e)


def fetch_activity_export(activity_type, start_date, end_date, headers, activity_label="EmailSend"):
    """
    Helper function to fetch a single activity type export.
    Returns list of items for the given activity type.
    """
    date_filter = f"'{{{{Activity.Type}}}}' = '{activity_type}' AND '{{{{Activity.CreatedAt}}}}' >= '{start_date}' AND '{{{{Activity.CreatedAt}}}}' < '{end_date}'"

    COMBINED_EMAIL_SEND_FIELDS = {
        "activityDate": "{{Activity.CreatedAt}}",
        "assetId": "{{Activity.Asset.Id}}",
        "assetName": "{{Activity.Asset.Name}}",
        "campaignId": "{{Activity.Campaign.Id}}",
        "contactId": "{{Activity.Contact.Id}}",
        "emailAddress": "{{Activity.Field(EmailAddress)}}", 
        "subjectLine": "{{Activity.Field(SubjectLine)}}",
        "emailSendType": "{{Activity.Type}}",  # Store the activity type itself
        "deploymentId": "{{Activity.Field(EmailDeploymentId)}}",  # Deployment information
        "externalId": "{{Activity.ExternalId}}",  # External ID might indicate forwards
        "contact_country": "{{Activity.Contact.Field(C_Country)}}",
        "contact_hp_role": "{{Activity.Contact.Field(C_HP_Role1)}}",
        "contact_hp_partner_id": "{{Activity.Contact.Field(C_HP_PartnerID1)}}",
        "contact_partner_name": "{{Activity.Contact.Field(C_Partner_Name1)}}",
        "contact_market": "{{Activity.Contact.Field(C_Market1)}}"
    }

    export_payload = {
        "name": f"Bulk_{activity_type}_with_Contacts_{start_date[:10]}",
        "fields": COMBINED_EMAIL_SEND_FIELDS,
        "filter": date_filter
    }

    save_debug_payload(export_payload, f"{activity_type.lower()}_export_payload_{start_date[:10]}.json")

    # Step 1: Create export
    logging.info(f"Creating {activity_type} export definition...")
    export_resp = requests.post(BULK_ACTIVITY_EXPORT_URL, headers=headers, json=export_payload, timeout=30)
    export_resp.raise_for_status()
    export_uri = export_resp.json().get("uri")
    logging.info(f"✓ Created {activity_type} export: {export_uri}")

    # Step 2: Start sync
    logging.info(f"Starting sync for {activity_type}...")
    sync_resp = requests.post(BULK_SYNC_URL, headers=headers, json={"syncedInstanceUri": export_uri}, timeout=30)
    sync_resp.raise_for_status()
    sync_uri = sync_resp.json().get("uri")
    logging.info(f"✓ Sync started for {activity_type}: {sync_uri}")

    # Step 3: Poll sync
    logging.info(f"Polling sync status for {activity_type} (max {SYNC_MAX_ATTEMPTS * SYNC_WAIT_SECONDS}s)...")
    for attempt in range(SYNC_MAX_ATTEMPTS):
        time.sleep(SYNC_WAIT_SECONDS)
        poll_url = f"{BULK_SYNC_URL}/{sync_uri.split('/')[-1]}"
        poll_resp = requests.get(poll_url, headers=headers, timeout=30)
        poll_resp.raise_for_status()
        sync_status = poll_resp.json().get("status")
        logging.info(f"  [{attempt+1}/{SYNC_MAX_ATTEMPTS}] Sync status: {sync_status}")
        if sync_status == "success":
            logging.info(f"✓ Sync completed for {activity_type}")
            break
        elif sync_status == "error":
            logging.error(f"✗ Sync failed for {activity_type}")
            return []
    else:
        logging.error(f"✗ Sync timeout after {SYNC_MAX_ATTEMPTS * SYNC_WAIT_SECONDS}s for {activity_type}")
        return []

    # Step 4: Download all data with pagination
    sync_id = sync_uri.split("/")[-1]
    base_data_url = f"{BASE_URL}/api/bulk/2.0/syncs/{sync_id}/data"
    # Extract token from authorization header
    token = headers['Authorization'].split(' ')[1]
    download_headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    all_items = []
    offset = 0
    limit = 5000
    
    logging.info(f"Downloading {activity_type} data...")

    while True:
        paged_url = f"{base_data_url}?offset={offset}&limit={limit}"
        data_resp = requests.get(paged_url, headers=download_headers, timeout=60)
        if data_resp.status_code != 200:
            logging.error(f"Failed to fetch data at offset {offset} for {activity_type}: {data_resp.text}")
            break

        try:
            data = data_resp.json()
            items = data.get("items", [])
            all_items.extend(items)
            logging.info(f"  Downloaded {len(all_items)} records so far...")

            if not data.get("hasMore"): 
                break

            offset += limit

        except json.JSONDecodeError as json_err:
            logging.error(f"JSON parse error at offset {offset} for {activity_type}: {json_err}")
            break
    
    logging.info(f"Fetched {len(all_items)} items for {activity_type}")
    return all_items


def fetch_email_sends_bulk(start_date, end_date):
    """
    Fetches EmailSend activities.
    Forwarded emails are identified in processing by finding opens/clicks without sends.
    """
    try:
        access_token = get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Fetch email sends
        logging.info("Fetching EmailSend activities...")
        all_items = fetch_activity_export("EmailSend", start_date, end_date, headers)
        
        logging.info(f"Fetched {len(all_items)} EmailSend activities")
        
        if DEBUG_MODE:
            os.makedirs("debug_email_sends", exist_ok=True)
            filename = f"debug_email_sends/email_sends_{start_date[:10]}.json"
            save_json({"items": all_items}, filename)

        return all_items

    except Exception as e:
        logging.exception("Failed to fetch email sends bulk: %s", e)
        return []