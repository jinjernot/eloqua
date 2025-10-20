import requests
import logging
import time
import os
import json
from auth import get_valid_access_token
from core.utils import save_json
from config import *

# Toggle debug mode
DEBUG_MODE = True

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


def fetch_bouncebacks_bulk(start_date, end_date):
    try:
        access_token = get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Construct filter for bounceback activities
        date_filter = f"'{{{{Activity.Type}}}}' = 'Bounceback' AND '{{{{Activity.CreatedAt}}}}' >= '{start_date}' AND '{{{{Activity.CreatedAt}}}}' < '{end_date}'"

        export_payload = {
            "name": f"Bulk_Bounceback_{start_date[:10]}",
            "fields": {
                "ActivityId": "{{Activity.Id}}",
                "ActivityType": "{{Activity.Type}}",
                "ActivityDate": "{{Activity.CreatedAt}}",
                "EmailAddress": "{{Activity.Field(EmailAddress)}}",
                "ContactId": "{{Activity.Contact.Id}}",
                "AssetType": "{{Activity.Asset.Type}}",
                "AssetName": "{{Activity.Asset.Name}}",
                "AssetId": "{{Activity.Asset.Id}}",
                "CampaignId": "{{Activity.Campaign.Id}}",
                "ExternalId": "{{Activity.ExternalId}}",
                "EmailRecipientId": "{{Activity.Field(EmailRecipientId)}}",
                "DeploymentId": "{{Activity.Field(EmailDeploymentId)}}",
                "SmtpErrorCode": "{{Activity.Field(SmtpErrorCode)}}",
                "SmtpStatusCode": "{{Activity.Field(SmtpStatusCode)}}",
                "SmtpMessage": "{{Activity.Field(SmtpMessage)}}"
            },
            "filter": date_filter 
        }

        save_debug_payload(export_payload, f"bounceback_export_payload_{start_date[:10]}.json")

        # Step 1: Create export
        export_resp = requests.post(BULK_ACTIVITY_EXPORT_URL, headers=headers, json=export_payload)
        export_resp.raise_for_status()
        export_uri = export_resp.json().get("uri")
        logging.info("Created Bounceback export: %s", export_uri)

        # Step 2: Start sync
        sync_resp = requests.post(BULK_SYNC_URL, headers=headers, json={"syncedInstanceUri": export_uri})
        sync_resp.raise_for_status()
        sync_uri = sync_resp.json().get("uri")
        logging.info("Started sync: %s", sync_uri)

        # Step 3: Poll sync
        for attempt in range(SYNC_MAX_ATTEMPTS):
            time.sleep(SYNC_WAIT_SECONDS)
            poll_url = f"{BULK_SYNC_URL}/{sync_uri.split('/')[-1]}"
            poll_resp = requests.get(poll_url, headers=headers)
            poll_resp.raise_for_status()
            if poll_resp.json().get("status") == "success":
                logging.info("Sync completed.")
                break
        else:
            logging.error("Sync did not complete in expected time.")
            return []

        # Step 4: Download all data with pagination
        sync_id = sync_uri.split("/")[-1]
        base_data_url = f"{BASE_URL}/api/bulk/2.0/syncs/{sync_id}/data"
        download_headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        all_items = []
        offset = 0
        limit = 1000

        while True:
            paged_url = f"{base_data_url}?offset={offset}&limit={limit}"
            data_resp = requests.get(paged_url, headers=download_headers)
            if data_resp.status_code != 200:
                logging.error(f"Failed to fetch data at offset {offset}: {data_resp.text}")
                break

            try:
                data = data_resp.json()
                items = data.get("items", [])
                all_items.extend(items)

                if len(items) < limit:
                    break

                offset += limit

            except json.JSONDecodeError as json_err:
                logging.error("JSON parse error at offset %d: %s", offset, json_err)
                break

        if DEBUG_MODE:
            os.makedirs("debug_bouncebacks", exist_ok=True)
            filename = f"debug_bouncebacks/bouncebacks_{start_date[:10]}.json"
            save_json({"items": all_items}, filename)

        return all_items

    except Exception as e:
        logging.exception("Failed to fetch bounceback activities: %s", e)
        return []