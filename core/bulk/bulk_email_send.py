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


def fetch_email_sends_bulk(start_date, end_date):
    """Fetches email send activities using the Eloqua Bulk API within a date range."""
    try:
        access_token = get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Construct filter
        date_filter = f"'{{{{Activity.Type}}}}' = 'EmailSend' AND '{{{{Activity.CreatedAt}}}}' >= '{start_date}' AND '{{{{Activity.CreatedAt}}}}' < '{end_date}'"

        export_payload = {
            "name": f"Bulk_EmailSend_{start_date[:10]}",
            "fields": EMAIL_SEND_FIELDS,
            "filter": date_filter
        }

        save_debug_payload(export_payload, f"email_send_export_payload_{start_date[:10]}.json")

        # Step 1: Create export
        export_resp = requests.post(BULK_ACTIVITY_EXPORT_URL, headers=headers, json=export_payload)
        export_resp.raise_for_status()
        export_uri = export_resp.json().get("uri")
        logging.info("Created EmailSend export: %s", export_uri)

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

        # Step 4: Download data
        sync_id = sync_uri.split("/")[-1]
        data_url = f"{BASE_URL}/api/bulk/2.0/syncs/{sync_id}/data"
        download_headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        for attempt in range(3):
            data_resp = requests.get(data_url, headers=download_headers)
            if not data_resp.text.strip():
                logging.warning("Attempt %d: Empty response, retrying...", attempt + 1)
                time.sleep(2)
                continue

            try:
                data = data_resp.json()

                if DEBUG_MODE:
                    os.makedirs("debug_email_sends", exist_ok=True)
                    filename = f"debug_email_sends/email_sends_{start_date[:10]}.json"
                    save_json(data, filename)

                return data.get("items", [])

            except json.JSONDecodeError as json_err:
                logging.error("Attempt %d: JSON parse error: %s", attempt + 1, json_err)
                time.sleep(2)

        logging.error("All download attempts failed for email sends.")
        return []

    except Exception as e:
        logging.exception("Failed to fetch email sends bulk: %s", e)
        return []
