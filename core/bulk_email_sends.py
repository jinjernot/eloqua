import requests
import time
import os
import json
import logging
from datetime import datetime, timedelta
from auth import get_valid_access_token
from core.utils import save_json

from config import *

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_payload_debug(payload, debug_name="email_sends", debug_dir="debug_payloads"):
    """Save the export payload to a file for debugging purposes."""
    os.makedirs(debug_dir, exist_ok=True)
    filename = f"{debug_dir}/{debug_name}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
        logging.info(f"Saved debug payload to: {filename}")
    except Exception as e:
        logging.error("Failed to save debug payload: %s", e)

def chunk_list(full_list, chunk_size):
    """Split a list into chunks of a specific size."""
    for i in range(0, len(full_list), chunk_size):
        yield full_list[i:i + chunk_size]

def build_email_send_filter(contact_ids, since):
    """Builds a valid Eloqua Bulk API filter for EmailSend activity."""
    contact_filters = [f"{{{{Activity.ContactId}}}} = '{cid}'" for cid in contact_ids]
    full_filter = f"{{{{Activity.Type}}}} = 'EmailSend' AND {{{{Activity.CreatedAt}}}} >= '{since}' AND (" + " OR ".join(contact_filters) + ")"
    return full_filter

def fetch_email_sends_bulk(contact_ids, batch_index=None):
    """Fetch EmailSend activity from Eloqua Bulk API for a batch of contact IDs."""
    try:
        if not contact_ids:
            logging.warning("No contact IDs provided. Skipping batch.")
            return []

        since = (datetime.utcnow() - timedelta(days=100)).strftime("%Y-%m-%dT%H:%M:%SZ")
        filter_query = build_email_send_filter(contact_ids, since)
        export_name = f"EmailSendExport_Batch_{batch_index or 'N'}"
        access_token = get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        export_payload = {
            "name": export_name,
            "fields": {
                "activityId": "{{Activity.Id}}",
                "emailSendDate": "{{Activity.CreatedAt}}",
                "emailId": "{{Activity.AssetId}}",
                "contactId": "{{Activity.ContactId}}"
            },
            "filter": filter_query,
            "dataRetentionDuration": "PT12H"
        }

        save_payload_debug(export_payload, debug_name=f"email_sends_batch_{batch_index}")

        # Step 1: Create export
        export_resp = requests.post(f"{BASE_URL}/api/bulk/2.0/activities/exports", headers=headers, json=export_payload)
        export_resp.raise_for_status()
        export_uri = export_resp.json()["uri"]

        # Step 2: Start sync
        sync_resp = requests.post(f"{BASE_URL}/api/bulk/2.0/syncs", headers=headers, json={"syncedInstanceUri": export_uri})
        sync_resp.raise_for_status()
        sync_uri = sync_resp.json()["uri"]
        sync_id = sync_uri.split("/")[-1]

        # Step 3: Poll sync status
        for attempt in range(SYNC_MAX_ATTEMPTS):
            time.sleep(SYNC_WAIT_SECONDS)
            poll_resp = requests.get(f"{BASE_URL}/api/bulk/2.0/syncs/{sync_id}", headers=headers)
            poll_resp.raise_for_status()
            sync_status = poll_resp.json()
            if sync_status.get("status") == "success":
                break
            elif sync_status.get("status") == "error":
                raise Exception("Bulk API sync failed for email sends")

        # Step 4: Download data
        result_uri = sync_status["result"]["uri"]
        data_url = f"{BASE_URL}{result_uri}"

        for attempt in range(3):
            data_resp = requests.get(data_url, headers=headers)
            if not data_resp.text.strip():
                logging.warning("Attempt %d: Empty response, retrying...", attempt + 1)
                time.sleep(2)
                continue

            try:
                data = data_resp.json()
                output_dir = "debug_email_data"
                os.makedirs(output_dir, exist_ok=True)
                filename = os.path.join(output_dir, f"bulk_email_sends_batch_{batch_index}.json")
                save_json(data, filename)
                return data.get("items", [])

            except json.JSONDecodeError as json_err:
                html_file = f"debug_payloads/email_html_response_batch_{batch_index}.html"
                with open(html_file, 'w', encoding='utf-8') as f:
                    f.write(data_resp.text)
                logging.error("Attempt %d: JSON decode failed: %s", attempt + 1, json_err)
                time.sleep(2)

        logging.error("All attempts failed for email sends batch %s", batch_index)
        return []
    except Exception as e:
        logging.exception("Error fetching email sends batch: %s", e)
        return []

def batch_fetch_email_sends(contact_ids, batch_size=100):
    """Fetch email sends in batches by contact ID."""
    all_sends = []
    for idx, chunk in enumerate(chunk_list(contact_ids, batch_size), start=1):
        logging.info("Fetching email sends batch %d/%d", idx, (len(contact_ids) + batch_size - 1) // batch_size)
        batch = fetch_email_sends_bulk(chunk, batch_index=idx)
        all_sends.extend(batch)
    return all_sends
