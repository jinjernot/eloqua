from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
import logging
import os
import json
from auth import get_valid_access_token
from core.utils import save_json
from config import *

# Toggle debug mode
DEBUG_MODE = True

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def chunk_list(full_list, chunk_size):
    for i in range(0, len(full_list), chunk_size):
        yield full_list[i:i + chunk_size]


def build_activity_filter(contact_ids, activity_type="EmailSend") -> str:
    if isinstance(contact_ids, list) and len(contact_ids) == 1:
        contact_id = contact_ids[0]
    elif isinstance(contact_ids, list):
        raise ValueError("Multiple contact IDs not supported in filter. Send one at a time.")
    else:
        contact_id = contact_ids
    return f"'{{{{Activity.Type}}}}' = '{activity_type}' AND '{{{{Activity.Contact.Id}}}}' = '{contact_id}'"

def save_payload_debug(payload, batch_index=None, debug_dir="debug_payloads"):
    if not DEBUG_MODE:
        return
    os.makedirs(debug_dir, exist_ok=True)
    filename = f"{debug_dir}/activity_payload_batch_{batch_index or 'unknown'}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
        logging.info(f"Saved debug payload to: {filename}")
    except Exception as e:
        logging.error("Failed to save debug payload: %s", e)


def fetch_activities_bulk(contact_ids, activity_type, batch_index=None):
    try:
        if not contact_ids:
            logging.warning("No contact IDs provided.")
            return []

        # Convert all contact IDs to strings
        contact_ids = [str(cid) for cid in contact_ids]
        filter_query = build_activity_filter(contact_ids, activity_type)
        export_name = f"Bulk_Activity_Export_Batch_{batch_index or 'N'}"
        access_token = get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        export_payload = {
            "name": export_name,
            "fields": ACTIVITY_FIELDS,
            "filter": filter_query,
        }

        save_payload_debug(export_payload, batch_index)
        logging.info("Creating activity export with filter: %s", filter_query)

        export_resp = requests.post(BULK_ACTIVITY_EXPORT_URL, headers=headers, json=export_payload)
        export_resp.raise_for_status()
        export_uri = export_resp.json().get("uri")

        sync_resp = requests.post(BULK_SYNC_URL, headers=headers, json={"syncedInstanceUri": export_uri})
        sync_resp.raise_for_status()
        sync_uri = sync_resp.json().get("uri")

        for attempt in range(SYNC_MAX_ATTEMPTS):
            time.sleep(SYNC_WAIT_SECONDS)
            poll_url = f"{BULK_SYNC_URL}/{sync_uri.split('/')[-1]}"
            poll_resp = requests.get(poll_url, headers=headers)
            poll_resp.raise_for_status()
            if poll_resp.json().get("status") == "success":
                logging.info("Sync completed successfully.")
                break

        sync_id = sync_uri.split("/")[-1]
        data_url = f"{BASE_URL}/api/bulk/2.0/syncs/{sync_id}/data"
        logging.info("Downloading from: %s", data_url)

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
                    output_dir = "debug_activity_data"
                    os.makedirs(output_dir, exist_ok=True)
                    filename = os.path.join(output_dir, f"bulk_activity_data_batch_{batch_index}.json")
                    save_json(data, filename)
                return data.get("items", [])

            except json.JSONDecodeError as json_err:
                logging.error("Attempt %d: JSON decode error: %s", attempt + 1, json_err)
                if DEBUG_MODE:
                    html_debug_file = f"debug_payloads/html_activity_response_batch_{batch_index}.html"
                    with open(html_debug_file, 'w', encoding='utf-8') as f:
                        f.write(data_resp.text)
                    logging.info("Saved HTML debug to: %s", html_debug_file)
                time.sleep(2)

        logging.error("All download attempts failed for batch %s", batch_index)
        return []

    except requests.exceptions.HTTPError as e:
        logging.error("Payload causing error: %s", json.dumps(export_payload, indent=2))
        logging.error("Error response: %s", export_resp.text)
        raise


def batch_fetch_activities_bulk(contact_ids, max_workers=10, activity_type="EmailSend"):
    from threading import Lock

    def safe_fetch(contact_id, batch_index):
        try:
            logging.info(f"Fetching batch {batch_index} for contact ID {contact_id}...")
            result = fetch_activities_bulk([contact_id], activity_type, batch_index)
            logging.info(f"Finished batch {batch_index} with {len(result)} records.")
            return result
        except Exception as e:
            logging.error("Batch %s failed: %s", batch_index, e)
            return []

    all_activities = []
    futures = []
    lock = Lock()

    total_batches = len(contact_ids)
    completed_batches = 0

    logging.info("Starting bulk activity fetch for %d contacts with %d threads...", len(contact_ids), max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, contact_id in enumerate(contact_ids, start=1):
            futures.append(executor.submit(safe_fetch, contact_id, idx))

        for future in as_completed(futures):
            batch_result = future.result()
            with lock:
                all_activities.extend(batch_result)
                completed_batches += 1
                logging.info("Progress: %d/%d batches complete.", completed_batches, total_batches)

    logging.info("All batches complete. Total activity records: %d", len(all_activities))
    return all_activities