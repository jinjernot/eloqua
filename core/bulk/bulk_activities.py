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


def smart_chunk_contacts(contact_ids, max_chars=1000):
    chunks, current_chunk, current_len = [], [], 0
    for cid in contact_ids:
        clause = f"'{{Activity.Contact.Id}}' = '{cid}'"
        added_len = len(clause) + (4 if current_chunk else 0)
        if current_len + added_len > max_chars:
            chunks.append(current_chunk)
            current_chunk = [cid]
            current_len = len(clause)
        else:
            current_chunk.append(cid)
            current_len += added_len
    if current_chunk:
        chunks.append(current_chunk)
    return chunks


def build_activity_filter(contact_ids, activity_type="EmailSend") -> str:
    id_clauses = [f"'{{{{Activity.Contact.Id}}}}' = '{cid}'" for cid in contact_ids]
    id_filter = " OR ".join(id_clauses)
    return f"'{{{{Activity.Type}}}}' = '{activity_type}' AND ({id_filter})"


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

    def safe_fetch(contact_id_batch, batch_index):
        try:
            logging.info(f"Fetching batch {batch_index} for contact IDs {contact_id_batch}...")
            result = fetch_activities_bulk(contact_id_batch, activity_type, batch_index)
            logging.info(f"Finished batch {batch_index} with {len(result)} records.")
            return result
        except Exception as e:
            logging.error("Batch %s failed: %s", batch_index, e)
            return []

    all_activities = []
    futures = []
    lock = Lock()

    contact_batches = smart_chunk_contacts(contact_ids)
    total_batches = len(contact_batches)
    completed_batches = 0

    logging.info("Starting bulk activity fetch for %d contacts (%d batches) using %d threads...",
                 len(contact_ids), total_batches, max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, contact_batch in enumerate(contact_batches, start=1):
            futures.append(executor.submit(safe_fetch, contact_batch, idx))

        for future in as_completed(futures):
            batch_result = future.result()
            with lock:
                all_activities.extend(batch_result)
                completed_batches += 1
                logging.info("Progress: %d/%d batches complete.", completed_batches, total_batches)

    logging.info("All batches complete. Total activity records: %d", len(all_activities))
    return all_activities
