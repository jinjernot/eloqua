from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
import logging
import os
import json
from auth import get_valid_access_token
from core.utils import save_json
from config import *

# Toggle debug mode for saving payloads and responses
DEBUG_MODE = True

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def chunk_list(full_list, chunk_size):
    """Split a list into chunks of a specific size."""
    for i in range(0, len(full_list), chunk_size):
        yield full_list[i:i + chunk_size]

def build_contact_id_filter(contact_ids):
    """Builds a valid Eloqua Bulk API filter using OR comparisons for Contact.Id."""
    conditions = [f"'{{{{Contact.Id}}}}' = '{cid}'" for cid in contact_ids]
    return " OR ".join(conditions)

def save_payload_debug(payload, batch_index=None, debug_dir="debug_payloads"):
    """Save the export payload to a file for debugging purposes."""
    if not DEBUG_MODE:
        return

    os.makedirs(debug_dir, exist_ok=True)
    filename = f"{debug_dir}/payload_batch_{batch_index or 'unknown'}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
        logging.info(f"Saved debug payload to: {filename}")
    except Exception as e:
        logging.error("Failed to save debug payload: %s", e)

def fetch_contacts_bulk(contact_ids, batch_index=None):
    """Fetches contacts using the Eloqua Bulk API based on contact IDs."""
    try:
        if not contact_ids:
            logging.warning("No contact IDs provided. Skipping export.")
            return []

        filter_query = build_contact_id_filter(contact_ids)
        export_name = f"Bulk_Contact_Export_Batch_{batch_index or 'N'}"
        access_token = get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        export_payload = {
            "name": export_name,
            "fields": CONTACT_FIELDS,
            "filter": filter_query
        }

        save_payload_debug(export_payload, batch_index)
        logging.info("Initiating export with filter: %s", filter_query)

        # Step 1: Create export definition
        export_resp = requests.post(BULK_CONTACT_EXPORT_URL, headers=headers, json=export_payload)
        export_resp.raise_for_status()
        logging.info("Export created with status %s", export_resp.status_code)
        export_uri = export_resp.json().get("uri")

        # Step 2: Start sync
        sync_resp = requests.post(BULK_SYNC_URL, headers=headers, json={"syncedInstanceUri": export_uri})
        sync_resp.raise_for_status()
        sync_uri = sync_resp.json().get("uri")

        # Step 3: Poll sync status
        for attempt in range(SYNC_MAX_ATTEMPTS):
            time.sleep(SYNC_WAIT_SECONDS)
            poll_url = f"{BULK_SYNC_URL}/{sync_uri.split('/')[-1]}"
            poll_resp = requests.get(poll_url, headers=headers)
            poll_resp.raise_for_status()
            if poll_resp.json().get("status") == "success":
                logging.info("Sync completed successfully.")
                break

        # Step 4: Download data
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
                    output_dir = "debug_contact_data"
                    os.makedirs(output_dir, exist_ok=True)
                    filename = os.path.join(output_dir, f"bulk_contact_data_batch_{batch_index}.json")
                    save_json(data, filename)

                return data.get("items", [])

            except json.JSONDecodeError as json_err:
                logging.error("Attempt %d: JSON parse error: %s", attempt + 1, json_err)

                if DEBUG_MODE:
                    html_debug_file = f"debug_payloads/html_response_batch_{batch_index}.html"
                    with open(html_debug_file, 'w', encoding='utf-8') as f:
                        f.write(data_resp.text)
                    logging.info("Saved HTML debug to: %s", html_debug_file)

                time.sleep(2)

        logging.error("All attempts failed for batch %s", batch_index)
        return []

    except Exception as e:
        logging.exception("Error fetching contacts bulk: %s", e)
        return []

def batch_fetch_contacts_bulk(contact_ids, batch_size=30, max_workers=20):
    """
    Fetch contacts in batches using ThreadPoolExecutor.
    Tune batch_size and max_workers for performance.
    """
    from threading import Lock

    def safe_fetch(chunk, batch_index):
        try:
            logging.info(f"Starting batch {batch_index} with {len(chunk)} contacts...")
            result = fetch_contacts_bulk(chunk, batch_index)
            logging.info(f"Completed batch {batch_index} with {len(result)} contacts.")
            return result
        except Exception as e:
            logging.error("Batch %s failed: %s", batch_index, e)
            return []

    all_contacts = []
    futures = []
    lock = Lock()

    total_batches = (len(contact_ids) + batch_size - 1) // batch_size
    completed_batches = 0

    logging.info("Fetching %d contacts in parallel using %d threads...", len(contact_ids), max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, chunk in enumerate(chunk_list(contact_ids, batch_size), start=1):
            futures.append(executor.submit(safe_fetch, chunk, idx))

        for future in as_completed(futures):
            batch_result = future.result()
            print(f"Batch result count (before filtering): {len(batch_result)}")

            filtered = [
                contact for contact in batch_result
                if not contact.get("emailAddress", "").lower().endswith("@hp.com")
            ]

            with lock:
                all_contacts.extend(filtered)
                completed_batches += 1
                logging.info(f"Progress: {completed_batches}/{total_batches} batches complete.")

    logging.info("All batches processed. Total valid contacts: %d", len(all_contacts))
    return all_contacts