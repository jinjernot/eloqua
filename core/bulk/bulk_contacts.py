from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
import logging
import os
import json
from core.aws.auth import get_valid_access_token
from core.bulk.bulk_email_send import get_http_session
from core.utils import save_json
from config import *

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def smart_chunk_contacts(contact_ids, max_chars=30000):
    """
    Chunks a list of contact IDs into batches based on the Eloqua filter
    character limit, not a fixed number of IDs.
    """
    chunks, current_chunk, current_len = [], [], 0
    for cid in contact_ids:
        clause = f"'{{{{Contact.Id}}}}' = '{cid}'"
        
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
        session = get_http_session()

        export_payload = {
            "name": export_name,
            "fields": CONTACT_FIELDS,
            "filter": filter_query
        }

        logging.info("Creating contact export for %d IDs (batch %s)...", len(contact_ids), batch_index)

        # Step 1: Create export definition
        export_resp = session.post(BULK_CONTACT_EXPORT_URL, headers=headers, json=export_payload, timeout=HTTP_TIMEOUT_SHORT)
        export_resp.raise_for_status()
        export_uri = export_resp.json().get("uri")

        # Step 2: Start sync
        sync_resp = session.post(BULK_SYNC_URL, headers=headers, json={"syncedInstanceUri": export_uri}, timeout=HTTP_TIMEOUT_SHORT)
        sync_resp.raise_for_status()
        sync_uri = sync_resp.json().get("uri")

        # Step 3: Poll sync with exponential backoff (2s → 4s → 8s → cap at SYNC_WAIT_SECONDS)
        max_wait = SYNC_WAIT_SECONDS
        total_waited = 0
        poll_url = f"{BULK_SYNC_URL}/{sync_uri.split('/')[-1]}"
        for attempt in range(SYNC_MAX_ATTEMPTS):
            wait = min(2 * (2 ** attempt), max_wait)
            time.sleep(wait)
            total_waited += wait
            poll_resp = session.get(poll_url, headers=headers, timeout=HTTP_TIMEOUT_SHORT)
            poll_resp.raise_for_status()
            status = poll_resp.json().get("status")
            if status == "success":
                logging.info("Contact sync completed in %ds (batch %s).", total_waited, batch_index)
                break
            elif status == "error":
                logging.error("Contact sync failed (batch %s).", batch_index)
                return []
        else:
            logging.error("Contact sync timed out after %ds (batch %s).", total_waited, batch_index)
            return []

        # Step 4: Paginated download
        sync_id = sync_uri.split("/")[-1]
        base_data_url = f"{BASE_URL}/api/bulk/2.0/syncs/{sync_id}/data"
        download_headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

        all_items = []
        offset = 0
        limit = 5000
        while True:
            paged_url = f"{base_data_url}?offset={offset}&limit={limit}"
            data_resp = session.get(paged_url, headers=download_headers, timeout=HTTP_TIMEOUT_LONG)
            if data_resp.status_code != 200:
                logging.error("Failed to download contacts at offset %d: %s", offset, data_resp.text)
                break
            data = data_resp.json()
            items = data.get("items", [])
            all_items.extend(items)
            if not data.get("hasMore"):
                break
            offset += limit

        logging.info("Downloaded %d contacts (batch %s).", len(all_items), batch_index)
        return all_items

    except Exception as e:
        logging.exception("Error fetching contacts bulk (batch %s): %s", batch_index, e)
        return []


def fetch_all_contacts_bulk():
    """
    Export ALL contacts from Eloqua with no filter.
    Used by warm_contact_cache.py to pre-populate the contact cache before a backfill.
    Returns a list of contact dicts.
    """
    try:
        access_token = get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        session = get_http_session()

        export_payload = {
            "name": "Bulk_Contact_Export_All",
            "fields": CONTACT_FIELDS,
        }

        logging.info("Creating full contact export (no filter)...")
        export_resp = session.post(BULK_CONTACT_EXPORT_URL, headers=headers, json=export_payload, timeout=HTTP_TIMEOUT_SHORT)
        export_resp.raise_for_status()
        export_uri = export_resp.json().get("uri")

        sync_resp = session.post(BULK_SYNC_URL, headers=headers, json={"syncedInstanceUri": export_uri}, timeout=HTTP_TIMEOUT_SHORT)
        sync_resp.raise_for_status()
        sync_uri = sync_resp.json().get("uri")
        logging.info("Full contact sync started: %s", sync_uri)

        # Poll with exponential backoff — full export can take several minutes
        max_wait = 30  # allow longer waits for full export
        total_waited = 0
        poll_url = f"{BULK_SYNC_URL}/{sync_uri.split('/')[-1]}"
        for attempt in range(60):  # up to ~15 minutes
            wait = min(2 * (2 ** attempt), max_wait)
            time.sleep(wait)
            total_waited += wait
            poll_resp = session.get(poll_url, headers=headers, timeout=HTTP_TIMEOUT_SHORT)
            poll_resp.raise_for_status()
            status = poll_resp.json().get("status")
            print(f"  [{attempt+1}] +{wait}s (total {total_waited}s) status: {status}")
            if status == "success":
                logging.info("Full contact sync completed in %ds.", total_waited)
                break
            elif status == "error":
                logging.error("Full contact sync failed.")
                return []
        else:
            logging.error("Full contact sync timed out.")
            return []

        # Paginated download
        sync_id = sync_uri.split("/")[-1]
        base_data_url = f"{BASE_URL}/api/bulk/2.0/syncs/{sync_id}/data"
        download_headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

        all_items = []
        offset = 0
        limit = 5000
        while True:
            paged_url = f"{base_data_url}?offset={offset}&limit={limit}"
            data_resp = session.get(paged_url, headers=download_headers, timeout=HTTP_TIMEOUT_LONG)
            if data_resp.status_code != 200:
                logging.error("Failed to download contacts at offset %d: %s", offset, data_resp.text)
                break
            data = data_resp.json()
            items = data.get("items", [])
            all_items.extend(items)
            print(f"  Downloaded {len(all_items)} contacts so far...")
            if not data.get("hasMore"):
                break
            offset += limit

        logging.info("Full contact export complete: %d contacts.", len(all_items))
        return all_items

    except Exception as e:
        logging.exception("Error in fetch_all_contacts_bulk: %s", e)
        return []

def batch_fetch_contacts_bulk(contact_ids, max_workers=20):
    """
    Fetch contacts in batches using ThreadPoolExecutor.
    Uses smart_chunk_contacts to create batches based on filter length.
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

    # Use smart_chunk_contacts instead of chunk_list
    contact_batches = smart_chunk_contacts(contact_ids)
    total_batches = len(contact_batches)
    completed_batches = 0

    logging.info("Fetching %d contacts in parallel (%d batches) using %d threads...", 
                 len(contact_ids), total_batches, max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Use the new contact_batches
        for idx, chunk in enumerate(contact_batches, start=1):
            futures.append(executor.submit(safe_fetch, chunk, idx))

        for future in as_completed(futures):
            batch_result = future.result()
            print(f"Batch result count (before filtering): {len(batch_result)}")

            filtered = [
                contact for contact in batch_result
                if not contact.get("emailAddress", "").lower().endswith(EXCLUDE_EMAIL_DOMAIN)
            ]

            with lock:
                all_contacts.extend(filtered)
                completed_batches += 1
                logging.info(f"Progress: {completed_batches}/{total_batches} batches complete.")

    logging.info("All batches processed. Total valid contacts: %d", len(all_contacts))
    return all_contacts