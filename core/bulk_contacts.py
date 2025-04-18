import requests
import time
import logging
import os
import json
from auth import get_valid_access_token
from core.utils import save_json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://secure.p06.eloqua.com"
BULK_CONTACT_EXPORT_URL = f"{BASE_URL}/api/bulk/2.0/contacts/exports"
BULK_SYNC_URL = f"{BASE_URL}/api/bulk/2.0/syncs"

SYNC_MAX_ATTEMPTS = 10
SYNC_WAIT_SECONDS = 5

CONTACT_FIELDS = {
    "id": "{{Contact.Id}}",
    "emailAddress": "{{Contact.Field(C_EmailAddress)}}",
    "country": "{{Contact.Field(C_Country)}}",
    "hp_role": "{{Contact.Field(C_HP_Role1)}}",
    "hp_partner_id": "{{Contact.Field(C_HP_PartnerID1)}}",
    "partner_name": "{{Contact.Field(C_Partner_Name1)}}",
    "market": "{{Contact.Field(C_Market1)}}",
}

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

        # Retry logic
        for attempt in range(3):
            data_resp = requests.get(data_url, headers=download_headers)
            if not data_resp.text.strip():
                logging.warning("Attempt %d: Empty response, retrying...", attempt + 1)
                time.sleep(2)
                continue

            try:
                data = data_resp.json()
                filename = f"bulk_contact_data_batch_{batch_index}.json"
                save_json(data, filename)
                # ← Changed to pull from "items" instead of "elements"
                return data.get("items", [])
            except json.JSONDecodeError as json_err:
                logging.error("Attempt %d: JSON parse error: %s", attempt + 1, json_err)
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

def batch_fetch_contacts_bulk(contact_ids, batch_size=30):
    """Fetch contacts in batches, returns flat list of all contacts."""
    all_contacts = []
    for idx, chunk in enumerate(chunk_list(contact_ids, batch_size), start=1):
        logging.info("Fetching batch %d/%d", idx, (len(contact_ids) + batch_size - 1) // batch_size)
        batch = fetch_contacts_bulk(chunk, batch_index=idx)
        all_contacts.extend(batch)
    return all_contacts
