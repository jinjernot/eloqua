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


def fetch_email_sends_bulk(start_date, end_date):
    try:
        access_token = get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        date_filter = f"'{{{{Activity.Type}}}}' = 'EmailSend' AND '{{{{Activity.CreatedAt}}}}' >= '{start_date}' AND '{{{{Activity.CreatedAt}}}}' < '{end_date}'"

        COMBINED_EMAIL_SEND_FIELDS = {
            "activityDate": "{{Activity.CreatedAt}}",
            "assetId": "{{Activity.Asset.Id}}",
            "assetName": "{{Activity.Asset.Name}}",
            "campaignId": "{{Activity.Campaign.Id}}",
            "contactId": "{{Activity.Contact.Id}}",
            "emailAddress": "{{Activity.Field(EmailAddress)}}", 
            "subjectLine": "{{Activity.Field(SubjectLine)}}",
            "contact_country": "{{Activity.Contact.Field(C_Country)}}",
            "contact_hp_role": "{{Activity.Contact.Field(C_HP_Role1)}}",
            "contact_hp_partner_id": "{{Activity.Contact.Field(C_HP_PartnerID1)}}",
            "contact_partner_name": "{{Activity.Contact.Field(C_Partner_Name1)}}",
            "contact_market": "{{Activity.Contact.Field(C_Market1)}}"
        }

        export_payload = {
            "name": f"Bulk_EmailSend_with_Contacts_{start_date[:10]}",
            "fields": COMBINED_EMAIL_SEND_FIELDS, # Use the new combined fields
            "filter": date_filter
        }

        save_debug_payload(export_payload, f"email_send_export_payload_{start_date[:10]}.json")

        # Step 1: Create export
        export_resp = requests.post(BULK_ACTIVITY_EXPORT_URL, headers=headers, json=export_payload)
        export_resp.raise_for_status()
        export_uri = export_resp.json().get("uri")
        logging.info("Created EmailSend export (with contact fields): %s", export_uri)

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
        limit = 5000 

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

                if not data.get("hasMore"): 
                    break

                offset += limit

            except json.JSONDecodeError as json_err:
                logging.error("JSON parse error at offset %d: %s", offset, json_err)
                break
        if DEBUG_MODE:
            os.makedirs("debug_email_sends", exist_ok=True)
            filename = f"debug_email_sends/email_sends_{start_date[:10]}.json"
            save_json({"items": all_items}, filename)

        return all_items

    except Exception as e:
        logging.exception("Failed to fetch email sends bulk: %s", e)
        return []