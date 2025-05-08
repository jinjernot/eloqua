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

# Define proper activity fields with correct Eloqua ML syntax
# The error suggests that fields need proper Activity root references
ACTIVITY_FIELDS = {
    "id": "{{Activity.Id}}",
    "activityDate": "{{Activity.CreatedAt}}",
    "activityType": "{{Activity.Type}}",
    "assetName": "{{Activity.Asset.Name}}",
    "assetType": "{{Activity.Asset.Type}}",
    "assetId": "{{Activity.Asset.Id}}",
    "contactId": "{{Activity.Contact.Id}}",
    "emailId": "{{Activity.Asset.Id}}",  # For email activities
    "emailName": "{{Activity.Asset.Name}}",
    # Removing problematic field: "subjectLine": "{{Activity.Field:SubjectLine}}" 
}

def chunk_list(full_list, chunk_size):
    """Split a list into chunks of a specific size."""
    for i in range(0, len(full_list), chunk_size):
        yield full_list[i:i + chunk_size]

def build_activity_filter(activity_ids):
    """Builds a valid Eloqua Bulk API filter for EmailSend activity.
    
    According to Eloqua's API requirements, we need to use simpler expressions
    without complex operators like IN. We'll use multiple OR conditions
    for contact IDs but structured carefully.
    """
    if not activity_ids:
        return "'{{Activity.Type}}' = 'EmailSend'"

    # Basic filter for EmailSend activities
    type_filter = "'{{Activity.Type}}' = 'EmailSend'"
    
    # For a single contact ID, use a simple AND condition
    if len(activity_ids) == 1:
        return f"{type_filter} AND '{{{{Activity.Contact.Id}}}}' = '{activity_ids[0]}'"
    
    # For multiple IDs, we need to create a series of individual filters and run separate exports
    # Eloqua doesn't seem to support IN operators in the way we tried to use them
    # We'll return just the type filter, and split processing in the fetch function
    return type_filter

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

def fetch_activities_bulk(activity_ids, batch_index=None):
    """Fetches activities using the Eloqua Bulk API based on activity IDs.
    
    Due to limitations in Eloqua's filtering capabilities, we'll process
    contacts one by one if there are multiple IDs.
    """
    try:
        if not activity_ids:
            logging.warning("No activity IDs provided. Skipping export.")
            return []

        # If more than one contact ID, process them individually and combine results
        if len(activity_ids) > 1:
            logging.info(f"Processing {len(activity_ids)} contacts individually for batch {batch_index}")
            all_results = []
            for idx, contact_id in enumerate(activity_ids):
                sub_batch = f"{batch_index}_{idx+1}" if batch_index else f"Single_{idx+1}"
                contact_results = fetch_activities_bulk([contact_id], sub_batch)
                all_results.extend(contact_results)
            return all_results

        # Process a single contact ID
        contact_id = activity_ids[0]
        filter_query = f"'{{{{Activity.Type}}}}' = 'EmailSend' AND '{{{{Activity.Contact.Id}}}}' = '{contact_id}'"
        export_name = f"Bulk_Activity_Export_Contact_{contact_id}_Batch_{batch_index or 'N'}"
        
        access_token = get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Create the export payload with the correct field structure
        export_payload = {
            "name": export_name,
            "fields": ACTIVITY_FIELDS,
            "filter": filter_query
        }

        save_payload_debug(export_payload, batch_index)
        logging.info("Initiating export with filter: %s", filter_query)

        # Step 1: Create export definition for activities
        export_resp = requests.post(BULK_ACTIVITY_EXPORT_URL, headers=headers, json=export_payload)

        # Check response - note that a 200 status code with a JSON body is expected
        if export_resp.status_code != 200:
            logging.error("API Error: %s", export_resp.text)
            export_resp.raise_for_status()
        
        # Parse the export response
        try:
            export_data = export_resp.json()
            export_uri = export_data.get("uri")
            if not export_uri:
                logging.error("Export created but no URI returned: %s", json.dumps(export_data, indent=2))
                return []
                
            logging.info("Export created successfully with URI: %s", export_uri)
        except ValueError:
            logging.error("Failed to parse export response as JSON: %s", export_resp.text)
            return []

        # Step 2: Start sync
        sync_payload = {"syncedInstanceUri": export_uri}
        logging.info("Starting sync with payload: %s", json.dumps(sync_payload))
        
        sync_resp = requests.post(BULK_SYNC_URL, headers=headers, json=sync_payload)
        
        if sync_resp.status_code != 201:  # Note: Sync creation usually returns 201 Created
            logging.error("Sync creation failed with status %d: %s", 
                        sync_resp.status_code, sync_resp.text)
            return []
            
        try:
            sync_data = sync_resp.json()
            sync_uri = sync_data.get("uri")
            if not sync_uri:
                logging.error("Sync created but no URI returned: %s", json.dumps(sync_data, indent=2))
                return []
                
            logging.info("Sync started successfully with URI: %s", sync_uri)
        except ValueError:
            logging.error("Failed to parse sync response as JSON: %s", sync_resp.text)
            return []

        # Step 3: Poll sync status
        sync_id = sync_uri.split("/")[-1]
        poll_url = f"{BULK_SYNC_URL}/{sync_id}"
        
        status = "pending"
        success = False
        
        for attempt in range(SYNC_MAX_ATTEMPTS):
            time.sleep(SYNC_WAIT_SECONDS)
            
            try:
                poll_resp = requests.get(poll_url, headers=headers)
                
                if poll_resp.status_code != 200:
                    logging.warning("Sync status check failed with status %d: %s", 
                                   poll_resp.status_code, poll_resp.text)
                    continue
                    
                poll_data = poll_resp.json()
                status = poll_data.get("status", "unknown")
                
                logging.info("Sync status for contact %s (attempt %d): %s", 
                           contact_id, attempt + 1, status)
                
                if status.lower() == "success":
                    success = True
                    logging.info("Sync completed successfully for contact %s", contact_id)
                    break
                elif status.lower() in ["error", "failed", "canceled"]:
                    logging.error("Sync failed with status %s for contact %s: %s", 
                                 status, contact_id, json.dumps(poll_data, indent=2))
                    break
                    
            except Exception as e:
                logging.error("Error checking sync status (attempt %d): %s", attempt + 1, e)
        
        if not success:
            logging.warning("Sync never reached success state for contact %s. Final status: %s", 
                          contact_id, status)
            return []

        # Step 4: Download data
        data_url = f"{BASE_URL}/api/bulk/2.0/syncs/{sync_id}/data"
        logging.info("Downloading activity data for contact %s from: %s", contact_id, data_url)
        
        download_headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        activities = []
        success = False
        
        for attempt in range(3):
            try:
                data_resp = requests.get(data_url, headers=download_headers)
                
                if data_resp.status_code != 200:
                    logging.warning("Data download failed (attempt %d) with status %d: %s", 
                                  attempt + 1, data_resp.status_code, data_resp.text)
                    time.sleep(2)
                    continue
                
                if not data_resp.text.strip():
                    logging.warning("Empty response from data download (attempt %d), retrying...", 
                                   attempt + 1)
                    time.sleep(2)
                    continue

                data = data_resp.json()
                activities = data.get("items", [])
                success = True
                
                logging.info("Successfully downloaded %d activities for contact %s", 
                           len(activities), contact_id)
                
                if DEBUG_MODE and activities:
                    output_dir = "debug_activity_data"
                    os.makedirs(output_dir, exist_ok=True)
                    filename = os.path.join(output_dir, f"activities_contact_{contact_id}_batch_{batch_index}.json")
                    save_json(data, filename)
                    logging.info("Saved debug activity data to: %s", filename)
                
                break
                
            except json.JSONDecodeError as json_err:
                logging.error("JSON parse error (attempt %d): %s", attempt + 1, json_err)
                
                if DEBUG_MODE:
                    html_debug_file = f"debug_payloads/html_response_contact_{contact_id}_batch_{batch_index}.html"
                    with open(html_debug_file, 'w', encoding='utf-8') as f:
                        f.write(data_resp.text)
                    logging.info("Saved HTML debug to: %s", html_debug_file)
                
                time.sleep(2)
                
            except Exception as e:
                logging.error("Error downloading data (attempt %d): %s", attempt + 1, e)
                time.sleep(2)

        if not success:
            logging.error("All data download attempts failed for contact %s", contact_id)
            return []
            
        return activities

    except Exception as e:
        logging.exception("Error fetching activities bulk: %s", e)
        return []

    except Exception as e:
        logging.exception("Error fetching activities bulk: %s", e)
        return []

def batch_fetch_activities_bulk(activity_ids, batch_size=10, max_workers=3):
    """Fetch activities in batches using ThreadPoolExecutor."""
    from threading import Lock

    def safe_fetch(chunk, batch_index):
        try:
            logging.info(f"Starting batch {batch_index} with {len(chunk)} activities...")
            result = fetch_activities_bulk(chunk, batch_index)
            logging.info(f"Completed batch {batch_index} with {len(result)} activities.")
            return result
        except Exception as e:
            logging.error("Batch %s failed: %s", batch_index, e)
            return []

    all_activities = []
    futures = []
    lock = Lock()

    total_batches = (len(activity_ids) + batch_size - 1) // batch_size
    completed_batches = 0

    logging.info("Fetching %d activities in parallel using %d threads...", len(activity_ids), max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, chunk in enumerate(chunk_list(activity_ids, batch_size), start=1):
            futures.append(executor.submit(safe_fetch, chunk, idx))

        for future in as_completed(futures):
            batch_result = future.result()

            with lock:
                all_activities.extend(batch_result)
                completed_batches += 1
                logging.info(f"Progress: {completed_batches}/{total_batches} batches complete.")

    logging.info("All batches processed. Total valid activities: %d", len(all_activities))
    return all_activities