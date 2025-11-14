import os
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from auth import get_valid_access_token
from config import *
from core.utils import save_json

DATA_DIR = "data"

def fetch_data(endpoint, filename, extra_params=None):
    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    params = {"depth": "complete"}
    if extra_params:
        params.update(extra_params)

    full_data = {"value": []}
    url = endpoint

    while url:
        response = requests.get(url, headers=headers, params=params if url == endpoint else None)

        if response.status_code != 200:
            return {"error": "Failed to fetch data", "details": response.text}

        data = response.json()
        full_data["value"].extend(data.get("value", []))
        
        url = data.get("@odata.nextLink")
        params = None

    filepath = os.path.join(DATA_DIR, filename)
    save_json(full_data, filepath)
    return full_data


def fetch_contact_by_id(contact_id):
    """Fetch a single contact by ID from Eloqua REST API"""
    access_token = get_valid_access_token()
    if not access_token:
        return None

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    url = f"{BASE_URL}/api/REST/2.0/data/contact/{contact_id}?depth=complete"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            
            # Extract the contact fields we need
            contact_info = {
                "emailAddress": data.get("emailAddress", ""),
                "country": "",
                "hp_role": "",
                "hp_partner_id": "",
                "partner_name": "",
                "market": ""
            }
            
            # Debug: Print first contact's fields to understand structure (only once)
            import os
            debug_file = "data/contact_fields_debug.txt"
            if not os.path.exists(debug_file):
                with open(debug_file, "w") as f:
                    f.write(f"Contact ID: {contact_id}\n")
                    f.write(f"Email: {data.get('emailAddress', 'N/A')}\n\n")
                    f.write("Available field values:\n")
                    for field in data.get("fieldValues", []):
                        if field.get("value"):
                            f.write(f"  ID: {field.get('id', '')}, Name: {field.get('name', '')}, Value: {field.get('value', '')}\n")
            
            # Parse field values from the contact - map by field ID
            # Confirmed field IDs from Eloqua instance:
            # 100195 = C_Market1
            # 100196 = C_Country (likely based on position)
            # 100197 = C_Partner_Name1
            # 100198 = C_HP_PartnerID1
            # 100199 = C_HP_Role1
            field_values = data.get("fieldValues", [])
            for field in field_values:
                field_value = field.get("value", "")
                field_id = str(field.get("id", ""))
                
                if field_value:  # Only map if there's a value
                    if field_id == "100196":  # C_Country
                        contact_info["country"] = field_value
                    elif field_id == "100199":  # C_HP_Role1
                        contact_info["hp_role"] = field_value
                    elif field_id == "100198":  # C_HP_PartnerID1
                        contact_info["hp_partner_id"] = field_value
                    elif field_id == "100197":  # C_Partner_Name1
                        contact_info["partner_name"] = field_value
                    elif field_id == "100195":  # C_Market1
                        contact_info["market"] = field_value
            
            return contact_info
        else:
            return None
    except Exception as e:
        print(f"Error fetching contact {contact_id}: {e}")
        return None


def fetch_contacts_batch(contact_ids, max_workers=5):
    """Fetch multiple contacts in parallel with rate limiting"""
    if not contact_ids:
        return {}
    
    print(f"[INFO] Fetching {len(contact_ids)} contacts from API (this may take a while)...")
    contacts = {}
    
    def fetch_with_delay(contact_id):
        time.sleep(0.2)  # Rate limiting: 5 requests per second
        result = fetch_contact_by_id(contact_id)
        return contact_id, result
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_with_delay, cid): cid for cid in contact_ids}
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            if completed % 100 == 0:
                print(f"[INFO] Progress: {completed}/{len(contact_ids)} contacts fetched")
            
            try:
                contact_id, contact_data = future.result()
                if contact_data:
                    contacts[str(contact_id)] = contact_data
            except Exception as e:
                print(f"[ERROR] Failed to fetch contact: {e}")
    
    print(f"[INFO] Successfully fetched {len(contacts)}/{len(contact_ids)} contacts")
    return contacts
