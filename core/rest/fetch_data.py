import os
import requests
import time
import json
import gzip
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from auth import get_valid_access_token
from config import *
from core.utils import save_json

DATA_DIR = "data"
CONTACT_CACHE_FILE = "data/contact_cache.json.gz"  # Now compressed

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


def load_contact_cache():
    """
    Load previously fetched contacts from compressed cache file.
    
    Returns:
        Dictionary mapping contact_id (str) to contact data, or empty dict if cache doesn't exist
    """
    cache_path = Path(CONTACT_CACHE_FILE)
    
    # Try loading compressed cache first
    if cache_path.exists():
        try:
            with gzip.open(cache_path, 'rt', encoding='utf-8') as f:
                cache = json.load(f)
                print(f"[CACHE] Loaded {len(cache)} contacts from compressed cache")
                return cache
        except Exception as e:
            print(f"[CACHE] Warning: Could not load compressed cache: {e}")
            
    # Fallback: try loading old uncompressed cache
    old_cache_path = Path("data/contact_cache.json")
    if old_cache_path.exists():
        try:
            with open(old_cache_path, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                print(f"[CACHE] Loaded {len(cache)} contacts from old uncompressed cache")
                print(f"[CACHE] Converting to compressed format...")
                save_contact_cache(cache)  # Save in new compressed format
                print(f"[CACHE] Old cache backed up, you can delete data/contact_cache.json")
                return cache
        except Exception as e:
            print(f"[CACHE] Warning: Could not load old cache: {e}")
            
    print(f"[CACHE] No existing cache found, will create new one")
    return {}


def save_contact_cache(cache):
    """
    Save contact cache to compressed JSON file with proper UTF-8 encoding.
    Uses gzip compression to reduce file size significantly.
    
    Args:
        cache: Dictionary mapping contact_id (str) to contact data
    """
    cache_path = Path(CONTACT_CACHE_FILE)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with gzip.open(cache_path, 'wt', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, separators=(',', ':'))  # Compact format
        
        # Get file size for reporting
        size_mb = cache_path.stat().st_size / (1024 * 1024)
        print(f"[CACHE] Saved {len(cache)} contacts to compressed cache ({size_mb:.2f} MB)")
    except Exception as e:
        print(f"[CACHE] Warning: Could not save contact cache: {e}")


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


def fetch_contacts_batch(contact_ids, max_workers=5, use_cache=True):
    """
    Fetch multiple contacts in parallel with rate limiting and caching.
    
    Args:
        contact_ids: List of contact IDs to fetch
        max_workers: Number of parallel workers for API calls
        use_cache: If True, checks cache before fetching and saves new contacts to cache
    
    Returns:
        Dictionary mapping contact_id (str) to contact data
    """
    if not contact_ids:
        return {}
    
    contacts = {}
    
    # Load existing cache
    cache = load_contact_cache() if use_cache else {}
    
    # Separate cached vs. needs-fetch contacts
    contacts_to_fetch = []
    cache_hits = 0
    
    for cid in contact_ids:
        cid_str = str(cid)
        if cid_str in cache:
            contacts[cid_str] = cache[cid_str]
            cache_hits += 1
        else:
            contacts_to_fetch.append(cid)
    
    total = len(contact_ids)
    to_fetch_count = len(contacts_to_fetch)
    
    if cache_hits > 0:
        print(f"[CACHE] {cache_hits}/{total} contacts loaded from cache")
    
    if to_fetch_count == 0:
        print(f"[CACHE] All contacts found in cache, no API calls needed!")
        return contacts
    
    print(f"[API] Fetching {to_fetch_count} new contacts via API (this may take {to_fetch_count * 0.2 / 60:.1f} minutes)...")
    
    def fetch_with_delay(contact_id):
        time.sleep(0.2)  # Rate limiting: 5 requests per second
        result = fetch_contact_by_id(contact_id)
        return contact_id, result
    
    newly_fetched = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_with_delay, cid): cid for cid in contacts_to_fetch}
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            if completed % 100 == 0:
                print(f"[API] Progress: {completed}/{to_fetch_count} contacts fetched")
            
            try:
                contact_id, contact_data = future.result()
                if contact_data:
                    cid_str = str(contact_id)
                    contacts[cid_str] = contact_data
                    newly_fetched[cid_str] = contact_data
            except Exception as e:
                print(f"[ERROR] Failed to fetch contact: {e}")
    
    print(f"[API] Successfully fetched {len(newly_fetched)}/{to_fetch_count} new contacts")
    
    # Update and save cache with newly fetched contacts
    if use_cache and newly_fetched:
        cache.update(newly_fetched)
        save_contact_cache(cache)
    
    return contacts
