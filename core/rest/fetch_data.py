
import time
import json
import gzip
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.aws.auth import get_valid_access_token
from config import *
from core.utils import save_json

# HTTP Session for connection reuse - significantly improves performance
# by reusing TCP connections for multiple requests to the same host
_http_session = None

def get_http_session():
    """
    Get or create a shared requests.Session for connection pooling.
    Reusing connections reduces TCP handshake overhead and speeds up API calls.
    """
    global _http_session
    if _http_session is None:
        _http_session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=HTTP_POOL_CONNECTIONS,
            pool_maxsize=HTTP_POOL_MAXSIZE,
            max_retries=HTTP_MAX_RETRIES,
            pool_block=False
        )
        _http_session.mount('http://', adapter)
        _http_session.mount('https://', adapter)
    return _http_session

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
    old_cache_path = Path("data/cache/contact_cache.json")
    if old_cache_path.exists():
        try:
            with open(old_cache_path, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                print(f"[CACHE] Loaded {len(cache)} contacts from old uncompressed cache")
                print(f"[CACHE] Converting to compressed format...")
                save_contact_cache(cache)
                print(f"[CACHE] Migration complete. Old file can be deleted.")
                return cache
        except Exception as e:
            print(f"[CACHE] Warning: Could not load old cache: {e}")
            
    print(f"[CACHE] No existing cache found, will create new one")
    return {}


def save_contact_cache(cache):
    """
    Save contact cache to both compressed and uncompressed JSON files.
    Compressed version is used for loading (faster I/O).
    Uncompressed version is kept as human-readable backup.
    
    Args:
        cache: Dictionary mapping contact_id (str) to contact data
    """
    cache_path = Path(CONTACT_CACHE_FILE)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with gzip.open(cache_path, 'wt', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, separators=(',', ':'))  # Compact format
        
        size_mb = cache_path.stat().st_size / (1024 * 1024)
        print(f"[CACHE] Saved {len(cache)} contacts to compressed cache ({size_mb:.2f} MB)")
    except Exception as e:
        print(f"[CACHE] Warning: Could not save compressed cache: {e}")
    
    try:
        backup_path = Path("data/cache/contact_cache.json")
        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        
        backup_size_mb = backup_path.stat().st_size / (1024 * 1024)
        print(f"[CACHE] Saved backup to uncompressed cache ({backup_size_mb:.2f} MB)")
    except Exception as e:
        print(f"[CACHE] Warning: Could not save backup cache: {e}")


def fetch_contact_by_id(contact_id):
    """Fetch a single contact by ID from Eloqua REST API"""
    access_token = get_valid_access_token()
    if not access_token:
        return None

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    url = f"{BASE_URL}/api/REST/2.0/data/contact/{contact_id}?depth=complete"
    
    session = get_http_session()
    
    try:
        response = session.get(url, headers=headers, timeout=HTTP_TIMEOUT_SHORT)
        if response.status_code == 200:
            data = response.json()
            
            contact_info = {
                "emailAddress": data.get("emailAddress", ""),
                "country": data.get("country", ""),
                "hp_role": "",
                "hp_partner_id": "",
                "partner_name": "",
                "market": ""
            }
            
            # Parse field values from the contact - map by field ID
            field_values = data.get("fieldValues", [])
            for field in field_values:
                field_value = field.get("value", "")
                field_id = str(field.get("id", ""))
                
                if field_value:
                    if field_id == ELOQUA_FIELD_ID_HP_ROLE:
                        contact_info["hp_role"] = field_value
                    elif field_id == ELOQUA_FIELD_ID_HP_PARTNER_ID:
                        contact_info["hp_partner_id"] = field_value
                    elif field_id == ELOQUA_FIELD_ID_PARTNER_NAME:
                        contact_info["partner_name"] = field_value
                    elif field_id == ELOQUA_FIELD_ID_MARKET:
                        contact_info["market"] = field_value
            
            return contact_info
        else:
            return None
    except Exception as e:
        print(f"Error fetching contact {contact_id}: {e}")
        return None


def fetch_contacts_batch(contact_ids, max_workers=None, use_cache=True):
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
    
    # Use configured default if not specified
    if max_workers is None:
        max_workers = CONTACT_FETCH_MAX_WORKERS
    
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
    
    print(f"[API] Fetching {to_fetch_count} new contacts via API (this may take {to_fetch_count * REST_API_RATE_LIMIT_DELAY / 60:.1f} minutes)...")
    
    def fetch_with_delay(contact_id):
        time.sleep(REST_API_RATE_LIMIT_DELAY)
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


def fetch_data(endpoint, filename, extra_params=None):
    """
    Fetch data from Eloqua OData endpoint.
    
    Args:
        endpoint: The OData endpoint URL
        filename: Filename for saving (not used anymore, kept for compatibility)
        extra_params: Optional dict of additional query parameters (e.g., $filter)
    
    Returns:
        Dictionary with API response data
    """
    access_token = get_valid_access_token()
    if not access_token:
        return {}

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    
    session = get_http_session()  # Use shared session for connection reuse
    
    # Build parameters
    params = {"count": API_PAGE_SIZE}
    if extra_params:
        params.update(extra_params)
    
    all_data = []
    page = 1
    
    while True:
        params["page"] = page
        try:
            response = session.get(endpoint, headers=headers, params=params, timeout=HTTP_TIMEOUT_LONG)
            response.raise_for_status()
            
            data = response.json()
            elements = data.get("value", [])
            
            if not elements:
                break
                
            all_data.extend(elements)
            print(f"[INFO] Fetched page {page} from {endpoint.split('/')[-1]}: {len(elements)} records")
            
            if page >= API_MAX_PAGES:
                max_records = API_MAX_PAGES * API_PAGE_SIZE
                print(f"[INFO] Reached page limit ({API_MAX_PAGES} pages = {max_records} records max)")
                break
            
            if len(elements) < API_PAGE_SIZE:
                print(f"[INFO] Received partial page ({len(elements)} < {API_PAGE_SIZE}), stopping pagination")
                break
                
            page += 1
            time.sleep(REST_API_RATE_LIMIT_DELAY)  # Rate limiting from config
            
        except Exception as e:
            print(f"[ERROR] Failed to fetch data from {endpoint}: {e}")
            break
    
    return {"value": all_data}
