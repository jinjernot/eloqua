"""
Pre-warm the contact cache by exporting ALL contacts from Eloqua via the Bulk API.

Run this ONCE before starting a multi-day backfill. It populates data/cache/contact_cache.json.gz
so that daily report runs hit the cache instead of fetching contacts one-by-one via REST.

Usage:
    python scripts/warm_contact_cache.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.logging_config import setup_logging
setup_logging("warm_contact_cache")

from core.bulk.bulk_contacts import fetch_all_contacts_bulk
from core.rest.fetch_data import load_contact_cache, save_contact_cache

if __name__ == "__main__":
    print(f"\n{'='*70}")
    print(f"  CONTACT CACHE WARM-UP")
    print(f"  Exporting all contacts from Eloqua — this may take several minutes")
    print(f"{'='*70}\n")

    # Load any existing cache so we don't lose it
    existing_cache = load_contact_cache()
    print(f"Existing cache: {len(existing_cache)} contacts\n")

    # Fetch all contacts via Bulk API (one sync job, no filter)
    items = fetch_all_contacts_bulk()

    if not items:
        print("\nNo contacts returned. Check API credentials and try again.")
        sys.exit(1)

    # Convert list of field dicts to cache format (keyed by contact ID)
    new_entries = {}
    for item in items:
        cid = str(item.get("id", ""))
        if not cid:
            continue
        new_entries[cid] = {
            "emailAddress":  item.get("emailAddress", ""),
            "country":       item.get("country", ""),
            "hp_role":       item.get("hp_role", ""),
            "hp_partner_id": item.get("hp_partner_id", ""),
            "partner_name":  item.get("partner_name", ""),
            "market":        item.get("market", ""),
        }

    # Merge: existing REST-fetched entries take priority (they have accurate root-level country)
    merged = {**new_entries, **existing_cache}

    print(f"\nContacts from bulk export: {len(new_entries)}")
    print(f"Previously cached:         {len(existing_cache)}")
    print(f"Total after merge:         {len(merged)}\n")

    save_contact_cache(merged)
    print(f"\nDone! Contact cache pre-warmed with {len(merged)} contacts.")
    print(f"You can now run the 100-day backfill — most contacts will be served from cache.\n")
