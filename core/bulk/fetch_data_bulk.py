import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.utils import save_json
from core.bulk.bulk_email_send import fetch_email_sends_bulk
from core.rest.fetch_data import fetch_data

from config import (
    BOUNCEBACK_OData_ENDPOINT,
    CLICKTHROUGH_ENDPOINT,
    EMAIL_OPEN_ENDPOINT,
    CAMPAIGN_ANALYSIS_ENDPOINT,
    CAMPAIGN_USERS_ENDPOINT,
    EMAIL_ASSET_ENDPOINT,
)

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_and_save_data(target_date=None):
    if target_date:
        start = datetime.strptime(target_date, "%Y-%m-%d")
    else:
        start = datetime.utcnow() - timedelta(days=1)

    start_str = start.strftime("%Y-%m-%dT00:00:00Z")
    end_str = (start + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

    bounceback_end_date = start + timedelta(days=7)
    end_str_bounceback = bounceback_end_date.strftime("%Y-%m-%dT00:00:00Z")

    # For engagement (opens/clicks), use a 366-day window
    # This captures opens/clicks up to one year after the send date
    engagement_end_date = start + timedelta(days=366)
    end_str_engagement = engagement_end_date.strftime("%Y-%m-%dT00:00:00Z")

    results = {}
    print(f"[PERF_DEBUG] Step 1: Fetching email sends to determine email IDs sent on {start_str}")
    
    # Step 1: Fetch email sends first to get list of email IDs
    email_sends_list = fetch_email_sends_bulk(start_str, end_str)
    results["email_sends"] = {"items": email_sends_list}  # Wrap in dict for compatibility
    print(f"[PERF_DEBUG] Successfully fetched email_sends: {len(email_sends_list)} records")
    
    # Extract unique email IDs from sends
    email_ids = set()
    for send in email_sends_list:
        asset_id = send.get("assetId")  # Note: lowercase 'a' in assetId
        if asset_id:
            email_ids.add(str(asset_id))
    
    email_ids_list = sorted(email_ids)
    print(f"[INFO] Found {len(email_ids_list)} unique email IDs sent on {start_str}")
    print(f"[INFO] Email IDs: {email_ids_list[:10]}{'...' if len(email_ids_list) > 10 else ''}")
    
    # Step 2: Split email IDs into batches to avoid 200k API record limit
    # Strategy: Use moderate batch size - some batches may hit limit but better than individual queries
    # Individual queries (batch size 1) only returned 13k opens vs 554k with batch size 12
    # Accepted trade-off: Some data loss at 200k limit is better than massive data loss with individual queries
    BATCH_SIZE = 12  # Balanced approach - captures most data even if some batches hit limit
    email_id_batches = []
    if email_ids_list:
        for i in range(0, len(email_ids_list), BATCH_SIZE):
            batch = email_ids_list[i:i + BATCH_SIZE]
            email_id_batches.append(batch)
        print(f"[INFO] Split {len(email_ids_list)} email IDs into {len(email_id_batches)} batches of ~{BATCH_SIZE} IDs each")
    
    # Step 3: Fetch opens and clicks in batches
    all_opens = []
    all_clicks = []
    
    if email_id_batches:
        print(f"[INFO] Fetching opens/clicks for {len(email_id_batches)} batches...")
        for batch_num, batch in enumerate(email_id_batches, 1):
            email_ids_str = ",".join(batch)
            opens_filter = (
                f"emailID in ({email_ids_str}) and "
                f"openDateHour ge {start_str} and openDateHour lt {end_str_engagement}"
            )
            clicks_filter = (
                f"emailID in ({email_ids_str}) and "
                f"clickDateHour ge {start_str} and clickDateHour lt {end_str_engagement}"
            )
            opens_orderby = "openDateHour asc"
            clicks_orderby = "clickDateHour asc"
            
            print(f"[BATCH {batch_num}/{len(email_id_batches)}] Fetching opens/clicks for {len(batch)} email IDs...")
            
            batch_opens = fetch_data(EMAIL_OPEN_ENDPOINT, "email_open.json", 
                                   extra_params={"$filter": opens_filter, "$orderby": opens_orderby})
            batch_clicks = fetch_data(CLICKTHROUGH_ENDPOINT, "email_clickthrough.json",
                                    extra_params={"$filter": clicks_filter, "$orderby": clicks_orderby})
            
            batch_opens_list = batch_opens.get("value", [])
            batch_clicks_list = batch_clicks.get("value", [])
            
            all_opens.extend(batch_opens_list)
            all_clicks.extend(batch_clicks_list)
            
            print(f"[BATCH {batch_num}/{len(email_id_batches)}] Fetched {len(batch_opens_list)} opens, {len(batch_clicks_list)} clicks")
        
        print(f"[INFO] Total across all batches: {len(all_opens)} opens, {len(all_clicks)} clicks")
    
    # Remove the individual query code below
    
        print(f"[INFO] Total across all batches: {len(all_opens)} opens, {len(all_clicks)} clicks")
        results["email_opens"] = {"value": all_opens}
        results["email_clickthroughs"] = {"value": all_clicks}
    else:
        # Fallback if no email IDs found
        print(f"[WARNING] No email IDs found in sends")
        results["email_opens"] = {"value": []}
        results["email_clickthroughs"] = {"value": []}
    
    # Step 4: Fetch remaining data (bouncebacks, campaigns, etc.) in parallel
    print(f"[PERF_DEBUG] Step 4: Starting ThreadPoolExecutor for remaining 4 data fetches.")
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_key = {
            executor.submit(fetch_data, BOUNCEBACK_OData_ENDPOINT, "bouncebacks_odata.json", extra_params={"$filter": f"bounceBackDateHour ge {start_str} and bounceBackDateHour lt {end_str_bounceback}"}): "bouncebacks",
            executor.submit(fetch_data, CAMPAIGN_ANALYSIS_ENDPOINT, "campaign.json"): "campaign_analysis",
            executor.submit(fetch_data, CAMPAIGN_USERS_ENDPOINT, "campaign_users.json"): "campaign_users",
            executor.submit(fetch_data, EMAIL_ASSET_ENDPOINT, "email_asset.json"): "email_asset_data"
        }

        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                data = future.result()
                results[key] = data
                print(f"[PERF_DEBUG] Successfully fetched (from thread pool): {key}")
            except Exception as exc:
                print(f"[ERROR] {key} generated an exception: {exc}")
                results[key] = None
    
    print(f"[PERF_DEBUG] ThreadPoolExecutor finished.")

    email_sends = results.get("email_sends", [])
    save_json(email_sends, os.path.join(DATA_DIR, "email_sends.json"))
    print(f"[INFO] Fetched {len(email_sends)} email sends (with contact data included).")
    print(f"[PERF_DEBUG] Skipping separate contact fetch, data was included in email_sends export.")

    bouncebacks_raw_odata = results.get("bouncebacks", {})
    bouncebacks = bouncebacks_raw_odata.get("value", [])
    save_json(bouncebacks, os.path.join(DATA_DIR, "bouncebacks.json"))
    print(f"[INFO] Fetched {len(bouncebacks)} bouncebacks from OData.")

    email_clickthrough = results.get("email_clickthroughs", {})
    save_json(email_clickthrough, os.path.join(DATA_DIR, "email_clickthrough.json"))
    print(f"[INFO] Fetched {len(email_clickthrough.get('value', []))} email clickthroughs.")

    email_opens = results.get("email_opens", {})
    save_json(email_opens, os.path.join(DATA_DIR, "email_open.json"))
    print(f"[INFO] Fetched {len(email_opens.get('value', []))} email opens.")

    campaign_analysis = results.get("campaign_analysis", {})
    save_json(campaign_analysis, os.path.join(DATA_DIR, "campaign.json"))
    print(f"[INFO] Fetched {len(campaign_analysis.get('value', []))} records.")

    campaign_users = results.get("campaign_users", {})
    save_json(campaign_users, os.path.join(DATA_DIR, "campaign_users.json"))
    print(f"[INFO] Fetched {len(campaign_users.get('value', []))} records.")

    email_asset_data = results.get("email_asset_data", {})
    save_json(email_asset_data, os.path.join(DATA_DIR, "email_asset.json"))
    print(f"[INFO] Fetched {len(email_asset_data.get('value', []))} records.")

    print("[PERF_DEBUG] Returning all data from fetch_and_save_data.")
    return {
        "email_sends": email_sends,
        "bouncebacks": bouncebacks,
        "email_clickthroughs": email_clickthrough,
        "email_opens": email_opens,
        "campaign_analysis": campaign_analysis,
        "campaign_users": campaign_users,
        "email_asset_data": email_asset_data,
    }