import os
import time
from datetime import datetime, timedelta, timezone
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
    EMAIL_ID_BATCH_SIZE,
    BATCH_PARALLEL_WORKERS,
    CAPTURE_WINDOW_START_OFFSET_HOURS,
    CAPTURE_WINDOW_END_OFFSET_HOURS,
    CAPTURE_WINDOW_BOUNCEBACK_DAYS,
)

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_and_save_data(target_date=None):
    if target_date:
        target_start = datetime.strptime(target_date, "%Y-%m-%d")
    else:
        target_start = datetime.now(timezone.utc) - timedelta(days=1)

    # Optional start offset helps capture sends near timezone boundaries.
    window_start = target_start - timedelta(hours=CAPTURE_WINDOW_START_OFFSET_HOURS)
    window_end = target_start + timedelta(days=1)
    engagement_window_end = window_end + timedelta(hours=CAPTURE_WINDOW_END_OFFSET_HOURS)

    start_str = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str_engagement = engagement_window_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    bounceback_end_date = window_end + timedelta(days=CAPTURE_WINDOW_BOUNCEBACK_DAYS)
    end_str_bounceback = bounceback_end_date.strftime("%Y-%m-%dT00:00:00Z")
    print(f"[INFO] Bounceback capture window: {start_str} to {end_str_bounceback} ({CAPTURE_WINDOW_BOUNCEBACK_DAYS} days after send date)")

    # Optional end offset helps capture late local-time sends that cross UTC midnight.
    if CAPTURE_WINDOW_END_OFFSET_HOURS > 0:
        print(f"[INFO] Applying capture window end offset: +{CAPTURE_WINDOW_END_OFFSET_HOURS} hours")

    results = {}
    print(f"[PERF_DEBUG] Step 1: Fetching email sends in window {start_str} to {end_str}")
    if CAPTURE_WINDOW_START_OFFSET_HOURS > 0:
        print(f"[INFO] Applying capture window start offset: -{CAPTURE_WINDOW_START_OFFSET_HOURS} hours")
    
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
    print(f"[INFO] Found {len(email_ids_list)} unique email IDs in send window")
    print(f"[INFO] Email IDs: {email_ids_list[:10]}{'...' if len(email_ids_list) > 10 else ''}")
    
    # Step 2: Split email IDs into batches to avoid 200k API record limit
    # Strategy: Use moderate batch size - some batches may hit limit but better than individual queries
    # Individual queries (batch size 1) only returned 13k opens vs 554k with batch size 12
    # Accepted trade-off: Some data loss at 200k limit is better than massive data loss with individual queries
    BATCH_SIZE = EMAIL_ID_BATCH_SIZE  # Configurable via config.py or environment variable
    email_id_batches = []
    if email_ids_list:
        for i in range(0, len(email_ids_list), BATCH_SIZE):
            batch = email_ids_list[i:i + BATCH_SIZE]
            email_id_batches.append(batch)
        print(f"[INFO] Split {len(email_ids_list)} email IDs into {len(email_id_batches)} batches of ~{BATCH_SIZE} IDs each")
    
    # Step 3: Fetch opens and clicks in batches (PARALLELIZED for speed)
    all_opens = []
    all_clicks = []
    
    if email_id_batches:
        print(f"[INFO] Fetching opens/clicks for {len(email_id_batches)} batches in parallel...")
        print(f"[INFO] Using sentDateHour filter for send window {start_str} to {end_str_engagement}")
        
        def fetch_batch_data(batch_info):
            """Fetch opens and clicks for a single batch"""
            batch_num, batch = batch_info

            def fetch_batch_recursive(email_id_subset, label):
                email_ids_str = ",".join(email_id_subset)
                opens_filter = (
                    f"emailID in ({email_ids_str}) and "
                    f"sentDateHour ge {start_str} and sentDateHour lt {end_str_engagement}"
                )
                clicks_filter = (
                    f"emailID in ({email_ids_str}) and "
                    f"sentDateHour ge {start_str} and sentDateHour lt {end_str_engagement}"
                )

                batch_opens = fetch_data(
                    EMAIL_OPEN_ENDPOINT,
                    "email_open.json",
                    extra_params={"$filter": opens_filter, "$orderby": "openDateHour asc"},
                )
                batch_clicks = fetch_data(
                    CLICKTHROUGH_ENDPOINT,
                    "email_clickthrough.json",
                    extra_params={"$filter": clicks_filter, "$orderby": "clickDateHour asc"},
                )

                batch_opens_list = batch_opens.get("value", [])
                batch_clicks_list = batch_clicks.get("value", [])

                opens_truncated = batch_opens.get("_meta", {}).get("truncated", False)
                clicks_truncated = batch_clicks.get("_meta", {}).get("truncated", False)

                # If pagination was clipped and we have multiple email IDs, split and retry.
                if (opens_truncated or clicks_truncated) and len(email_id_subset) > 1:
                    midpoint = len(email_id_subset) // 2
                    left = email_id_subset[:midpoint]
                    right = email_id_subset[midpoint:]
                    print(
                        f"[BATCH {batch_num}/{len(email_id_batches)} {label}] "
                        f"Detected truncated results (opens={opens_truncated}, clicks={clicks_truncated}) "
                        f"for {len(email_id_subset)} email IDs; splitting into {len(left)} + {len(right)}"
                    )

                    left_opens, left_clicks = fetch_batch_recursive(left, f"{label}L")
                    right_opens, right_clicks = fetch_batch_recursive(right, f"{label}R")
                    return left_opens + right_opens, left_clicks + right_clicks

                if (opens_truncated or clicks_truncated) and len(email_id_subset) == 1:
                    print(
                        f"[WARNING] [BATCH {batch_num}/{len(email_id_batches)} {label}] "
                        f"Single-email query still truncated (opens={opens_truncated}, clicks={clicks_truncated}) "
                        f"for emailID {email_id_subset[0]}"
                    )

                return batch_opens_list, batch_clicks_list

            print(f"[BATCH {batch_num}/{len(email_id_batches)}] Starting fetch for {len(batch)} email IDs...")
            batch_opens_list, batch_clicks_list = fetch_batch_recursive(batch, "root")
            print(
                f"[BATCH {batch_num}/{len(email_id_batches)}] "
                f"Fetched {len(batch_opens_list)} opens, {len(batch_clicks_list)} clicks"
            )
            return batch_opens_list, batch_clicks_list
        
        # Process batches in parallel with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(BATCH_PARALLEL_WORKERS, len(email_id_batches))) as executor:
            batch_info_list = [(i+1, batch) for i, batch in enumerate(email_id_batches)]
            future_to_batch = {executor.submit(fetch_batch_data, batch_info): batch_info for batch_info in batch_info_list}
            
            for future in as_completed(future_to_batch):
                try:
                    batch_opens_list, batch_clicks_list = future.result()
                    all_opens.extend(batch_opens_list)
                    all_clicks.extend(batch_clicks_list)
                except Exception as exc:
                    batch_info = future_to_batch[future]
                    print(f"[ERROR] Batch {batch_info[0]} generated an exception: {exc}")
        
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