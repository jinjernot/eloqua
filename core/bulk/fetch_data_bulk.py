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

    # For engagement (opens/clicks), look back 30 days BEFORE the target date
    # This captures forwards where someone opened an email that was sent earlier
    # Extend end date by +7 days to capture opens that happen up to a week after the send
    # This accounts for timezone differences and delayed opens
    engagement_start_date = start - timedelta(days=30)
    engagement_start_str = engagement_start_date.strftime("%Y-%m-%dT00:00:00Z")
    # Extend engagement window to +7 days after target date to capture late opens
    engagement_end_date = start + timedelta(days=8)  # +8 because start+1 is already next day start
    end_str_engagement = engagement_end_date.strftime("%Y-%m-%dT00:00:00Z")

    results = {}
    print(f"[PERF_DEBUG] Starting ThreadPoolExecutor for all 7 data fetches.")
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_key = {
            executor.submit(fetch_email_sends_bulk, start_str, end_str): "email_sends",
            
            executor.submit(fetch_data, BOUNCEBACK_OData_ENDPOINT, "bouncebacks_odata.json", extra_params={"$filter": f"bounceBackDateHour ge {start_str} and bounceBackDateHour lt {end_str_bounceback}"}): "bouncebacks",
            executor.submit(fetch_data, CLICKTHROUGH_ENDPOINT, "email_clickthrough.json", extra_params={"$filter": f"clickDateHour ge {engagement_start_str} and clickDateHour lt {end_str_engagement}"}): "email_clickthroughs",
            executor.submit(fetch_data, EMAIL_OPEN_ENDPOINT, "email_open.json", extra_params={"$filter": f"openDateHour ge {engagement_start_str} and openDateHour lt {end_str_engagement}"}): "email_opens",
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