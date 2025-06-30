import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.utils import save_json
from core.bulk.bulk_contacts import batch_fetch_contacts_bulk
from core.bulk.bulk_email_send import fetch_email_sends_bulk
from core.rest.fetch_data import fetch_data

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

    engagement_end_date = start + timedelta(days=30)
    end_str_engagement = engagement_end_date.strftime("%Y-%m-%dT00:00:00Z")


    BOUNCEBACK_OData_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/ActivityDetails/1/EmailBounceback"
    CLICKTHROUGH_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/ActivityDetails/1/EmailClickthrough"
    EMAIL_OPEN_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/ActivityDetails/1/EmailOpen"
    CAMPAIGN_ANALYSIS_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/CampaignAnalysis/1/Campaign"
    CAMPAIGN_USERS_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/CampaignAnalysis/1/User"
    EMAIL_ASSET_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/FormSubmission/1/EmailAsset"

    results = {}
    # Using ThreadPoolExecutor to run independent API fetches in parallel
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_key = {
            executor.submit(fetch_email_sends_bulk, start_str, end_str): "email_sends",
            executor.submit(fetch_data, BOUNCEBACK_OData_ENDPOINT, "bouncebacks_odata.json", extra_params={"$filter": f"bounceBackDateHour ge {start_str} and bounceBackDateHour lt {end_str_bounceback}"}): "bouncebacks",
            executor.submit(fetch_data, CLICKTHROUGH_ENDPOINT, "email_clickthrough.json", extra_params={"$filter": f"clickDateHour ge {start_str} and clickDateHour lt {end_str_engagement}"}): "email_clickthroughs",
            executor.submit(fetch_data, EMAIL_OPEN_ENDPOINT, "email_open.json", extra_params={"$filter": f"openDateHour ge {start_str} and openDateHour lt {end_str_engagement}"}): "email_opens",
            executor.submit(fetch_data, CAMPAIGN_ANALYSIS_ENDPOINT, "campaign.json"): "campaign_analysis",
            executor.submit(fetch_data, CAMPAIGN_USERS_ENDPOINT, "campaign_users.json"): "campaign_users",
            executor.submit(fetch_data, EMAIL_ASSET_ENDPOINT, "email_asset.json"): "email_asset_data"
        }

        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                data = future.result()
                results[key] = data
                print(f"[INFO] Successfully fetched {key}")
            except Exception as exc:
                print(f"[ERROR] {key} generated an exception: {exc}")
                results[key] = None

    email_sends = results.get("email_sends", [])
    save_json(email_sends, os.path.join(DATA_DIR, "email_sends.json"))
    print(f"[INFO] Fetched {len(email_sends)} email sends.")

    contact_activities = []
    if email_sends:
        contact_ids = {str(send.get("contactId")) for send in email_sends if send.get("contactId")}
        contact_id_list = list(contact_ids)
        print(f"[INFO] Extracted {len(contact_id_list)} unique contact IDs.")
        if contact_id_list:
            contact_activities = batch_fetch_contacts_bulk(contact_ids=contact_id_list, batch_size=30, max_workers=20)
            print(f"[INFO] Fetched {len(contact_activities)} contact activities.")
    else:
        print("[ERROR] No email sends found. Skipping contact fetch.")

    bouncebacks_raw_odata = results.get("bouncebacks", {})
    bouncebacks = bouncebacks_raw_odata.get("value", [])
    save_json(bouncebacks, os.path.join(DATA_DIR, "bouncebacks.json"))
    print(f"[INFO] Fetched {len(bouncebacks)} bouncebacks from OData.")

    # Save other results
    email_clickthrough = results.get("email_clickthroughs", {})
    save_json(email_clickthrough, os.path.join(DATA_DIR, "email_clickthrough.json"))
    print(f"[INFO] Fetched {len(email_clickthrough.get('value', []))} email clickthroughs.")

    email_opens = results.get("email_opens", {})
    save_json(email_opens, os.path.join(DATA_DIR, "email_open.json"))
    print(f"[INFO] Fetched {len(email_opens.get('value', []))} email opens.")

    campaign_analysis = results.get("campaign_analysis", {})
    save_json(campaign_analysis, os.path.join(DATA_DIR, "campaign.json"))
    print(f"[INFO] Fetched campaign analysis with {len(campaign_analysis.get('value', []))} records.")

    campaign_users = results.get("campaign_users", {})
    save_json(campaign_users, os.path.join(DATA_DIR, "campaign_users.json"))
    print(f"[INFO] Fetched campaign users with {len(campaign_users.get('value', []))} records.")

    email_asset_data = results.get("email_asset_data", {})
    save_json(email_asset_data, os.path.join(DATA_DIR, "email_asset.json"))
    print(f"[INFO] Fetched email asset data with {len(email_asset_data.get('value', []))} records.")


    return {
        "email_sends": email_sends,
        "contact_activities": contact_activities,
        "bouncebacks": bouncebacks,
        "email_clickthroughs": email_clickthrough,
        "email_opens": email_opens,
        "campaign_analysis": campaign_analysis,
        "campaign_users": campaign_users,
        "email_asset_data": email_asset_data,
    }