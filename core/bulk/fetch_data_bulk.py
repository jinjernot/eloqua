import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from core.utils import save_json
from core.bulk.bulk_contacts import batch_fetch_contacts_bulk
from core.bulk.bulk_email_send import fetch_email_sends_bulk
# from core.bulk.bulk_bouncebacks import fetch_bouncebacks_bulk # <--- REMOVED THIS IMPORT
from core.rest.fetch_data import fetch_data # <--- ENSURE THIS IMPORT IS PRESENT

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_and_save_data(target_date=None):
    if target_date:
        start = datetime.strptime(target_date, "%Y-%m-%d")
    else:
        start = datetime.utcnow() - timedelta(days=1)

    start_str = start.strftime("%Y-%m-%dT00:00:00Z")
    end_str = (start + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

    BOUNCEBACK_OData_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/ActivityDetails/1/EmailBounceback"
    CLICKTHROUGH_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/ActivityDetails/1/EmailClickthrough"
    EMAIL_OPEN_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/ActivityDetails/1/EmailOpen"
    CAMPAIGN_ANALYSIS_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/CampaignAnalysis/1/Campaign"
    CAMPAIGN_USERS_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/CampaignAnalysis/1/User"
    EMAIL_ASSET_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/FormSubmission/1/EmailAsset"

    email_sends_future = None
    bouncebacks_future = None
    bounceback_preview_future = None
    email_clickthrough_future = None
    email_opens_future = None
    campaign_analysis_future = None
    campaign_users_future = None
    email_asset_data_future = None

    # Using ThreadPoolExecutor to run independent API fetches in parallel
    with ThreadPoolExecutor(max_workers=8) as executor: # You can adjust max_workers based on your system's capabilities and network
        # Submit bulk fetches first as they can take time due to polling
        email_sends_future = executor.submit(fetch_email_sends_bulk, start_str, end_str)
        
        # --- MODIFICATION START: Fetch main bouncebacks from OData endpoint using fetch_data ---
        # Filter by bounceBackDateHour, which is available in OData for bouncebacks
        filter_str_bounceback = f"bounceBackDateHour ge {start_str} and bounceBackDateHour lt {end_str}"
        bouncebacks_future = executor.submit(
            fetch_data, BOUNCEBACK_OData_ENDPOINT, "bouncebacks_odata.json", extra_params={"$filter": filter_str_bounceback}
        )
        # --- MODIFICATION END ---

        # Submit all independent OData/REST fetches to run concurrently
        # The bounceback_preview can remain if you want a small sample for debugging.
        preview_filter = f"bounceBackDateHour ge {start_str} and bounceBackDateHour lt {end_str}"
        bounceback_preview_future = executor.submit(
            fetch_data, BOUNCEBACK_OData_ENDPOINT, "email_bouncebacks_raw.json", extra_params={"$filter": preview_filter, "$top": 5}
        )

        filter_str = f"clickDateHour ge {start_str} and clickDateHour lt {end_str}"
        email_clickthrough_future = executor.submit(
            fetch_data, CLICKTHROUGH_ENDPOINT, "email_clickthrough.json", extra_params={"$filter": filter_str}
        )

        open_filter_str = f"openDateHour ge {start_str} and openDateHour lt {end_str}"
        email_opens_future = executor.submit(
            fetch_data, EMAIL_OPEN_ENDPOINT, "email_open.json", extra_params={"$filter": open_filter_str}
        )

        campaign_analysis_future = executor.submit(fetch_data, CAMPAIGN_ANALYSIS_ENDPOINT, "campaign.json")
        campaign_users_future = executor.submit(fetch_data, CAMPAIGN_USERS_ENDPOINT, "campaign_users.json")
        email_asset_data_future = executor.submit(fetch_data, EMAIL_ASSET_ENDPOINT, "email_asset.json")

        # Wait for email_sends to complete before proceeding with contact fetching,
        # as contact IDs are extracted from email sends.
        email_sends = email_sends_future.result()
        save_json(email_sends, os.path.join(DATA_DIR, "email_sends.json"))
        print(f"[INFO] Fetched {len(email_sends)} email sends.")

        contact_activities = []
        if not email_sends:
            print("[ERROR] No email sends found. Skipping contact fetch.")
        else:
            contact_ids = {str(send.get("contactId")) for send in email_sends if send.get("contactId")}
            contact_id_list = list(contact_ids)
            print(f"[INFO] Extracted {len(contact_id_list)} unique contact IDs.")

            # batch_fetch_contacts_bulk itself uses ThreadPoolExecutor internally,
            # so it's already optimized for parallel contact fetching.
            if contact_id_list:
                contact_activities = batch_fetch_contacts_bulk(contact_ids=contact_id_list, batch_size=20, max_workers=15)
                print(f"[INFO] Fetched {len(contact_activities)} contact activities.")
            else:
                print("[WARNING] No valid contact IDs found.")

        # Collect results from other futures that were submitted in parallel
        # --- MODIFICATION START: Collect main bouncebacks from OData result ---
        bouncebacks_raw_odata = bouncebacks_future.result()
        bouncebacks = bouncebacks_raw_odata.get("value", []) # fetch_data returns {'value': [...]}, so extract the list
        save_json(bouncebacks, os.path.join(DATA_DIR, "bouncebacks.json"))
        print(f"[INFO] Fetched {len(bouncebacks)} bouncebacks from OData.")
        # --- MODIFICATION END ---

        bounceback_preview = bounceback_preview_future.result()
        if "error" in bounceback_preview:
            print(f"[ERROR] Failed to preview bouncebacks: {bounceback_preview['error']}")
        else:
            save_json(bounceback_preview, os.path.join(DATA_DIR, "email_bouncebacks_raw.json"))
            sample_bouncebacks = bounceback_preview.get("value", [])
            if sample_bouncebacks:
                print(f"[INFO] Sample bounceback fields:\n{list(sample_bouncebacks[0].keys())}")
            else:
                print("[INFO] No bouncebacks found in preview.")

        email_clickthrough = email_clickthrough_future.result()
        if "error" in email_clickthrough:
            print(f"[ERROR] Failed to fetch email clickthrough: {email_clickthrough['error']}")
        else:
            print(f"[INFO] Fetched {len(email_clickthrough.get('value', []))} email clickthroughs.")
        save_json(email_clickthrough, os.path.join(DATA_DIR, "email_clickthrough.json"))

        email_opens = email_opens_future.result()
        if "error" in email_opens:
            print(f"[ERROR] Failed to fetch email opens: {email_opens['error']}")
        else:
            print(f"[INFO] Fetched {len(email_opens.get('value', []))} email opens.")
        save_json(email_opens, os.path.join(DATA_DIR, "email_open.json"))

        campaign_analysis = campaign_analysis_future.result()
        if "error" in campaign_analysis:
            print(f"[ERROR] Failed to fetch campaign analysis: {campaign_analysis['error']}")
        else:
            print(f"[INFO] Fetched campaign analysis with {len(campaign_analysis.get('value', []))} records.")

        campaign_users = campaign_users_future.result()
        if "error" in campaign_users:
            print(f"[ERROR] Failed to fetch campaign users: {campaign_users['error']}")
        else:
            print(f"[INFO] Fetched campaign users with {len(campaign_users.get('value', []))} records.")
            
        email_asset_data = email_asset_data_future.result()
        if "error" in email_asset_data:
            print(f"[ERROR] Failed to fetch email asset data: {email_asset_data['error']}")
        else:
            print(f"[INFO] Fetched email asset data with {len(email_asset_data.get('value', []))} records.")

    return {
        "email_sends": email_sends,
        "contact_activities": contact_activities,
        "bouncebacks": bouncebacks, # This will now be the OData fetched list
        "email_clickthroughs": email_clickthrough,
        "email_opens": email_opens,
        "campaign_analysis": campaign_analysis,
        "campaign_users": campaign_users,
        "email_asset_data": email_asset_data,
    }