import os
from datetime import datetime, timedelta

from core.utils import save_json
from core.bulk.bulk_contacts import batch_fetch_contacts_bulk
from core.bulk.bulk_email_send import fetch_email_sends_bulk
from core.bulk.bulk_bouncebacks import fetch_bouncebacks_bulk
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

    # Step 1: Fetch EmailSend activities
    email_sends = fetch_email_sends_bulk(start_str, end_str)
    save_json(email_sends, os.path.join(DATA_DIR, "email_sends.json"))
    print(f"[INFO] Fetched {len(email_sends)} email sends.")

    if not email_sends:
        print("[ERROR] No email sends found. Skipping contact and bounceback fetch.")
        return

    # Step 2: Extract contact IDs
    contact_ids = {str(send.get("contactId")) for send in email_sends if send.get("contactId")}
    contact_id_list = list(contact_ids)
    print(f"[INFO] Extracted {len(contact_id_list)} unique contact IDs.")

    # Step 3: Fetch contacts
    contact_activities = []
    if contact_id_list:
        contact_activities = batch_fetch_contacts_bulk(contact_ids=contact_id_list, batch_size=20, max_workers=15)
        print(f"[INFO] Fetched {len(contact_activities)} contact activities.")
    else:
        print("[WARNING] No valid contact IDs found.")

    # Step 4: Fetch bouncebacks
    bouncebacks = fetch_bouncebacks_bulk(start_str, end_str)
    save_json(bouncebacks, os.path.join(DATA_DIR, "bouncebacks.json"))
    print(f"[INFO] Fetched {len(bouncebacks)} bouncebacks.")

    # TEMP: Preview fields in raw EmailBounceback data from OData
    BOUNCEBACK_OData_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/ActivityDetails/1/EmailBounceback"
    preview_filter = f"bouncebackDateHour ge {start_str} and bouncebackDateHour lt {end_str}"

    bounceback_preview = fetch_data(
        BOUNCEBACK_OData_ENDPOINT,
        "email_bouncebacks_raw.json",
        extra_params={"$filter": preview_filter, "$top": 5}
    )

    if "error" in bounceback_preview:
        print(f"[ERROR] Failed to preview bouncebacks: {bounceback_preview['error']}")
    else:
        save_json(bounceback_preview, os.path.join(DATA_DIR, "email_bouncebacks_raw.json"))
        sample_bouncebacks = bounceback_preview.get("value", [])
        if sample_bouncebacks:
            print(f"[INFO] Sample bounceback fields:\n{list(sample_bouncebacks[0].keys())}")
        else:
            print("[INFO] No bouncebacks found in preview.")

    # Step 5: Fetch clickthrough activities filtered by clickDateHour
    CLICKTHROUGH_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/ActivityDetails/1/EmailClickthrough"
    filter_str = f"clickDateHour ge {start_str} and clickDateHour lt {end_str}"

    email_clickthrough = fetch_data(
        CLICKTHROUGH_ENDPOINT,
        "email_clickthrough.json",
        extra_params={"$filter": filter_str}
    )

    if "error" in email_clickthrough:
        print(f"[ERROR] Failed to fetch email clickthrough: {email_clickthrough['error']}")
    else:
        print(f"[INFO] Fetched {len(email_clickthrough.get('value', []))} email clickthroughs.")

    save_json(email_clickthrough, os.path.join(DATA_DIR, "email_clickthrough.json"))

    # === Step 7: Fetch email open activities filtered by openDateHour ===
    EMAIL_OPEN_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/ActivityDetails/1/EmailOpen"
    open_filter_str = f"openDateHour ge {start_str} and openDateHour lt {end_str}"

    email_opens = fetch_data(
        EMAIL_OPEN_ENDPOINT,
        "email_open.json",
        extra_params={"$filter": open_filter_str}
    )

    if "error" in email_opens:
        print(f"[ERROR] Failed to fetch email opens: {email_opens['error']}")
    else:
        print(f"[INFO] Fetched {len(email_opens.get('value', []))} email opens.")

    save_json(email_opens, os.path.join(DATA_DIR, "email_open.json"))

    # Step 6: Fetch campaign data from REST endpoints
    CAMPAIGN_ANALYSIS_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/CampaignAnalysis/1/Campaign"
    CAMPAIGN_USERS_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/CampaignAnalysis/1/User"

    campaign_analysis = fetch_data(CAMPAIGN_ANALYSIS_ENDPOINT, "campaign.json")
    if "error" in campaign_analysis:
        print(f"[ERROR] Failed to fetch campaign analysis: {campaign_analysis['error']}")
    else:
        print(f"[INFO] Fetched campaign analysis with {len(campaign_analysis.get('value', []))} records.")

    campaign_users = fetch_data(CAMPAIGN_USERS_ENDPOINT, "campaign_users.json")
    if "error" in campaign_users:
        print(f"[ERROR] Failed to fetch campaign users: {campaign_users['error']}")
    else:
        print(f"[INFO] Fetched campaign users with {len(campaign_users.get('value', []))} records.")

    return {
        "email_sends": email_sends,
        "contact_activities": contact_activities,
        "bouncebacks": bouncebacks,
        "email_clickthroughs": email_clickthrough,
        "email_opens": email_opens,
        "campaign_analysis": campaign_analysis,
        "campaign_users": campaign_users,
    }
