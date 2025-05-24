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

    # Step 3: Fetch contacts if available
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

    # Step 5: Fetch campaign data from REST endpoints
    CAMPAIGN_ANALYSIS_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/CampaignAnalysis/1/Campaign"  # Replace with actual URL
    CAMPAING_USERS_ENDPOINT = "https://secure.p06.eloqua.com/API/OData/CampaignAnalysis/1/User"      # Replace with actual URL

    campaign_analysis = fetch_data(CAMPAIGN_ANALYSIS_ENDPOINT, "campaign.json")
    if "error" in campaign_analysis:
        print(f"[ERROR] Failed to fetch campaign analysis: {campaign_analysis['error']}")
    else:
        print(f"[INFO] Fetched campaign analysis with {len(campaign_analysis.get('value', []))} records.")

    campaign_users = fetch_data(CAMPAING_USERS_ENDPOINT, "campaign_users.json")
    if "error" in campaign_users:
        print(f"[ERROR] Failed to fetch campaign users: {campaign_users['error']}")
    else:
        print(f"[INFO] Fetched campaign users with {len(campaign_users.get('value', []))} records.")

    return {
        "email_sends": email_sends,
        "contact_activities": contact_activities,
        "bouncebacks": bouncebacks,
        "campaign_analysis": campaign_analysis,
        "campaign_users": campaign_users,
    }
