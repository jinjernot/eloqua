import time
import requests
from core.bulk.fetch_data_bulk import fetch_and_save_data
from core.bulk.bulk_contacts import batch_fetch_contacts_bulk
from core.utils import save_csv
from dateutil import parser

def fetch_data_with_retries(fetch_function, max_retries=3):
    for attempt in range(max_retries):
        try:
            return fetch_function()
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error: {e}. Retrying {attempt + 1}/{max_retries}...")
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break
    return None

def generate_daily_report(target_date):
    def fetch_wrapper():
        return fetch_and_save_data(target_date)

    data = fetch_data_with_retries(fetch_wrapper)
    if not data:
        print("Failed to fetch data after retries.")
        return None

    email_sends, email_assets, _, _, campaign_analysis, campaign_users = data  # Ignored email_activities

    # Normalize data
    email_sends_list = email_sends if isinstance(email_sends, list) else email_sends.get("items", [])
    email_assets_list = email_assets if isinstance(email_assets, list) else email_assets.get("items", [])
    campaign_analysis_list = campaign_analysis if isinstance(campaign_analysis, list) else campaign_analysis.get("items", [])
    campaign_users_list = campaign_users if isinstance(campaign_users, list) else campaign_users.get("items", [])

    seen = set()
    unique_email_sends = []
    for send in email_sends_list:
        key = (send.get("assetId"), send.get("contactId"))
        if key not in seen:
            seen.add(key)
            unique_email_sends.append(send)

    contact_ids = {
        str(send.get("contactId"))
        for send in unique_email_sends
        if send.get("contactId")
    }

    enriched_contacts = batch_fetch_contacts_bulk(list(contact_ids), batch_size=20)
    print(f"[DEBUG] Retrieved {len(enriched_contacts)} enriched contacts")

    contact_map = {str(c["id"]): c for c in enriched_contacts if c.get("id") is not None}
    campaign_map = {c.get("eloquaCampaignId"): c for c in campaign_analysis_list}
    user_map = {u.get("userID"): u.get("userName", "") for u in campaign_users_list}

    report_rows = []
    for send in unique_email_sends:
        cid = str(send.get("contactId", ""))
        contact = contact_map.get(cid, {})

        asset_id = send.get("assetId")

        campaign_id = send.get("campaignId")
        campaign = campaign_map.get(campaign_id, {})
        creator_id = campaign.get("createdBy")
        user = user_map.get(creator_id, "")

        # PLACEHOLDERS for missing fields from activities:
        total_sends = 0
        total_delivered = 0
        total_hard_bouncebacks = 0
        total_soft_bouncebacks = 0
        total_bouncebacks = 0
        unique_opens = 0
        clickthrough_rate = 0
        unique_clickthrough_rate = 0
        delivered_rate = 0
        unique_open_rate = 0
        hard_bounceback_rate = 0
        soft_bounceback_rate = 0
        bounceback_rate = 0

        report_rows.append({
            "Email Name": send.get("assetName", ""),
            "Email ID": asset_id,
            "Email Subject Line": send.get("subjectLine", ""),
            "Last Activated by User": user,
            "Total Delivered": total_delivered,
            "Total Hard Bouncebacks": total_hard_bouncebacks,
            "Total Sends": total_sends,
            "Total Soft Bouncebacks": total_soft_bouncebacks,
            "Total Bouncebacks": total_bouncebacks,
            "Unique Opens": unique_opens,
            "Hard Bounceback Rate": hard_bounceback_rate,
            "Soft Bounceback Rate": soft_bounceback_rate,
            "Bounceback Rate": bounceback_rate,
            "Clickthrough Rate": clickthrough_rate,
            "Unique Clickthrough Rate": unique_clickthrough_rate,
            "Delivered Rate": delivered_rate,
            "Unique Open Rate": unique_open_rate,
            "Email Group": "",  # Not available in new structure
            "Email Send Date": parser.parse(send.get("activityDate", send.get("campaignResponseDate", ""))).strftime("%Y-%m-%d %I:%M:%S %p") if send.get("activityDate") else "",
            "Email Address": contact.get("emailAddress", ""),
            "Contact Country": contact.get("country", ""),
            "HP Role": contact.get("hp_role", ""),
            "HP Partner Id": contact.get("hp_partner_id", ""),
            "Partner Name": contact.get("partner_name", ""),
            "Market": contact.get("market", ""),
        })

    return save_csv(report_rows, f"{target_date}.csv")
