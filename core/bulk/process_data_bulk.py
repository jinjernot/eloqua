import time
import requests
from dateutil import parser

from core.bulk.fetch_data_bulk import fetch_and_save_data
from core.bulk.bulk_contacts import batch_fetch_contacts_bulk
from core.utils import save_csv


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
    data = fetch_data_with_retries(lambda: fetch_and_save_data(target_date))
    if not data:
        print("Failed to fetch data after retries.")
        return None

    email_sends = data.get("email_sends", [])
    contact_activities = data.get("contact_activities", [])
    bouncebacks = data.get("bouncebacks", []) # This will now contain the OData response with 'isHardBounceback'
    campaign_analysis = data.get("campaign_analysis", {}).get("value", [])
    campaign_users = data.get("campaign_users", {}).get("value", [])
    email_clickthroughs = data.get("email_clickthroughs", {}).get("value", [])
    email_opens = data.get("email_opens", {}).get("value", [])
    email_asset_data = data.get("email_asset_data", {}).get("value", [])

    email_group_map = {}
    for item in email_asset_data:
        email_id = item.get("emailID")
        if email_id is not None:
            email_group_map[int(email_id)] = item.get("emailGroup", "")
    print(f"Email groups mapped: {email_group_map}")

    seen = set()
    unique_email_sends = []
    for send in email_sends:
        key = (str(send.get("assetId")), str(send.get("contactId")))
        if key not in seen:
            seen.add(key)
            unique_email_sends.append(send)
    print(f"Unique email sends: {len(unique_email_sends)}")

    # Identify bounceback contact-asset keys
    bounceback_keys = set()
    bounceback_counts = {}
    for bb in bouncebacks:
        cid = str(bb.get("contactID") or bb.get("ContactId") or "")
        asset_id = str(bb.get("emailID") or bb.get("AssetId") or bb.get("assetId") or "")
        if not cid or not asset_id:
            continue
        key = (asset_id, cid)
        bounceback_keys.add(key) # Add to total bounce keys

        # Initialize counts for this key if not already present
        bounceback_counts.setdefault(key, {"hard": 0, "soft": 0, "total": 0})
        bounceback_counts[key]["total"] += 1 # Count all bounces

        # --- UPDATED BOUNCE CLASSIFICATION LOGIC ---
        # Directly use the 'isHardBounceback' field from the OData response
        is_hard_bounceback_flag = bb.get("isHardBounceback") 

        if is_hard_bounceback_flag is True:
            bounceback_counts[key]["hard"] += 1
        elif is_hard_bounceback_flag is False:
            bounceback_counts[key]["soft"] += 1
        # If the flag is None (missing), it won't be counted as hard or soft, but still in total.
        # This covers cases where the flag might not be present (though it should be for bounces).
        # --- END OF UPDATED BOUNCE CLASSIFICATION LOGIC ---

    # Filter out bouncebacks for accurate delivery count (uses bounceback_keys, which includes all bounces regardless of type)
    non_bounce_email_sends = [send for send in unique_email_sends
                              if (str(send.get("assetId")), str(send.get("contactId"))) not in bounceback_keys]

    # Combine contact IDs from sends, opens, and clicks to ensure all are enriched
    all_contact_ids = {str(send.get("contactId")) for send in unique_email_sends if send.get("contactId")}
    for open_evt in email_opens:
        all_contact_ids.add(str(open_evt.get("contactID")))
    for click in email_clickthroughs:
        all_contact_ids.add(str(click.get("contactID")))

    print(f"Contact IDs to enrich: {len(all_contact_ids)}")

    enriched_contacts = batch_fetch_contacts_bulk(list(all_contact_ids), batch_size=20)
    contact_map = {str(c["id"]): c for c in enriched_contacts if c.get("id") is not None}

    campaign_map = {c.get("eloquaCampaignId"): c for c in campaign_analysis if c.get("eloquaCampaignId") is not None}
    user_map = {u.get("userID"): u.get("userName", "") for u in campaign_users if u.get("userID") is not None}

    click_map = {}
    unique_clicks_by_asset = {}
    print(f"Loaded {len(email_clickthroughs)} clickthrough records")
    for click in email_clickthroughs:
        asset_id = str(click.get("emailID"))
        cid = str(click.get("contactID"))
        if not asset_id or not cid:
            continue
        key = (asset_id, cid)
        click_map[key] = click_map.get(key, 0) + 1
        unique_clicks_by_asset.setdefault(asset_id, set()).add(cid)

    open_map = {}
    unique_opens_by_asset = {}
    print(f"Loaded {len(email_opens)} open records")
    for open_evt in email_opens:
        asset_id = str(open_evt.get("emailID"))
        cid = str(open_evt.get("contactID"))
        if not asset_id or not cid:
            continue
        key = (asset_id, cid)
        open_map[key] = open_map.get(key, 0) + 1
        unique_opens_by_asset.setdefault(asset_id, set()).add(cid)

    report_rows = []
    processed_keys = set() # To prevent duplicate rows for direct sends

    # --- Process Direct Email Sends ---
    for send in unique_email_sends:
        email_address = send.get("emailAddress", "").lower()
        if "@hp.com" in email_address:
            continue  # Skip internal HP emails

        cid = str(send.get("contactId", ""))
        contact = contact_map.get(cid, {})

        asset_id_raw = send.get("assetId")
        try:
            asset_id = int(asset_id_raw)
        except (ValueError, TypeError):
            asset_id = None

        key = (str(asset_id), cid) if asset_id is not None else (None, cid)
        processed_keys.add(key) # Mark this key as processed

        bb_counts = bounceback_counts.get(key, {"hard": 0, "soft": 0, "total": 0})

        total_sends = 1
        total_bouncebacks = bb_counts["total"]
        total_hard_bouncebacks = bb_counts["hard"]
        total_soft_bouncebacks = bb_counts["soft"]
        total_delivered = 0 if key in bounceback_keys else 1 # Based on whether a bounceback was registered

        bounceback_rate = (total_bouncebacks / total_sends) if total_sends else 0
        hard_bounceback_rate = (total_hard_bouncebacks / total_sends) if total_sends else 0
        soft_bounceback_rate = (total_soft_bouncebacks / total_sends) if total_sends else 0
        delivered_rate = (total_delivered / total_sends) if total_sends else 0

        total_clicks = click_map.get(key, 0)
        unique_clicks = 1 if total_clicks > 0 else 0
        clickthrough_rate = (total_clicks / total_sends) if total_sends else 0
        unique_clickthrough_rate = (unique_clicks / total_sends) if total_sends else 0

        total_opens = open_map.get(key, 0)
        unique_opens = 1 if total_opens > 0 else 0
        unique_open_rate = (unique_opens / total_sends) if total_sends else 0

        date_str = send.get("activityDate") or send.get("campaignResponseDate") or ""
        try:
            formatted_date = parser.parse(date_str).strftime("%Y-%m-%d %I:%M:%S %p") if date_str else ""
        except Exception:
            formatted_date = ""

        campaign_id = send.get("campaignId")
        user = ""
        if campaign_id:
            try:
                campaign_id_int = int(campaign_id)
                campaign = campaign_map.get(campaign_id_int, {})
                creator_id = campaign.get("campaignCreatedByUserId")
                if creator_id:
                    user = user_map.get(creator_id, "")
            except (ValueError, TypeError):
                pass

        email_group = email_group_map.get(asset_id, "") if asset_id is not None else ""

        report_rows.append({
            "Email Name": send.get("assetName", ""),
            "Email ID": str(asset_id_raw),
            "Email Subject Line": send.get("subjectLine", ""),
            "Last Activated by User": user,
            "Total Delivered": total_delivered,
            "Total Hard Bouncebacks": total_hard_bouncebacks,
            "Total Sends": total_sends, # Will be 1 for direct sends
            "Total Soft Bouncebacks": total_soft_bouncebacks,
            "Total Bouncebacks": total_bouncebacks,
            "Unique Opens": unique_opens,
            "Hard Bounceback Rate": int(hard_bounceback_rate * 100),
            "Soft Bounceback Rate": int(soft_bounceback_rate * 100),
            "Bounceback Rate": int(bounceback_rate * 100),
            "Clickthrough Rate": round(clickthrough_rate * 100),
            "Unique Clickthrough Rate": round(unique_clickthrough_rate * 100),
            "Delivered Rate": int(delivered_rate * 100),
            "Unique Open Rate": round(unique_open_rate * 100),
            "Email Group": email_group,
            "Email Send Date": formatted_date,
            "Email Address": email_address,
            "Contact Country": contact.get("country", ""),
            "HP Role": contact.get("hp_role", ""),
            "HP Partner Id": contact.get("hp_partner_id", ""),
            "Partner Name": contact.get("partner_name", ""),
            "Market": contact.get("market", ""),
        })

    # --- Process Indirect (Forwarded) Email Engagement ---
    all_indirect_engagements = {} # Use this to consolidate opens/clicks for unique (asset_id, cid) not in direct sends

    for open_evt in email_opens:
        asset_id_raw = str(open_evt.get("emailID"))
        cid = str(open_evt.get("contactID"))
        if not asset_id_raw or not cid:
            continue
        key = (asset_id_raw, cid)
        
        if key not in processed_keys: # Only process if not a direct send
            all_indirect_engagements.setdefault(key, {
                "Email ID": asset_id_raw,
                "Email Address": open_evt.get("emailAddress", "").lower(),
                "Contact ID": cid,
                "Last Activity Date": open_evt.get("activityDate") or open_evt.get("openDateHour")
            })

    for click in email_clickthroughs:
        asset_id_raw = str(click.get("emailID"))
        cid = str(click.get("contactID"))
        if not asset_id_raw or not cid:
            continue
        key = (asset_id_raw, cid)

        if key not in processed_keys: # Only process if not a direct send
             all_indirect_engagements.setdefault(key, {
                "Email ID": asset_id_raw,
                "Email Address": click.get("emailAddress", "").lower(),
                "Contact ID": cid,
                "Last Activity Date": click.get("activityDate") or click.get("clickDateHour")
            })
             # Update last activity date if click is more recent than open
             if all_indirect_engagements[key]["Last Activity Date"] and \
                click.get("activityDate") or click.get("clickDateHour") and \
                parser.parse(click.get("activityDate") or click.get("clickDateHour")) > \
                parser.parse(all_indirect_engagements[key]["Last Activity Date"]):
                 all_indirect_engagements[key]["Last Activity Date"] = click.get("activityDate") or click.get("clickDateHour")


    for key, engagement_data in all_indirect_engagements.items():
        email_address = engagement_data["Email Address"]
        if "@hp.com" in email_address:
            continue # Skip internal HP emails

        cid = engagement_data["Contact ID"]
        contact = contact_map.get(cid, {})
        asset_id_raw = engagement_data["Email ID"]

        try:
            asset_id = int(asset_id_raw)
        except (ValueError, TypeError):
            asset_id = None
        
        # For forwarded emails, "sends" and "delivered" are 0
        total_sends = 0 # This will be 0 for forwarded emails
        total_delivered = 0
        total_bouncebacks = 0
        total_hard_bouncebacks = 0
        total_soft_bouncebacks = 0

        # Calculate open/click metrics for these indirect engagements
        total_clicks = click_map.get(key, 0)
        unique_clicks = 1 if total_clicks > 0 else 0
        clickthrough_rate = 0 # Cannot calculate meaningful rates as 0 sends
        unique_clickthrough_rate = 0 # Cannot calculate meaningful rates as 0 sends

        total_opens = open_map.get(key, 0)
        unique_opens = 1 if total_opens > 0 else 0
        unique_open_rate = 0 # Cannot calculate meaningful rates as 0 sends

        date_str = engagement_data["Last Activity Date"] or ""
        try:
            formatted_date = parser.parse(date_str).strftime("%Y-%m-%d %I:%M:%S %p") if date_str else ""
        except Exception:
            formatted_date = ""

        # Attempt to get email name and subject from email_asset_data
        email_name = ""
        email_subject_line = ""
        email_group = ""
        for asset in email_asset_data:
            if str(asset.get("emailID")) == asset_id_raw:
                email_name = asset.get("emailName", "")
                email_subject_line = asset.get("subject", "")
                email_group = asset.get("emailGroup", "")
                break


        report_rows.append({
            "Email Name": email_name,
            "Email ID": asset_id_raw,
            "Email Subject Line": email_subject_line,
            "Last Activated by User": "", # Not applicable for forwarded emails
            "Total Delivered": total_delivered,
            "Total Hard Bouncebacks": total_hard_bouncebacks,
            "Total Sends": total_sends, # Will be 0 for forwarded emails
            "Total Soft Bouncebacks": total_soft_bouncebacks,
            "Total Bouncebacks": total_bouncebacks,
            "Unique Opens": unique_opens,
            "Hard Bounceback Rate": 0,
            "Soft Bounceback Rate": 0,
            "Bounceback Rate": 0,
            "Clickthrough Rate": 0,
            "Unique Clickthrough Rate": 0,
            "Delivered Rate": 0,
            "Unique Open Rate": 0,
            "Email Group": email_group,
            "Email Send Date": formatted_date, # This is actually the activity date for forwarded emails
            "Email Address": email_address,
            "Contact Country": contact.get("country", ""),
            "HP Role": contact.get("hp_role", ""),
            "HP Partner Id": contact.get("hp_partner_id", ""),
            "Partner Name": contact.get("partner_name", ""),
            "Market": contact.get("market", ""),
        })

    return save_csv(report_rows, f"{target_date}.csv")