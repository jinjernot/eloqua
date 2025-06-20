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

    # Parse the target date once for efficient comparison
    target_date_obj = parser.parse(target_date).date()

    email_sends = data.get("email_sends", [])
    contact_activities = data.get("contact_activities", [])
    bouncebacks = data.get("bouncebacks", [])
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

    bounceback_keys = set()
    bounceback_counts = {}
    for bb in bouncebacks:
        cid = str(bb.get("contactID") or bb.get("ContactId") or "")
        asset_id = str(bb.get("emailID") or bb.get("AssetId") or bb.get("assetId") or "")
        if not cid or not asset_id:
            continue
        key = (asset_id, cid)
        bounceback_keys.add(key)

        bounceback_counts.setdefault(key, {"hard": 0, "soft": 0, "total": 0})
        bounceback_counts[key]["total"] += 1

        is_hard_bounceback_flag = bb.get("isHardBounceback")

        if is_hard_bounceback_flag is True:
            bounceback_counts[key]["hard"] += 1
        elif is_hard_bounceback_flag is False:
            bounceback_counts[key]["soft"] += 1

    non_bounce_email_sends = [send for send in unique_email_sends
                              if (str(send.get("assetId")), str(send.get("contactId"))) not in bounceback_keys]

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
    processed_keys = set()

    for send in unique_email_sends:
        date_str = send.get("activityDate") or send.get("campaignResponseDate") or ""
        try:
            if not date_str or parser.parse(date_str).date() != target_date_obj:
                continue
            formatted_date = parser.parse(date_str).strftime("%Y-%m-%d %I:%M:%S %p")
        except (ValueError, TypeError):
            continue

        email_name = send.get("assetName", "")
        email_subject = send.get("subjectLine", "")
        if not email_name or not email_subject:
            continue

        email_address = send.get("emailAddress", "").lower()
        if "@hp.com" in email_address:
            continue

        cid = str(send.get("contactId", ""))
        contact = contact_map.get(cid, {})
        asset_id_raw = send.get("assetId")
        try:
            asset_id = int(asset_id_raw)
        except (ValueError, TypeError):
            asset_id = None

        key = (str(asset_id), cid) if asset_id is not None else (None, cid)
        processed_keys.add(key)

        bb_counts = bounceback_counts.get(key, {"hard": 0, "soft": 0, "total": 0})
        total_sends = 1
        total_bouncebacks = bb_counts["total"]
        total_hard_bouncebacks = bb_counts["hard"]
        total_soft_bouncebacks = bb_counts["soft"]
        total_delivered = 0 if key in bounceback_keys else 1
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
        
        if user:
            email_group = email_group_map.get(asset_id, "") if asset_id is not None else ""

            report_rows.append({
                "Email Name": email_name,
                "Email ID": str(asset_id_raw),
                "Email Subject Line": email_subject,
                "Last Activated by User": user,
                "Total Delivered": total_delivered,
                "Total Hard Bouncebacks": total_hard_bouncebacks,
                "Total Sends": total_sends,
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

    return save_csv(report_rows, f"{target_date}.csv")