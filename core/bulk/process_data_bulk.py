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
    bouncebacks = data.get("bouncebacks", [])
    campaign_analysis = data.get("campaign_analysis", {}).get("value", [])
    campaign_users = data.get("campaign_users", {}).get("value", [])
    email_clickthroughs = data.get("email_clickthroughs", {}).get("value", [])

    # Prepare unique email sends (de-duplicate by assetId and contactId)
    seen = set()
    unique_email_sends = []
    for send in email_sends:
        key = (str(send.get("assetId")), str(send.get("contactId")))
        if key not in seen:
            seen.add(key)
            unique_email_sends.append(send)
    print(f"Unique email sends: {len(unique_email_sends)}")

    # Get contact ids for enrichment
    contact_ids = {str(send.get("contactId")) for send in unique_email_sends if send.get("contactId")}
    print(f"Contact IDs to enrich: {contact_ids}")

    enriched_contacts = batch_fetch_contacts_bulk(list(contact_ids), batch_size=20)
    contact_map = {str(c["id"]): c for c in enriched_contacts if c.get("id") is not None}

    # Build campaign and user lookup maps
    campaign_map = {c.get("eloquaCampaignId"): c for c in campaign_analysis if c.get("eloquaCampaignId") is not None}
    user_map = {u.get("userID"): u.get("userName", "") for u in campaign_users if u.get("userID") is not None}

    # Process bouncebacks for counts
    bounceback_counts = {}
    for bb in bouncebacks:
        cid = str(bb.get("ContactId") or bb.get("contactID") or "")
        asset_id = str(bb.get("AssetId") or bb.get("assetId") or "")

        if not cid or not asset_id:
            continue

        key = (asset_id, cid)
        bounceback_counts.setdefault(key, {"hard": 0, "soft": 0, "total": 0})
        bounceback_counts[key]["total"] += 1

        smtp_error = str(bb.get("SmtpErrorCode", ""))
        if smtp_error.startswith("5."):
            bounceback_counts[key]["hard"] += 1
        elif smtp_error.startswith("4."):
            bounceback_counts[key]["soft"] += 1
        # else: ignore or handle unknown bounce types if needed

    # Process clickthroughs
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

        if asset_id not in unique_clicks_by_asset:
            unique_clicks_by_asset[asset_id] = set()
        unique_clicks_by_asset[asset_id].add(cid)

    # Build report rows
    report_rows = []
    for send in unique_email_sends:
        cid = str(send.get("contactId", ""))
        contact = contact_map.get(cid, {})

        asset_id = str(send.get("assetId"))
        key = (asset_id, cid)
        bb_counts = bounceback_counts.get(key, {"hard": 0, "soft": 0, "total": 0})

        total_sends = 1
        total_hard_bouncebacks = bb_counts["hard"]
        total_soft_bouncebacks = bb_counts["soft"]
        total_bouncebacks = bb_counts["total"]

        bounceback_rate = total_bouncebacks / total_sends if total_sends else 0
        hard_bounceback_rate = total_hard_bouncebacks / total_sends if total_sends else 0
        soft_bounceback_rate = total_soft_bouncebacks / total_sends if total_sends else 0

        total_delivered = total_sends - total_bouncebacks
        delivered_rate = total_delivered / total_sends if total_sends else 0

        # Clickthrough rates
        total_clicks = click_map.get(key, 0)
        unique_clicks = len(unique_clicks_by_asset.get(asset_id, set()))
        total_unique_sends = len({str(s["contactId"]) for s in unique_email_sends if str(s.get("assetId")) == asset_id})

        clickthrough_rate = total_clicks / total_sends if total_sends else 0
        unique_clickthrough_rate = unique_clicks / total_unique_sends if total_unique_sends else 0

        date_str = send.get("activityDate") or send.get("campaignResponseDate") or ""
        try:
            formatted_date = parser.parse(date_str).strftime("%Y-%m-%d %I:%M:%S %p") if date_str else ""
        except Exception:
            formatted_date = ""

        # Lookup "Last Activated by User"
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
            "Unique Opens": 0,
            "Hard Bounceback Rate": int(hard_bounceback_rate * 100),
            "Soft Bounceback Rate": int(soft_bounceback_rate * 100),
            "Bounceback Rate": int(bounceback_rate * 100),
            "Clickthrough Rate": round(clickthrough_rate * 100, 2),
            "Unique Clickthrough Rate": round(unique_clickthrough_rate * 100, 2),
            "Delivered Rate": round(delivered_rate, 4),
            "Unique Open Rate": 0,
            "Email Group": "",
            "Email Send Date": formatted_date,
            "Email Address": contact.get("emailAddress", ""),
            "Contact Country": contact.get("country", ""),
            "HP Role": contact.get("hp_role", ""),
            "HP Partner Id": contact.get("hp_partner_id", ""),
            "Partner Name": contact.get("partner_name", ""),
            "Market": contact.get("market", ""),
        })

    return save_csv(report_rows, f"{target_date}.csv")
