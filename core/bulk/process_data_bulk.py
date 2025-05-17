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

    seen = set()
    unique_email_sends = []
    for send in email_sends:
        key = (send.get("assetId"), send.get("contactID"))
        if key not in seen:
            seen.add(key)
            unique_email_sends.append(send)
            print(f"Unique email sends: {len(unique_email_sends)}")

    contact_ids = {str(send.get("contactID")) for send in unique_email_sends if send.get("contactID")}
    print(f"Contact IDs to enrich: {contact_ids}")
    enriched_contacts = batch_fetch_contacts_bulk(list(contact_ids), batch_size=20)
    contact_map = {str(c["id"]): c for c in enriched_contacts if c.get("id") is not None}

    bounceback_counts = {}
    for bb in bouncebacks:
        cid = str(bb.get("ContactId") or bb.get("contactID") or "")
        asset_id = bb.get("AssetId") or bb.get("assetId") or None
        bb_type = bb.get("BouncebackType", "").lower()

        if not cid or not asset_id:
            continue

        key = (asset_id, cid)
        bounceback_counts.setdefault(key, {"hard": 0, "soft": 0, "total": 0})
        bounceback_counts[key]["total"] += 1
        if "hard" in bb_type:
            bounceback_counts[key]["hard"] += 1
        elif "soft" in bb_type:
            bounceback_counts[key]["soft"] += 1

    report_rows = []
    for send in unique_email_sends:
        cid = str(send.get("contactID", ""))
        contact = contact_map.get(cid, {})

        asset_id = send.get("assetId")
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

        date_str = send.get("activityDate") or send.get("campaignResponseDate") or ""
        try:
            formatted_date = parser.parse(date_str).strftime("%Y-%m-%d %I:%M:%S %p") if date_str else ""
        except Exception:
            formatted_date = ""

        report_rows.append({
            "Email Name": send.get("assetName", ""),
            "Email ID": asset_id,
            "Email Subject Line": send.get("subjectLine", ""),
            "Last Activated by User": "",  # Removed: Reporting API User enrichment
            "Total Delivered": total_delivered,
            "Total Hard Bouncebacks": total_hard_bouncebacks,
            "Total Sends": total_sends,
            "Total Soft Bouncebacks": total_soft_bouncebacks,
            "Total Bouncebacks": total_bouncebacks,
            "Unique Opens": 0,
            "Hard Bounceback Rate": round(hard_bounceback_rate, 4),
            "Soft Bounceback Rate": round(soft_bounceback_rate, 4),
            "Bounceback Rate": round(bounceback_rate, 4),
            "Clickthrough Rate": 0,
            "Unique Clickthrough Rate": 0,
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
