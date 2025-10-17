import time
import requests
from dateutil import parser
import logging
from core.bulk.fetch_data_bulk import fetch_and_save_data
from core.bulk.bulk_contacts import batch_fetch_contacts_bulk
from core.utils import save_csv

logger = logging.getLogger(__name__)

def fetch_data_with_retries(fetch_function, max_retries=3):
    for attempt in range(max_retries):
        try:
            return fetch_function()
        except requests.exceptions.ConnectionError as e:
            logger.warning("Connection error: %s. Retrying %d/%d...", e, attempt + 1, max_retries)
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as e:
            logger.error("Request failed: %s", e)
            break
    return None


def generate_daily_report(target_date):
    start_time = time.time()
    logger.info("Starting daily report generation for %s", target_date)

    data = fetch_data_with_retries(lambda: fetch_and_save_data(target_date))
    if not data:
        logger.error("Failed to fetch data after retries.")
        return None

    # Parse the target date once for efficient comparison
    target_date_obj = parser.parse(target_date).date()

    email_sends = data.get("email_sends", [])
    bouncebacks = data.get("bouncebacks", [])
    campaign_analysis = data.get("campaign_analysis", {}).get("value", [])
    campaign_users = data.get("campaign_users", {}).get("value", [])
    email_clickthroughs = data.get("email_clickthroughs", {}).get("value", [])
    email_opens = data.get("email_opens", {}).get("value", [])
    email_asset_data = data.get("email_asset_data", {}).get("value", [])

    logger.info("Fetched %d email sends, %d bouncebacks, %d campaign analysis records, %d campaign users, %d email clickthroughs, %d email opens, %d email asset data records.",
                len(email_sends), len(bouncebacks), len(campaign_analysis), len(campaign_users), len(email_clickthroughs), len(email_opens), len(email_asset_data))

    email_group_map = {int(item["emailID"]): item.get("emailGroup", "") for item in email_asset_data if item.get("emailID")}
    logger.info("Email groups mapped: %d", len(email_group_map))

    # Use a dictionary to get unique email sends based on a composite key
    unique_email_sends = list({(str(s.get("assetId")), str(s.get("contactId"))): s for s in email_sends}.values())
    logger.info("Unique email sends: %d", len(unique_email_sends))

    bounceback_keys = set()
    bounceback_counts = {}
    for bb in bouncebacks:
        cid = str(bb.get("contactID") or bb.get("ContactId") or "")
        asset_id = str(bb.get("emailID") or bb.get("AssetId") or bb.get("assetId") or "")
        if not cid or not asset_id:
            continue
        key = (asset_id, cid)
        bounceback_keys.add(key)
        bounceback_counts.setdefault(key, {"hard": 0, "soft": 0, "total": 0})["total"] = 1
        if bb.get("isHardBounceback") is True:
            bounceback_counts[key]["hard"] = 1
        elif bb.get("isHardBounceback") is False:
            bounceback_counts[key]["soft"] = 1

    all_contact_ids = {str(send.get("contactId")) for send in unique_email_sends if send.get("contactId")}
    all_contact_ids.update(str(open_evt.get("contactID")) for open_evt in email_opens if open_evt.get("contactID"))
    all_contact_ids.update(str(click.get("contactID")) for click in email_clickthroughs if click.get("contactID"))

    logger.info("Contact IDs to enrich: %d", len(all_contact_ids))
    enrich_start_time = time.time()
    enriched_contacts = batch_fetch_contacts_bulk(list(all_contact_ids), batch_size=30)
    enrich_end_time = time.time()
    logger.info("Enriched %d contacts in %.2f seconds.", len(enriched_contacts), enrich_end_time - enrich_start_time)

    contact_map = {str(c["id"]): c for c in enriched_contacts if c.get("id")}

    campaign_map = {c.get("eloquaCampaignId"): c for c in campaign_analysis if c.get("eloquaCampaignId")}
    user_map = {u.get("userID"): u.get("userName", "") for u in campaign_users if u.get("userID")}

    click_map = {}
    for click in email_clickthroughs:
        key = (str(click.get("emailID")), str(click.get("contactID")))
        if key[0] and key[1]:
            click_map[key] = click_map.get(key, 0) + 1

    open_map = {}
    for open_evt in email_opens:
        key = (str(open_evt.get("emailID")), str(open_evt.get("contactID")))
        if key[0] and key[1]:
            open_map[key] = open_map.get(key, 0) + 1

    report_rows = []
    processing_start_time = time.time()
    for send in unique_email_sends:
        date_str = send.get("activityDate") or send.get("campaignResponseDate") or ""
        try:
            if not date_str or parser.parse(date_str).date() != target_date_obj:
                continue
            formatted_date = parser.parse(date_str).strftime("%Y-%m-%d %I:%M:%S %p")
        except (ValueError, TypeError):
            continue

        if "@hp.com" in send.get("emailAddress", "").lower():
            continue

        cid = str(send.get("contactId", ""))
        asset_id_raw = send.get("assetId")
        try:
            asset_id = int(asset_id_raw)
        except (ValueError, TypeError):
            continue

        key = (str(asset_id), cid)
        bb_counts = bounceback_counts.get(key, {"hard": 0, "soft": 0, "total": 0})
        total_delivered = 0 if key in bounceback_keys else 1

        total_clicks = click_map.get(key, 0)
        total_opens = open_map.get(key, 0)

        user = ""
        campaign_id = send.get("campaignId")
        if campaign_id:
            try:
                campaign = campaign_map.get(int(campaign_id), {})
                user = user_map.get(campaign.get("campaignCreatedByUserId"), "")
            except (ValueError, TypeError):
                pass
        
        if user:
            report_rows.append({
                "Email Name": send.get("assetName", ""),
                "Email ID": str(asset_id_raw),
                "Email Subject Line": send.get("subjectLine", ""),
                "Last Activated by User": user,
                "Total Delivered": total_delivered,
                "Total Hard Bouncebacks": bb_counts["hard"],
                "Total Sends": 1,
                "Total Soft Bouncebacks": bb_counts["soft"],
                "Total Bouncebacks": bb_counts["total"],
                "Unique Opens": 1 if total_opens > 0 else 0,
                "Hard Bounceback Rate": bb_counts["hard"] * 100,
                "Soft Bounceback Rate": bb_counts["soft"] * 100,
                "Bounceback Rate": bb_counts["total"] * 100,
                "Clickthrough Rate": round(total_clicks * 100),
                "Unique Clickthrough Rate": round((1 if total_clicks > 0 else 0) * 100),
                "Delivered Rate": total_delivered * 100,
                "Unique Open Rate": round((1 if total_opens > 0 else 0) * 100),
                "Email Group": email_group_map.get(asset_id, ""),
                "Email Send Date": formatted_date,
                "Email Address": send.get("emailAddress", "").lower(),
                "Contact Country": contact_map.get(cid, {}).get("country", ""),
                "HP Role": contact_map.get(cid, {}).get("hp_role", ""),
                "HP Partner Id": contact_map.get(cid, {}).get("hp_partner_id", ""),
                "Partner Name": contact_map.get(cid, {}).get("partner_name", ""),
                "Market": contact_map.get(cid, {}).get("market", ""),
            })
    processing_end_time = time.time()
    logger.info("Processed %d report rows in %.2f seconds.", len(report_rows), processing_end_time - processing_start_time)

    output_file = f"data/{target_date}.csv"
    save_csv(report_rows, output_file)
    end_time = time.time()
    logger.info("Daily report generation for %s completed in %.2f seconds. Report saved to %s", target_date, end_time - start_time, output_file)
    return output_file