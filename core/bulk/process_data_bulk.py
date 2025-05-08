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

    email_sends, email_assets, email_activities, _, campaign_analysis, campaign_users = data

    # Now using bulk fetched activities data
    activities_list = email_activities if isinstance(email_activities, list) else email_activities.get("items", [])

    seen = set()
    unique_email_sends = []
    for send in email_sends.get("value", []):
        key = (send.get("emailID"), send.get("contactID"))
        if key not in seen:
            seen.add(key)
            unique_email_sends.append(send)

    contact_ids = {
        str(send.get("contactID"))
        for send in unique_email_sends
        if send.get("contactID")
    }

    enriched_contacts = batch_fetch_contacts_bulk(list(contact_ids), batch_size=20)
    print(f"[DEBUG] Retrieved {len(enriched_contacts)} enriched contacts")

    contact_map = {str(c["id"]): c for c in enriched_contacts if c.get("id") is not None}
    campaign_map = {c.get("eloquaCampaignId"): c for c in campaign_analysis.get("value", [])}
    user_map = {u.get("userID"): u.get("userName", "") for u in campaign_users.get("value", [])}

    report_rows = []
    for send in unique_email_sends:
        cid = str(send.get("contactID", ""))
        contact = contact_map.get(cid, {})

        ea = next((x for x in email_assets.get("value", []) if x.get("emailID") == send.get("emailID")), {})
        act = next((x for x in activities_list if x.get("emailId") == send.get("emailID") and x.get("contactId") == send.get("contactID")), {})

        creator_id = ea.get("emailCreatedByUserID")
        user = user_map.get(creator_id, "")
        unique_clickthroughs = (act.get("existingVisitorClickthroughs", 0) or 0) + (act.get("newVisitorClickthroughs", 0) or 0)
        total_sends = act.get("totalSends", 1) or 1
        total_delivered = act.get("totalDelivered", 1) or 1

        hr = int((act.get("totalHardBouncebacks", 0) / total_sends) * 100)
        sr = int((act.get("totalSoftBouncebacks", 0) / total_sends) * 100)
        br = int((act.get("totalBouncebacks", 0) / total_sends) * 100)
        cr = act.get("clickthroughRate", 0)
        ucr = int((unique_clickthroughs / total_delivered) * 100)
        dr = int((total_delivered / total_sends) * 100)
        uor = int((act.get("totalOpens", 0) / total_delivered) * 100)

        report_rows.append({
            "Email Name": ea.get("emailName", ""),
            "Email ID": send.get("emailID"),
            "Email Subject Line": ea.get("subjectLine", ""),
            "Last Activated by User": user,
            "Total Delivered": total_delivered,
            "Total Hard Bouncebacks": act.get("totalHardBouncebacks", 0),
            "Total Sends": total_sends,
            "Total Soft Bouncebacks": act.get("totalSoftBouncebacks", 0),
            "Total Bouncebacks": act.get("totalBouncebacks", 0),
            "Unique Opens": act.get("totalOpens", 0),
            "Hard Bounceback Rate": hr,
            "Soft Bounceback Rate": sr,
            "Bounceback Rate": br,
            "Clickthrough Rate": cr,
            "Unique Clickthrough Rate": ucr,
            "Delivered Rate": dr,
            "Unique Open Rate": uor,
            "Email Group": ea.get("emailGroup", ""),
            "Email Send Date": parser.parse(send.get("sentDateHour", "")).strftime("%Y-%m-%d %I:%M:%S %p") if send.get("sentDateHour") else "",
            "Email Address": contact.get("emailAddress", ""),
            "Contact Country": contact.get("country", ""),
            "HP Role": contact.get("hp_role", ""),
            "HP Partner Id": contact.get("hp_partner_id", ""),
            "Partner Name": contact.get("partner_name", ""),
            "Market": contact.get("market", ""),
        })

    return save_csv(report_rows, f"{target_date}.csv")