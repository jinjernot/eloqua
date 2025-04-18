import time
import requests
from core.fetch_data import fetch_and_save_data
from core.bulk_contacts import batch_fetch_contacts_bulk
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

def generate_monthly_report():
    data = fetch_data_with_retries(fetch_and_save_data)
    if not data:
        print("Failed to fetch data after retries.")
        return None

    email_sends, email_assets, email_activities, _, campaign_analysis, campaign_users = data

    # 🔁 Extract the 100 most recent contactIDs for testing
    contact_ids = [
        str(send.get("contactID"))
        for send in sorted(email_sends.get("value", []),
                           key=lambda x: x.get("sentDateHour", ""),
                           reverse=True)[:100]
        if send.get("contactID")
    ]

    # 🔁 Enrich via Bulk API
    enriched_contacts = batch_fetch_contacts_bulk(contact_ids, batch_size=30)
    print(f"[DEBUG] Retrieved {len(enriched_contacts)} enriched contacts")

    # Build lookup map directly from flat list
    contact_map = {
        str(c["id"]): c
        for c in enriched_contacts
        if c.get("id") is not None
    }

    # Build other maps
    campaign_map = {c.get("eloquaCampaignId"): c for c in campaign_analysis.get("value", [])}
    user_map     = {u.get("userID"): u.get("userName", "") for u in campaign_users.get("value", [])}

    report_rows = []
    for send in email_sends.get("value", []):
        cid = str(send.get("contactID", ""))
        contact = contact_map.get(cid, {})

        # lookup email asset & activity
        ea = next((x for x in email_assets.get("value", []) if x.get("emailID") == send.get("emailID")), {})
        act = next((x for x in email_activities.get("value", []) if x.get("emailId")  == send.get("emailID")), {})

        # campaign/user lookup
        camp = campaign_map.get(act.get("eloquaCampaignId", ""), {})
        user = user_map.get(camp.get("lastActivatedByUserId", ""), "")

        # rates and metrics (as before)...
        total_sends    = act.get("totalSends", 1) or 1
        total_delivered= act.get("totalDelivered",1) or 1
        hr            = int((act.get("totalHardBouncebacks",0)/total_sends)*100)
        sr            = int((act.get("totalSoftBouncebacks",0)/total_sends)*100)
        br            = int((act.get("totalBouncebacks",0)/total_sends)*100)
        cr            = int((act.get("totalClickthroughs",0)/total_delivered)*100)
        ucr           = int((act.get("uniqueClickthroughs",0)/total_delivered)*100)
        dr            = int((total_delivered/total_sends)*100)
        uor           = int((act.get("uniqueOpens",0)/total_delivered)*100)

        report_rows.append({
            "Email Name":            ea.get("emailName", ""),
            "Email ID":              send.get("emailID"),
            "Email Subject Line":    ea.get("subjectLine", ""),
            "Last Activated by User":user,
            "Total Delivered":       total_delivered,
            "Total Hard Bouncebacks":act.get("totalHardBouncebacks",0),
            "Total Sends":           total_sends,
            "Total Soft Bouncebacks":act.get("totalSoftBouncebacks",0),
            "Total Bouncebacks":     act.get("totalBouncebacks",0),
            "Unique Opens":          act.get("uniqueOpens",0),
            "Hard Bounceback Rate":  hr,
            "Soft Bounceback Rate":  sr,
            "Bounceback Rate":       br,
            "Clickthrough Rate":     cr,
            "Unique Clickthrough Rate": ucr,
            "Delivered Rate":        dr,
            "Unique Open Rate":      uor,
            "Email Group":           ea.get("emailGroup",""),
            "Email Send Date":       send.get("sentDateHour",""),
            "Email Address":         contact.get("emailAddress",""),
            "Contact Country":       contact.get("country",""),
            "HP Role":               contact.get("hp_role",""),
            "HP Partner Id":         contact.get("hp_partner_id",""),
            "Partner Name":          contact.get("partner_name",""),
            "Market":                contact.get("market",""),
        })

    return save_csv(report_rows, "monthly_report.csv")
