import time
import requests
from datetime import datetime, timedelta
from core.fetch_data import fetch_and_save_data
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
    # Get the date 100 days ago
    date_100_days_ago = (datetime.now() - timedelta(days=100)).date()

    data = fetch_data_with_retries(fetch_and_save_data)
    if not data:
        print("Failed to fetch data after retries.")
        return None

    email_sends, email_assets, email_activities, contact_activities, campaing, campaign_users = data

    contact_elements = contact_activities.get("elements", [])
    contact_map = {str(contact.get("id")): contact for contact in contact_elements}

    campaign_map = {campaign.get("eloquaCampaignId"): campaign for campaign in campaing.get("value", [])}
    user_map = {user.get("userID"): user.get("userName", "") for user in campaign_users.get("value", [])}

    blocked_emails = {
        "yl_110@hotmail.com",
        "1021001399@qq.com",
        "2604815709@qq.com",
        "496707864@qq.com"
    }

    report_data = []
    for send in email_sends.get("value", []):
        email_id = send.get("emailID")
        contact_id = str(send.get("contactID", ""))

        email_asset = next((ea for ea in email_assets.get("value", []) if ea.get("emailID") == email_id), {})
        email_activity = next((ea for ea in email_activities.get("value", []) if ea.get("emailId") == email_id), {})
        contact_info = next((c for c in contact_activities.get("elements", []) if str(c.get("id")) == contact_id), {})

        email_send_date = send.get("sentDateHour", "")
        email_address = contact_info.get("emailAddress", "")

        # Convert the send date to a datetime object for comparison
        try:
            email_send_date_obj = datetime.strptime(email_send_date, "%Y-%m-%d")
        except ValueError:
            email_send_date_obj = None

        # FILTERS
        if not email_send_date_obj or email_send_date_obj.date() < date_100_days_ago:
            continue
        if "@hp.com" in email_address:
            continue
        if email_address in blocked_emails:
            continue

        eloqua_campaign_id = email_activity.get("eloquaCampaignId", "")
        campaign_info = campaign_map.get(eloqua_campaign_id, {})

        last_activated_user_id = campaign_info.get("lastActivatedByUserId", "")
        last_activated_user_name = user_map.get(last_activated_user_id, last_activated_user_id)

        total_sends = email_activity.get("totalSends", 0) or 1
        total_delivered = email_activity.get("totalDelivered", 0) or 1
        total_hard_bouncebacks = email_activity.get("totalHardBouncebacks", 0)
        total_soft_bouncebacks = email_activity.get("totalSoftBouncebacks", 0)
        total_bouncebacks = email_activity.get("totalBouncebacks", 0)
        total_clickthroughs = email_activity.get("totalClickthroughs", 0)
        unique_clickthroughs = email_activity.get("uniqueClickthroughs", 0)
        unique_opens = email_activity.get("uniqueOpens", 0)

        hard_bounceback_rate = int((total_hard_bouncebacks / total_sends) * 100)
        soft_bounceback_rate = int((total_soft_bouncebacks / total_sends) * 100)
        bounceback_rate = int((total_bouncebacks / total_sends) * 100)
        clickthrough_rate = int((total_clickthroughs / total_delivered) * 100)
        unique_clickthrough_rate = int((unique_clickthroughs / total_delivered) * 100)
        delivered_rate = int((total_delivered / total_sends) * 100)
        unique_open_rate = int((unique_opens / total_delivered) * 100)

        report_data.append({
            "Email Name": email_asset.get("emailName", ""),
            "Email ID": email_id,
            "Email Subject Line": email_asset.get("subjectLine", ""),
            "Last Activated by User": last_activated_user_name,
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
            "Email Group": email_asset.get("emailGroup", ""),
            "Email Send Date": email_send_date,
            "Email Address": email_address,
            "Contact Country": contact_info.get("country", ""),
            "HP Role": contact_info.get("C_HP_Role1", ""),
            "HP Partner Id": contact_info.get("C_HP_PartnerID1", ""),
            "Partner Name": contact_info.get("C_Partner_Name1", ""),
            "Market": contact_info.get("C_Market1", ""),
        })

    return save_csv(report_data, "monthly_report.csv")
