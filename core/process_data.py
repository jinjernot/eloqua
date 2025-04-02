import time
import requests
from core.fetch_data import fetch_and_save_data
from core.utils import save_csv

# Function to retry API requests
def fetch_data_with_retries(fetch_function, max_retries=3):
    for attempt in range(max_retries):
        try:
            return fetch_function()  # Call the original function
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error: {e}. Retrying {attempt + 1}/{max_retries}...")
            time.sleep(2 ** attempt)  # Exponential backoff (2s, 4s, 8s)
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break
    return None  # Return None if all retries fail

def generate_monthly_report():
    # Fetch data with retry logic
    data = fetch_data_with_retries(fetch_and_save_data)
    if not data:
        print("Failed to fetch data after retries.")
        return None

    email_sends, email_assets, email_activities, contact_activities, campaing, campaign_users = data

    # Create mappings for quick lookup
    campaign_map = {campaign.get("eloquaCampaignId"): campaign for campaign in campaing.get("value", [])}
    user_map = {user.get("userID"): user.get("userName", "") for user in campaign_users.get("value", [])}

    report_data = []
    for send in email_sends.get("value", []):
        email_id = send.get("emailID")
        contact_id = send.get("contactID", "")

        email_asset = next((ea for ea in email_assets.get("value", []) if ea.get("emailID") == email_id), {})
        email_activity = next((ea for ea in email_activities.get("value", []) if ea.get("emailId") == email_id), {})
        contact_info = next((c for c in contact_activities.get("value", []) if c.get("contactId") == contact_id), {})

        # Fetch the eloquaCampaignId from email activities
        eloqua_campaign_id = email_activity.get("eloquaCampaignId", "")

        # Lookup the campaign using eloquaCampaignId
        campaign_info = campaign_map.get(eloqua_campaign_id, {})

        # Get the last activated user ID and resolve it to userName
        last_activated_user_id = campaign_info.get("lastActivatedByUserId", "")
        last_activated_user_name = user_map.get(last_activated_user_id, last_activated_user_id)  # Fallback to ID if name not found

        # Extract necessary values for calculations
        total_sends = email_activity.get("totalSends", 0) or 1  # Avoid division by zero
        total_delivered = email_activity.get("totalDelivered", 0) or 1
        total_hard_bouncebacks = email_activity.get("totalHardBouncebacks", 0)
        total_soft_bouncebacks = email_activity.get("totalSoftBouncebacks", 0)
        total_bouncebacks = email_activity.get("totalBouncebacks", 0)
        total_clickthroughs = email_activity.get("totalClickthroughs", 0)
        unique_clickthroughs = email_activity.get("uniqueClickthroughs", 0)
        unique_opens = email_activity.get("uniqueOpens", 0)  # Ensure correct mapping

        # Calculate rates as integers
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
            "Email Send Date": send.get("sentDateHour", ""),
            "Email Address": contact_info.get("emailAddress", ""),
            "Contact Country": contact_info.get("contactCountry", ""),
            "HP Role": contact_info.get("C_HP_Role1", ""),
            "HP Partner Id": contact_info.get("C_HP_PartnerID1", ""),
            "Partner Name": contact_info.get("C_Partner_Name1", ""),
            "Market": contact_info.get("C_Market1", ""),
        })

    return save_csv(report_data, "monthly_report.csv")
