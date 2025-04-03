import time
import requests
from core.fetch_data_new import fetch_and_save_data
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

def get_custom_field_value(field_values, field_id):
    """
    Utility function to extract a custom field's value from the fieldValues list.
    Returns the value if found, otherwise returns an empty string.
    """
    if isinstance(field_values, list):
        for field in field_values:
            if isinstance(field, dict) and field.get("id") == field_id:
                return field.get("value", "")
    return ""  # Return empty if field is not found

def generate_monthly_report():
    # Fetch data with retry logic
    data = fetch_data_with_retries(fetch_and_save_data)
    if not data:
        print("Failed to fetch data after retries.")
        return None

    email_sends, email_assets, email_activities, contacts, campaign, campaign_users = data

    # Create mappings for quick lookup
    campaign_map = {campaign.get("eloquaCampaignId"): campaign for campaign in campaign.get("value", [])}
    user_map = {user.get("userID"): user.get("userName", "") for user in campaign_users.get("value", [])}
    contact_map = {contact.get("id"): contact for contact in contacts}  # Map contact data by contact ID

    report_data = []
    for send in email_sends.get("value", []):
        email_id = send.get("emailID")
        contact_id = send.get("contactID", "")

        # Find the matching email asset and activity using email ID
        email_asset = next((ea for ea in email_assets.get("value", []) if ea.get("emailID") == email_id), {})
        email_activity = next((ea for ea in email_activities.get("value", []) if ea.get("emailId") == email_id), {})

        # Get contact info using the contact ID directly from the contact_map
        contact_info = contact_map.get(contact_id, {})

        # Extract custom field values using the field IDs from contact_info
        hp_role = get_custom_field_value(contact_info.get("fieldValues", []), "100199")
        hp_partner_id = get_custom_field_value(contact_info.get("fieldValues", []), "100198")
        partner_name = get_custom_field_value(contact_info.get("fieldValues", []), "100197")
        market = get_custom_field_value(contact_info.get("fieldValues", []), "100195")

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

        # Add data to the report
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
            "Contact Country": contact_info.get("country", ""),
            "HP Role": hp_role,
            "HP Partner Id": hp_partner_id,
            "Partner Name": partner_name,
            "Market": market,
        })

    return save_csv(report_data, "monthly_report.csv")