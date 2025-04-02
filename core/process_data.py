from core.fetch_data import fetch_and_save_data
from core.utils import save_csv

def generate_monthly_report():
    email_sends, email_assets, email_activities, contact_activities, campaing, campaign_users = fetch_and_save_data()

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

        report_data.append({
            "Email Name": email_asset.get("emailName", ""),
            "Email ID": email_id,
            "Email Subject Line": email_asset.get("subjectLine", ""),
            "Last Activated by User": last_activated_user_name,  # Now shows userName instead of userID
            "Total Delivered": email_activity.get("totalDelivered", 0),
            "Total Hard Bouncebacks": email_activity.get("totalHardBouncebacks", 0),
            "Total Sends": email_activity.get("totalSends", 0),
            "Total Soft Bouncebacks": email_activity.get("totalSoftBouncebacks", 0),
            "Total Bouncebacks": email_activity.get("totalBouncebacks", 0),
            "Unique Opens": email_activity.get("totalOpens", 0),
            "Hard Bounceback Rate": email_activity.get("hardBouncebackRate", 0.0),
            "Soft Bounceback Rate": email_activity.get("softBouncebackRate", 0.0),
            "Bounceback Rate": email_activity.get("bouncebackRate", 0.0),
            "Clickthrough Rate": email_activity.get("clickthroughRate", 0.0),
            "Unique Clickthrough Rate": email_activity.get("uniqueClickthroughRate", 0.0),
            "Delivered Rate": email_activity.get("deliveredRate", 0),
            "Unique Open Rate": email_activity.get("uniqueOpenRate", 0),
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