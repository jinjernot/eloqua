from core.fetch_data import fetch_and_save_data
from core.utils import save_csv

def generate_monthly_report():
    """Generate a monthly report combining all email-related data."""
    email_sends, email_assets, email_activities, contact_activities = fetch_and_save_data()


    # Map contact activities by contactId
    contact_activity_map = {
        activity.get("contactId"): activity
        for activity in contact_activities.get("value", [])
    }

    report_data = []
    for send in email_sends.get("value", []):
        email_id = send.get("emailID")
        contact_id = send.get("contactID", "")

        email_asset = next((ea for ea in email_assets.get("value", []) if ea.get("emailID") == email_id), {})
        email_activity = next((ea for ea in email_activities.get("value", []) if ea.get("emailId") == email_id), {})
        contact_info = contact_activity_map.get(contact_id, {})  # Get contact info from contact_activities

        report_data.append({
            "Email Name": email_asset.get("emailName", ""),
            "Email ID": email_id,
            "Email Subject Line": email_asset.get("subjectLine", ""),
            "Last Activated by User": email_asset.get("emailCreatedByUserID", ""),
            "Total Delivered": email_activity.get("totalDelivered", 0),
            "Total Hard Bouncebacks": email_activity.get("totalHardBouncebacks", 0),
            "Total Sends": email_activity.get("totalSends", 0),
            "Total Soft Bouncebacks": email_activity.get("totalSoftBouncebacks", 0),
            "Total Bouncebacks": email_activity.get("totalBouncebacks", 0),
            "Unique Opens": email_activity.get("totalOpens", 0),
            "Hard Bounceback Rate": email_activity.get("openRate", 0.0),
            "Soft Bounceback Rate": email_activity.get("clickthroughRate", 0.0),
            "Bounceback Rate": email_activity.get("clickToOpenRate", 0.0),
            "Clickthrough Rate": email_activity.get("clickthroughRate", 0.0),
            "Unique Clickthrough Rate": email_activity.get("clickToOpenRate", 0.0),
            "Delivered Rate": email_activity.get("totalDelivered", 0),
            "Unique Open Rate": email_activity.get("totalOpens", 0),
            "Email Group": email_asset.get("emailGroup", ""),
            "Email Send Date": send.get("sentDateHour", ""),
            "Email Address": contact_info.get("emailAddress", ""),
            "Contact Country": contact_info.get("contactCountry", ""),
            "HP Role": contact_info.get("hpRole", ""),
            "HP Partner Id": contact_info.get("hpPartnerId", ""),
            "Partner Name": contact_info.get("partnerName", ""),
            "Market": contact_info.get("market", ""),
        })

    return save_csv(report_data, "monthly_report.csv")
