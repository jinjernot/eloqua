import requests
from auth import get_valid_access_token
from config import EMAIL_SEND_ENDPOINT, EMAIL_ASSET_ENDPOINT, EMAIL_ACTIVITY_ENDPOINT, CONTACTS_ENDPOINT, CAMPAING_ANALYSIS_ENDPOINT, CAMPAING_USERS_ENDPOINT, TEST_ENDPOINT
from core.utils import save_json

# Custom field mapping (Field ID to Field Name)
FIELD_MAPPING = {
    "100199": "HP Role",
    "100198": "HP Partner Id",
    "100197": "Partner Name",
    "100195": "Market"
}

def fetch_data(endpoint, filename, extra_params=None):
    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    params = {"depth": "complete"}

    if extra_params:
        params.update(extra_params)

    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        save_json(data, filename)
        return data
    else:
        return {"error": "Failed to fetch data", "details": response.text}

def save_custom_fields(enriched_contacts):
    # Extract just the custom fields (HP Role, Partner Name, etc.) from each contact
    custom_fields_data = []
    for contact in enriched_contacts:
        # Create a dictionary with just the custom fields (ignoring id, emailAddress, etc.)
        custom_fields = {key: value for key, value in contact.items() if key in FIELD_MAPPING.values()}
        custom_fields_data.append(custom_fields)

    # Save the custom fields data to a separate JSON file for review
    save_json(custom_fields_data, "custom_fields.json")

    print("Custom fields saved to 'custom_fields.json'")

# Call this function after you have enriched the contacts with custom fields
def fetch_and_save_data():
    email_sends = fetch_data(EMAIL_SEND_ENDPOINT, "email_sends.json")
    email_assets = fetch_data(EMAIL_ASSET_ENDPOINT, "email_assets.json")
    email_activities = fetch_data(EMAIL_ACTIVITY_ENDPOINT, "email_activities.json")

    # Fetch contacts with basic fields (id, emailAddress, fieldValues)
    contact_data = fetch_data(
        CONTACTS_ENDPOINT,  # Correct Contacts API
        "contacts.json",
        extra_params={"fields": "id,emailAddress,country,fieldValues,firstName,lastName"}  # Fetch relevant fields including custom fields
    )
    print("Fetched contact data:", contact_data)  # Log the contact data for debugging

    campaign_analysis = fetch_data(CAMPAING_ANALYSIS_ENDPOINT, "campaign.json")
    campaign_users = fetch_data(CAMPAING_USERS_ENDPOINT, "campaign_users.json")

    # Process the contacts and add custom field values
    enriched_contacts = []
    for contact in contact_data.get("elements", []):
        custom_fields = {}
        
        # Map fieldValues to field names using the FIELD_MAPPING
        for field in contact.get("fieldValues", []):
            field_id = str(field["id"])
            field_name = FIELD_MAPPING.get(field_id, f"Unknown_Field_{field_id}")
            # Handle cases where value might be missing
            custom_fields[field_name] = field.get("value", "")  # Default to empty string if no value exists

        # Add the contact's basic details and custom fields to the final data
        enriched_contacts.append({
            "id": contact["id"],
            "emailAddress": contact["emailAddress"],
            "country": contact.get("country", ""),
            "firstName": contact.get("firstName", ""),
            "lastName": contact.get("lastName", ""),
            **custom_fields  # Add custom fields (HP Role, Partner Name, etc.)
        })

    # Save the enriched contacts with custom fields
    save_json(enriched_contacts, "enriched_contacts.json")

    # Save just the custom fields to a separate JSON file for review
    save_custom_fields(enriched_contacts)

    return email_sends, email_assets, email_activities, enriched_contacts, campaign_analysis, campaign_users

def fetch_account_activity():
    return fetch_data(TEST_ENDPOINT, "test.json")
