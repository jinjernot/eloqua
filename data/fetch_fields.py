from auth import get_valid_access_token
import requests
from core.utils import save_json


def fetch_field_definitions():
    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    url = "https://secure.p06.eloqua.com/api/bulk/2.0/activities/emailSend/fields"

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        save_json(data, "field_definitions.json")
        print("✅ Contact field definitions saved to contact_field_definitions.json")
        return data
    else:
        print(f"❌ Failed to fetch contact fields: {response.status_code} - {response.text}")
        return {"error": "Failed to fetch contact fields", "details": response.text}
