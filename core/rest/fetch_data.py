import os
import requests
from auth import get_valid_access_token
from config import *
from core.utils import save_json

DATA_DIR = "data"

def fetch_data(endpoint, filename, extra_params=None):
    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    params = {"depth": "complete"}
    if extra_params:
        params.update(extra_params)

    full_data = {"value": []}
    url = endpoint

    while url:
        response = requests.get(url, headers=headers, params=params if url == endpoint else None)

        if response.status_code != 200:
            return {"error": "Failed to fetch data", "details": response.text}

        data = response.json()
        full_data["value"].extend(data.get("value", []))
        
        url = data.get("@odata.nextLink")
        params = None

    filepath = os.path.join(DATA_DIR, filename)
    save_json(full_data, filepath)
    return full_data


def fetch_contact_by_id(contact_id):
    """Fetch a single contact by ID from Eloqua REST API"""
    access_token = get_valid_access_token()
    if not access_token:
        return None

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    url = f"{BASE_URL}/api/REST/2.0/data/contact/{contact_id}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            
            # Extract the contact fields we need
            contact_info = {
                "emailAddress": data.get("emailAddress", ""),
                "country": "",
                "hp_role": "",
                "hp_partner_id": "",
                "partner_name": "",
                "market": ""
            }
            
            # Parse field values from the contact
            field_values = data.get("fieldValues", [])
            for field in field_values:
                field_id = field.get("id", "")
                field_value = field.get("value", "")
                
                # Map field IDs to our contact fields (you may need to adjust these IDs)
                if "Country" in str(field_id):
                    contact_info["country"] = field_value
                elif "HP_Role" in str(field_id) or "C_HP_Role1" in str(field_id):
                    contact_info["hp_role"] = field_value
                elif "HP_PartnerID" in str(field_id) or "C_HP_PartnerID1" in str(field_id):
                    contact_info["hp_partner_id"] = field_value
                elif "Partner_Name" in str(field_id) or "C_Partner_Name1" in str(field_id):
                    contact_info["partner_name"] = field_value
                elif "Market" in str(field_id) or "C_Market1" in str(field_id):
                    contact_info["market"] = field_value
            
            return contact_info
        else:
            return None
    except Exception as e:
        print(f"Error fetching contact {contact_id}: {e}")
        return None

