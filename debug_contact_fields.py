import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.rest.fetch_data import fetch_contact_by_id
import requests
from auth import get_valid_access_token
from config import BASE_URL

# Get a sample contact ID from the June 13 report (line 33944)
# This contact has email address: anke.bonse@strothkamp.de
# We need to find the contactId for this email

contact_id = "572429"  # Example - you may need to adjust this

print(f"Fetching contact {contact_id} to see field structure...")

access_token = get_valid_access_token()
headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
url = f"{BASE_URL}/api/REST/2.0/data/contact/{contact_id}?depth=complete"

response = requests.get(url, headers=headers)
if response.status_code == 200:
    data = response.json()
    
    print(f"\nEmail Address: {data.get('emailAddress', 'N/A')}")
    print(f"\nAvailable fields in contact:")
    
    field_values = data.get("fieldValues", [])
    for i, field in enumerate(field_values):
        field_id = field.get("id", "")
        field_name = field.get("name", "")
        field_value = field.get("value", "")
        field_type = field.get("type", "")
        
        if field_value:  # Only show fields with values
            print(f"\n  Field {i+1}:")
            print(f"    ID: {field_id}")
            print(f"    Name: {field_name}")
            print(f"    Value: {field_value}")
            print(f"    Type: {field_type}")
else:
    print(f"Error: {response.status_code}")
    print(response.text)
