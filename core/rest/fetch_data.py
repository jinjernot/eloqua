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

