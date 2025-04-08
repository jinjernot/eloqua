import requests
from datetime import datetime, timedelta
from auth import get_valid_access_token
from config import (
    EMAIL_SEND_ENDPOINT, EMAIL_ASSET_ENDPOINT, EMAIL_ACTIVITY_ENDPOINT, CONTACTS_ENDPOINT,
    CAMPAING_ANALYSIS_ENDPOINT, CAMPAING_USERS_ENDPOINT, TEST_ENDPOINT, CONTACT_BULK_ENDPOINT
)
from core.utils import save_json


def fetch_data(endpoint, filename, use_date_filter=False, is_contacts=False):
    access_token = get_valid_access_token()
    if not access_token:
        return {"error": "Authorization required. Please re-authenticate."}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    if is_contacts:
        # Fetch contacts using Bulk API
        all_contacts = []
        page = 1
        page_size = 1000

        # Define the bulk job parameters
        bulk_job_params = {
            "depth": "complete",
            "count": page_size,
            "page": page
        }

        # Start the bulk job by making a request
        bulk_response = requests.get(CONTACT_BULK_ENDPOINT, headers=headers, params=bulk_job_params)

        print(f"Requesting URL: {bulk_response.url}")
        print(f"Response Status: {bulk_response.status_code}")

        if bulk_response.status_code != 200:
            print(f"Error starting bulk fetch for contacts: {bulk_response.text}")
            return {"error": "Failed to start bulk fetch for contacts", "details": bulk_response.text}

        bulk_data = bulk_response.json()
        total_contacts = bulk_data.get("total", 0)

        print(f"Starting to fetch {total_contacts} contacts using bulk")

        while total_contacts > 0:
            # Fetch data in pages
            bulk_job_params["page"] = page
            bulk_response = requests.get(CONTACT_BULK_ENDPOINT, headers=headers, params=bulk_job_params)

            if bulk_response.status_code != 200:
                print(f"Error fetching contacts page {page}: {bulk_response.text}")
                break

            bulk_data = bulk_response.json()
            elements = bulk_data.get("elements", [])
            all_contacts.extend(elements)

            print(f"Fetched {len(elements)} contacts on page {page}. Total fetched so far: {len(all_contacts)}")

            # Check if there are more pages
            if len(elements) < page_size:
                break  # No more pages

            page += 1

        save_json({"elements": all_contacts}, filename)
        return {"elements": all_contacts}

    else:
        # OData endpoints (Email Sends, Assets, Activities, etc.)
        params = {"depth": "complete"}

        if use_date_filter:
            start_date = (datetime.utcnow() - timedelta(days=100)).strftime('%Y-%m-%dT%H:%M:%SZ')
            end_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            params.update({
                "$filter": f"sentDateHour ge {start_date} and sentDateHour le {end_date}",
                "$orderby": "sentDateHour desc",
                "$top": 100
            })

        response = requests.get(endpoint, headers=headers, params=params)

        print(f"Requesting URL: {response.url}")
        print(f"Response Status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print(f"Fetched {len(data.get('value', []))} records.")
            save_json(data, filename)
            return data
        else:
            print(f"Error details: {response.text}")
            return {"error": "Failed to fetch data", "details": response.text}


def fetch_and_save_data():
    email_sends = fetch_data(EMAIL_SEND_ENDPOINT, "email_sends.json", use_date_filter=True)
    email_assets = fetch_data(EMAIL_ASSET_ENDPOINT, "email_assets.json")
    email_activities = fetch_data(EMAIL_ACTIVITY_ENDPOINT, "email_activities.json")
    contact_activities = fetch_data(CONTACTS_ENDPOINT, "contact_activities.json", is_contacts=True)
    campaign_analysis = fetch_data(CAMPAING_ANALYSIS_ENDPOINT, "campaign.json")
    campaign_users = fetch_data(CAMPAING_USERS_ENDPOINT, "campaign_users.json")

    return email_sends, email_assets, email_activities, contact_activities, campaign_analysis, campaign_users


def fetch_account_activity():
    return fetch_data(TEST_ENDPOINT, "test.json")
