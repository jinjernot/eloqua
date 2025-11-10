import requests
import logging
import os
import json
from auth import get_valid_access_token
from config import BASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_email_html(email_id, save_dir="email_downloads"):
    """
    Fetches the full JSON representation of an email asset and extracts its HTML.
    Saves the HTML content to a file.
    """
    try:
        access_token = get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        # Use the REST 2.0 endpoint for a specific email asset
        endpoint_url = f"{BASE_URL}/api/REST/2.0/assets/email/{email_id}"
        
        logger.info(f"Fetching email HTML for asset ID: {email_id}")
        resp = requests.get(endpoint_url, headers=headers)
        resp.raise_for_status() # Will raise an error for 4xx/5xx responses

        data = resp.json()
        
        # The HTML content is usually nested inside 'htmlContent'
        html_content = data.get('htmlContent', {}).get('htmlBody')
        
        if not html_content:
            logger.warning(f"No 'htmlContent.htmlBody' found for email ID: {email_id}")
            # Sometimes it's just 'html'
            html_content = data.get('html')
            if not html_content:
                logger.error(f"Could not find HTML content in response for {email_id}. Full keys: {data.keys()}")
                return None

        # Create the save directory if it doesn't exist
        os.makedirs(save_dir, exist_ok=True)
        
        # Save the HTML to a file
        file_path = os.path.join(save_dir, f"{email_id}.html")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        logger.info(f"Saved HTML for {email_id} to {file_path}")
        return file_path

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error fetching email {email_id}: {http_err} - {resp.text}")
    except Exception as e:
        logger.exception(f"Failed to fetch email HTML for {email_id}: {e}")
        
    return None