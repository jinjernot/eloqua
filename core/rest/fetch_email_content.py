import requests
import logging
import os
import json
import re
from auth import get_valid_access_token
from config import BASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sanitize_filename(filename):
    """
    Removes invalid characters from a string to make it a safe filename.
    """
    if not filename:
        return "untitled"
    safe_name = re.sub(r'[\\/*?:"<>|]', "", filename)
    safe_name = safe_name.replace(" ", "_")
    return safe_name[:100]

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
        endpoint_url = f"{BASE_URL}/api/REST/2.0/assets/email/{email_id}"
        
        logger.info(f"Fetching email HTML for asset ID: {email_id}")
        resp = requests.get(endpoint_url, headers=headers)
        resp.raise_for_status()

        data = resp.json()
        email_name = data.get('name')
        safe_email_name = sanitize_filename(email_name)
        
        html_content = None
        html_content_obj = data.get('htmlContent')

        if html_content_obj:
            if html_content_obj.get('htmlBody'):
                html_content = html_content_obj['htmlBody']
            elif html_content_obj.get('html'):
                html_content = html_content_obj['html']
        if not html_content:
            html_content = data.get('html')
        
        if not html_content:
            plain_text = data.get('plainText')
            if plain_text:
                logger.warning(f"Could not find HTML for {email_id}. Saving 'plainText' content instead.")
                html_content = f"<html><head><title>{data.get('name', 'Plain Text Email')}</title></head><body><pre>{plain_text}</pre></body></html>"
            else:
                logger.error(f"Could not find 'htmlContent.htmlBody', 'htmlContent.html', 'html', or 'plainText' in response for {email_id}. Full keys: {data.keys()}")
                return None
        
        os.makedirs(save_dir, exist_ok=True)
        
        # Format: SafeEmailName_[email_id].html
        file_name = f"{safe_email_name}_{email_id}.html"
        file_path = os.path.join(save_dir, file_name)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        logger.info(f"Saved content for {email_id} ({email_name}) to {file_path}")
        return file_path

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error fetching email {email_id}: {http_err} - {resp.text}")
    except Exception as e:
        logger.exception(f"Failed to fetch email HTML for {email_id}: {e}")
        
    return None