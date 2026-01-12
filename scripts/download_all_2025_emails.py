"""
Script to download all email HTML files from 2025 using Eloqua API
"""
import sys
import os
import requests
import logging
from datetime import datetime

# Add parent directory to path to import core and config modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.aws.auth import get_valid_access_token
from config import BASE_URL, EMAIL_DOWNLOADS_DIR
from core.rest.fetch_email_content import fetch_email_html
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def fetch_all_email_assets_from_2025():
    """
    Fetches all email assets from Eloqua that were created or updated in 2025.
    Returns a list of email IDs.
    """
    access_token = get_valid_access_token()
    if not access_token:
        logger.error("Failed to get access token")
        return []
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    # Use the REST API to get emails - this is simpler than OData for getting all assets
    # The REST API endpoint for emails
    endpoint_url = f"{BASE_URL}/api/REST/2.0/assets/emails"
    
    params = {
        "depth": "minimal",  # We only need basic info to get IDs
        "count": 1000,  # Max per page
        "page": 1
    }
    
    all_emails = []
    seen_email_ids = set()  # Track unique email IDs to avoid duplicates
    page_count = 0
    
    logger.info("Fetching email assets from Eloqua...")
    
    while True:
        page_count += 1
        try:
            response = requests.get(endpoint_url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            emails = data.get("elements", [])
            
            # Debug: log first email to see timestamp format
            if page_count == 1 and emails:
                logger.info(f"Sample email data: {emails[0]}")
            
            if not emails:
                break
            
            # Filter emails from 2025
            for email in emails:
                # Check both createdAt and updatedAt timestamps
                created_at = email.get("createdAt")
                updated_at = email.get("updatedAt")
                
                # Parse timestamps and check if they're from 2025
                is_from_2025 = False
                
                if created_at:
                    try:
                        # Timestamp is Unix epoch as string
                        created_date = datetime.fromtimestamp(int(created_at))
                        if created_date.year == 2025:
                            is_from_2025 = True
                    except (ValueError, TypeError):
                        pass
                
                if updated_at and not is_from_2025:
                    try:
                        updated_date = datetime.fromtimestamp(int(updated_at))
                        if updated_date.year == 2025:
                            is_from_2025 = True
                    except (ValueError, TypeError):
                        pass
                
                if is_from_2025:
                    email_id = email.get("id")
                    # Only add if we haven't seen this email ID before
                    if email_id not in seen_email_ids:
                        seen_email_ids.add(email_id)
                        all_emails.append({
                            "id": email_id,
                            "name": email.get("name", "Unknown"),
                            "createdAt": created_at,
                            "updatedAt": updated_at
                        })
            
            logger.info(f"Processed page {page_count}, found {len(all_emails)} emails from 2025 so far...")
            
            # Check if there are more pages
            total = data.get("total", 0)
            if len(all_emails) >= total or len(emails) < params["count"]:
                break
            
            params["page"] += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching email assets: {e}")
            break
    
    logger.info(f"Found {len(all_emails)} total email assets from 2025")
    return all_emails


def download_all_2025_emails(max_workers=5):
    """
    Downloads HTML content for all emails from 2025.
    Uses threading for parallel downloads.
    
    Args:
        max_workers: Number of parallel download threads (default 5, be careful not to overwhelm the API)
    """
    # Fetch all email assets from 2025
    emails = fetch_all_email_assets_from_2025()
    
    if not emails:
        logger.warning("No emails found from 2025")
        return
    
    # Create output directory
    save_dir = f"{EMAIL_DOWNLOADS_DIR}/2025"
    os.makedirs(save_dir, exist_ok=True)
    
    logger.info(f"Starting download of {len(emails)} emails to {save_dir}")
    
    # Download emails in parallel
    success_count = 0
    failure_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_email = {
            executor.submit(fetch_email_html, email["id"], save_dir): email 
            for email in emails
        }
        
        # Process completed downloads
        for future in as_completed(future_to_email):
            email = future_to_email[future]
            try:
                result = future.result()
                if result:
                    success_count += 1
                    if success_count % 10 == 0:  # Log progress every 10 emails
                        logger.info(f"Progress: {success_count}/{len(emails)} emails downloaded")
                else:
                    failure_count += 1
                    logger.warning(f"Failed to download email ID {email['id']} ({email['name']})")
            except Exception as e:
                failure_count += 1
                logger.error(f"Error downloading email ID {email['id']} ({email['name']}): {e}")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Download completed!")
    logger.info(f"Total emails from 2025: {len(emails)}")
    logger.info(f"Successfully downloaded: {success_count}")
    logger.info(f"Failed: {failure_count}")
    logger.info(f"Files saved to: {save_dir}")
    logger.info(f"{'='*60}\n")


if __name__ == "__main__":
    # You can adjust max_workers to control parallel downloads
    # Lower number = slower but safer for API rate limits
    # Higher number = faster but may hit rate limits
    download_all_2025_emails(max_workers=5)
