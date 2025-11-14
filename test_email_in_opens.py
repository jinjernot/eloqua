import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.rest.fetch_data import fetch_email_opens, fetch_email_clickthroughs
from datetime import datetime

# Fetch opens for June 13
target_date = "2025-06-13"
target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()

print("Fetching email opens...")
opens = fetch_email_opens(target_date)
print(f"Total opens: {len(opens)}")

# Check if emailAddress is in the data
if opens:
    sample = opens[0]
    print(f"\nSample open record:")
    print(f"Keys: {sample.keys()}")
    print(f"Has emailAddress: {'emailAddress' in sample}")
    if 'emailAddress' in sample:
        print(f"Sample emailAddress: {sample.get('emailAddress', 'N/A')}")
    
    # Count how many have emailAddress
    with_email = sum(1 for o in opens if o.get('emailAddress'))
    print(f"\nOpens with emailAddress: {with_email}/{len(opens)}")
