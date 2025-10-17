import time
import requests
from dateutil import parser
import logging
import pandas as pd
import csv # For tab delimiter constant
from core.bulk.fetch_data_bulk import fetch_and_save_data
# from core.utils import save_csv # No longer needed

logger = logging.getLogger(__name__)

def fetch_data_with_retries(fetch_function, max_retries=3):
    for attempt in range(max_retries):
        try:
            print(f"[PERF_DEBUG] Calling fetch_and_save_data (attempt {attempt+1})...")
            return fetch_function()
        except requests.exceptions.ConnectionError as e:
            logger.warning("Connection error: %s. Retrying %d/%d...", e, attempt + 1, max_retries)
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as e:
            logger.error("Request failed: %s", e)
            break
    return None

def sanitize_dataframe_for_csv(df):
    """
    Applies sanitization rules directly to a pandas DataFrame before saving.
    """
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            # Replace newlines and strip whitespace
            df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False).str.replace('\r', ' ', regex=False).str.strip()
        elif pd.api.types.is_float_dtype(df[col]):
            # Convert float to int, filling NaNs with 0
            df[col] = df[col].fillna(0).astype(int)
    return df

def generate_daily_report(target_date):
    start_time = time.time()
    logger.info("Starting daily report generation for %s", target_date)

    data = fetch_data_with_retries(lambda: fetch_and_save_data(target_date))
    if not data:
        logger.error("Failed to fetch data after retries.")
        return None
    
    print("[PERF_DEBUG] Data fetch complete. Starting pandas processing...")

    # Parse the target date once for efficient comparison
    target_date_obj = parser.parse(target_date).date()

    # --- Get all data from the fetcher ---
    email_sends = data.get("email_sends", [])
    bouncebacks = data.get("bouncebacks", [])
    campaign_analysis = data.get("campaign_analysis", {}).get("value", [])
    campaign_users = data.get("campaign_users", {}).get("value", [])
    email_clickthroughs = data.get("email_clickthroughs", {}).get("value", [])
    email_opens = data.get("email_opens", {}).get("value", [])
    email_asset_data = data.get("email_asset_data", {}).get("value", [])
    
    # We no longer get 'enriched_contacts'
    logger.info("Fetched %d email sends (w/ contacts), %d bouncebacks, %d clicks, %d opens.",
                len(email_sends), len(bouncebacks), len(email_clickthroughs), len(email_opens))

    # --- START OF PANDAS REWRITE ---
    processing_start_time = time.time()

    # 1. Create helper maps (these are small and fast)
    pd_step_start = time.time()
    email_group_map = {int(item["emailID"]): item.get("emailGroup", "") for item in email_asset_data if item.get("emailID")}
    campaign_map = {c.get("eloquaCampaignId"): c for c in campaign_analysis if c.get("eloquaCampaignId")}
    user_map = {u.get("userID"): u.get("userName", "") for u in campaign_users if u.get("userID")}
    print(f"[PERF_DEBUG] Step 1: Helper maps created in {time.time() - pd_step_start:.2f}s.")

    # 2. Load SENDS DataFrame
    pd_step_start = time.time()
    # Use a dictionary to get unique email sends (as in original logic)
    unique_sends_dict = {(str(s.get("assetId")), str(s.get("contactId"))): s for s in email_sends}
    if not unique_sends_dict:
        logger.warning("No unique email sends found. Aborting report.")
        return None
    
    df_sends = pd.DataFrame(list(unique_sends_dict.values()))
    
    # Filter sends by target date *early*
    df_sends["activityDateParsed"] = pd.to_datetime(df_sends["activityDate"], errors='coerce')
    df_sends = df_sends.dropna(subset=["activityDateParsed"]) # Drop rows that couldn't be parsed
    df_sends = df_sends[df_sends["activityDateParsed"].dt.date == target_date_obj].copy()
    if df_sends.empty:
        logger.warning("No email sends found for target date %s. Aborting.", target_date)
        return None
    
    # Clean up key fields
    df_sends["contactId_str"] = df_sends["contactId"].astype(str)
    df_sends["assetId_str"] = df_sends["assetId"].astype(str)
    df_sends["assetId_int"] = pd.to_numeric(df_sends["assetId"], errors='coerce').fillna(0).astype(int)
    print(f"[PERF_DEBUG] Step 2: SENDS DataFrame created and filtered ({len(df_sends)} rows) in {time.time() - pd_step_start:.2f}s.")

    # 3. Load and Aggregate BOUNCEBACKS
    pd_step_start = time.time()
    if bouncebacks:
        df_bb = pd.DataFrame(bouncebacks)
        df_bb["cid_str"] = (df_bb.get("contactID", df_bb.get("ContactId"))).astype(str)
        df_bb["asset_id_str"] = (df_bb.get("emailID", df_bb.get("AssetId", df_bb.get("assetId")))).astype(str)
        df_bb = df_bb.dropna(subset=["cid_str", "asset_id_str"])
        
        df_bb['hard'] = (df_bb['isHardBounceback'] == True).astype(int)
        df_bb['soft'] = (df_bb['isHardBounceback'] == False).astype(int)
        df_bb['total_bb'] = 1
        
        bb_key = ["asset_id_str", "cid_str"]
        df_bb_counts = df_bb.groupby(bb_key)[['hard', 'soft', 'total_bb']].sum().reset_index()
        
        df_sends = df_sends.merge(df_bb_counts, left_on=["assetId_str", "contactId_str"], right_on=bb_key, how="left")
        print(f"[PERF_DEBUG] Step 3: BOUNCEBACKS DataFrame merged in {time.time() - pd_step_start:.2f}s.")
    else:
        df_sends["hard"] = 0
        df_sends["soft"] = 0
        df_sends["total_bb"] = 0
        print("[PERF_DEBUG] Step 3: Skipped BOUNCEBACKS (no data).")

    # 4. Load and Aggregate CLICKS
    pd_step_start = time.time()
    if email_clickthroughs:
        df_clicks = pd.DataFrame(email_clickthroughs)
        df_clicks["cid_str"] = df_clicks["contactID"].astype(str)
        df_clicks["asset_id_str"] = df_clicks["emailID"].astype(str)
        
        click_key = ["asset_id_str", "cid_str"]
        df_click_counts = df_clicks.groupby(click_key).size().to_frame("total_clicks").reset_index()
        
        df_sends = df_sends.merge(df_click_counts, left_on=["assetId_str", "contactId_str"], right_on=click_key, how="left")
        print(f"[PERF_DEBUG] Step 4: CLICKS DataFrame merged in {time.time() - pd_step_start:.2f}s.")
    else:
        df_sends["total_clicks"] = 0
        print("[PERF_DEBUG] Step 4: Skipped CLICKS (no data).")

    # 5. Load and Aggregate OPENS
    pd_step_start = time.time()
    if email_opens:
        df_opens = pd.DataFrame(email_opens)
        df_opens["cid_str"] = df_opens["contactID"].astype(str)
        df_opens["asset_id_str"] = df_opens["emailID"].astype(str)
        
        open_key = ["asset_id_str", "cid_str"]
        df_open_counts = df_opens.groupby(open_key).size().to_frame("total_opens").reset_index()
        
        df_sends = df_sends.merge(df_open_counts, left_on=["assetId_str", "contactId_str"], right_on=open_key, how="left")
        print(f"[PERF_DEBUG] Step 5: OPENS DataFrame merged in {time.time() - pd_step_start:.2f}s.")
    else:
        df_sends["total_opens"] = 0
        print("[PERF_DEBUG] Step 5: Skipped OPENS (no data).")

    # Fill NaNs from merges with 0
    pd_step_start = time.time()
    fill_cols = ['hard', 'soft', 'total_bb', 'total_clicks', 'total_opens']
    for col in fill_cols:
        if col in df_sends.columns:
            df_sends[col] = df_sends[col].fillna(0).astype(int)
    print(f"[PERF_DEBUG] Step 5b: NaNs filled in {time.time() - pd_step_start:.2f}s.")
    

    # 6. Load and Merge ENRICHMENT Data
    # --- THIS STEP IS NOW REMOVED ---
    print(f"[PERF_DEBUG] Step 6: Skipped CONTACTS merge (data already included in sends).")


    # 7. Apply Final Logic and Mappings
    pd_step_start = time.time()
    
    # Vectorized user lookup
    def get_user(campaign_id):
        try:
            campaign = campaign_map.get(int(campaign_id), {})
            return user_map.get(campaign.get("campaignCreatedByUserId"), "")
        except (ValueError, TypeError):
            return ""
    
    df_sends["Last Activated by User"] = df_sends["campaignId"].apply(get_user)
    
    # Filter out rows *after* getting user (as in original logic)
    df_sends = df_sends[df_sends["Last Activated by User"] != ""].copy()
    if df_sends.empty:
        logger.warning("No sends found associated with a valid user. Aborting.")
        return None
    print(f"[PERF_DEBUG] Filtered for valid users, {len(df_sends)} rows remaining.")
        
    # Filter @hp.com (as in original logic)
    df_sends = df_sends[~df_sends["emailAddress"].str.lower().str.contains("@hp.com", na=False)]
    print(f"[PERF_DEBUG] Filtered @hp.com emails, {len(df_sends)} rows remaining.")

    # Map Email Group
    df_sends["Email Group"] = df_sends["assetId_int"].map(email_group_map).fillna("")
    
    # Final Calculations
    df_sends["Total Delivered"] = (df_sends["total_bb"] == 0).astype(int)
    df_sends["Total Sends"] = 1
    df_sends["Unique Opens"] = (df_sends["total_opens"] > 0).astype(int)
    df_sends["Unique Clicks"] = (df_sends["total_clicks"] > 0).astype(int)
    
    # Rates (matching original logic)
    df_sends["Hard Bounceback Rate"] = df_sends["hard"] * 100
    df_sends["Soft Bounceback Rate"] = df_sends["soft"] * 100
    df_sends["Bounceback Rate"] = df_sends["total_bb"] * 100
    df_sends["Delivered Rate"] = df_sends["Total Delivered"] * 100
    df_sends["Unique Open Rate"] = df_sends["Unique Opens"] * 100
    df_sends["Clickthrough Rate"] = df_sends["total_clicks"] * 100 # This was original logic
    df_sends["Unique Clickthrough Rate"] = df_sends["Unique Clicks"] * 100
    print(f"[PERF_DEBUG] Step 7: Final logic and calculations applied in {time.time() - pd_step_start:.2f}s.")

    # Rename and select final columns
    pd_step_start = time.time()
    
    # --- MODIFIED: Map new contact field names from the EmailSend export ---
    final_column_map = {
        "assetName": "Email Name",
        "assetId_str": "Email ID",
        "subjectLine": "Email Subject Line",
        "Last Activated by User": "Last Activated by User",
        "Total Delivered": "Total Delivered",
        "hard": "Total Hard Bouncebacks",
        "Total Sends": "Total Sends",
        "soft": "Total Soft Bouncebacks",
        "total_bb": "Total Bouncebacks",
        "Unique Opens": "Unique Opens",
        "Hard Bounceback Rate": "Hard Bounceback Rate",
        "Soft Bounceback Rate": "Soft Bounceback Rate",
        "Bounceback Rate": "Bounceback Rate",
        "Clickthrough Rate": "Clickthrough Rate",
        "Unique Clickthrough Rate": "Unique Clickthrough Rate",
        "Delivered Rate": "Delivered Rate",
        "Unique Open Rate": "Unique Open Rate",
        "Email Group": "Email Group",
        "activityDateParsed": "Email Send Date",
        "emailAddress": "Email Address",
        
        # Map the new fields from the bulk export
        # These keys MUST match the keys in COMBINED_EMAIL_SEND_FIELDS
        "contact_country": "Contact Country",
        "contact_hp_role": "HP Role",
        "contact_hp_partner_id": "HP Partner Id",
        "contact_partner_name": "Partner Name",
        "contact_market": "Market"
    }
    
    df_report = df_sends.rename(columns=final_column_map)
    
    # Ensure all columns exist, adding any missing ones with None
    final_columns_ordered = []
    for col_name in final_column_map.values():
        if col_name not in df_report.columns:
            df_report[col_name] = None
        final_columns_ordered.append(col_name)
            
    df_report = df_report[final_columns_ordered]
    
    # Format date
    df_report["Email Send Date"] = df_report["Email Send Date"].dt.strftime("%Y-%m-%d %I:%M:%S %p")
    # Lowercase email
    df_report["Email Address"] = df_report["Email Address"].str.lower()
    print(f"[PERF_DEBUG] Step 8: Final column renaming and formatting in {time.time() - pd_step_start:.2f}s.")
    
    # --- END OF PANDAS REWRITE ---
    processing_end_time = time.time()
    logger.info("Processed %d report rows in %.2f seconds.", len(df_report), processing_end_time - processing_start_time)

    output_file = f"data/{target_date}.csv"
    
    # Sanitize and save using pandas, matching original save_csv format
    pd_step_start = time.time()
    df_report = sanitize_dataframe_for_csv(df_report)
    df_report.to_csv(
        output_file, 
        sep="\t", # Use tab delimiter
        index=False, 
        encoding="utf-8-sig", # Use utf-8-sig
        quoting=csv.QUOTE_MINIMAL # Match DictWriter default
    )
    print(f"[PERF_DEBUG] Step 9: Sanitized and saved final CSV to {output_file} in {time.time() - pd_step_start:.2f}s.")

    end_time = time.time()
    logger.info("Daily report generation for %s completed in %.2f seconds. Report saved to %s", target_date, end_time - start_time, output_file)
    return output_file