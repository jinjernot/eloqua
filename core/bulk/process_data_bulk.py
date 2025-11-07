import time
import requests
from dateutil import parser
import logging
import pandas as pd
import csv
from core.bulk.fetch_data_bulk import fetch_and_save_data
# --- 1. IMPORT THE CONTACT FETCHER ---
from core.bulk.bulk_contacts import batch_fetch_contacts_bulk

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
    Applies sanitization rules (like removing newlines) to string columns.
    """
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            # Explicitly cast to string to handle mixed types before .str
            df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False).str.replace('\r', ' ', regex=False).str.strip()
    return df

def generate_daily_report(target_date):
    start_time = time.time()
    logger.info("Starting daily report generation for %s", target_date)

    data = fetch_data_with_retries(lambda: fetch_and_save_data(target_date))
    if not data:
        logger.error("Failed to fetch data after retries.")
        return None
    
    print("[PERF_DEBUG] Data fetch complete. Starting pandas processing...")

    target_date_obj = parser.parse(target_date).date()
    email_sends = data.get("email_sends", [])
    bouncebacks = data.get("bouncebacks", [])
    campaign_analysis = data.get("campaign_analysis", {}).get("value", [])
    campaign_users = data.get("campaign_users", {}).get("value", [])
    email_clickthroughs = data.get("email_clickthroughs", {}).get("value", [])
    email_opens = data.get("email_opens", {}).get("value", [])
    email_asset_data = data.get("email_asset_data", {}).get("value", [])
    
    logger.info("Fetched %d email sends (w/ contacts), %d bouncebacks, %d clicks, %d opens.",
                len(email_sends), len(bouncebacks), len(email_clickthroughs), len(email_opens))

    processing_start_time = time.time()

    # 1. Create helper maps (these are small and fast)
    pd_step_start = time.time()
    email_group_map = {int(item["emailID"]): item.get("emailGroup", "") for item in email_asset_data if item.get("emailID")}
    campaign_map = {c.get("eloquaCampaignId"): c for c in campaign_analysis if c.get("eloquaCampaignId")}
    user_map = {u.get("userID"): u.get("userName", "") for u in campaign_users if u.get("userID")}
    print(f"[PERF_DEBUG] Step 1: Helper maps created in {time.time() - pd_step_start:.2f}s.")

    # 2. Load SENDS DataFrame
    pd_step_start = time.time()
    
    if not email_sends:
        logger.warning("No email sends found for this day. Report will be activity-based.")
        df_sends = pd.DataFrame() # Create empty DF if no sends
    else:
        unique_sends_dict = {}
        for s in email_sends:
            key = (
                str(s.get("assetId")), 
                str(s.get("contactId")),
                str(s.get("emailSendType"))
            )
            unique_sends_dict[key] = s

        df_sends = pd.DataFrame(list(unique_sends_dict.values()))
        
        # Filter sends by target date *early*
        df_sends["activityDateParsed"] = pd.to_datetime(df_sends["activityDate"], errors='coerce')
        df_sends = df_sends.dropna(subset=["activityDateParsed"]) # Drop rows that couldn't be parsed
        df_sends = df_sends[df_sends["activityDateParsed"].dt.date == target_date_obj].copy()
        if df_sends.empty:
            logger.warning("No email sends found for target date %s.", target_date)
    
    if not df_sends.empty:
        # Clean up key fields
        df_sends["contactId_str"] = df_sends["contactId"].astype(str)
        df_sends["assetId_str"] = df_sends["assetId"].astype(str)
        df_sends["assetId_int"] = pd.to_numeric(df_sends["assetId"], errors='coerce').fillna(0).astype(int)
    else:
        # Define columns to ensure merge works later
        df_sends = pd.DataFrame(columns=["contactId_str", "assetId_str", "assetId_int", "activityDateParsed", "activityDate",
                                         "assetName", "subjectLine", "campaignId", "emailAddress", "contact_country",
                                         "contact_hp_role", "contact_hp_partner_id", "contact_partner_name", "contact_market"])

    print(f"[PERF_DEBUG] Step 2: SENDS DataFrame created ({len(df_sends)} rows) in {time.time() - pd_step_start:.2f}s.")

    # 3. Load and Aggregate BOUNCEBACKS
    pd_step_start = time.time()
    df_bb_counts = None
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
        # RENAME KEYS for outer merge
        df_bb_counts = df_bb_counts.rename(columns={'asset_id_str': 'assetId_str', 'cid_str': 'contactId_str'})
        print(f"[PERF_DEBUG] Step 3: BOUNCEBACKS DataFrame created in {time.time() - pd_step_start:.2f}s.")
    else:
        print("[PERF_DEBUG] Step 3: Skipped BOUNCEBACKS (no data).")


    # 4. Load and Aggregate CLICKS
    pd_step_start = time.time()
    df_click_counts = None
    if email_clickthroughs:
        df_clicks = pd.DataFrame(email_clickthroughs)
        df_clicks["cid_str"] = df_clicks["contactID"].astype(str)
        df_clicks["asset_id_str"] = df_clicks["emailID"].astype(str)
        
        click_key = ["asset_id_str", "cid_str"]
        df_click_counts = df_clicks.groupby(click_key).size().to_frame("total_clicks").reset_index()
        # RENAME KEYS for outer merge
        df_click_counts = df_click_counts.rename(columns={'asset_id_str': 'assetId_str', 'cid_str': 'contactId_str'})
        print(f"[PERF_DEBUG] Step 4: CLICKS DataFrame created in {time.time() - pd_step_start:.2f}s.")
    else:
        print("[PERF_DEBUG] Step 4: Skipped CLICKS (no data).")

    # 5. Load and Aggregate OPENS
    pd_step_start = time.time()
    df_open_counts = None
    if email_opens:
        df_opens = pd.DataFrame(email_opens)
        df_opens["cid_str"] = df_opens["contactID"].astype(str)
        df_opens["asset_id_str"] = df_opens["emailID"].astype(str)
        
        open_key = ["asset_id_str", "cid_str"]
        df_open_counts = df_opens.groupby(open_key).size().to_frame("total_opens").reset_index()
        # RENAME KEYS for outer merge
        df_open_counts = df_open_counts.rename(columns={'asset_id_str': 'assetId_str', 'cid_str': 'contactId_str'})
        print(f"[PERF_DEBUG] Step 5: OPENS DataFrame created in {time.time() - pd_step_start:.2f}s.")
    else:
        print("[PERF_DEBUG] Step 5: Skipped OPENS (no data).")


    # 5a. Perform OUTER Merges
    pd_step_start = time.time()
    
    # Start with df_sends as the "master" for enrichment data
    df_report = df_sends.copy()
    merge_keys = ["assetId_str", "contactId_str"]

    if df_bb_counts is not None:
        df_report = pd.merge(df_report, df_bb_counts, on=merge_keys, how="outer")
    
    if df_click_counts is not None:
        df_report = pd.merge(df_report, df_click_counts, on=merge_keys, how="outer")

    if df_open_counts is not None:
        df_report = pd.merge(df_report, df_open_counts, on=merge_keys, how="outer")
    
    df_sends = df_report 
    print(f"[PERF_DEBUG] Step 5a: All outer merges complete ({len(df_sends)} rows) in {time.time() - pd_step_start:.2f}s.")
    
    # 5b. Fill NaNs from merges with 0 (but NOT for total_bb)
    pd_step_start = time.time()
    
    # Clicks, Opens, and hard/soft bounces can be filled with 0
    fill_cols = ['total_clicks', 'total_opens', 'hard', 'soft']
    for col in fill_cols:
        if col in df_sends.columns:
            df_sends[col] = df_sends[col].fillna(0).astype(int)
    
    if 'total_bb' not in df_sends.columns and 'hard' in df_sends.columns:
        df_sends['total_bb'] = df_sends['hard'] + df_sends['soft']
    elif 'total_bb' not in df_sends.columns:
         df_sends['total_bb'] = pd.NA # Use pd.NA for "unknown"

    print(f"[PERF_DEBUG] Step 5b: NaNs filled (leaving total_bb) in {time.time() - pd_step_start:.2f}s.")
    
    # --- 6. NEW ENRICHMENT STEP ---
    pd_step_start = time.time()
    
    # 6a. Build enrichment maps from the 'send' data we already have
    # (This fills in asset data for activities that share an assetId with a send)
    asset_name_map = df_sends.dropna(subset=['assetName']).set_index('assetId_str')['assetName'].to_dict()
    subject_line_map = df_sends.dropna(subset=['subjectLine']).set_index('assetId_str')['subjectLine'].to_dict()

    df_sends['assetName'] = df_sends['assetName'].fillna(df_sends['assetId_str'].map(asset_name_map))
    df_sends['subjectLine'] = df_sends['subjectLine'].fillna(df_sends['assetId_str'].map(subject_line_map))
    
    # 6b. Identify and fetch missing *contact* data
    missing_contact_mask = pd.isna(df_sends['emailAddress'])
    contact_ids_to_fetch = df_sends.loc[missing_contact_mask, 'contactId_str'].dropna().unique()
    
    if len(contact_ids_to_fetch) > 0:
        print(f"[PERF_DEBUG] Found {len(contact_ids_to_fetch)} contacts from activities to enrich...")
        # Assuming batch_fetch_contacts_bulk returns a list of dicts
        # and that CONTACT_FIELDS in config.py includes these fields
        new_contact_data = batch_fetch_contacts_bulk(list(contact_ids_to_fetch))
        
        # Build maps for each field
        email_map = {}
        country_map = {}
        role_map = {}
        partner_id_map = {}
        partner_name_map = {}
        market_map = {}

        for c in new_contact_data:
            cid_str = str(c.get('id'))
            email_map[cid_str] = c.get('emailAddress')
            country_map[cid_str] = c.get('C_Country')
            role_map[cid_str] = c.get('C_HP_Role1')
            partner_id_map[cid_str] = c.get('C_HP_PartnerID1')
            partner_name_map[cid_str] = c.get('C_Partner_Name1')
            market_map[cid_str] = c.get('C_Market1')
            
        # 6c. Fill in missing contact data
        df_sends['emailAddress'] = df_sends['emailAddress'].fillna(df_sends['contactId_str'].map(email_map))
        df_sends['contact_country'] = df_sends['contact_country'].fillna(df_sends['contactId_str'].map(country_map))
        df_sends['contact_hp_role'] = df_sends['contact_hp_role'].fillna(df_sends['contactId_str'].map(role_map))
        df_sends['contact_hp_partner_id'] = df_sends['contact_hp_partner_id'].fillna(df_sends['contactId_str'].map(partner_id_map))
        df_sends['contact_partner_name'] = df_sends['contact_partner_name'].fillna(df_sends['contactId_str'].map(partner_name_map))
        df_sends['contact_market'] = df_sends['contact_market'].fillna(df_sends['contactId_str'].map(market_map))
        
        print(f"[PERF_DEBUG] Enrichment complete in {time.time() - pd_step_start:.2f}s.")
    else:
        print("[PERF_DEBUG] Step 6: No missing contact data to enrich.")


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
    
    print(f"[PERF_DEBUG] Skipping user filter, keeping all {len(df_sends)} rows.")
        
    # Add .copy() here to prevent SettingWithCopyWarning
    df_sends = df_sends[~df_sends["emailAddress"].str.lower().str.contains("@hp.com", na=False)].copy()
    print(f"[PERF_DEBUG] Filtered @hp.com emails, {len(df_sends)} rows remaining.")

    if 'assetId_int' not in df_sends.columns and 'assetId_str' in df_sends.columns:
         df_sends["assetId_int"] = pd.to_numeric(df_sends["assetId_str"], errors='coerce').fillna(0).astype(int)
    elif 'assetId_int' in df_sends.columns:
         df_sends["assetId_int"] = df_sends["assetId_int"].fillna(0).astype(int)

    df_sends["Email Group"] = df_sends["assetId_int"].map(email_group_map) # .fillna("") handled later

    def calc_delivered(total_bb):
        if pd.isna(total_bb):
            return pd.NA
        return 1 if total_bb == 0 else 0

    df_sends["Total Delivered"] = df_sends["total_bb"].apply(calc_delivered).astype('Int64')

    df_sends["Total Sends"] = (~df_sends["activityDateParsed"].isna()).astype(int)
    
    df_sends["Unique Opens"] = (df_sends["total_opens"] > 0).astype(int)
    df_sends["Unique Clicks"] = (df_sends["total_clicks"] > 0).astype(int)
    
    df_sends['hard'] = df_sends['hard'].fillna(0)
    df_sends['soft'] = df_sends['soft'].fillna(0)
    
    df_sends["Hard Bounceback Rate"] = df_sends["hard"] * 100
    df_sends["Soft Bounceback Rate"] = df_sends["soft"] * 100
    
    df_sends["Bounceback Rate"] = (df_sends["total_bb"].fillna(0) > 0).astype(int) * 100 # Show 100 or 0
    df_sends["Delivered Rate"] = df_sends["Total Delivered"].fillna(0).astype(int) * 100 # fillna(0) for calc
    
    df_sends["Unique Open Rate"] = df_sends["Unique Opens"] * 100
    df_sends["Clickthrough Rate"] = df_sends["total_clicks"] * 100 
    df_sends["Unique Clickthrough Rate"] = df_sends["Unique Clicks"] * 100
    print(f"[PERF_DEBUG] Step 7: Final logic and calculations applied in {time.time() - pd_step_start:.2f}s.")

    pd_step_start = time.time()
    
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
        "contact_country": "Contact Country",
        "contact_hp_role": "HP Role",
        "contact_hp_partner_id": "HP Partner Id",
        "contact_partner_name": "Partner Name",
        "contact_market": "Market"
    }
    
    df_report = df_sends.rename(columns=final_column_map)
    
    final_columns_ordered = []
    for col_name in final_column_map.values():
        if col_name not in df_report.columns:
            df_report[col_name] = pd.NA # Use pd.NA
        final_columns_ordered.append(col_name)
            
    df_report = df_report[final_columns_ordered]
    
    df_report["Email Send Date"] = pd.to_datetime(df_report["Email Send Date"]).dt.strftime("%Y-%m-%d %I:%M:%S %p")
    
    df_report["Email Address"] = df_report["Email Address"].str.lower()
    print(f"[PERF_DEBUG] Step 8: Final column renaming and formatting in {time.time() - pd_step_start:.2f}s.")
    
    processing_end_time = time.time()
    logger.info("Processed %d report rows in %.2f seconds.", len(df_report), processing_end_time - processing_start_time)

    output_file = f"data/{target_date}.csv"
    
    pd_step_start = time.time()
    
    # 9. SAVING LOGIC
    
    # 9a. Sanitize strings (remove newlines, etc.)
    df_report = sanitize_dataframe_for_csv(df_report)
    
    # 9b. Fill NaNs for numeric counts that should be 0, not blank
    fill_zero_cols = [
        'Total Hard Bouncebacks', 'Total Soft Bouncebacks', 
        'Total Bouncebacks', 'Unique Opens', 'Unique Clicks', 
        'Total Sends'
    ]
    for col in fill_zero_cols:
        if col in df_report.columns:
            df_report[col] = df_report[col].fillna(0).astype(int)

    # 9c. Replace "NaT" from blank dates
    df_report["Email Send Date"] = df_report["Email Send Date"].replace("NaT", "")
    
    # 9d. fillna("") on non-numeric columns
    for col in df_report.select_dtypes(include=['object', 'string']).columns:
        df_report[col] = df_report[col].fillna("")

    # 9e. Save to CSV, using na_rep=""
    df_report.to_csv(
        output_file, 
        sep="\t",
        index=False, 
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL,
        na_rep="" # This handles pd.NA in Int64 columns
    )
    print(f"[PERF_DEBUG] Step 9: Sanitized and saved final CSV to {output_file} in {time.time() - pd_step_start:.2f}s.")

    end_time = time.time()
    logger.info("Daily report generation for %s completed in %.2f seconds. Report saved to %s", target_date, end_time - start_time, output_file)
    return output_file