import time
import requests
from dateutil import parser
import logging
import pandas as pd
import csv
import os
from core.bulk.fetch_data_bulk import fetch_and_save_data

from core.rest.fetch_email_content import fetch_email_html 

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
            df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False).str.replace('\r', ' ', regex=False).str.strip()
        elif pd.api.types.is_float_dtype(df[col]):
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

    # 1. Create helper maps
    pd_step_start = time.time()
    
    # Debug: check what fields are available in email_asset_data
    if email_asset_data and len(email_asset_data) > 0:
        sample_fields = list(email_asset_data[0].keys())
        logger.info(f"Email asset data fields: {sample_fields}")
    
    email_group_map = {int(item["emailID"]): item.get("emailGroup", "") for item in email_asset_data if item.get("emailID")}
    email_name_map = {int(item["emailID"]): item.get("emailName", "") for item in email_asset_data if item.get("emailID")}
    
    # Try multiple possible field names for subject
    email_subject_map = {}
    for item in email_asset_data:
        if item.get("emailID"):
            email_id = int(item["emailID"])
            subject = item.get("subject") or item.get("emailSubject") or item.get("subjectLine") or item.get("Subject") or ""
            email_subject_map[email_id] = subject
    
    logger.info(f"Built email_subject_map with {len(email_subject_map)} entries, sample: {list(email_subject_map.items())[:3]}")
    
    campaign_map = {c.get("eloquaCampaignId"): c for c in campaign_analysis if c.get("eloquaCampaignId")}
    user_map = {u.get("userID"): u.get("userName", "") for u in campaign_users if u.get("userID")}
    
    # Create contact lookup from email_sends for forwarded emails
    contact_lookup = {}
    for send in email_sends:
        cid = str(send.get("contactId", ""))
        if cid and cid not in contact_lookup:
            contact_lookup[cid] = {
                "emailAddress": send.get("emailAddress", ""),
                "contact_country": send.get("contact_country", ""),
                "contact_hp_role": send.get("contact_hp_role", ""),
                "contact_hp_partner_id": send.get("contact_hp_partner_id", ""),
                "contact_partner_name": send.get("contact_partner_name", ""),
                "contact_market": send.get("contact_market", "")
            }
    
    print(f"[PERF_DEBUG] Step 1: Helper maps created ({len(contact_lookup)} contacts) in {time.time() - pd_step_start:.2f}s.")

    # 2. Load SENDS DataFrame
    pd_step_start = time.time()
    
    if not email_sends:
        logger.warning("No email sends found. Aborting report.")
        return None

    unique_sends_dict = {}
    for s in email_sends:
        key = (
            str(s.get("assetId")), 
            str(s.get("contactId")),
            str(s.get("emailSendType"))
        )
        unique_sends_dict[key] = s

    df_sends = pd.DataFrame(list(unique_sends_dict.values()))
    
    # Filter sends by target date
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

    # 3. Load bouncebacks
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

    # 4. Load Clicks
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

    # 5. Load Open
    pd_step_start = time.time()
    if email_opens:
        df_opens = pd.DataFrame(email_opens)
        df_opens["cid_str"] = df_opens["contactID"].astype(str)
        df_opens["asset_id_str"] = df_opens["emailID"].astype(str)
        df_opens["emailAddress"] = df_opens.get("emailAddress", "")
        
        open_key = ["asset_id_str", "cid_str"]
        df_open_counts = df_opens.groupby(open_key).size().to_frame("total_opens").reset_index()
        
        df_sends = df_sends.merge(df_open_counts, left_on=["assetId_str", "contactId_str"], right_on=open_key, how="left")
        print(f"[PERF_DEBUG] Step 5: OPENS DataFrame merged in {time.time() - pd_step_start:.2f}s.")
    else:
        df_sends["total_opens"] = 0
        df_opens = pd.DataFrame()
        print("[PERF_DEBUG] Step 5: Skipped OPENS (no data).")

    # Fill NaNs from merges with 0
    pd_step_start = time.time()
    fill_cols = ['hard', 'soft', 'total_bb', 'total_clicks', 'total_opens']
    for col in fill_cols:
        if col in df_sends.columns:
            df_sends[col] = df_sends[col].fillna(0).astype(int)
    print(f"[PERF_DEBUG] Step 5b: NaNs filled in {time.time() - pd_step_start:.2f}s.")
    
    # 5c. Identify forwarded emails (opens/clicks without sends) - VECTORIZED APPROACH
    pd_step_start = time.time()
    
    # Combine opens and clicks to find all engagement without sends
    engagement_list = []
    
    # Process opens
    if not df_opens.empty:
        df_opens_filtered = df_opens.copy()
        df_opens_filtered["activityDateParsed"] = pd.to_datetime(df_opens_filtered.get("openDateHour", ""), errors='coerce')
        df_opens_filtered = df_opens_filtered[df_opens_filtered["activityDateParsed"].notna()]
        df_opens_filtered = df_opens_filtered[df_opens_filtered["activityDateParsed"].dt.date == target_date_obj]
        
        if not df_opens_filtered.empty:
            # Keep emailAddress if available
            cols_to_keep = ["asset_id_str", "cid_str", "openDateHour"]
            if "emailAddress" in df_opens_filtered.columns:
                cols_to_keep.append("emailAddress")
            
            engagement_list.append(df_opens_filtered[cols_to_keep].rename(
                columns={"openDateHour": "activityDate"}
            ))
    
    # Process clicks
    if email_clickthroughs and not df_clicks.empty:
        df_clicks_filtered = df_clicks.copy()
        df_clicks_filtered["activityDateParsed"] = pd.to_datetime(df_clicks_filtered.get("clickDateHour", ""), errors='coerce')
        df_clicks_filtered = df_clicks_filtered[df_clicks_filtered["activityDateParsed"].notna()]
        df_clicks_filtered = df_clicks_filtered[df_clicks_filtered["activityDateParsed"].dt.date == target_date_obj]
        
        if not df_clicks_filtered.empty:
            # Keep emailAddress if available
            cols_to_keep = ["asset_id_str", "cid_str", "clickDateHour"]
            if "emailAddress" in df_clicks_filtered.columns:
                cols_to_keep.append("emailAddress")
                
            engagement_list.append(df_clicks_filtered[cols_to_keep].rename(
                columns={"clickDateHour": "activityDate"}
            ))
    
    if engagement_list:
        # Combine all engagement
        df_engagement = pd.concat(engagement_list, ignore_index=True)
        df_engagement = df_engagement.drop_duplicates(subset=["asset_id_str", "cid_str"])
        
        # Create merge key in both dataframes
        df_sends["merge_key"] = df_sends["assetId_str"] + "_" + df_sends["contactId_str"]
        df_engagement["merge_key"] = df_engagement["asset_id_str"] + "_" + df_engagement["cid_str"]
        
        # Find engagement without sends using anti-join
        df_forwarded = df_engagement[~df_engagement["merge_key"].isin(df_sends["merge_key"])].copy()
        
        # Clean up merge_key column
        df_sends.drop(columns=["merge_key"], inplace=True)
        
        if not df_forwarded.empty:
            # Build forwarded email records
            df_forwarded = df_forwarded.rename(columns={"asset_id_str": "assetId_str", "cid_str": "contactId_str"})
            df_forwarded["assetId"] = df_forwarded["assetId_str"]
            df_forwarded["contactId"] = df_forwarded["contactId_str"]
            df_forwarded["assetId_int"] = pd.to_numeric(df_forwarded["assetId"], errors='coerce').fillna(0).astype(int)
            
            # Keep activityDate as datetime object for Email Send Date
            # Convert to datetime and remove timezone info to match format of regular sends
            df_forwarded["activityDateParsed"] = pd.to_datetime(df_forwarded["activityDate"], errors='coerce', utc=True).dt.tz_localize(None)
            
            df_forwarded["emailSendType"] = "Forwarded"
            df_forwarded["assetName"] = ""
            df_forwarded["campaignId"] = ""
            df_forwarded["subjectLine"] = ""
            
            # Initialize contact fields
            df_forwarded["emailAddress"] = ""
            df_forwarded["contact_country"] = ""
            df_forwarded["contact_hp_role"] = ""
            df_forwarded["contact_hp_partner_id"] = ""
            df_forwarded["contact_partner_name"] = ""
            df_forwarded["contact_market"] = ""
            
            # Add engagement metrics
            df_forwarded = df_forwarded.merge(df_open_counts, left_on=["assetId_str", "contactId_str"], right_on=open_key, how="left")
            df_forwarded = df_forwarded.merge(df_click_counts, left_on=["assetId_str", "contactId_str"], right_on=click_key, how="left")
            
            # Forwarded emails have no sends or bouncebacks
            df_forwarded["hard"] = 0
            df_forwarded["soft"] = 0
            df_forwarded["total_bb"] = 0
            df_forwarded["total_opens"] = df_forwarded["total_opens"].fillna(0).astype(int)
            df_forwarded["total_clicks"] = df_forwarded["total_clicks"].fillna(0).astype(int)
            
            # Enrich forwarded emails with asset and contact information using pre-built maps
            df_forwarded["assetName"] = df_forwarded["assetId_int"].map(email_name_map).fillna("")
            df_forwarded["subjectLine"] = df_forwarded["assetId_int"].map(email_subject_map).fillna("")
            
            # Enrich contact information from contact_lookup dictionary
            def get_contact_field(contact_id, field):
                contact = contact_lookup.get(str(contact_id), {})
                return contact.get(field, "")
            
            # For emailAddress, keep the one from engagement data if available, otherwise use contact_lookup
            if "emailAddress" not in df_forwarded.columns:
                df_forwarded["emailAddress"] = ""
            
            df_forwarded["emailAddress_from_lookup"] = df_forwarded["contactId_str"].apply(lambda x: get_contact_field(x, "emailAddress"))
            df_forwarded["emailAddress"] = df_forwarded.apply(
                lambda row: row["emailAddress"] if pd.notna(row["emailAddress"]) and row["emailAddress"] != "" 
                else row["emailAddress_from_lookup"], 
                axis=1
            )
            df_forwarded.drop(columns=["emailAddress_from_lookup"], inplace=True)
            
            df_forwarded["contact_country"] = df_forwarded["contactId_str"].apply(lambda x: get_contact_field(x, "contact_country"))
            df_forwarded["contact_hp_role"] = df_forwarded["contactId_str"].apply(lambda x: get_contact_field(x, "contact_hp_role"))
            df_forwarded["contact_hp_partner_id"] = df_forwarded["contactId_str"].apply(lambda x: get_contact_field(x, "contact_hp_partner_id"))
            df_forwarded["contact_partner_name"] = df_forwarded["contactId_str"].apply(lambda x: get_contact_field(x, "contact_partner_name"))
            df_forwarded["contact_market"] = df_forwarded["contactId_str"].apply(lambda x: get_contact_field(x, "contact_market"))
            
            # Log how many contacts are missing email addresses
            missing_count = (df_forwarded["emailAddress"] == "").sum()
            if missing_count > 0:
                logger.info(f"{missing_count} forwarded email contacts have no email address (new contacts not in sends)")
            
            # Append forwarded emails to sends
            df_sends = pd.concat([df_sends, df_forwarded], ignore_index=True)
            print(f"[PERF_DEBUG] Step 5c: Added {len(df_forwarded)} forwarded email records in {time.time() - pd_step_start:.2f}s.")
        else:
            print(f"[PERF_DEBUG] Step 5c: No forwarded emails found in {time.time() - pd_step_start:.2f}s.")
    else:
        print(f"[PERF_DEBUG] Step 5c: No engagement data to check for forwards in {time.time() - pd_step_start:.2f}s.")
    

    # 6. Load and Merge Data
    print(f"[PERF_DEBUG] Step 6: Skipped CONTACTS merge (data already included in sends).")

    # Temporarily disabled for faster testing
    logger.info("Skipping HTML email download (disabled for testing).")
    # html_fetch_start = time.time()
    # if not df_sends.empty:
    #     # Get all unique, valid email asset IDs
    #     unique_email_ids = df_sends['assetId_str'].dropna().unique()
    #     
    #     logger.info(f"Found {len(unique_email_ids)} unique email assets to fetch HTML for.")
    #     
    #     fetched_count = 0
    #     # You could use a ThreadPoolExecutor here if you have many, but be wary of API rate limits
    #     for email_id in unique_email_ids:
    #         # Ensure it's a valid ID and not 'nan' or empty
    #         if email_id and pd.notna(email_id) and str(email_id).strip():
    #             try:
    #                 # Use the "email_downloads" folder as requested
    #                 save_directory = os.path.join("data", "email_downloads", target_date)
    #                 fetch_email_html(str(email_id), save_dir=save_directory)
    #                 fetched_count += 1
    #             except Exception as e:
    #                 logger.error(f"Error fetching HTML for asset {email_id}: {e}")
    #     
    #     logger.info(f"Fetched HTML for {fetched_count} emails in {time.time() - html_fetch_start:.2f}s.")
    # else:
    #     logger.info("Skipping HTML fetch as there were no sends.")
    # --- END NEW STEP ---


    # 7. Apply Final Logic and Mappings
    pd_step_start = time.time()
    
    # Vectorized user lookup
    def get_user(campaign_id):
        try:
            campaign = campaign_map.get(int(campaign_id), {})
            return user_map.get(campaign.get("campaignCreatedByUserId"), "")
        except (ValueError, TypeError):
            return ""
    
    # Create asset-to-user lookup from regular sends for forwarded emails
    asset_user_map = {}
    for _, row in df_sends[df_sends["emailSendType"] != "Forwarded"].iterrows():
        asset_id = str(row.get("assetId_str", ""))
        campaign_id = row.get("campaignId", "")
        if asset_id and campaign_id and asset_id not in asset_user_map:
            user = get_user(campaign_id)
            if user:
                asset_user_map[asset_id] = user
    
    logger.info(f"Built asset_user_map with {len(asset_user_map)} entries for forwarded email user lookup")
    
    # Apply user lookup - for regular sends use campaign, for forwarded use asset lookup
    def get_user_for_row(row):
        if row["emailSendType"] == "Forwarded":
            asset_id = str(row["assetId_str"])
            user = asset_user_map.get(asset_id, "")
            if not user:
                logger.debug(f"No user found for forwarded email assetId {asset_id}")
            return user
        else:
            return get_user(row["campaignId"])
    
    df_sends["Last Activated by User"] = df_sends.apply(get_user_for_row, axis=1)
    
    # Debug: Check user population for forwarded emails
    forwarded_mask = df_sends["emailSendType"] == "Forwarded"
    users_populated = (df_sends[forwarded_mask]["Last Activated by User"] != "").sum()
    logger.info(f"Last Activated by User populated for {users_populated}/{forwarded_mask.sum()} forwarded emails")
    
    # df_sends = df_sends[df_sends["Last Activated by User"] != ""].copy()
    print(f"[PERF_DEBUG] Skipping user filter, keeping all {len(df_sends)} rows.")
        
    df_sends = df_sends[~df_sends["emailAddress"].str.lower().str.contains("@hp.com", na=False)]
    print(f"[PERF_DEBUG] Filtered @hp.com emails, {len(df_sends)} rows remaining.")

    df_sends["Email Group"] = df_sends["assetId_int"].map(email_group_map).fillna("")
    
    # For forwarded emails, Total Sends and Total Delivered should be blank/0
    df_sends["Total Sends"] = df_sends["emailSendType"].apply(lambda x: 0 if x == "Forwarded" else 1)
    df_sends["Total Delivered"] = df_sends.apply(
        lambda row: 0 if row["emailSendType"] == "Forwarded" else (1 if row["total_bb"] == 0 else 0), 
        axis=1
    )
    
    df_sends["Unique Opens"] = (df_sends["total_opens"] > 0).astype(int)
    df_sends["Unique Clicks"] = (df_sends["total_clicks"] > 0).astype(int)
    df_sends["Hard Bounceback Rate"] = df_sends["hard"] * 100
    df_sends["Soft Bounceback Rate"] = df_sends["soft"] * 100
    df_sends["Bounceback Rate"] = df_sends["total_bb"] * 100
    df_sends["Delivered Rate"] = df_sends["Total Delivered"] * 100
    df_sends["Unique Open Rate"] = df_sends["Unique Opens"] * 100
    df_sends["Clickthrough Rate"] = df_sends["total_clicks"] * 100 # This was original logic
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
        "contact_market": "Market",
        "emailSendType": "Email Send Type"
    }
    
    df_report = df_sends.rename(columns=final_column_map)
    
    final_columns_ordered = []
    for col_name in final_column_map.values():
        if col_name not in df_report.columns:
            df_report[col_name] = None
        final_columns_ordered.append(col_name)
            
    df_report = df_report[final_columns_ordered]
    
    # Format Email Send Date - it's already a datetime, just needs formatting
    df_report["Email Send Date"] = df_report["Email Send Date"].dt.strftime("%Y-%m-%d %I:%M:%S %p")
    df_report["Email Address"] = df_report["Email Address"].str.lower()
    print(f"[PERF_DEBUG] Step 8: Final column renaming and formatting in {time.time() - pd_step_start:.2f}s.")
    
    processing_end_time = time.time()
    logger.info("Processed %d report rows in %.2f seconds.", len(df_report), processing_end_time - processing_start_time)

    output_file = f"data/{target_date}.csv"
    
    pd_step_start = time.time()
    df_report = sanitize_dataframe_for_csv(df_report)
    df_report.to_csv(
        output_file, 
        sep="\t",
        index=False, 
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL
    )
    print(f"[PERF_DEBUG] Step 9: Sanitized and saved final CSV to {output_file} in {time.time() - pd_step_start:.2f}s.")

    end_time = time.time()
    logger.info("Daily report generation for %s completed in %.2f seconds. Report saved to %s", target_date, end_time - start_time, output_file)
    return output_file