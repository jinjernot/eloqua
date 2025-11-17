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
    
    # Load contact cache to get proper-cased email addresses
    from core.rest.fetch_data import load_contact_cache
    contact_cache = load_contact_cache()
    logger.info(f"Loaded {len(contact_cache)} contacts from cache for email case restoration")
    
    # Create contact lookup from email_sends for forwarded emails
    contact_lookup = {}
    debug_sample_count = 0
    for send in email_sends:
        cid = str(send.get("contactId", ""))
        if cid and cid not in contact_lookup:
            # Get proper-cased email from cache if available
            cached_contact = contact_cache.get(cid, {})
            bulk_email = send.get("emailAddress", "")
            proper_cased_email = cached_contact.get("emailAddress", bulk_email)
            
            # Debug: Print first 3 cases where we restored case
            if debug_sample_count < 3 and proper_cased_email != bulk_email:
                print(f"[DEBUG] Contact {cid}: Bulk='{bulk_email}' → Cache='{proper_cased_email}'")
                debug_sample_count += 1
            
            contact_lookup[cid] = {
                "emailAddress": proper_cased_email,  # Use cached email (proper case) if available
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
    
    # Log emailSendType distribution
    if "emailSendType" in df_sends.columns:
        send_type_counts = df_sends["emailSendType"].value_counts()
        logger.info(f"EmailSendType distribution (before date filter): {dict(send_type_counts)}")
    
    # Filter sends by target date
    df_sends["activityDateParsed"] = pd.to_datetime(df_sends["activityDate"], errors='coerce')
    df_sends = df_sends.dropna(subset=["activityDateParsed"]) # Drop rows that couldn't be parsed
    if df_sends.empty:
        logger.warning("No valid email sends with parseable dates. Aborting.")
        return None
    df_sends = df_sends[df_sends["activityDateParsed"].dt.date == target_date_obj].copy()
    if df_sends.empty:
        logger.warning("No email sends found for target date %s. Aborting.", target_date)
        return None
    
    # Log emailSendType distribution after date filter
    if "emailSendType" in df_sends.columns:
        send_type_counts_after = df_sends["emailSendType"].value_counts()
        logger.info(f"EmailSendType distribution (after date filter): {dict(send_type_counts_after)}")
    
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
    
    # 5c. Detect forwarded emails (opens/clicks without sends)
    pd_step_start = time.time()
    forward_contacts = set()
    
    # Get set of campaigns (asset IDs) that had sends on the target date
    campaigns_with_sends = set(df_sends['assetId_str'].unique())
    
    # Find contacts who opened or clicked but didn't receive the email
    if not df_opens.empty:
        opens_set = set(zip(df_opens['asset_id_str'], df_opens['cid_str']))
        sends_set = set(zip(df_sends['assetId_str'], df_sends['contactId_str']))
        forward_contacts.update(opens_set - sends_set)
    
    if not df_clicks.empty:
        clicks_set = set(zip(df_clicks['asset_id_str'], df_clicks['cid_str']))
        sends_set = set(zip(df_sends['assetId_str'], df_sends['contactId_str']))
        forward_contacts.update(clicks_set - sends_set)
    
    if forward_contacts:
        forward_rows = []
        for asset_id, contact_id in forward_contacts:
            # Only include forwards for campaigns that had sends on this date
            # This matches Eloqua Analytics behavior
            if asset_id not in campaigns_with_sends:
                continue
                
            # Get contact info from contact_lookup
            contact_info = contact_lookup.get(contact_id, {})
            
            # Get email campaign info from asset maps
            asset_id_int = int(asset_id) if asset_id and str(asset_id).isdigit() else -1
            email_name = email_name_map.get(asset_id_int, "")
            email_group = email_group_map.get(asset_id_int, "")
            subject_line = email_subject_map.get(asset_id_int, "")
            
            # Get opens and clicks for this forward
            opens_count = 0
            clicks_count = 0
            
            if not df_opens.empty:
                opens_count = len(df_opens[(df_opens['asset_id_str'] == asset_id) & 
                                          (df_opens['cid_str'] == contact_id)])
            
            if not df_clicks.empty:
                clicks_count = len(df_clicks[(df_clicks['asset_id_str'] == asset_id) & 
                                            (df_clicks['cid_str'] == contact_id)])
            
            forward_rows.append({
                'assetId_str': asset_id,
                'contactId_str': contact_id,
                'assetName': email_name,
                'emailGroup': email_group,
                'subjectLine': subject_line,
                'emailAddress': contact_info.get('emailAddress', ''),
                'contact_country': contact_info.get('contact_country', ''),
                'contact_hp_role': contact_info.get('contact_hp_role', ''),
                'contact_hp_partner_id': contact_info.get('contact_hp_partner_id', ''),
                'contact_partner_name': contact_info.get('contact_partner_name', ''),
                'contact_market': contact_info.get('contact_market', ''),
                'emailSendType': 'EmailForward',
                'total_opens': opens_count,
                'total_clicks': clicks_count,
                'hard': 0,
                'soft': 0,
                'total_bb': 0
            })
        
        if forward_rows:
            df_forwards = pd.DataFrame(forward_rows)
            df_sends = pd.concat([df_sends, df_forwards], ignore_index=True)
            print(f"[PERF_DEBUG] Step 5c: Detected {len(forward_rows)} forwarded emails (anti-join) in {time.time() - pd_step_start:.2f}s.")
        else:
            print(f"[PERF_DEBUG] Step 5c: No forwarded emails detected in {time.time() - pd_step_start:.2f}s.")
    else:
        print(f"[PERF_DEBUG] Step 5c: No forwarded emails detected in {time.time() - pd_step_start:.2f}s.")
    

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
    
    # Create asset-to-user lookup for forwarded emails
    # First, populate from email_asset_data (email creators)
    asset_user_map = {}
    for asset in email_asset_data:
        asset_id = str(asset.get("emailID", ""))
        user_id = asset.get("emailCreatedByUserID")
        if asset_id and user_id:
            user = user_map.get(user_id, "")
            if user:
                asset_user_map[asset_id] = user
    
    # Then, override with campaign user from regular sends if available (more specific)
    for _, row in df_sends[df_sends["emailSendType"] != "Forwarded"].iterrows():
        asset_id = str(row.get("assetId_str", ""))
        campaign_id = row.get("campaignId", "")
        if asset_id and campaign_id:
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
        
    # Filter out emails matching Eloqua Analytics exclusion criteria
    initial_count = len(df_sends)
    
    # Exclude @hp.com emails
    df_sends = df_sends[~df_sends["emailAddress"].str.lower().str.contains("@hp.com", na=False)]
    
    # Exclude specific test/spam email addresses from Eloqua Analytics filter
    excluded_emails = [
        "y_110@hotmail.com",
        "1021001399@qq.com",
        "2604815709@qq.com",
        "496707864@qq.com"
    ]
    df_sends = df_sends[~df_sends["emailAddress"].str.lower().isin([e.lower() for e in excluded_emails])]
    
    # Filter out incomplete forward records (forwards with no email address or no activity)
    # These are data artifacts where we detected activity but have no valid contact info
    before_forward_filter = len(df_sends)
    incomplete_forwards = (df_sends["emailSendType"] == "EmailForward") & (
        (df_sends["emailAddress"].isna()) | 
        (df_sends["emailAddress"] == "") |
        ((df_sends["total_opens"] == 0) & (df_sends["total_clicks"] == 0))  # Filter only if NO opens AND NO clicks
    )
    df_sends = df_sends[~incomplete_forwards]
    incomplete_count = before_forward_filter - len(df_sends)
    if incomplete_count > 0:
        print(f"[PERF_DEBUG] Filtered out {incomplete_count} incomplete forward records (no email address or no activity).")
    
    print(f"[PERF_DEBUG] Filtered @hp.com and excluded test emails, {len(df_sends)} rows remaining (removed {initial_count - len(df_sends)}).")

    # Restore proper email case from contact_lookup (Bulk API returns lowercase, but we want original case)
    def get_proper_cased_email(contact_id, lowercase_email):
        """Get the proper-cased email from contact_lookup, fallback to lowercase if not found"""
        contact = contact_lookup.get(str(contact_id), {})
        cached_email = contact.get("emailAddress", "")
        # Only use cached email if it matches (case-insensitive)
        if cached_email and cached_email.lower() == lowercase_email.lower():
            return cached_email
        return lowercase_email
    
    df_sends["emailAddress"] = df_sends.apply(
        lambda row: get_proper_cased_email(row["contactId_str"], row["emailAddress"]),
        axis=1
    )
    print(f"[PERF_DEBUG] Restored proper email case from contact lookup.")

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
    if not df_report.empty and pd.api.types.is_datetime64_any_dtype(df_report["Email Send Date"]):
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