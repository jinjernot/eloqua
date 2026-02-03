import time
import requests
from dateutil import parser
import logging
import pandas as pd
import csv
import os
from core.bulk.fetch_data_bulk import fetch_and_save_data
from core.rest.fetch_data import fetch_contacts_batch, save_contact_cache
from core.rest.fetch_email_content import fetch_email_html
from config import EXCLUDE_EMAIL_DOMAIN, EXCLUDE_TEST_EMAILS, DAILY_REPORTS_DIR, LOGS_DIR, REST_API_RATE_LIMIT_DELAY 
from config import VERBOSE_DEBUG

logger = logging.getLogger(__name__)

# Campaigns to exclude from reports (test/internal campaigns)
EXCLUDED_CAMPAIGN_IDS = {
    '17056',  # HP OPEN HOUSE 2026 Donnerstag Nachmittag
    '17076',  # HP OPEN HOUSE 2026 Freitag_Vormittag
}

def debug_print(message):
    """Print debug messages only if VERBOSE_DEBUG is enabled"""
    if VERBOSE_DEBUG:
        print(message)

def clean_country_name(country):
    """
    Clean country names by removing 'HP ' prefix that appears in some Eloqua contact data.
    Examples: 'HP US' -> 'USA', 'HP Canada' -> 'Canada', 'HP Colombia' -> 'Colombia'
    """
    if not country or not isinstance(country, str):
        return country
    
    # Remove 'HP ' prefix
    cleaned = country.strip()
    if cleaned.startswith('HP '):
        cleaned = cleaned[3:].strip()  # Remove 'HP ' (3 characters)
    
    # Normalize specific country codes
    country_mappings = {
        'US': 'USA',
        'UK': 'United Kingdom',
    }
    
    return country_mappings.get(cleaned, cleaned)

def should_exclude_campaign(campaign_id):
    """
    Check if a campaign should be excluded from the report.
    Returns True if campaign should be excluded.
    """
    return str(campaign_id) in EXCLUDED_CAMPAIGN_IDS

def sanitize_dataframe_for_csv(df):
    """
    Applies sanitization rules directly to a pandas DataFrame before saving.
    Removes tabs, newlines, and carriage returns to prevent CSV corruption.
    """
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            # Remove tabs (critical for tab-delimited CSV), newlines, and carriage returns
            df[col] = (df[col].astype(str)
                      .str.replace('\t', ' ', regex=False)  # Replace tabs with spaces
                      .str.replace('\n', ' ', regex=False)
                      .str.replace('\r', ' ', regex=False)
                      .str.strip())
        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].fillna(0).astype(int)
    return df

def generate_daily_report(target_date):
    start_time = time.time()
    logger.info("Starting daily report generation for %s", target_date)

    data = fetch_and_save_data(target_date)
    if not data:
        logger.error("Failed to fetch data.")
        return None
    
    debug_print(f"[PERF_DEBUG] Data fetch complete. Starting pandas processing...")

    target_date_obj = parser.parse(target_date).date()
    
    # Extract email_sends: could be a list or wrapped in {"items": list}
    email_sends_raw = data.get("email_sends", [])
    if isinstance(email_sends_raw, dict):
        email_sends = email_sends_raw.get("items", [])
    else:
        email_sends = email_sends_raw
        
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
    
    # Filter out excluded campaigns from email_asset_data
    email_asset_data = [item for item in email_asset_data if not should_exclude_campaign(item.get("emailID", ""))]
    logger.info(f"Filtered out excluded campaigns, remaining: {len(email_asset_data)} email assets")
    
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
                debug_print(f"[DEBUG] Contact {cid}: Bulk='{bulk_email}' → Cache='{proper_cased_email}'")
                debug_sample_count += 1
            
            contact_lookup[cid] = {
                "emailAddress": proper_cased_email,  # Use cached email (proper case) if available
                "contact_country": clean_country_name(send.get("contact_country", "")),
                "contact_hp_role": send.get("contact_hp_role", ""),
                "contact_hp_partner_id": send.get("contact_hp_partner_id", ""),
                "contact_partner_name": send.get("contact_partner_name", ""),
                "contact_market": send.get("contact_market", "")
            }
    
    debug_print(f"[PERF_DEBUG] Step 1: Helper maps created ({len(contact_lookup)} contacts) in {time.time() - pd_step_start:.2f}s.")

    # 2. Load SENDS DataFrame
    pd_step_start = time.time()
    
    if not email_sends:
        logger.warning("No email sends found. Aborting report.")
        return None

    # Filter out sends without campaignId (non-standard eComm data)
    email_sends_filtered = [s for s in email_sends if s.get("campaignId")]
    excluded_no_campaign = len(email_sends) - len(email_sends_filtered)
    if excluded_no_campaign > 0:
        logger.info(f"Excluded {excluded_no_campaign} sends without campaignId (non-standard eComm data)")
    
    unique_sends_dict = {}
    na34078_debug = []
    for s in email_sends_filtered:
        # Use externalId as unique identifier - this is Eloqua's unique activity ID
        # Each send activity has a unique externalId even if same contact/asset/time
        # If externalId is missing, fall back to compound key with activityDate
        external_id = s.get("externalId")
        
        # Debug NA_34078 (email ID 18010)
        if str(s.get("assetId")) == "18010":
            na34078_debug.append({
                'contactId': s.get("contactId"),
                'externalId': external_id,
                'activityDate': s.get("activityDate"),
                'emailAddress': s.get("emailAddress")
            })
        
        if external_id:
            key = str(external_id)
        else:
            # Fallback: Include activityDate in key to preserve multiple sends to same contact on same day
            key = (
                str(s.get("assetId")), 
                str(s.get("contactId")),
                str(s.get("emailSendType")),
                str(s.get("activityDate"))
            )
        unique_sends_dict[key] = s
    
    # Debug output for NA_34078
    if len(na34078_debug) > 0:
        print(f"\n[NA_34078 DEBUG] Total sends before dedup: {len(na34078_debug)}")
        contact_groups = {}
        for item in na34078_debug:
            cid = item['contactId']
            if cid not in contact_groups:
                contact_groups[cid] = []
            contact_groups[cid].append(item)
        
        multi_send_contacts = {k: v for k, v in contact_groups.items() if len(v) > 1}
        print(f"[NA_34078 DEBUG] Contacts with multiple sends: {len(multi_send_contacts)}")
        
        # Check externalId availability
        has_external_id = sum(1 for item in na34078_debug if item['externalId'] is not None and item['externalId'] != '')
        print(f"[NA_34078 DEBUG] Sends with externalId: {has_external_id}/{len(na34078_debug)}")
        
        if len(multi_send_contacts) > 0:
            print(f"[NA_34078 DEBUG] Sample multi-send contacts:")
            for cid, sends in list(multi_send_contacts.items())[:3]:
                print(f"  Contact {cid} ({sends[0]['emailAddress']}): {len(sends)} sends")
                for send in sends:
                    print(f"    - {send['activityDate']} | externalId={send['externalId']}")
        print()

    print(f"[DEBUG] unique_sends_dict has {len(unique_sends_dict)} unique keys (was {len(email_sends_filtered)} before dedup)")
    na34078_in_dict = sum(1 for v in unique_sends_dict.values() if str(v.get('assetId')) == '18010')
    print(f"[DEBUG] NA_34078 in dict: {na34078_in_dict} rows (was 1812 before)")
    
    df_sends = pd.DataFrame(list(unique_sends_dict.values()))
    
    print(f"[DEBUG] After DataFrame creation: df_sends has {len(df_sends)} rows")
    na34078_after_dedup = len(df_sends[df_sends['assetId'] == '18010'])
    print(f"[DEBUG] NA_34078 after DataFrame: {na34078_after_dedup} rows")
    
    # Log emailSendType distribution
    if "emailSendType" in df_sends.columns:
        send_type_counts = df_sends["emailSendType"].value_counts()
        logger.info(f"EmailSendType distribution (before date filter): {dict(send_type_counts)}")
    
    # Filter sends by target date
    if "activityDate" not in df_sends.columns:
        logger.error("Missing 'activityDate' column in email sends data. Available columns: %s", df_sends.columns.tolist())
        return None
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
    debug_print(f"[PERF_DEBUG] Step 2: SENDS DataFrame created and filtered ({len(df_sends)} rows) in {time.time() - pd_step_start:.2f}s.")

    # 3. Load bouncebacks
    pd_step_start = time.time()
    if bouncebacks:
        df_bb = pd.DataFrame(bouncebacks)
        # Handle different possible column names for contactID
        if "contactID" in df_bb.columns:
            df_bb["contactId_str"] = df_bb["contactID"].astype(str)
        elif "ContactId" in df_bb.columns:
            df_bb["contactId_str"] = df_bb["ContactId"].astype(str)
        elif "contactId" in df_bb.columns:
            df_bb["contactId_str"] = df_bb["contactId"].astype(str)
        else:
            logger.error(f"No contactID column found in bouncebacks. Available columns: {df_bb.columns.tolist()}")
            df_bb["contactId_str"] = ""
        
        # Handle different possible column names for emailID/assetID
        if "emailID" in df_bb.columns:
            df_bb["assetId_str"] = df_bb["emailID"].astype(str)
        elif "AssetId" in df_bb.columns:
            df_bb["assetId_str"] = df_bb["AssetId"].astype(str)
        elif "assetId" in df_bb.columns:
            df_bb["assetId_str"] = df_bb["assetId"].astype(str)
        else:
            logger.error(f"No assetID column found in bouncebacks. Available columns: {df_bb.columns.tolist()}")
            df_bb["assetId_str"] = ""
        
        df_bb = df_bb.dropna(subset=["contactId_str", "assetId_str"])
        df_bb['hard'] = (df_bb['isHardBounceback'] == True).astype(int)
        df_bb['soft'] = (df_bb['isHardBounceback'] == False).astype(int)
        df_bb['total_bb'] = 1
        
        bb_key = ["assetId_str", "contactId_str"]
        df_bb_counts = df_bb.groupby(bb_key)[['hard', 'soft', 'total_bb']].sum().reset_index()
        
        # Cap bouncebacks at 1 per email/contact combination
        # Eloqua can generate multiple BB records for retries, but logically it's 1 bounce per send
        df_bb_counts['hard'] = df_bb_counts['hard'].clip(upper=1)
        df_bb_counts['soft'] = df_bb_counts['soft'].clip(upper=1)
        df_bb_counts['total_bb'] = df_bb_counts['total_bb'].clip(upper=1)
        
        df_sends = df_sends.merge(df_bb_counts, on=bb_key, how="left")
        debug_print(f"[PERF_DEBUG] Step 3: BOUNCEBACKS DataFrame merged in {time.time() - pd_step_start:.2f}s.")
    else:
        df_sends["hard"] = 0
        df_sends["soft"] = 0
        df_sends["total_bb"] = 0
        debug_print(f"[PERF_DEBUG] Step 3: Skipped BOUNCEBACKS (no data).")

    # 4. Load Clicks
    pd_step_start = time.time()
    df_clicks = pd.DataFrame()  # Initialize to empty DataFrame
    if email_clickthroughs:
        df_clicks = pd.DataFrame(email_clickthroughs)
        if not df_clicks.empty:
            # Handle different possible column names
            if "contactID" in df_clicks.columns:
                df_clicks["contactId_str"] = df_clicks["contactID"].astype(str)
            elif "contactId" in df_clicks.columns:
                df_clicks["contactId_str"] = df_clicks["contactId"].astype(str)
            else:
                logger.warning(f"No contactID column in clicks. Available: {df_clicks.columns.tolist()}")
                df_clicks["contactId_str"] = ""
            
            if "emailID" in df_clicks.columns:
                df_clicks["assetId_str"] = df_clicks["emailID"].astype(str)
            elif "assetId" in df_clicks.columns:
                df_clicks["assetId_str"] = df_clicks["assetId"].astype(str)
            else:
                logger.warning(f"No emailID column in clicks. Available: {df_clicks.columns.tolist()}")
                df_clicks["assetId_str"] = ""
            
            click_key = ["assetId_str", "contactId_str"]
            df_click_counts = df_clicks.groupby(click_key).size().to_frame("total_clicks").reset_index()
            
            df_sends = df_sends.merge(df_click_counts, on=click_key, how="left")
            
            # Assign activity to LAST (most recent) send for each contact+email
            # When email sent multiple times, user typically responds to the latest one
            # Sort by activityDateParsed first to ensure 'last' means most recent by time
            df_sends = df_sends.sort_values('activityDateParsed', ascending=True)
            df_sends['is_last_send'] = ~df_sends.duplicated(subset=['assetId_str', 'contactId_str'], keep='last')
            
            # Zero out activity for all except the last send
            df_sends.loc[~df_sends['is_last_send'], 'total_clicks'] = 0
            logging.info(f"[CLICKS DEBUG] Activity assigned to LAST send only (by activityDateParsed)")
            
            debug_print(f"[PERF_DEBUG] Step 4: CLICKS DataFrame merged in {time.time() - pd_step_start:.2f}s.")
        else:
            df_sends["total_clicks"] = 0
            debug_print(f"[PERF_DEBUG] Step 4: No valid clicks data (missing required columns).")
    else:
        df_sends["total_clicks"] = 0
        debug_print(f"[PERF_DEBUG] Step 4: Skipped CLICKS (no data).")

    # 5. Load Open
    pd_step_start = time.time()
    if email_opens:
        df_opens = pd.DataFrame(email_opens)
        if not df_opens.empty:
            # Handle different possible column names
            if "contactID" in df_opens.columns:
                df_opens["contactId_str"] = df_opens["contactID"].astype(str)
            elif "contactId" in df_opens.columns:
                df_opens["contactId_str"] = df_opens["contactId"].astype(str)
            else:
                logger.warning(f"No contactID column in opens. Available: {df_opens.columns.tolist()}")
                df_opens["contactId_str"] = ""
            
            if "emailID" in df_opens.columns:
                df_opens["assetId_str"] = df_opens["emailID"].astype(str)
            elif "assetId" in df_opens.columns:
                df_opens["assetId_str"] = df_opens["assetId"].astype(str)
            else:
                logger.warning(f"No emailID column in opens. Available: {df_opens.columns.tolist()}")
                df_opens["assetId_str"] = ""
            
            df_opens["emailAddress"] = df_opens.get("emailAddress", "")
            
            # DIAGNOSTIC: Analyze what email IDs are in opens vs sends
            opens_email_ids = set(df_opens['assetId_str'].unique())
            sends_email_ids = set(df_sends['assetId_str'].unique())
            overlap = opens_email_ids.intersection(sends_email_ids)
            missing_email_ids = sends_email_ids - opens_email_ids
            
            print(f"\n[DIAGNOSTIC] ==========================================")
            print(f"[DIAGNOSTIC] Total opens fetched: {len(df_opens)}")
            print(f"[DIAGNOSTIC] Unique email IDs in opens: {len(opens_email_ids)}")
            print(f"[DIAGNOSTIC] Unique email IDs in sends (target date): {len(sends_email_ids)}")
            print(f"[DIAGNOSTIC] Email IDs with opens data: {len(overlap)}")
            print(f"[DIAGNOSTIC] Email IDs sent but NO opens fetched: {len(missing_email_ids)}")
            if missing_email_ids:
                print(f"[DIAGNOSTIC] Sample missing email IDs: {list(missing_email_ids)[:5]}")
            print(f"[DIAGNOSTIC] ==========================================\n")
            
            print(f"\n[OPENS DEBUG] df_opens shape: {df_opens.shape}")
            print(f"[OPENS DEBUG] df_opens columns: {df_opens.columns.tolist()}")
            print(f"[OPENS DEBUG] df_opens sample assetId_str: {df_opens['assetId_str'].head(3).tolist()}")
            print(f"[OPENS DEBUG] df_sends sample assetId_str: {df_sends['assetId_str'].head(3).tolist()}")
            
            debug_print(f"[OPENS_MERGE_DEBUG] df_opens shape: {df_opens.shape}")
            debug_print(f"[OPENS_MERGE_DEBUG] df_opens sample assetId_str: {df_opens['assetId_str'].head(3).tolist()}")
            debug_print(f"[OPENS_MERGE_DEBUG] df_sends sample assetId_str: {df_sends['assetId_str'].head(3).tolist()}")
            
            open_key = ["assetId_str", "contactId_str"]
            # Keep the earliest open timestamp to match with the correct send
            # Group by assetId+contactId and get total opens + first open timestamp
            df_open_agg = df_opens.groupby(open_key).agg({
                'openDateHour': 'min',  # Get earliest open timestamp
            }).reset_index()
            df_open_agg['total_opens'] = df_opens.groupby(open_key).size().values
            # Convert to datetime and strip timezone for comparison
            df_open_agg['firstOpenDate'] = pd.to_datetime(df_open_agg['openDateHour'], errors='coerce', utc=True).dt.tz_localize(None)
            
            print(f"[OPENS DEBUG] df_open_agg shape: {df_open_agg.shape}")
            print(f"[OPENS DEBUG] df_open_agg sample: {df_open_agg.head(3).to_dict('records')}")
            
            debug_print(f"[OPENS_MERGE_DEBUG] df_open_agg shape: {df_open_agg.shape}")
            debug_print(f"[OPENS_MERGE_DEBUG] Before merge, df_sends columns: {df_sends.columns.tolist()}")
            
            # Debug: Check sample records before merge
            sample_send_keys = list(zip(df_sends['assetId_str'].head(5), df_sends['contactId_str'].head(5)))
            sample_open_keys = list(zip(df_open_agg['assetId_str'].head(5), df_open_agg['contactId_str'].head(5)))
            print(f"[MERGE DEBUG] Sample send keys (assetId, contactId): {sample_send_keys[:3]}")
            print(f"[MERGE DEBUG] Sample open keys (assetId, contactId): {sample_open_keys[:3]}")
            print(f"[MERGE DEBUG] df_sends dtypes: assetId_str={df_sends['assetId_str'].dtype}, contactId_str={df_sends['contactId_str'].dtype}")
            print(f"[MERGE DEBUG] df_open_agg dtypes: assetId_str={df_open_agg['assetId_str'].dtype}, contactId_str={df_open_agg['contactId_str'].dtype}")
            
            # Debug: Save sample data for investigation
            debug_sample_file = f"{LOGS_DIR}/merge_debug_2025-11-21.txt"
            with open(debug_sample_file, 'w') as f:
                f.write("=== SENDS SAMPLE (first 10) ===\n")
                for idx, row in df_sends.head(10).iterrows():
                    f.write(f"assetId_str='{row['assetId_str']}', contactId_str='{row['contactId_str']}', emailAddress='{row.get('emailAddress', '')}'\n")
                f.write("\n=== OPENS SAMPLE (first 10) ===\n")
                for idx, row in df_open_agg.head(10).iterrows():
                    f.write(f"assetId_str='{row['assetId_str']}', contactId_str='{row['contactId_str']}', total_opens={row['total_opens']}, firstOpenDate={row['firstOpenDate']}\n")
                f.write(f"\n=== CHECKING michael.jones@mymlc.com ===\n")
                michael_sends = df_sends[df_sends.get('emailAddress', pd.Series()) == 'michael.jones@mymlc.com']
                if not michael_sends.empty:
                    for idx, row in michael_sends.iterrows():
                        f.write(f"SEND: assetId='{row['assetId_str']}', contactId='{row['contactId_str']}'\n")
                        # Check if this combination exists in opens
                        matching_open = df_open_agg[
                            (df_open_agg['assetId_str'] == row['assetId_str']) & 
                            (df_open_agg['contactId_str'] == row['contactId_str'])
                        ]
                        if not matching_open.empty:
                            f.write(f"  OPEN FOUND: total_opens={matching_open.iloc[0]['total_opens']}, firstOpenDate={matching_open.iloc[0]['firstOpenDate']}\n")
                        else:
                            f.write(f"  NO MATCHING OPEN\n")
            print(f"[DEBUG] Saved merge debug info to {debug_sample_file}")
            
            df_sends = df_sends.merge(df_open_agg, on=open_key, how="left")
            
            # Assign opens to the send that happened IMMEDIATELY BEFORE the first open
            # For duplicate sends, we need to match the open to whichever send it actually responded to
            # Sort by activityDateParsed to ensure chronological order
            df_sends = df_sends.sort_values(['assetId_str', 'contactId_str', 'activityDateParsed'], ascending=True)
            
            # For contacts with opens, find which send the open belongs to
            # The open belongs to the send that happened right before it (not necessarily the last send)
            def assign_opens_to_correct_send(group):
                if group['total_opens'].isna().all() or (group['total_opens'] == 0).all():
                    # No opens for this contact+email, nothing to do
                    return group
                
                # Get the first open timestamp
                first_open = group['firstOpenDate'].iloc[0]
                if pd.isna(first_open):
                    # No valid open date, fall back to last send
                    group['is_open_send'] = False
                    group.iloc[-1, group.columns.get_loc('is_open_send')] = True
                    return group
                
                # Find the send that happened right before the first open
                # Filter sends that happened before the open
                sends_before_open = group[group['activityDateParsed'] <= first_open]
                
                if len(sends_before_open) > 0:
                    # Assign opens to the most recent send before the open
                    group['is_open_send'] = False
                    last_send_before_open_idx = sends_before_open.index[-1]
                    group.loc[last_send_before_open_idx, 'is_open_send'] = True
                else:
                    # Open happened before any recorded send (edge case) - assign to first send
                    group['is_open_send'] = False
                    group.iloc[0, group.columns.get_loc('is_open_send')] = True
                
                return group
            
            # Apply the logic to each contact+email group
            df_sends = df_sends.groupby(['assetId_str', 'contactId_str'], group_keys=False).apply(assign_opens_to_correct_send)
            
            # Zero out opens for sends that didn't trigger the open
            df_sends['is_open_send'] = df_sends['is_open_send'].fillna(False)
            df_sends.loc[~df_sends['is_open_send'], 'total_opens'] = 0
            
            logging.info(f"[OPENS DEBUG] Activity assigned to send IMMEDIATELY BEFORE first open (by timestamp matching)")
            
            print(f"[OPENS DEBUG] After merge, total_opens: min={df_sends['total_opens'].min()}, max={df_sends['total_opens'].max()}, nulls={df_sends['total_opens'].isna().sum()}")
            print(f"[OPENS DEBUG] Rows with total_opens > 0: {(df_sends['total_opens'] > 0).sum()}")
            print(f"[OPENS DEBUG] Opens assigned to send that happened before the first open")
            if 'firstOpenDate' in df_sends.columns and 'activityDateParsed' in df_sends.columns:
                print(f"[OPENS DEBUG] Sample with opens: {df_sends[df_sends['total_opens'] > 0][['assetId_str', 'contactId_str', 'total_opens', 'activityDateParsed', 'firstOpenDate']].head(3).to_dict('records')}\n")
            else:
                print(f"[OPENS DEBUG] Sample with opens: {df_sends[df_sends['total_opens'] > 0][['assetId_str', 'contactId_str', 'total_opens']].head(3).to_dict('records')}\n")
            
            debug_print(f"[OPENS_MERGE_DEBUG] After merge, df_sends shape: {df_sends.shape}")
            debug_print(f"[OPENS_MERGE_DEBUG] total_opens column stats: min={df_sends['total_opens'].min()}, max={df_sends['total_opens'].max()}, null_count={df_sends['total_opens'].isna().sum()}")
            debug_print(f"[PERF_DEBUG] Step 5: OPENS DataFrame merged in {time.time() - pd_step_start:.2f}s.")
        else:
            df_sends["total_opens"] = 0
            df_opens = pd.DataFrame()
            debug_print(f"[PERF_DEBUG] Step 5: No valid opens data (missing required columns).")
    else:
        df_sends["total_opens"] = 0
        df_opens = pd.DataFrame()
        debug_print(f"[PERF_DEBUG] Step 5: Skipped OPENS (no data).")

    # Filter out excluded campaigns from sends
    pd_step_start = time.time()
    df_sends['assetId_int'] = pd.to_numeric(df_sends['assetId'], errors='coerce')
    initial_count = len(df_sends)
    df_sends = df_sends[~df_sends['assetId_int'].astype(str).isin(EXCLUDED_CAMPAIGN_IDS)]
    excluded_count = initial_count - len(df_sends)
    if excluded_count > 0:
        logger.info(f"Excluded {excluded_count} sends from filtered campaigns")
    debug_print(f"[PERF_DEBUG] Step 5.5: CAMPAIGN FILTERING completed in {time.time() - pd_step_start:.2f}s.")

    # Fill NaNs from merges with 0
    pd_step_start = time.time()
    fill_cols = ['hard', 'soft', 'total_bb', 'total_clicks', 'total_opens']
    for col in fill_cols:
        if col in df_sends.columns:
            df_sends[col] = df_sends[col].fillna(0).astype(int)
    debug_print(f"[PERF_DEBUG] Step 5b: NaNs filled in {time.time() - pd_step_start:.2f}s.")
    
    # 5c. Detect forwarded emails (opens OR clicks without sends)
    # Eloqua detects forwards when someone who didn't receive the email has activity (opens or clicks)
    pd_step_start = time.time()
    forward_contacts = set()
    
    # Get set of campaigns (asset IDs) that had sends on the target date
    campaigns_with_sends = set(df_sends['assetId_str'].unique())
    debug_print(f"[CAMPAIGNS_DEBUG] Campaigns with sends: {sorted(campaigns_with_sends)}")
    
    # Initialize filtered dataframes
    df_opens_filtered = pd.DataFrame()
    df_clicks_filtered = pd.DataFrame()
    
    # Find contacts who opened OR clicked but didn't receive the email (forwards)
    # IMPORTANT: Only consider opens for campaigns that had sends on target date
    if not df_opens.empty:
        # Filter to only campaigns that had sends on the target date
        df_opens_filtered = df_opens[df_opens['assetId_str'].isin(campaigns_with_sends)].copy()
        print(f"[OPENS DEBUG] Opens before campaign filter: {len(df_opens)}")
        print(f"[OPENS DEBUG] Opens after campaign filter: {len(df_opens_filtered)}")
        
        opens_set = set(zip(df_opens_filtered['assetId_str'], df_opens_filtered['contactId_str']))
        sends_set = set(zip(df_sends['assetId_str'], df_sends['contactId_str']))
        forward_contacts.update(opens_set - sends_set)
        
        # Debug: Log forward detection statistics
        debug_msg = f"[FORWARD_DEBUG] Total opens fetched: {len(df_opens)}\n"
        debug_msg += f"[FORWARD_DEBUG] Opens after filtering to campaigns with sends: {len(df_opens_filtered)}\n"
        debug_msg += f"[FORWARD_DEBUG] Campaigns with sends on target date: {len(campaigns_with_sends)}\n"
        debug_msg += f"[FORWARD_DEBUG] Opens set size: {len(opens_set)}\n"
        debug_msg += f"[FORWARD_DEBUG] Sends set size: {len(sends_set)}\n"
        debug_msg += f"[FORWARD_DEBUG] Forwards from opens: {len(forward_contacts)}\n"
        
        # Check for opens for Campaign 15269 (the main missing campaign)
        campaign_15269_opens = [(a, c) for a, c in opens_set if a == '15269']
        campaign_15269_sends = [(a, c) for a, c in sends_set if a == '15269']
        debug_msg += f"[FORWARD_DEBUG] Campaign 15269: {len(campaign_15269_opens)} opens, {len(campaign_15269_sends)} sends\n"
        
        # Debug: Check for specific missing forwards
        sample_check = [
            ('15269', '389436'),  # Email ID 15269, Contact ID 389436
            ('15269', '6059'),
            ('15269', '9343')
        ]
        for asset, cid in sample_check:
            in_opens = (asset, cid) in opens_set
            in_sends = (asset, cid) in sends_set
            in_forwards = (asset, cid) in forward_contacts
            debug_msg += f"[FORWARD_DEBUG] Asset {asset}, Contact {cid}: Opens={in_opens}, Sends={in_sends}, Forward={in_forwards}\n"
        
        print(debug_msg)
        with open(f'{LOGS_DIR}/forward_debug.log', 'w') as f:
            f.write(debug_msg)
    
    # Also check for clicks without sends (additional forwards)
    if not df_clicks.empty:
        # Filter to only campaigns that had sends on the target date
        df_clicks_filtered = df_clicks[df_clicks['assetId_str'].isin(campaigns_with_sends)].copy()
        print(f"[CLICKS DEBUG] Clicks before campaign filter: {len(df_clicks)}")
        print(f"[CLICKS DEBUG] Clicks after campaign filter: {len(df_clicks_filtered)}")
        
        if not df_clicks_filtered.empty:
            clicks_set = set(zip(df_clicks_filtered['assetId_str'], df_clicks_filtered['contactId_str']))
            sends_set = set(zip(df_sends['assetId_str'], df_sends['contactId_str']))
            clicks_forwards = clicks_set - sends_set
            
            # Only add if not already in forward_contacts from opens
            new_click_forwards = clicks_forwards - forward_contacts
            forward_contacts.update(new_click_forwards)
            
            debug_print(f"[FORWARD_DEBUG] Clicks set size: {len(clicks_set)}")
            debug_print(f"[FORWARD_DEBUG] Forwards from clicks (not already from opens): {len(new_click_forwards)}")
            debug_print(f"[FORWARD_DEBUG] Total forwards (opens + clicks): {len(forward_contacts)}")
    
    # Add forward contacts to contact_lookup by fetching from cache
    if forward_contacts:
        debug_print(f"[FORWARD_LOOKUP_DEBUG] Adding {len(forward_contacts)} forward contacts to contact_lookup...")
        contacts_added = 0
        contacts_not_in_cache = []
        
        for asset_id, contact_id in forward_contacts:
            if contact_id not in contact_lookup:
                # Try to get contact info from cache
                if contact_id in contact_cache:
                    cached_contact = contact_cache[contact_id]
                    contact_lookup[contact_id] = {
                        "emailAddress": cached_contact.get("emailAddress", ""),
                        "contact_country": clean_country_name(cached_contact.get("country", "")),
                        "contact_hp_role": cached_contact.get("hp_role", ""),
                        "contact_hp_partner_id": cached_contact.get("hp_partner_id", ""),
                        "contact_partner_name": cached_contact.get("partner_name", ""),
                        "contact_market": cached_contact.get("market", "")
                    }
                    contacts_added += 1
                else:
                    contacts_not_in_cache.append(contact_id)
        
        debug_print(f"[FORWARD_LOOKUP_DEBUG] Added {contacts_added} forward contacts from cache")
        debug_print(f"[FORWARD_LOOKUP_DEBUG] {len(contacts_not_in_cache)} forward contacts not found in cache")
        
        # Fetch missing contacts from Eloqua API
        if contacts_not_in_cache:
            debug_print(f"[FORWARD_LOOKUP_DEBUG] Fetching {len(contacts_not_in_cache)} missing contacts from Eloqua API...")
            try:
                # Fetch contacts in batch
                fetched_contacts = fetch_contacts_batch(contacts_not_in_cache, max_workers=10, use_cache=False)
                
                if fetched_contacts:
                    debug_print(f"[FORWARD_LOOKUP_DEBUG] Successfully fetched {len(fetched_contacts)} contacts from API")
                    
                    # Add fetched contacts to both contact_lookup and contact_cache
                    for contact_id, contact_data in fetched_contacts.items():
                        contact_lookup[contact_id] = {
                            "emailAddress": contact_data.get("emailAddress", ""),
                            "contact_country": clean_country_name(contact_data.get("country", "")),
                            "contact_hp_role": contact_data.get("hp_role", ""),
                            "contact_hp_partner_id": contact_data.get("hp_partner_id", ""),
                            "contact_partner_name": contact_data.get("partner_name", ""),
                            "contact_market": contact_data.get("market", "")
                        }
                        
                        # Also add to cache for future use
                        contact_cache[contact_id] = contact_data
                    
                    # Save updated cache
                    debug_print(f"[FORWARD_LOOKUP_DEBUG] Saving updated cache with {len(fetched_contacts)} new contacts...")
                    save_contact_cache(contact_cache)
                    debug_print(f"[FORWARD_LOOKUP_DEBUG] Cache saved successfully")
                else:
                    debug_print(f"[FORWARD_LOOKUP_DEBUG] ⚠️ No contacts returned from API")
                    
            except Exception as e:
                debug_print(f"[FORWARD_LOOKUP_DEBUG] ⚠️ Error fetching contacts from API: {e}")
                logger.warning(f"Failed to fetch missing forward contacts: {e}")
        
        debug_print(f"[FORWARD_LOOKUP_DEBUG] Total contacts in lookup now: {len(contact_lookup)}")
    
    if forward_contacts:
        # Create mapping of campaign send dates for forwards
        # Forwards should use the same send date as the actual campaign sends
        campaign_send_dates = {}
        for asset_id in campaigns_with_sends:
            campaign_sends = df_sends[df_sends['assetId_str'] == asset_id]
            if not campaign_sends.empty and 'activityDateParsed' in campaign_sends.columns:
                # Use the first send date for this campaign
                send_date = campaign_sends['activityDateParsed'].iloc[0]
                campaign_send_dates[asset_id] = send_date
        
        debug_print(f"[FORWARD_DEBUG] Campaign send dates collected for {len(campaign_send_dates)} campaigns")
        
        # Pre-compute opens and clicks counts using groupby (much faster than row-by-row)
        # Use FILTERED opens/clicks data (only campaigns with sends on target date)
        opens_counts = {}
        clicks_counts = {}
        
        if not df_opens_filtered.empty:
            opens_grouped = df_opens_filtered.groupby(['assetId_str', 'contactId_str']).size()
            opens_counts = opens_grouped.to_dict()
            debug_print(f"[OPENS_COUNTS_DEBUG] opens_counts dictionary size: {len(opens_counts)}")
            # Debug: Show sample keys and their types
            if len(opens_counts) > 0:
                sample_key = list(opens_counts.keys())[0]
                debug_print(f"[OPENS_COUNTS_DEBUG] Sample key: {sample_key}, type: {type(sample_key)}, key types: ({type(sample_key[0])}, {type(sample_key[1])})")
            # Check if our sample forwards are in opens_counts
            sample_keys = [('15269', '389436'), ('15269', '6059'), ('15269', '9343')]
            for key in sample_keys:
                count = opens_counts.get(key, 0)
                debug_print(f"[OPENS_COUNTS_DEBUG] Key {key}: {count} opens")
        
        if not df_clicks_filtered.empty:
            # Use FILTERED clicks (only campaigns with sends on target date)
            clicks_grouped = df_clicks_filtered.groupby(['assetId_str', 'contactId_str']).size()
            clicks_counts = clicks_grouped.to_dict()
        
        forward_rows = []
        skipped_not_in_campaigns = 0
        skipped_no_opens = 0
        created_rows = 0
        
        # Track which campaigns we're creating forwards for
        forward_campaigns_created = {}
        
        for asset_id, contact_id in forward_contacts:
            # Only include forwards for campaigns that had sends on this date
            # This matches Eloqua Analytics behavior
            if asset_id not in campaigns_with_sends:
                skipped_not_in_campaigns += 1
                continue
                
            # Get contact info from contact_lookup
            contact_info = contact_lookup.get(contact_id, {})
            
            # Get email campaign info from asset maps
            asset_id_int = int(asset_id) if asset_id and str(asset_id).isdigit() else -1
            email_name = email_name_map.get(asset_id_int, "")
            email_group = email_group_map.get(asset_id_int, "")
            subject_line = email_subject_map.get(asset_id_int, "")
            
            # Get opens and clicks counts from pre-computed dictionaries
            opens_count = opens_counts.get((asset_id, contact_id), 0)
            clicks_count = clicks_counts.get((asset_id, contact_id), 0)
            
            # Debug first few lookups
            if created_rows < 3:
                debug_print(f"[FORWARD_LOOKUP_DEBUG] Forward #{created_rows+1}: asset={asset_id} (type={type(asset_id)}), contact={contact_id} (type={type(contact_id)}), opens={opens_count}, clicks={clicks_count}")
            
            # CRITICAL: Skip if BOTH opens and clicks are zero or negative (invalid forward data)
            # A valid forward MUST have at least one open OR one click (greater than 0)
            # This also catches any data quality issues where counts might be None or negative
            if not (opens_count > 0 or clicks_count > 0):
                skipped_no_opens += 1
                if created_rows < 5:  # Log first few skipped for debugging
                    debug_print(f"[FORWARD_SKIP_DEBUG] Skipping forward: asset={asset_id}, contact={contact_id}, opens={opens_count}, clicks={clicks_count}")
                continue
            
            created_rows += 1
            
            # Track campaigns
            if asset_id not in forward_campaigns_created:
                forward_campaigns_created[asset_id] = 0
            forward_campaigns_created[asset_id] += 1
            
            # Get the campaign send date for this forward
            campaign_send_date = campaign_send_dates.get(asset_id)
            
            forward_rows.append({
                'assetId_str': asset_id,
                'assetId_int': asset_id_int,  # Add this for Email Group mapping
                'contactId_str': contact_id,
                'assetName': email_name,
                'subjectLine': subject_line,
                'emailAddress': contact_info.get('emailAddress', ''),
                'contact_country': contact_info.get('contact_country', ''),
                'contact_hp_role': contact_info.get('contact_hp_role', ''),
                'contact_hp_partner_id': contact_info.get('contact_hp_partner_id', ''),
                'contact_partner_name': contact_info.get('contact_partner_name', ''),
                'contact_market': contact_info.get('contact_market', ''),
                'emailSendType': 'EmailForward',
                'activityDateParsed': campaign_send_date,  # Use campaign's send date
                'total_opens': opens_count,
                'total_clicks': clicks_count,
                'hard': 0,
                'soft': 0,
                'total_bb': 0
            })
        
        debug_print(f"[FORWARD_CREATION_DEBUG] Forwards in set: {len(forward_contacts)}")
        debug_print(f"[FORWARD_CREATION_DEBUG] Rows created: {created_rows}")
        debug_print(f"[FORWARD_CREATION_DEBUG] Skipped (not in campaigns): {skipped_not_in_campaigns}")
        debug_print(f"[FORWARD_CREATION_DEBUG] Skipped (no opens count): {skipped_no_opens}")
        debug_print(f"[FORWARD_CREATION_DEBUG] Final forward_rows: {len(forward_rows)}")
        debug_print(f"[FORWARD_CREATION_DEBUG] Forward campaigns: {sorted(forward_campaigns_created.keys())}")
        debug_print(f"[FORWARD_CREATION_DEBUG] Forward counts by campaign: {dict(sorted(forward_campaigns_created.items()))}")
        
        if forward_rows:
            df_forwards = pd.DataFrame(forward_rows)
            debug_print(f"[FORWARD_DF_DEBUG] df_forwards shape: {df_forwards.shape}")
            debug_print(f"[FORWARD_DF_DEBUG] df_forwards columns: {df_forwards.columns.tolist()}")
            debug_print(f"[FORWARD_DF_DEBUG] total_opens in df_forwards: min={df_forwards['total_opens'].min()}, max={df_forwards['total_opens'].max()}, mean={df_forwards['total_opens'].mean():.2f}")
            debug_print(f"[FORWARD_DF_DEBUG] Forwards with total_opens > 0: {(df_forwards['total_opens'] > 0).sum()}")
            debug_print(f"[FORWARD_DF_DEBUG] Forwards with total_opens == 0: {(df_forwards['total_opens'] == 0).sum()}")
            
            df_sends = pd.concat([df_sends, df_forwards], ignore_index=True)
            debug_print(f"[PERF_DEBUG] Step 5c: Detected {len(forward_rows)} forwarded emails (anti-join) in {time.time() - pd_step_start:.2f}s.")
            debug_print(f"[MERGE_DEBUG] df_sends length after merge: {len(df_sends)}")
            debug_print(f"[MERGE_DEBUG] EmailForward count after merge: {(df_sends['emailSendType'] == 'EmailForward').sum()}")
            debug_print(f"[MERGE_DEBUG] Forwards with total_opens > 0 after merge: {((df_sends['emailSendType'] == 'EmailForward') & (df_sends['total_opens'] > 0)).sum()}")
            debug_print(f"[MERGE_DEBUG] Forwards with total_opens == 0 after merge: {((df_sends['emailSendType'] == 'EmailForward') & (df_sends['total_opens'] == 0)).sum()}")
            debug_print(f"[MERGE_DEBUG] Forwards with total_opens NaN after merge: {((df_sends['emailSendType'] == 'EmailForward') & (df_sends['total_opens'].isna())).sum()}")
        else:
            debug_print(f"[PERF_DEBUG] Step 5c: No forwarded emails detected in {time.time() - pd_step_start:.2f}s.")
    else:
        debug_print(f"[PERF_DEBUG] Step 5c: No forwarded emails detected in {time.time() - pd_step_start:.2f}s.")
    

    # 6. Load and Merge Data
    debug_print(f"[PERF_DEBUG] Step 6: Skipped CONTACTS merge (data already included in sends).")

    # Temporarily disabled for faster testing
    logger.info("Skipping HTML email download (disabled).")
    # --- NEW STEP: Fetch Email HTML ---
    # For each unique asset/email ID in df_sends, download the HTML content and save to disk.
    # logger.info("Fetching HTML content for email assets...")
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
    for _, row in df_sends[df_sends["emailSendType"] != "EmailForward"].iterrows():
        asset_id = str(row.get("assetId_str", ""))
        campaign_id = row.get("campaignId", "")
        if asset_id and campaign_id:
            user = get_user(campaign_id)
            if user:
                asset_user_map[asset_id] = user
    
    logger.info(f"Built asset_user_map with {len(asset_user_map)} entries for forwarded email user lookup")
    
    # Apply user lookup - for regular sends use campaign, for forwarded use asset lookup
    def get_user_for_row(row):
        if row["emailSendType"] == "EmailForward":
            asset_id = str(row["assetId_str"])
            user = asset_user_map.get(asset_id, "")
            if not user:
                logger.debug(f"No user found for forwarded email assetId {asset_id}")
            return user
        else:
            return get_user(row["campaignId"])
    
    df_sends["Last Activated by User"] = df_sends.apply(get_user_for_row, axis=1)
    
    # Debug: Check user population for forwarded emails
    forwarded_mask = df_sends["emailSendType"] == "EmailForward"
    users_populated = (df_sends[forwarded_mask]["Last Activated by User"] != "").sum()
    logger.info(f"Last Activated by User populated for {users_populated}/{forwarded_mask.sum()} forwarded emails")
    
    # df_sends = df_sends[df_sends["Last Activated by User"] != ""].copy()
    debug_print(f"[PERF_DEBUG] Skipping user filter, keeping all {len(df_sends)} rows.")
        
    # Filter out emails matching Eloqua Analytics exclusion criteria
    initial_count = len(df_sends)
    
    # Exclude emails from configured domain
    # Convert to string first to avoid .str accessor errors
    df_sends["emailAddress"] = df_sends["emailAddress"].astype(str)
    df_sends = df_sends[~df_sends["emailAddress"].str.lower().str.contains(EXCLUDE_EMAIL_DOMAIN, na=False)]
    
    # Exclude specific test/spam email addresses from config
    df_sends = df_sends[~df_sends["emailAddress"].str.lower().isin([e.lower() for e in EXCLUDE_TEST_EMAILS])]
    
    # Filter out incomplete forward records (forwards with no email address or no activity)
    # These are data artifacts where we detected activity but have no valid contact info
    before_forward_filter = len(df_sends)
    
    # Debug: Check forwards before filtering
    forwards_before = df_sends[df_sends["emailSendType"] == "EmailForward"]
    debug_print(f"[FILTER_DEBUG] Forwards before filtering: {len(forwards_before)}")
    debug_print(f"[FILTER_DEBUG] Forwards with NaN email: {forwards_before['emailAddress'].isna().sum()}")
    debug_print(f"[FILTER_DEBUG] Forwards with empty email: {(forwards_before['emailAddress'] == '').sum()}")
    debug_print(f"[FILTER_DEBUG] Forwards with no activity: {((forwards_before['total_opens'] == 0) & (forwards_before['total_clicks'] == 0)).sum()}")
    
    incomplete_forwards = (df_sends["emailSendType"] == "EmailForward") & (
        (df_sends["emailAddress"].isna()) | 
        (df_sends["emailAddress"] == "") |
        ((df_sends["total_opens"] == 0) & (df_sends["total_clicks"] == 0))  # Filter only if NO opens AND NO clicks
    )
    df_sends = df_sends[~incomplete_forwards]
    incomplete_count = before_forward_filter - len(df_sends)
    if incomplete_count > 0:
        debug_print(f"[PERF_DEBUG] Filtered out {incomplete_count} incomplete forward records (no email address or no activity).")
    
    debug_print(f"[PERF_DEBUG] Filtered {EXCLUDE_EMAIL_DOMAIN} and excluded test emails, {len(df_sends)} rows remaining (removed {initial_count - len(df_sends)}).")
    
    # After filtering, remove forwards for campaigns that no longer have any sends
    # This matches Eloqua Analytics behavior: forwards only shown if campaign has valid sends
    campaigns_after_filtering = set(df_sends[df_sends['emailSendType'] == 'EmailSend']['assetId_str'].unique())
    forwards_before = (df_sends['emailSendType'].isin(['EmailForward', 'Forwarded'])).sum()
    
    # Remove forwards for campaigns without sends
    forwards_to_remove = (
        df_sends['emailSendType'].isin(['EmailForward', 'Forwarded']) & 
        ~df_sends['assetId_str'].isin(campaigns_after_filtering)
    )
    removed_forwards = forwards_to_remove.sum()
    df_sends = df_sends[~forwards_to_remove]
    
    debug_print(f"[FILTER_DEBUG] Campaigns after filtering: {len(campaigns_after_filtering)}")
    debug_print(f"[FILTER_DEBUG] Removed {removed_forwards} forwards from campaigns without valid sends")
    debug_print(f"[FILTER_DEBUG] Forwards remaining: {forwards_before - removed_forwards}")

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
    debug_print(f"[PERF_DEBUG] Restored proper email case from contact lookup.")

    df_sends["Email Group"] = df_sends["assetId_int"].map(email_group_map).fillna("")
    
    # For forwarded emails, Total Sends and Total Delivered should be blank/0
    df_sends["Total Sends"] = df_sends["emailSendType"].apply(lambda x: 0 if x in ["Forwarded", "EmailForward"] else 1)
    df_sends["Total Delivered"] = df_sends.apply(
        lambda row: 0 if row["emailSendType"] in ["Forwarded", "EmailForward"] else (1 if row["total_bb"] == 0 else 0), 
        axis=1
    )
    
    df_sends["Unique Opens"] = (df_sends["total_opens"] > 0).astype(int)
    df_sends["Unique Clicks"] = (df_sends["total_clicks"] > 0).astype(int)
    
    # Rate calculations per Oracle Eloqua documentation:
    # - Bounceback rates: Metric / Total Sends * 100
    # - Other rates (Open, Click, Delivered): Metric / Total Delivered * 100
    # - Forwards have 0 for all rates (no send = no rate)
    
    # Bounceback rates use Total Sends as denominator
    df_sends["Hard Bounceback Rate"] = df_sends.apply(
        lambda row: 0 if row["emailSendType"] in ["Forwarded", "EmailForward"] or row["Total Sends"] == 0 
                    else (row["hard"] / row["Total Sends"]) * 100,
        axis=1
    )
    df_sends["Soft Bounceback Rate"] = df_sends.apply(
        lambda row: 0 if row["emailSendType"] in ["Forwarded", "EmailForward"] or row["Total Sends"] == 0
                    else (row["soft"] / row["Total Sends"]) * 100,
        axis=1
    )
    df_sends["Bounceback Rate"] = df_sends.apply(
        lambda row: 0 if row["emailSendType"] in ["Forwarded", "EmailForward"] or row["Total Sends"] == 0
                    else (row["total_bb"] / row["Total Sends"]) * 100,
        axis=1
    )
    
    # Delivered Rate uses Total Sends as denominator: (Total Sends - Total Bouncebacks) / Total Sends
    df_sends["Delivered Rate"] = df_sends.apply(
        lambda row: 0 if row["emailSendType"] in ["Forwarded", "EmailForward"] or row["Total Sends"] == 0
                    else (row["Total Delivered"] / row["Total Sends"]) * 100,
        axis=1
    )
    
    # Open and Click rates use Total Delivered as denominator (per Oracle documentation)
    df_sends["Unique Open Rate"] = df_sends.apply(
        lambda row: 0 if row["emailSendType"] in ["Forwarded", "EmailForward"] or row["Total Delivered"] == 0
                    else (row["Unique Opens"] / row["Total Delivered"]) * 100,
        axis=1
    )
    df_sends["Clickthrough Rate"] = df_sends.apply(
        lambda row: 0 if row["emailSendType"] in ["Forwarded", "EmailForward"] or row["Total Delivered"] == 0
                    else (row["total_clicks"] / row["Total Delivered"]) * 100,
        axis=1
    )
    df_sends["Unique Clickthrough Rate"] = df_sends.apply(
        lambda row: 0 if row["emailSendType"] in ["Forwarded", "EmailForward"] or row["Total Delivered"] == 0
                    else (row["Unique Clicks"] / row["Total Delivered"]) * 100,
        axis=1
    )
    debug_print(f"[PERF_DEBUG] Step 7: Final logic and calculations applied in {time.time() - pd_step_start:.2f}s.")

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
        # "emailSendType": "Email Send Type"
    }
    
    print(f"\n[DEBUG] Before creating report: df_sends has {len(df_sends)} rows")
    na34078_before_report = len(df_sends[df_sends['assetId'] == '18010'])
    print(f"[DEBUG] NA_34078 before report: {na34078_before_report} rows")
    
    df_report = df_sends.rename(columns=final_column_map)
    
    print(f"[DEBUG] After creating report: df_report has {len(df_report)} rows")
    print()
    final_columns_ordered = []
    for col_name in final_column_map.values():
        if col_name not in df_report.columns:
            df_report[col_name] = None
        final_columns_ordered.append(col_name)
            
    df_report = df_report[final_columns_ordered]
    
    # Format Email Send Date - it's already a datetime, just needs formatting
    if not df_report.empty and pd.api.types.is_datetime64_any_dtype(df_report["Email Send Date"]):
        df_report["Email Send Date"] = df_report["Email Send Date"].dt.strftime("%Y-%m-%d %I:%M:%S %p")
    # Convert to string first to avoid .str accessor errors
    df_report["Email Address"] = df_report["Email Address"].astype(str).str.lower()
    debug_print(f"[PERF_DEBUG] Step 8: Final column renaming and formatting in {time.time() - pd_step_start:.2f}s.")
    
    processing_end_time = time.time()
    logger.info("Processed %d report rows in %.2f seconds.", len(df_report), processing_end_time - processing_start_time)

    output_file = f"{DAILY_REPORTS_DIR}/{target_date}.csv"
    
    pd_step_start = time.time()
    df_report = sanitize_dataframe_for_csv(df_report)
    df_report.to_csv(
        output_file, 
        sep="\t",
        index=False, 
        encoding="utf-16",
        quoting=csv.QUOTE_MINIMAL
    )
    debug_print(f"[PERF_DEBUG] Step 9: Sanitized and saved final CSV to {output_file} in {time.time() - pd_step_start:.2f}s.")

    # Count forwards before removing the column (if it exists)
    forwards_count = 0
    if 'emailSendType' in df_sends.columns:
        forwards_count = (df_sends['emailSendType'] == 'EmailForward').sum()
    
    end_time = time.time()
    logger.info("Daily report generation for %s completed in %.2f seconds. Report saved to %s", target_date, end_time - start_time, output_file)
    logger.info("Report contains %d forwards (not shown in output)", forwards_count)
    
    # Return both the file path and the forward count as a tuple
    return output_file, forwards_count
