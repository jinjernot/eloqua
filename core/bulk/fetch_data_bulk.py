import os
import gzip
import json
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.utils import save_json
from core.bulk.bulk_email_send import fetch_email_sends_bulk
from core.rest.fetch_data import fetch_data

from config import (
    BOUNCEBACK_OData_ENDPOINT,
    CLICKTHROUGH_ENDPOINT,
    EMAIL_OPEN_ENDPOINT,
    CAMPAIGN_ANALYSIS_ENDPOINT,
    CAMPAIGN_USERS_ENDPOINT,
    EMAIL_ASSET_ENDPOINT,
    EMAIL_ID_BATCH_SIZE,
    BATCH_PARALLEL_WORKERS,
    CAPTURE_WINDOW_START_OFFSET_HOURS,
    CAPTURE_WINDOW_END_OFFSET_HOURS,
    CAPTURE_WINDOW_BOUNCEBACK_DAYS,
)

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Static data (campaigns, users, email assets) rarely changes day-to-day.
# Cache it locally to avoid redundant API calls during multi-day backfills.
_STATIC_CACHE_DIR = os.path.join(DATA_DIR, "cache")
_STATIC_CACHE_TTL_SECONDS = 4 * 3600  # 4 hours

def _load_static_cache(cache_file):
    """Return cached data if the file exists and is younger than the TTL, else None."""
    if not os.path.exists(cache_file):
        return None
    age = time.time() - os.path.getmtime(cache_file)
    if age > _STATIC_CACHE_TTL_SECONDS:
        print(f"[STATIC CACHE] Expired ({age/3600:.1f}h old): {cache_file}")
        return None
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"[STATIC CACHE] Hit ({age/60:.1f}m old): {os.path.basename(cache_file)} — {len(data.get('value', []))} records")
        return data
    except Exception as e:
        print(f"[STATIC CACHE] Failed to load {cache_file}: {e}")
        return None

def _save_static_cache(data, cache_file):
    """Persist static data to a JSON cache file."""
    try:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, separators=(',', ':'))
        print(f"[STATIC CACHE] Saved: {os.path.basename(cache_file)} — {len(data.get('value', []))} records")
    except Exception as e:
        print(f"[STATIC CACHE] Failed to save {cache_file}: {e}")

# Per-date raw data (sends, opens, clicks) cached to allow fast backfill re-runs.
# Historical activity records are immutable so no TTL is needed.
_DAILY_CACHE_DIR = os.path.join(DATA_DIR, "cache", "daily")

def _load_daily_cache(cache_file):
    """Return cached list data if the file exists. No TTL — historical data never changes."""
    if not os.path.exists(cache_file):
        return None
    try:
        with gzip.open(cache_file, 'rt', encoding='utf-8') as f:
            data = json.load(f)
        print(f"[DAILY CACHE] Hit: {os.path.basename(cache_file)} \u2014 {len(data)} records")
        return data
    except Exception as e:
        print(f"[DAILY CACHE] Failed to load {cache_file}: {e}")
        return None

def _save_daily_cache(data, cache_file):
    """Persist list data to a gzip JSON cache file."""
    try:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with gzip.open(cache_file, 'wt', encoding='utf-8') as f:
            json.dump(data, f, separators=(',', ':'))
        size_kb = os.path.getsize(cache_file) / 1024
        print(f"[DAILY CACHE] Saved: {os.path.basename(cache_file)} \u2014 {len(data)} records ({size_kb:.0f} KB)")
    except Exception as e:
        print(f"[DAILY CACHE] Failed to save {cache_file}: {e}")

def fetch_and_save_data(target_date=None):
    if target_date:
        target_start = datetime.strptime(target_date, "%Y-%m-%d")
    else:
        target_start = datetime.now(timezone.utc) - timedelta(days=1)

    # Optional start offset helps capture sends near timezone boundaries.
    window_start = target_start - timedelta(hours=CAPTURE_WINDOW_START_OFFSET_HOURS)
    window_end = target_start + timedelta(days=1)
    engagement_window_end = window_end + timedelta(hours=CAPTURE_WINDOW_END_OFFSET_HOURS)

    start_str = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str_engagement = engagement_window_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    bounceback_end_date = window_end + timedelta(days=CAPTURE_WINDOW_BOUNCEBACK_DAYS)
    end_str_bounceback = bounceback_end_date.strftime("%Y-%m-%dT00:00:00Z")
    print(f"[INFO] Bounceback capture window: {start_str} to {end_str_bounceback} ({CAPTURE_WINDOW_BOUNCEBACK_DAYS} days after send date)")

    # Optional end offset helps capture late local-time sends that cross UTC midnight.
    if CAPTURE_WINDOW_END_OFFSET_HOURS > 0:
        print(f"[INFO] Applying capture window end offset: +{CAPTURE_WINDOW_END_OFFSET_HOURS} hours")

    results = {}
    # Per-date cache file paths (keyed on target date)
    date_key   = target_start.strftime("%Y-%m-%d")
    _sends_cf  = os.path.join(_DAILY_CACHE_DIR, f"{date_key}_sends.json.gz")
    _opens_cf  = os.path.join(_DAILY_CACHE_DIR, f"{date_key}_opens.json.gz")
    _clicks_cf = os.path.join(_DAILY_CACHE_DIR, f"{date_key}_clicks.json.gz")
    _pre_sends  = _load_daily_cache(_sends_cf)
    _pre_opens  = _load_daily_cache(_opens_cf)
    _pre_clicks = _load_daily_cache(_clicks_cf)

    print(f"[PERF_DEBUG] Step 1: Fetching email sends in window {start_str} to {end_str}")
    if CAPTURE_WINDOW_START_OFFSET_HOURS > 0:
        print(f"[INFO] Applying capture window start offset: -{CAPTURE_WINDOW_START_OFFSET_HOURS} hours")
    
    # Step 1: Fetch email sends first to get list of email IDs
    if _pre_sends is not None:
        email_sends_list = _pre_sends
        print(f"[DAILY CACHE] Loaded {len(email_sends_list)} sends from cache for {date_key}")
    else:
        email_sends_list = fetch_email_sends_bulk(start_str, end_str)
        _save_daily_cache(email_sends_list, _sends_cf)
    results["email_sends"] = {"items": email_sends_list}  # Wrap in dict for compatibility
    print(f"[PERF_DEBUG] Successfully fetched email_sends: {len(email_sends_list)} records")

    # Short-circuit: no sends means no opens/clicks/bouncebacks to fetch
    if not email_sends_list:
        print(f"[INFO] No sends found for {date_key} — skipping opens/clicks/bouncebacks fetch")
        return {
            "email_sends": [],
            "bouncebacks": [],
            "email_clickthroughs": {"value": []},
            "email_opens": {"value": []},
            "campaign_analysis": {},
            "campaign_users": {},
            "email_asset_data": {},
        }
    
    # Extract unique email IDs from sends
    email_ids = set()
    for send in email_sends_list:
        asset_id = send.get("assetId")  # Note: lowercase 'a' in assetId
        if asset_id:
            email_ids.add(str(asset_id))
    
    email_ids_list = sorted(email_ids)
    print(f"[INFO] Found {len(email_ids_list)} unique email IDs in send window")
    print(f"[INFO] Email IDs: {email_ids_list[:10]}{'...' if len(email_ids_list) > 10 else ''}")
    
    # Step 2: Split email IDs into batches to avoid 200k API record limit
    # Strategy: Use moderate batch size - some batches may hit limit but better than individual queries
    # Individual queries (batch size 1) only returned 13k opens vs 554k with batch size 12
    # Accepted trade-off: Some data loss at 200k limit is better than massive data loss with individual queries
    BATCH_SIZE = EMAIL_ID_BATCH_SIZE  # Configurable via config.py or environment variable
    email_id_batches = []
    if email_ids_list:
        for i in range(0, len(email_ids_list), BATCH_SIZE):
            batch = email_ids_list[i:i + BATCH_SIZE]
            email_id_batches.append(batch)
        print(f"[INFO] Split {len(email_ids_list)} email IDs into {len(email_id_batches)} batches of ~{BATCH_SIZE} IDs each")
    
    # Step 3: Fetch opens and clicks in batches (PARALLELIZED for speed)
    all_opens = []
    all_clicks = []

    if _pre_opens is not None and _pre_clicks is not None:
        print(f"[DAILY CACHE] Loaded {len(_pre_opens)} opens, {len(_pre_clicks)} clicks from cache for {date_key}")
        results["email_opens"] = {"value": _pre_opens}
        results["email_clickthroughs"] = {"value": _pre_clicks}
    elif email_id_batches:
        print(f"[INFO] Fetching opens/clicks for {len(email_id_batches)} batches in parallel...")
        print(f"[INFO] Using sentDateHour filter for send window {start_str} to {end_str_engagement}")
        
        def fetch_batch_data(batch_info):
            """Fetch opens and clicks for a single batch"""
            batch_num, batch = batch_info

            def fetch_batch_recursive(email_id_subset, label):
                email_ids_str = ",".join(email_id_subset)
                opens_filter = (
                    f"emailID in ({email_ids_str}) and "
                    f"sentDateHour ge {start_str} and sentDateHour lt {end_str_engagement}"
                )
                clicks_filter = (
                    f"emailID in ({email_ids_str}) and "
                    f"sentDateHour ge {start_str} and sentDateHour lt {end_str_engagement}"
                )

                batch_opens = fetch_data(
                    EMAIL_OPEN_ENDPOINT,
                    "email_open.json",
                    extra_params={"$filter": opens_filter, "$orderby": "openDateHour asc"},
                )
                batch_clicks = fetch_data(
                    CLICKTHROUGH_ENDPOINT,
                    "email_clickthrough.json",
                    extra_params={"$filter": clicks_filter, "$orderby": "clickDateHour asc"},
                )

                batch_opens_list = batch_opens.get("value", [])
                batch_clicks_list = batch_clicks.get("value", [])

                opens_truncated = batch_opens.get("_meta", {}).get("truncated", False)
                clicks_truncated = batch_clicks.get("_meta", {}).get("truncated", False)

                # If pagination was clipped and we have multiple email IDs, split and retry.
                if (opens_truncated or clicks_truncated) and len(email_id_subset) > 1:
                    midpoint = len(email_id_subset) // 2
                    left = email_id_subset[:midpoint]
                    right = email_id_subset[midpoint:]
                    print(
                        f"[BATCH {batch_num}/{len(email_id_batches)} {label}] "
                        f"Detected truncated results (opens={opens_truncated}, clicks={clicks_truncated}) "
                        f"for {len(email_id_subset)} email IDs; splitting into {len(left)} + {len(right)}"
                    )

                    left_opens, left_clicks = fetch_batch_recursive(left, f"{label}L")
                    right_opens, right_clicks = fetch_batch_recursive(right, f"{label}R")
                    return left_opens + right_opens, left_clicks + right_clicks

                if (opens_truncated or clicks_truncated) and len(email_id_subset) == 1:
                    print(
                        f"[WARNING] [BATCH {batch_num}/{len(email_id_batches)} {label}] "
                        f"Single-email query still truncated (opens={opens_truncated}, clicks={clicks_truncated}) "
                        f"for emailID {email_id_subset[0]}"
                    )

                    # Two supplemental fetches to recover late openers cut off by the pagination cap.
                    #
                    # Root cause: emails with heavy bot/scanner traffic generate hundreds of repeat open
                    # events per contact. These fill the 200k page cap (pages 1-40) with early opens
                    # (e.g., Feb 16-19), leaving genuine late human openers (Feb 20+) unreachable.
                    #
                    # Supplemental 1 — firstOpen eq 1 (ascending):
                    #   Returns exactly one record per contact who is opening this email for the first
                    #   time ever. Catches late first-time openers cleanly with no page cap risk.
                    #
                    # Supplemental 2 — openDateHour desc (reverse chronological, 1 page):
                    #   Fetches the 5000 most-recent open events. After dedup with the primary and
                    #   supplemental-1 results, this catches late repeat-openers (firstOpen=0) who
                    #   previously opened the same email asset in a prior campaign.
                    #
                    # All results are unioned additively via (contactID, sentDateHour) dedup — no
                    # double-counting, no data loss.
                    if opens_truncated:
                        existing_keys = frozenset(
                            (r.get("contactID"), r.get("sentDateHour")) for r in batch_opens_list
                        )

                        # ── Supplemental 1: firstOpen eq 1 ───────────────────────────────────────
                        supp1_filter = (
                            f"emailID in ({email_ids_str}) and "
                            f"sentDateHour ge {start_str} and sentDateHour lt {end_str_engagement} and firstOpen eq 1"
                        )
                        supp1_result = fetch_data(
                            EMAIL_OPEN_ENDPOINT,
                            "email_open_supp1.json",
                            extra_params={"$filter": supp1_filter, "$orderby": "openDateHour asc"},
                        )
                        supp1_list = supp1_result.get("value", [])
                        if supp1_result.get("_meta", {}).get("truncated", False):
                            print(
                                f"[WARNING] [BATCH {batch_num}/{len(email_id_batches)} {label}] "
                                f"firstOpen=1 supplemental also truncated for emailID {email_id_subset[0]}"
                            )
                        new1 = [r for r in supp1_list if (r.get("contactID"), r.get("sentDateHour")) not in existing_keys]
                        batch_opens_list = batch_opens_list + new1
                        existing_keys = existing_keys | frozenset((r.get("contactID"), r.get("sentDateHour")) for r in new1)
                        print(
                            f"[SUPPLEMENTAL-1] emailID {email_id_subset[0]}: "
                            f"firstOpen=1 recovered {len(new1)} late first-time openers "
                            f"({len(supp1_list)} firstOpen records total)"
                        )

                        # ── Supplemental 2: most-recent opens (desc) to catch firstOpen=0 late openers ─
                        # Fetching in reverse chronological order surfaces late human opens (weeks after
                        # send) before bot repeats (concentrated in first 2-3 days after send).
                        # 1 page (5000 records) is sufficient: late human opens number in the hundreds,
                        # and dedup ensures no overlap with the primary or supplemental-1 results.
                        supp2_filter = (
                            f"emailID in ({email_ids_str}) and "
                            f"sentDateHour ge {start_str} and sentDateHour lt {end_str_engagement}"
                        )
                        supp2_result = fetch_data(
                            EMAIL_OPEN_ENDPOINT,
                            "email_open_supp2.json",
                            extra_params={"$filter": supp2_filter, "$orderby": "openDateHour desc"},
                            max_pages=1,
                        )
                        supp2_list = supp2_result.get("value", [])
                        new2 = [r for r in supp2_list if (r.get("contactID"), r.get("sentDateHour")) not in existing_keys]
                        batch_opens_list = batch_opens_list + new2
                        print(
                            f"[SUPPLEMENTAL-2] emailID {email_id_subset[0]}: "
                            f"reverse-order fetch recovered {len(new2)} additional late openers "
                            f"({len(supp2_list)} records fetched desc)"
                        )

                return batch_opens_list, batch_clicks_list

            print(f"[BATCH {batch_num}/{len(email_id_batches)}] Starting fetch for {len(batch)} email IDs...")
            batch_opens_list, batch_clicks_list = fetch_batch_recursive(batch, "root")
            print(
                f"[BATCH {batch_num}/{len(email_id_batches)}] "
                f"Fetched {len(batch_opens_list)} opens, {len(batch_clicks_list)} clicks"
            )
            return batch_opens_list, batch_clicks_list
        
        # Process batches in parallel with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(BATCH_PARALLEL_WORKERS, len(email_id_batches))) as executor:
            batch_info_list = [(i+1, batch) for i, batch in enumerate(email_id_batches)]
            future_to_batch = {executor.submit(fetch_batch_data, batch_info): batch_info for batch_info in batch_info_list}
            
            for future in as_completed(future_to_batch):
                try:
                    batch_opens_list, batch_clicks_list = future.result()
                    all_opens.extend(batch_opens_list)
                    all_clicks.extend(batch_clicks_list)
                except Exception as exc:
                    batch_info = future_to_batch[future]
                    print(f"[ERROR] Batch {batch_info[0]} generated an exception: {exc}")
        
        print(f"[INFO] Total across all batches: {len(all_opens)} opens, {len(all_clicks)} clicks")
        results["email_opens"] = {"value": all_opens}
        results["email_clickthroughs"] = {"value": all_clicks}
        _save_daily_cache(all_opens, _opens_cf)
        _save_daily_cache(all_clicks, _clicks_cf)
    else:
        # Fallback if no email IDs found
        print(f"[WARNING] No email IDs found in sends")
        results["email_opens"] = {"value": []}
        results["email_clickthroughs"] = {"value": []}
    
    # Step 4: Fetch remaining data (bouncebacks, campaigns, etc.) in parallel.
    # campaign_analysis, campaign_users, and email_asset_data are static across days
    # and are served from a local cache (TTL=4h) to avoid redundant API calls.
    print(f"[PERF_DEBUG] Step 4: Fetching bouncebacks + static data (with cache).")

    _campaign_cache_file  = os.path.join(_STATIC_CACHE_DIR, "campaign_analysis_cache.json")
    _users_cache_file     = os.path.join(_STATIC_CACHE_DIR, "campaign_users_cache.json")
    _asset_cache_file     = os.path.join(_STATIC_CACHE_DIR, "email_asset_cache.json")

    cached_campaign  = _load_static_cache(_campaign_cache_file)
    cached_users     = _load_static_cache(_users_cache_file)
    cached_assets    = _load_static_cache(_asset_cache_file)

    # Only enqueue API fetches for data that isn't cached
    futures_to_enqueue = {
        "bouncebacks": (fetch_data, BOUNCEBACK_OData_ENDPOINT, "bouncebacks_odata.json",
                        {"$filter": f"bounceBackDateHour ge {start_str} and bounceBackDateHour lt {end_str_bounceback}"}),
    }
    if cached_campaign is None:
        futures_to_enqueue["campaign_analysis"] = (fetch_data, CAMPAIGN_ANALYSIS_ENDPOINT, "campaign.json", None)
    if cached_users is None:
        futures_to_enqueue["campaign_users"] = (fetch_data, CAMPAIGN_USERS_ENDPOINT, "campaign_users.json", None)
    if cached_assets is None:
        futures_to_enqueue["email_asset_data"] = (fetch_data, EMAIL_ASSET_ENDPOINT, "email_asset.json", None)

    fetched_static = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_key = {}
        for key, args in futures_to_enqueue.items():
            fn, endpoint, fname, extra = args[0], args[1], args[2], args[3]
            if extra:
                future_to_key[executor.submit(fn, endpoint, fname, extra_params=extra)] = key
            else:
                future_to_key[executor.submit(fn, endpoint, fname)] = key

        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                data = future.result()
                fetched_static[key] = data
                print(f"[PERF_DEBUG] Successfully fetched (from thread pool): {key}")
            except Exception as exc:
                print(f"[ERROR] {key} generated an exception: {exc}")
                fetched_static[key] = None

    # Persist newly fetched static data to cache
    if "campaign_analysis" in fetched_static and fetched_static["campaign_analysis"]:
        _save_static_cache(fetched_static["campaign_analysis"], _campaign_cache_file)
    if "campaign_users" in fetched_static and fetched_static["campaign_users"]:
        _save_static_cache(fetched_static["campaign_users"], _users_cache_file)
    if "email_asset_data" in fetched_static and fetched_static["email_asset_data"]:
        _save_static_cache(fetched_static["email_asset_data"], _asset_cache_file)

    # Merge cached + freshly fetched into results
    results["bouncebacks"]      = fetched_static.get("bouncebacks")
    results["campaign_analysis"]  = cached_campaign  or fetched_static.get("campaign_analysis")
    results["campaign_users"]     = cached_users     or fetched_static.get("campaign_users")
    results["email_asset_data"]   = cached_assets    or fetched_static.get("email_asset_data")
    
    print(f"[PERF_DEBUG] ThreadPoolExecutor finished.")

    email_sends = results.get("email_sends", [])
    save_json(email_sends, os.path.join(DATA_DIR, "email_sends.json"))
    print(f"[INFO] Fetched {len(email_sends)} email sends (with contact data included).")
    print(f"[PERF_DEBUG] Skipping separate contact fetch, data was included in email_sends export.")

    bouncebacks_raw_odata = results.get("bouncebacks", {})
    bouncebacks = bouncebacks_raw_odata.get("value", [])
    save_json(bouncebacks, os.path.join(DATA_DIR, "bouncebacks.json"))
    print(f"[INFO] Fetched {len(bouncebacks)} bouncebacks from OData.")

    email_clickthrough = results.get("email_clickthroughs", {})
    save_json(email_clickthrough, os.path.join(DATA_DIR, "email_clickthrough.json"))
    print(f"[INFO] Fetched {len(email_clickthrough.get('value', []))} email clickthroughs.")

    email_opens = results.get("email_opens", {})
    save_json(email_opens, os.path.join(DATA_DIR, "email_open.json"))
    print(f"[INFO] Fetched {len(email_opens.get('value', []))} email opens.")

    campaign_analysis = results.get("campaign_analysis", {})
    save_json(campaign_analysis, os.path.join(DATA_DIR, "campaign.json"))
    print(f"[INFO] Fetched {len(campaign_analysis.get('value', []))} records.")

    campaign_users = results.get("campaign_users", {})
    save_json(campaign_users, os.path.join(DATA_DIR, "campaign_users.json"))
    print(f"[INFO] Fetched {len(campaign_users.get('value', []))} records.")

    email_asset_data = results.get("email_asset_data", {})
    save_json(email_asset_data, os.path.join(DATA_DIR, "email_asset.json"))
    print(f"[INFO] Fetched {len(email_asset_data.get('value', []))} records.")

    print("[PERF_DEBUG] Returning all data from fetch_and_save_data.")
    return {
        "email_sends": email_sends,
        "bouncebacks": bouncebacks,
        "email_clickthroughs": email_clickthrough,
        "email_opens": email_opens,
        "campaign_analysis": campaign_analysis,
        "campaign_users": campaign_users,
        "email_asset_data": email_asset_data,
    }