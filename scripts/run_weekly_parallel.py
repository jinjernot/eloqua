"""
Run daily reports for the past week with PARALLEL processing for maximum speed.
Uses concurrent futures to process multiple days simultaneously.
Tracks detailed timing and volume metrics.
"""
import sys
import os
import csv
import traceback
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path to import core and config modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.bulk.process_data_bulk import generate_daily_report
from config import SAVE_LOCALLY, WEEKLY_REPORTS_DIR, DEFAULT_MAX_WORKERS, MAX_WORKERS_LIMIT
import logging
import threading
from core.logging_config import setup_thread_safe_logging

# Setup logging with file output
setup_thread_safe_logging("run_weekly_parallel")

# Automatically authenticate to AWS if needed
if not SAVE_LOCALLY:
    from core.aws.auto_authenticate import ensure_authenticated
    if not ensure_authenticated(auto_refresh=True, use_poetry=True):
        print("\n✗ Unable to authenticate to AWS. Exiting.\n")
        sys.exit(1)

# Conditionally import S3 utils
if not SAVE_LOCALLY:
    try:
        from core.aws.s3_utils import upload_to_s3, ping_s3_bucket
        from config import S3_BUCKET_NAME, S3_FOLDER_PATH
    except ImportError:
        logging.error("boto3 not installed. Install with: pip install boto3")
        sys.exit(1)

# Thread-safe print lock
print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    """Thread-safe print function"""
    with print_lock:
        print(*args, **kwargs)

def process_single_date(date_obj, report_num, total_reports):
    """
    Process a single date's report with metrics tracking.
    Returns a dictionary with all metrics.
    """
    date_str = date_obj.strftime('%Y-%m-%d')
    
    safe_print(f"\n[{report_num}/{total_reports}] Starting {date_str} ({date_obj.strftime('%A')})")
    
    report_path = ""
    error_msg = ""
    status = "Failed"
    elapsed = 0
    
    # Volume metrics
    email_sends_count = 0
    bouncebacks_count = 0
    clicks_count = 0
    opens_count = 0
    forwards_count = 0
    total_records = 0
    
    try:
        start_time = time.time()
        
        result = generate_daily_report(date_str)
        
        # Handle both old (single value) and new (tuple) return formats
        if isinstance(result, tuple):
            report_path, forwards_count = result
        else:
            report_path = result
            forwards_count = 0
        
        elapsed = time.time() - start_time
        
        if report_path:
            status = "Success"
            
            # Extract metrics from the generated file
            try:
                import pandas as pd
                # Handle BOM and encoding issues
                df = pd.read_csv(report_path, sep='\t', encoding='utf-16', on_bad_lines='skip')
                total_records = len(df)
                
                # Extract volume metrics if columns exist
                if 'Total Sends' in df.columns:
                    email_sends_count = df['Total Sends'].sum()
                
                if 'Total Bouncebacks' in df.columns:
                    bouncebacks_count = df['Total Bouncebacks'].sum()
                
                if 'Unique Clickthrough Rate' in df.columns or 'Clickthrough Rate' in df.columns:
                    # Count rows with clicks (non-zero clickthrough rate)
                    click_col = 'Unique Clickthrough Rate' if 'Unique Clickthrough Rate' in df.columns else 'Clickthrough Rate'
                    clicks_count = len(df[df[click_col] > 0])
                
                if 'Unique Opens' in df.columns:
                    # Count rows with opens (not sum, since Unique Opens is binary 0/1)
                    opens_count = len(df[df['Unique Opens'] > 0])
                
                # forwards_count is already set from generate_daily_report return value
                
            except Exception as read_error:
                logging.warning(f"Could not read metrics from report file: {read_error}")
            
            safe_print(f"✓ [{report_num}/{total_reports}] {date_str} completed in {elapsed:.1f}s - {total_records:,} records")
            
            # Upload to S3 if enabled (skip empty files with only headers)
            if not SAVE_LOCALLY:
                if total_records > 0:
                    try:
                        upload_success = upload_to_s3(report_path, S3_BUCKET_NAME, S3_FOLDER_PATH)
                        if upload_success:
                            safe_print(f"  ✓ Uploaded to S3")
                        else:
                            safe_print(f"  ✗ S3 upload failed")
                            error_msg = "S3 upload failed"
                    except Exception as upload_error:
                        safe_print(f"  ✗ S3 upload error: {upload_error}")
                        error_msg = f"S3 upload error: {upload_error}"
                else:
                    safe_print(f"  ⊘ Skipped S3 upload (empty file)")
        else:
            status = "No Data"
            error_msg = "No email sends found for this date"
            safe_print(f"⊘ [{report_num}/{total_reports}] {date_str} - No data ({elapsed:.1f}s)")
        
    except Exception as e:
        error_msg = str(e)
        elapsed = time.time() - start_time if 'start_time' in locals() else 0
        safe_print(f"✗ [{report_num}/{total_reports}] {date_str} failed: {e}")
        logging.error(f"Error generating report for {date_str}: {e}", exc_info=True)
    
    return {
        'report_num': report_num,
        'date': date_str,
        'status': status,
        'elapsed': elapsed,
        'email_sends_count': email_sends_count,
        'bouncebacks_count': bouncebacks_count,
        'clicks_count': clicks_count,
        'opens_count': opens_count,
        'forwards_count': forwards_count,
        'total_records': total_records,
        'report_path': report_path,
        'error_msg': error_msg
    }

def run_weekly_reports_parallel(max_workers=None):
    """
    Generate daily reports for the past week using parallel processing.
    
    Args:
        max_workers: Number of parallel workers (default from config, recommended 2-4 to avoid API rate limits)
    """
    if max_workers is None:
        max_workers = DEFAULT_MAX_WORKERS
    num_days = 7
    
    print(f"\n{'='*80}")
    print(f"  WEEKLY REPORT GENERATION - PARALLEL MODE")
    print(f"  Processing {num_days} days with {max_workers} parallel workers")
    print(f"{'='*80}\n")
    
    # Check S3 connectivity if upload is enabled
    if not SAVE_LOCALLY:
        logging.info("S3 upload is enabled. Checking S3 connectivity...")
        is_s3_ok, s3_message = ping_s3_bucket(S3_BUCKET_NAME)
        if not is_s3_ok:
            logging.error(f"S3 bucket check failed: {s3_message}")
            logging.error("Aborting report generation.")
            sys.exit(1)
        logging.info("S3 connectivity verified.")
        print(f"✓ S3 bucket verified: {S3_BUCKET_NAME}/{S3_FOLDER_PATH}\n")
    else:
        print("ℹ S3 upload disabled - saving locally only\n")
    
    # Calculate date range (last 7 days, ending yesterday)
    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=num_days - 1)
    
    print(f"Date range: {start_date} to {end_date}")
    print(f"Total reports to generate: {num_days}")
    print(f"Parallel workers: {max_workers}\n")
    
    # Prepare metrics file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    metrics_file = f"{WEEKLY_REPORTS_DIR}/weekly_report_metrics_parallel_{timestamp}.csv"
    
    with open(metrics_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Report Number',
            'Date', 
            'Status',
            'Time (seconds)',
            'Email Sends',
            'Bouncebacks',
            'Clicks',
            'Opens',
            'Forwards',
            'Total Records',
            'Report Path',
            'Error Message'
        ])
    
    print(f"Metrics will be saved to: {metrics_file}\n")
    print(f"{'─'*80}")
    print("Starting parallel processing...")
    print(f"{'─'*80}")
    
    overall_start = time.time()
    all_metrics = []
    
    # Submit all tasks to thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create futures for all dates
        future_to_date = {}
        for i in range(num_days):
            current_date = end_date - timedelta(days=i)
            future = executor.submit(process_single_date, current_date, i + 1, num_days)
            future_to_date[future] = current_date
        
        # Process completed tasks as they finish
        for future in as_completed(future_to_date):
            try:
                metrics = future.result()
                all_metrics.append(metrics)
                
                # Write to CSV immediately
                with open(metrics_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        metrics['report_num'],
                        metrics['date'],
                        metrics['status'],
                        f"{metrics['elapsed']:.2f}",
                        metrics['email_sends_count'],
                        metrics['bouncebacks_count'],
                        metrics['clicks_count'],
                        metrics['opens_count'],
                        metrics['forwards_count'],
                        metrics['total_records'],
                        metrics['report_path'],
                        metrics['error_msg']
                    ])
            except Exception as exc:
                safe_print(f"Task generated an exception: {exc}")
                traceback.print_exc()
    
    overall_elapsed = time.time() - overall_start
    
    # Sort metrics by report number for summary
    all_metrics.sort(key=lambda x: x['report_num'])
    
    # Generate summary
    successful = sum(1 for m in all_metrics if m['status'] == 'Success')
    failed = sum(1 for m in all_metrics if m['status'] == 'Failed')
    no_data = sum(1 for m in all_metrics if m['status'] == 'No Data')
    
    print(f"\n{'='*80}")
    print(f"  WEEKLY SUMMARY - PARALLEL PROCESSING")
    print(f"{'='*80}")
    print(f"Successful reports:  {successful}/{num_days}")
    print(f"Failed reports:      {failed}/{num_days}")
    print(f"No data reports:     {no_data}/{num_days}")
    print(f"\nTiming:")
    print(f"  Total wall time:    {overall_elapsed/60:.1f} minutes ({overall_elapsed:.1f} seconds)")
    print(f"  Parallel workers:   {max_workers}")
    
    if successful > 0:
        successful_metrics = [m for m in all_metrics if m['status'] == 'Success']
        
        total_processing_time = sum(m['elapsed'] for m in successful_metrics)
        avg_time = total_processing_time / len(successful_metrics)
        min_time = min(m['elapsed'] for m in successful_metrics)
        max_time = max(m['elapsed'] for m in successful_metrics)
        
        total_emails = sum(m['email_sends_count'] for m in successful_metrics)
        total_forwards = sum(m['forwards_count'] for m in successful_metrics)
        total_bounces = sum(m['bouncebacks_count'] for m in successful_metrics)
        total_clicks = sum(m['clicks_count'] for m in successful_metrics)
        total_opens = sum(m['opens_count'] for m in successful_metrics)
        total_recs = sum(m['total_records'] for m in successful_metrics)
        
        print(f"  Avg report time:    {avg_time:.1f} seconds")
        print(f"  Fastest report:     {min_time:.1f} seconds")
        print(f"  Slowest report:     {max_time:.1f} seconds")
        
        # Calculate speedup
        sequential_time = total_processing_time
        speedup = sequential_time / overall_elapsed
        efficiency = (speedup / max_workers) * 100
        
        print(f"\nParallelization Efficiency:")
        print(f"  Sequential time:    {sequential_time/60:.1f} minutes (estimated)")
        print(f"  Parallel time:      {overall_elapsed/60:.1f} minutes")
        print(f"  Speedup:            {speedup:.2f}x")
        print(f"  Efficiency:         {efficiency:.1f}%")
        
        print(f"\nVolume Summary:")
        print(f"  Total email sends:  {total_emails:,}")
        print(f"  Total forwards:     {total_forwards:,}")
        print(f"  Total bouncebacks:  {total_bounces:,}")
        print(f"  Total clicks:       {total_clicks:,}")
        print(f"  Total opens:        {total_opens:,}")
        print(f"  Total records:      {total_recs:,}")
        
        if overall_elapsed > 0:
            print(f"\nThroughput:")
            print(f"  Records/second:     {total_recs/overall_elapsed:.1f}")
            print(f"  Emails/second:      {total_emails/overall_elapsed:.1f}")
    
    print(f"\n{'='*80}")
    print(f"Detailed metrics saved to: {metrics_file}")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    # Get max workers from command line argument, default from config
    # Recommended: 2-4 workers to balance speed with API rate limits
    max_workers = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_MAX_WORKERS
    
    if max_workers < 1 or max_workers > MAX_WORKERS_LIMIT:
        print(f"Warning: max_workers should be between 1 and {MAX_WORKERS_LIMIT}")
        print(f"Using default: {DEFAULT_MAX_WORKERS}")
        max_workers = DEFAULT_MAX_WORKERS
    
    run_weekly_reports_parallel(max_workers)
