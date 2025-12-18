"""
Run daily reports for the past month with PARALLEL processing - Production Version.
Optimized for long-running production workloads with better error handling,
progress tracking, and configurable parameters.
"""
import sys
import csv
import traceback
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.bulk.process_data_bulk import generate_daily_report
from config import SAVE_LOCALLY
import logging
import threading
import signal

# Conditionally import S3 utils
if not SAVE_LOCALLY:
    try:
        from core.aws.s3_utils import upload_to_s3, ping_s3_bucket
        from config import S3_BUCKET_NAME, S3_FOLDER_PATH
    except ImportError:
        logging.error("boto3 not installed. Install with: pip install boto3")
        sys.exit(1)

# Setup logging with thread-safe handler
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)

# Thread-safe print lock
print_lock = threading.Lock()

# Global flag for graceful shutdown
shutdown_flag = threading.Event()

def safe_print(*args, **kwargs):
    """Thread-safe print function"""
    with print_lock:
        print(*args, **kwargs)

def signal_handler(signum, frame):
    """Handle interrupt signals gracefully"""
    safe_print("\n\n⚠ Interrupt received. Finishing current tasks and shutting down gracefully...")
    safe_print("⚠ Press Ctrl+C again to force quit (may lose progress)")
    shutdown_flag.set()

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

def process_single_date(date_obj, report_num, total_reports):
    """
    Process a single date's report with metrics tracking.
    Returns a dictionary with all metrics.
    """
    # Check if shutdown was requested
    if shutdown_flag.is_set():
        safe_print(f"⚠ [{report_num}/{total_reports}] Skipping {date_obj.strftime('%Y-%m-%d')} due to shutdown request")
        return None
    
    date_str = date_obj.strftime('%Y-%m-%d')
    day_name = date_obj.strftime('%A')
    
    safe_print(f"\n[{report_num}/{total_reports}] Starting {date_str} ({day_name})")
    
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
        
        report_path = generate_daily_report(date_str)
        
        elapsed = time.time() - start_time
        
        if report_path:
            status = "Success"
            
            # Extract metrics from the generated file
            try:
                import pandas as pd
                df = pd.read_csv(report_path)
                total_records = len(df)
                
                # Extract volume metrics if columns exist
                if 'Email Type' in df.columns:
                    email_sends_count = len(df[df['Email Type'].str.lower().str.contains('send', na=False)])
                    forwards_count = len(df[df['Email Type'].str.lower().str.contains('forward', na=False)])
                
                if 'Bounced' in df.columns:
                    bouncebacks_count = df['Bounced'].sum() if pd.api.types.is_numeric_dtype(df['Bounced']) else len(df[df['Bounced'] == 'Yes'])
                
                if 'Clicked' in df.columns:
                    clicks_count = df['Clicked'].sum() if pd.api.types.is_numeric_dtype(df['Clicked']) else len(df[df['Clicked'] == 'Yes'])
                
                if 'Opened' in df.columns:
                    opens_count = df['Opened'].sum() if pd.api.types.is_numeric_dtype(df['Opened']) else len(df[df['Opened'] == 'Yes'])
                
            except Exception as read_error:
                logging.warning(f"Could not read metrics from report file: {read_error}")
            
            safe_print(f"✓ [{report_num}/{total_reports}] {date_str} completed in {elapsed:.1f}s - {total_records:,} records")
            
            # Upload to S3 if enabled
            if not SAVE_LOCALLY and not shutdown_flag.is_set():
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

def run_monthly_reports_parallel(num_days=30, max_workers=3):
    """
    Generate reports for the specified number of days using parallel processing.
    
    Args:
        num_days: Number of days to generate reports for (default 30 for ~1 month)
        max_workers: Number of parallel workers (default 3, recommended 2-4)
    """
    print(f"\n{'='*80}")
    print(f"  MONTHLY REPORT GENERATION - PRODUCTION PARALLEL MODE")
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
    
    # Calculate date range (ending yesterday)
    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=num_days - 1)
    
    print(f"Date range: {start_date} to {end_date}")
    print(f"Total reports to generate: {num_days}")
    print(f"Parallel workers: {max_workers}")
    print(f"Estimated time: {(num_days * 60) / max_workers / 60:.1f} - {(num_days * 90) / max_workers / 60:.1f} minutes\n")
    
    # Prepare metrics file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    metrics_file = f"data/monthly_report_metrics_{timestamp}.csv"
    
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
    print("Press Ctrl+C once to stop gracefully after current tasks complete")
    print(f"{'─'*80}")
    
    overall_start = time.time()
    all_metrics = []
    completed = 0
    
    # Submit all tasks to thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create futures for all dates
        future_to_date = {}
        for i in range(num_days):
            if shutdown_flag.is_set():
                safe_print("⚠ Shutdown requested, not submitting more tasks")
                break
            current_date = end_date - timedelta(days=i)
            future = executor.submit(process_single_date, current_date, i + 1, num_days)
            future_to_date[future] = current_date
        
        # Process completed tasks as they finish
        for future in as_completed(future_to_date):
            try:
                metrics = future.result()
                
                if metrics:  # Only process if not skipped due to shutdown
                    all_metrics.append(metrics)
                    completed += 1
                    
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
                    
                    # Progress update every 5 reports
                    if completed % 5 == 0:
                        elapsed_so_far = time.time() - overall_start
                        avg_time = elapsed_so_far / completed
                        remaining = num_days - completed
                        eta = (remaining * avg_time) / 60
                        safe_print(f"\n📊 Progress: {completed}/{num_days} completed ({completed/num_days*100:.1f}%) - ETA: {eta:.1f} min")
                
                if shutdown_flag.is_set():
                    safe_print("⚠ Waiting for running tasks to complete...")
                    
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
    print(f"  MONTHLY SUMMARY - PARALLEL PROCESSING")
    print(f"{'='*80}")
    print(f"Completed reports:   {len(all_metrics)}/{num_days}")
    print(f"Successful reports:  {successful}")
    print(f"Failed reports:      {failed}")
    print(f"No data reports:     {no_data}")
    
    if shutdown_flag.is_set():
        print(f"⚠ Interrupted:       {num_days - len(all_metrics)} reports not started")
    
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
        speedup = sequential_time / overall_elapsed if overall_elapsed > 0 else 0
        efficiency = (speedup / max_workers) * 100 if max_workers > 0 else 0
        
        print(f"\nParallelization Efficiency:")
        print(f"  Sequential time:    {sequential_time/60:.1f} minutes (estimated)")
        print(f"  Parallel time:      {overall_elapsed/60:.1f} minutes")
        print(f"  Speedup:            {speedup:.2f}x")
        print(f"  Efficiency:         {efficiency:.1f}%")
        print(f"  Time saved:         {(sequential_time - overall_elapsed)/60:.1f} minutes")
        
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
            print(f"  Reports/hour:       {successful/(overall_elapsed/3600):.1f}")
    
    print(f"\n{'='*80}")
    print(f"Detailed metrics saved to: {metrics_file}")
    print(f"{'='*80}\n")
    
    if shutdown_flag.is_set():
        print("⚠ Run was interrupted. Re-run to process remaining dates.")
        sys.exit(1)

if __name__ == "__main__":
    # Parse command line arguments
    # Usage: python run_monthly_parallel.py [days] [workers]
    # Examples:
    #   python run_monthly_parallel.py           # 30 days, 3 workers
    #   python run_monthly_parallel.py 60        # 60 days, 3 workers
    #   python run_monthly_parallel.py 30 4      # 30 days, 4 workers
    
    num_days = 30  # Default to ~1 month
    max_workers = 3  # Default workers
    
    if len(sys.argv) > 1:
        try:
            num_days = int(sys.argv[1])
            if num_days < 1 or num_days > 365:
                print("Warning: num_days should be between 1 and 365")
                print("Using default: 30")
                num_days = 30
        except ValueError:
            print(f"Invalid num_days: {sys.argv[1]}")
            print("Using default: 30")
    
    if len(sys.argv) > 2:
        try:
            max_workers = int(sys.argv[2])
            if max_workers < 1 or max_workers > 10:
                print("Warning: max_workers should be between 1 and 10")
                print("Using default: 3")
                max_workers = 3
        except ValueError:
            print(f"Invalid max_workers: {sys.argv[2]}")
            print("Using default: 3")
    
    print(f"Configuration: {num_days} days, {max_workers} workers")
    
    try:
        run_monthly_reports_parallel(num_days, max_workers)
    except KeyboardInterrupt:
        print("\n\n✗ Force quit received. Some progress may be lost.")
        sys.exit(1)
