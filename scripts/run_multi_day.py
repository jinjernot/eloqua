"""
Run daily reports for multiple consecutive days.
Useful for backfilling historical data and testing cache performance.
"""
import sys
import os
import csv
import traceback
from datetime import datetime, timedelta

# Add parent directory to path to import core and config modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.bulk.process_data_bulk import generate_daily_report
from config import SAVE_LOCALLY, WEEKLY_REPORTS_DIR
import logging

# Conditionally import S3 utils
if not SAVE_LOCALLY:
    try:
        from core.aws.s3_utils import upload_to_s3, ping_s3_bucket
        from config import S3_BUCKET_NAME, S3_FOLDER_PATH
    except ImportError:
        logging.error("boto3 not installed. Install with: pip install boto3")
        sys.exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_multi_day_reports(num_days=100):
    """
    Generate reports for the last N days.
    
    Args:
        num_days: Number of days to generate reports for (default 100)
    """
    print(f"\n{'='*70}")
    print(f"  MULTI-DAY REPORT GENERATION")
    print(f"  Running reports for the last {num_days} days")
    print(f"{'='*70}\n")
    
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
    
    # Calculate date range
    end_date = datetime.utcnow().date() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=num_days - 1)
    
    print(f"Date range: {start_date} to {end_date}")
    print(f"Total reports to generate: {num_days}\n")
    
    successful = 0
    failed = 0
    total_time = 0
    
    # Prepare timing log file
    timing_log_file = f"{WEEKLY_REPORTS_DIR}/report_timing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(timing_log_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Report Number', 'Date', 'Status', 'Time (seconds)', 'Report Path', 'Error Message'])
    
    print(f"Timing log will be saved to: {timing_log_file}\n")
    
    for i in range(num_days):
        current_date = end_date - timedelta(days=i)
        date_str = current_date.strftime('%Y-%m-%d')
        
        print(f"\n{'─'*70}")
        print(f"[{i+1}/{num_days}] Processing {date_str}")
        print(f"{'─'*70}")
        
        report_path = ""
        error_msg = ""
        status = "Failed"
        elapsed = 0
        
        try:
            start_time = datetime.now()
            result = generate_daily_report(date_str)
            
            # Handle both old (single value) and new (tuple) return formats
            if isinstance(result, tuple):
                report_path, _ = result  # Ignore forward count in this script
            else:
                report_path = result
            elapsed = (datetime.now() - start_time).total_seconds()
            
            total_time += elapsed
            
            if report_path:
                successful += 1
                status = "Success"
                print(f"✓ Completed in {elapsed:.1f} seconds")
                print(f"  Report saved: {report_path}")
                
                # Upload to S3 if enabled
                if not SAVE_LOCALLY:
                    print(f"  Uploading to S3...", end=" ")
                    upload_success = upload_to_s3(report_path, S3_BUCKET_NAME, S3_FOLDER_PATH)
                    if upload_success:
                        print(f"✓ Uploaded to s3://{S3_BUCKET_NAME}/{S3_FOLDER_PATH}/")
                    else:
                        print(f"✗ Upload failed")
                        error_msg = "S3 upload failed"
            else:
                failed += 1
                status = "No Data"
                error_msg = "No email sends found for this date"
                print(f"⊘ No data for this date ({elapsed:.1f} seconds)")
            
        except Exception as e:
            failed += 1
            error_msg = str(e)
            print(f"✗ Failed: {e}")
            print(f"Full traceback:")
            traceback.print_exc()
            logging.error(f"Error generating report for {date_str}: {e}", exc_info=True)
        
        # Write timing record
        with open(timing_log_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([i+1, date_str, status, f"{elapsed:.2f}", report_path, error_msg])
    
    # Summary
    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    print(f"Successful: {successful}/{num_days}")
    print(f"Failed: {failed}/{num_days}")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Average per report: {total_time/num_days:.1f} seconds")
    print(f"\nTiming log saved to: {timing_log_file}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    # Get number of days from command line argument, default to 100
    num_days = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    
    run_multi_day_reports(num_days)
