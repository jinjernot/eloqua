"""
Run daily reports for the past week with detailed performance metrics.
Tracks timing and volume for each processing step to identify bottlenecks.
"""
import sys
import os
import csv
import traceback
import time
from datetime import datetime, timedelta

# Add parent directory to path to import core and config modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import WEEKLY_REPORTS_DIR

from core.bulk.process_data_bulk import generate_daily_report
from core.aws.auto_authenticate import ensure_authenticated
from config import SAVE_LOCALLY
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

def run_weekly_reports_with_metrics():
    """
    Generate reports for the last 7 days with detailed performance tracking.
    Creates a comprehensive metrics file showing timing and volume for each process.
    """
    num_days = 7
    
    print(f"\n{'='*80}")
    print(f"  WEEKLY REPORT GENERATION WITH PERFORMANCE METRICS")
    print(f"  Generating reports for the last {num_days} days")
    print(f"{'='*80}\n")
    
    # Ensure AWS credentials are fresh before checking S3
    if not SAVE_LOCALLY:
        logging.info("Ensuring AWS credentials are authenticated...")
        if not ensure_authenticated():
            logging.error("Failed to authenticate AWS credentials.")
            logging.error("Aborting report generation.")
            sys.exit(1)
        logging.info("AWS credentials verified.")
    
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
    end_date = datetime.utcnow().date() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=num_days - 1)
    
    print(f"Date range: {start_date} to {end_date}")
    print(f"Total reports to generate: {num_days}\n")
    
    # Performance tracking
    successful = 0
    failed = 0
    no_data = 0
    total_time = 0
    
    # Prepare detailed metrics log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    metrics_file = f"{WEEKLY_REPORTS_DIR}/weekly_report_metrics_{timestamp}.csv"
    
    with open(metrics_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Report Number',
            'Date', 
            'Status',
            'Total Time (seconds)',
            'Email Sends Count',
            'Bouncebacks Count',
            'Clicks Count',
            'Opens Count',
            'Forwards Count',
            'Total Output Records',
            'Report Path',
            'Error Message'
        ])
    
    print(f"Performance metrics will be saved to: {metrics_file}\n")
    
    # Store all metrics for summary analysis
    all_metrics = []
    
    for i in range(num_days):
        current_date = end_date - timedelta(days=i)
        date_str = current_date.strftime('%Y-%m-%d')
        
        print(f"\n{'─'*80}")
        print(f"[{i+1}/{num_days}] Processing {date_str} ({current_date.strftime('%A')})")
        print(f"{'─'*80}")
        
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
            
            # Hook into the logging to capture metrics
            # Note: This is a simplified approach - for full metrics we'd need to modify
            # the generate_daily_report function to return metrics
            result = generate_daily_report(date_str)
            
            # Handle both old (single value) and new (tuple) return formats
            if isinstance(result, tuple):
                report_path, _ = result  # Ignore forward count in this script
            else:
                report_path = result
            
            elapsed = time.time() - start_time
            total_time += elapsed
            
            if report_path:
                successful += 1
                status = "Success"
                
                # Try to get record count from the generated file
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
                        opens_count = len(df[df['Opened'] > 0]) if pd.api.types.is_numeric_dtype(df['Opened']) else len(df[df['Opened'] == 'Yes'])
                    
                except Exception as read_error:
                    logging.warning(f"Could not read metrics from report file: {read_error}")
                
                print(f"✓ Completed in {elapsed:.1f} seconds")
                print(f"  Report saved: {report_path}")
                print(f"  Total records: {total_records:,}")
                if email_sends_count > 0:
                    print(f"  Email sends: {email_sends_count:,}")
                if forwards_count > 0:
                    print(f"  Forwards: {forwards_count:,}")
                if bouncebacks_count > 0:
                    print(f"  Bouncebacks: {bouncebacks_count:,}")
                if clicks_count > 0:
                    print(f"  Clicks: {clicks_count:,}")
                if opens_count > 0:
                    print(f"  Opens: {opens_count:,}")
                
                # Upload to S3 if enabled
                if not SAVE_LOCALLY:
                    print(f"  Uploading to S3...", end=" ")
                    upload_success = upload_to_s3(report_path, S3_BUCKET_NAME, S3_FOLDER_PATH)
                    if upload_success:
                        print(f"✓")
                    else:
                        print(f"✗")
                        error_msg = "S3 upload failed"
            else:
                no_data += 1
                status = "No Data"
                error_msg = "No email sends found for this date"
                print(f"⊘ No data for this date ({elapsed:.1f} seconds)")
            
        except Exception as e:
            failed += 1
            error_msg = str(e)
            elapsed = time.time() - start_time if 'start_time' in locals() else 0
            print(f"✗ Failed: {e}")
            print(f"Full traceback:")
            traceback.print_exc()
            logging.error(f"Error generating report for {date_str}: {e}", exc_info=True)
        
        # Store metrics
        metrics = {
            'report_num': i + 1,
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
        all_metrics.append(metrics)
        
        # Write metrics record
        with open(metrics_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                i + 1,
                date_str,
                status,
                f"{elapsed:.2f}",
                email_sends_count,
                bouncebacks_count,
                clicks_count,
                opens_count,
                forwards_count,
                total_records,
                report_path,
                error_msg
            ])
    
    # Generate summary
    print(f"\n{'='*80}")
    print(f"  WEEKLY SUMMARY")
    print(f"{'='*80}")
    print(f"Successful reports:  {successful}/{num_days}")
    print(f"Failed reports:      {failed}/{num_days}")
    print(f"No data reports:     {no_data}/{num_days}")
    print(f"\nTiming:")
    print(f"  Total time:        {total_time/60:.1f} minutes")
    print(f"  Average per report: {total_time/num_days:.1f} seconds")
    
    if successful > 0:
        successful_metrics = [m for m in all_metrics if m['status'] == 'Success']
        avg_time = sum(m['elapsed'] for m in successful_metrics) / len(successful_metrics)
        min_time = min(m['elapsed'] for m in successful_metrics)
        max_time = max(m['elapsed'] for m in successful_metrics)
        
        total_emails = sum(m['email_sends_count'] for m in successful_metrics)
        total_forwards = sum(m['forwards_count'] for m in successful_metrics)
        total_bounces = sum(m['bouncebacks_count'] for m in successful_metrics)
        total_clicks = sum(m['clicks_count'] for m in successful_metrics)
        total_opens = sum(m['opens_count'] for m in successful_metrics)
        total_recs = sum(m['total_records'] for m in successful_metrics)
        
        print(f"  Fastest report:    {min_time:.1f} seconds")
        print(f"  Slowest report:    {max_time:.1f} seconds")
        
        print(f"\nVolume Summary:")
        print(f"  Total email sends: {total_emails:,}")
        print(f"  Total forwards:    {total_forwards:,}")
        print(f"  Total bouncebacks: {total_bounces:,}")
        print(f"  Total clicks:      {total_clicks:,}")
        print(f"  Total opens:       {total_opens:,}")
        print(f"  Total records:     {total_recs:,}")
        
        if total_time > 0:
            print(f"\nThroughput:")
            print(f"  Records/second:    {total_recs/total_time:.1f}")
            print(f"  Emails/second:     {total_emails/total_time:.1f}")
    
    print(f"\n{'='*80}")
    print(f"Detailed metrics saved to: {metrics_file}")
    print(f"{'='*80}\n")
    
    # Performance recommendations
    print(f"\n{'='*80}")
    print(f"  PERFORMANCE OPTIMIZATION SUGGESTIONS")
    print(f"{'='*80}")
    if successful > 0:
        avg_time = sum(m['elapsed'] for m in all_metrics if m['status'] == 'Success') / successful
        
        if avg_time > 60:
            print("⚠ Average processing time is > 1 minute per report")
            print("  Recommendations:")
            print("  1. Enable parallel processing for independent data fetches")
            print("  2. Implement caching for static data (campaigns, users)")
            print("  3. Use database indexing if querying from database")
            print("  4. Consider batch processing optimization")
        elif avg_time > 30:
            print("ℹ Average processing time is moderate (30-60 seconds)")
            print("  Consider implementing caching for frequently accessed data")
        else:
            print("✓ Processing time is good (< 30 seconds per report)")
        
        if total_time > 300:  # 5 minutes total
            print(f"\n⚠ Total processing time ({total_time/60:.1f} min) could be improved")
            print("  Consider:")
            print("  - Parallel report generation for multiple dates")
            print("  - Reducing data fetch window if possible")
            print("  - Optimizing pandas operations")
    
    print(f"{'='*80}\n")

if __name__ == "__main__":
    run_weekly_reports_with_metrics()
