import sys
import os
from datetime import datetime, timedelta
import logging

# Add parent directory to path to import core and config modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.logging_config import setup_logging

# Setup logging with file output
setup_logging("run_report")

from core.bulk.process_data_bulk import generate_daily_report
from config import SAVE_LOCALLY

# Override SAVE_LOCALLY for this run to skip AWS checks
SAVE_LOCALLY = True


# Automatically authenticate to AWS if needed
if not SAVE_LOCALLY:
    try:
        from core.aws.auto_authenticate import ensure_authenticated
        if not ensure_authenticated(auto_refresh=True, use_poetry=True):
            print("\n✗ Unable to authenticate to AWS. Continuing with local save only...\n")
            # Don't exit - continue with local save
    except Exception as e:
        print(f"\n⚠ AWS authentication error: {e}. Continuing with local save only...\n")

# Conditionally import S3 utils only if needed
if not SAVE_LOCALLY:
    try:
        from core.aws.s3_utils import upload_to_s3, ping_s3_bucket
        from config import S3_BUCKET_NAME, S3_FOLDER_PATH
    except ImportError:
        logging.error("boto3 not installed. Install with: pip install boto3")
        sys.exit(1)

if __name__ == "__main__":
    
    print(f"\n{'='*80}")
    print(f"  DAILY REPORT GENERATION")
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
        print(f"[OK] S3 bucket verified: {S3_BUCKET_NAME}/{S3_FOLDER_PATH}\n")
    else:
        print("[INFO] S3 upload disabled - saving locally only\n")
    
    target_date_obj = datetime.utcnow().date() - timedelta(days=1)
    
    if len(sys.argv) > 1:
        try:
            target_date_obj = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        except ValueError:
            logging.error("Invalid date format. Use YYYY-MM-DD.")
            sys.exit(1)
            
    target_date_str = target_date_obj.strftime("%Y-%m-%d")
    day_name = target_date_obj.strftime('%A')

    print(f"Target date: {target_date_str} ({day_name})")
    print(f"{'-'*80}\n")
    
    try:
        import time
        start_time = time.time()
        
        result = generate_daily_report(target_date_str)
        
        # Handle both old (single value) and new (tuple) return formats
        if isinstance(result, tuple):
            report_path, _ = result  # Ignore forward count in this script
        else:
            report_path = result
        
        elapsed = time.time() - start_time
        
        if report_path:
            # Extract metrics from the generated file
            total_records = 0  # Initialize to avoid NameError if CSV reading fails
            try:
                import pandas as pd
                try:
                    df = pd.read_csv(report_path, sep='\t', encoding='utf-16', on_bad_lines='skip')
                except TypeError:
                    # on_bad_lines parameter doesn't exist in older pandas versions
                    df = pd.read_csv(report_path, sep='\t', encoding='utf-16', error_bad_lines=False)
                total_records = len(df)
                
                print(f"\n{'='*80}")
                print(f"  REPORT GENERATION SUCCESSFUL")
                print(f"{'='*80}")
                print(f"Report file:     {report_path}")
                print(f"Total records:   {total_records:,}")
                print(f"Processing time: {elapsed:.1f} seconds")
                
                # Extract volume metrics if columns exist
                if 'Total Sends' in df.columns:
                    email_sends = df['Total Sends'].sum()
                    print(f"Email sends:     {email_sends:,}")
                
                if 'Total Bouncebacks' in df.columns:
                    bouncebacks = df['Total Bouncebacks'].sum()
                    print(f"Bouncebacks:     {bouncebacks:,}")
                
                if 'Unique Opens' in df.columns:
                    # Count rows with opens (not sum, since Unique Opens is binary 0/1)
                    opens = len(df[df['Unique Opens'] > 0])
                    print(f"Opens:           {opens:,}")
                    
            except Exception as read_error:
                logging.warning(f"Could not read metrics from report file: {read_error}")
                print(f"\n✓ Report generation successful: {report_path}")
                print(f"  Processing time: {elapsed:.1f} seconds")
            
            # Upload to S3 if enabled (skip empty files with only headers)
            if not SAVE_LOCALLY:
                if total_records > 0:
                    print(f"\nUploading to S3...")
                    upload_success = upload_to_s3(report_path, S3_BUCKET_NAME, S3_FOLDER_PATH)
                    if upload_success:
                        print(f"✓ Successfully uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FOLDER_PATH}/{os.path.basename(report_path)}")
                    else:
                        print(f"✗ Failed to upload to S3")
                        logging.error(f"Failed to upload to S3")
                else:
                    print(f"\n⊘ Skipped S3 upload (empty file)")
            else:
                print(f"\nFile saved locally only (S3 upload disabled)")
            
            print(f"{'='*80}\n")
        else:
            print(f"\n⊘ No data found for {target_date_str}")
            print(f"   Processing time: {elapsed:.1f} seconds\n")
            
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"  ERROR")
        print(f"{'='*80}")
        print(f"✗ An error occurred during report generation")
        print(f"  {e}")
        print(f"{'='*80}\n")
        logging.exception(f"An error occurred during report generation: {e}")
        sys.exit(1)