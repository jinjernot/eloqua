import sys
import os
from datetime import datetime, timedelta
import logging

# Set up logging so you can see the output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure the 'core' module can be found
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.bulk.process_data_bulk import generate_daily_report
from core.aws.s3_utils import upload_to_s3, ping_s3_bucket
from config import S3_BUCKET_NAME, S3_FOLDER_PATH, SAVE_LOCALLY

if __name__ == "__main__":
    
    # Check S3 connectivity if upload is enabled
    if not SAVE_LOCALLY:
        logging.info("S3 upload is enabled. Checking S3 connectivity...")
        is_s3_ok, s3_message = ping_s3_bucket(S3_BUCKET_NAME)
        if not is_s3_ok:
            logging.error(f"S3 bucket check failed: {s3_message}")
            logging.error("Aborting report generation.")
            sys.exit(1)
        logging.info("S3 connectivity verified.")
    
    target_date_obj = datetime.utcnow().date() - timedelta(days=1)
    
    if len(sys.argv) > 1:
        try:
            target_date_obj = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        except ValueError:
            logging.error("Invalid date format. Use YYYY-MM-DD.")
            sys.exit(1)
            
    target_date_str = target_date_obj.strftime("%Y-%m-%d")

    logging.info(f"--- Starting Daily Report for {target_date_str} ---")
    
    try:
        report_path = generate_daily_report(target_date_str)
        
        if report_path:
            logging.info(f"--- Report generation successful: {report_path} ---")
            
            # Upload to S3 if enabled
            if not SAVE_LOCALLY:
                logging.info(f"Uploading {report_path} to S3 bucket: {S3_BUCKET_NAME}/{S3_FOLDER_PATH}")
                upload_success = upload_to_s3(report_path, S3_BUCKET_NAME, S3_FOLDER_PATH)
                if upload_success:
                    logging.info(f"✓ Successfully uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FOLDER_PATH}/{os.path.basename(report_path)}")
                else:
                    logging.error(f"✗ Failed to upload to S3")
            else:
                logging.info(f"File saved locally only (S3 upload disabled)")
        else:
            logging.warning("--- Report generation finished, but no file was created. ---")
            
    except Exception as e:
        logging.exception(f"An error occurred during report generation: {e}")
        sys.exit(1)