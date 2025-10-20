import sys
import os
from datetime import datetime, timedelta
import logging

# Set up logging so you can see the output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure the 'core' module can be found
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.bulk.process_data_bulk import generate_daily_report

if __name__ == "__main__":
    
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
        else:
            logging.warning("--- Report generation finished, but no file was created. ---")
            
    except Exception as e:
        logging.exception(f"An error occurred during report generation: {e}")
        sys.exit(1)