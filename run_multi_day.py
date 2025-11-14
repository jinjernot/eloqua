"""
Run daily reports for multiple consecutive days.
Useful for backfilling historical data and testing cache performance.
"""
import sys
from datetime import datetime, timedelta
from core.bulk.process_data_bulk import generate_daily_report
import logging

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
    
    # Calculate date range
    end_date = datetime.utcnow().date() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=num_days - 1)
    
    print(f"Date range: {start_date} to {end_date}")
    print(f"Total reports to generate: {num_days}\n")
    
    successful = 0
    failed = 0
    total_time = 0
    
    for i in range(num_days):
        current_date = end_date - timedelta(days=i)
        date_str = current_date.strftime('%Y-%m-%d')
        
        print(f"\n{'─'*70}")
        print(f"[{i+1}/{num_days}] Processing {date_str}")
        print(f"{'─'*70}")
        
        try:
            start_time = datetime.now()
            report_path = generate_daily_report(date_str)
            elapsed = (datetime.now() - start_time).total_seconds()
            
            total_time += elapsed
            successful += 1
            
            print(f"✓ Completed in {elapsed:.1f} seconds")
            print(f"  Report saved: {report_path}")
            
        except Exception as e:
            failed += 1
            print(f"✗ Failed: {e}")
            logging.error(f"Error generating report for {date_str}: {e}", exc_info=True)
    
    # Summary
    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    print(f"Successful: {successful}/{num_days}")
    print(f"Failed: {failed}/{num_days}")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Average per report: {total_time/num_days:.1f} seconds")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    # Get number of days from command line argument, default to 100
    num_days = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    
    run_multi_day_reports(num_days)
