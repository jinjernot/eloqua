from datetime import datetime, timedelta
from process_data import generate_daily_report
import os

start_date = datetime.utcnow() - timedelta(days=4)

for i in range(5):
    date = start_date + timedelta(days=i)
    date_str = date.strftime("%Y-%m-%d")
    output_file = f"data/{date_str}.csv"
    
    if os.path.exists(output_file):
        print(f"[INFO] Skipping {date_str}, report already exists.")
        continue

    print(f"\n[INFO] Generating report for {date_str}")
    result = generate_daily_report(date_str)
    print(f"[INFO] Report saved to {result}")
