"""
Run daily reports for all days in February using parallel processing.

Usage:
  python scripts/run_february_parallel.py
  python scripts/run_february_parallel.py 2026
  python scripts/run_february_parallel.py 2026 4
"""
import os
import sys
import csv
import time
import calendar
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure script-local imports work when called from repo root
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

# Reuse the existing monthly parallel infrastructure
import run_monthly_parallel as monthly
from config import MONTHLY_REPORTS_DIR, DEFAULT_MAX_WORKERS, MAX_WORKERS_LIMIT


def default_february_year(today: date) -> int:
    """Default to current year unless we are in Jan/Feb, then use previous year."""
    return today.year if today.month > 2 else today.year - 1


def get_february_range(year: int) -> tuple[date, date]:
    """Return start and end date for February in the given year."""
    last_day = calendar.monthrange(year, 2)[1]
    return date(year, 2, 1), date(year, 2, last_day)


def run_february_reports_parallel(year: int | None = None, max_workers: int | None = None) -> None:
    """Generate reports for every day in February of the selected year."""
    if year is None:
        year = default_february_year(datetime.now().date())

    if max_workers is None:
        max_workers = DEFAULT_MAX_WORKERS

    feb_start, feb_end = get_february_range(year)
    num_days = (feb_end - feb_start).days + 1

    print("\n" + "=" * 80)
    print(f"  FEBRUARY REPORT GENERATION - PARALLEL MODE ({year})")
    print(f"  Processing {num_days} days with {max_workers} parallel workers")
    print("=" * 80 + "\n")

    print(f"Date range: {feb_start} to {feb_end}")
    print(f"Total reports to generate: {num_days}")
    print(f"Parallel workers: {max_workers}\n")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    metrics_file = f"{MONTHLY_REPORTS_DIR}/february_report_metrics_{year}_{timestamp}.csv"

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

    overall_start = time.time()
    all_metrics = []
    completed = 0

    # Keep same order style as monthly script: most recent date first.
    all_dates = [feb_end - timedelta(days=i) for i in range(num_days)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_date = {}
        for i, current_date in enumerate(all_dates, start=1):
            if monthly.shutdown_flag.is_set():
                monthly.safe_print("Shutdown requested, not submitting more tasks")
                break
            future = executor.submit(monthly.process_single_date, current_date, i, num_days)
            future_to_date[future] = current_date

        for future in as_completed(future_to_date):
            metrics = future.result()
            if not metrics:
                continue

            all_metrics.append(metrics)
            completed += 1

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

            if completed % 5 == 0:
                elapsed_so_far = time.time() - overall_start
                avg_time = elapsed_so_far / completed
                remaining = num_days - completed
                eta_minutes = (remaining * avg_time) / 60
                monthly.safe_print(
                    f"Progress: {completed}/{num_days} completed ({completed / num_days * 100:.1f}%) - ETA: {eta_minutes:.1f} min"
                )

    overall_elapsed = time.time() - overall_start

    all_metrics.sort(key=lambda x: x['report_num'])
    successful = sum(1 for m in all_metrics if m['status'] == 'Success')
    failed = sum(1 for m in all_metrics if m['status'] == 'Failed')
    no_data = sum(1 for m in all_metrics if m['status'] == 'No Data')

    print("\n" + "=" * 80)
    print(f"  FEBRUARY SUMMARY ({year})")
    print("=" * 80)
    print(f"Completed reports:   {len(all_metrics)}/{num_days}")
    print(f"Successful reports:  {successful}")
    print(f"Failed reports:      {failed}")
    print(f"No data reports:     {no_data}")
    print(f"Total wall time:     {overall_elapsed / 60:.1f} minutes ({overall_elapsed:.1f} seconds)")
    print(f"Detailed metrics:    {metrics_file}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    selected_year = None
    workers = DEFAULT_MAX_WORKERS

    if len(sys.argv) > 1:
        try:
            selected_year = int(sys.argv[1])
            if selected_year < 2000 or selected_year > 2100:
                print(f"Warning: year {selected_year} looks invalid. Using default February year.")
                selected_year = None
        except ValueError:
            print(f"Invalid year: {sys.argv[1]}. Using default February year.")
            selected_year = None

    if len(sys.argv) > 2:
        try:
            workers = int(sys.argv[2])
            if workers < 1 or workers > MAX_WORKERS_LIMIT:
                print(f"Warning: workers should be between 1 and {MAX_WORKERS_LIMIT}. Using default {DEFAULT_MAX_WORKERS}.")
                workers = DEFAULT_MAX_WORKERS
        except ValueError:
            print(f"Invalid workers: {sys.argv[2]}. Using default {DEFAULT_MAX_WORKERS}.")
            workers = DEFAULT_MAX_WORKERS

    resolved_year = selected_year if selected_year is not None else default_february_year(datetime.now().date())
    print(f"Configuration: February {resolved_year}, workers={workers}")

    run_february_reports_parallel(selected_year, workers)
