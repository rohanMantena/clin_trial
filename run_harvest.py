"""
Main harvest runner.
Pulls data from ClinicalTrials.gov, transforms it, and loads into the database.

Usage:
    python run_harvest.py              # Incremental (auto-reads .last_harvest) or full if first run
    python run_harvest.py --since 2026-03-08   # Incremental from specific date
    python run_harvest.py --full       # Force full harvest (ignore .last_harvest)
    python run_harvest.py --test       # Test mode (5 studies)
    python run_harvest.py --batch-size 100     # Custom batch size
"""
import argparse
import json
import os
import time
from datetime import datetime

from harvester import harvest_studies
from transformer import transform_study
from database import init_db, upsert_batch, get_stats


DEFAULT_BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
LAST_HARVEST_FILE = ".last_harvest"
SLEEP_BETWEEN_BATCHES = float(os.environ.get("BATCH_SLEEP", "0.5"))


def _read_last_harvest():
    """Read the last harvest timestamp from file, if it exists."""
    if os.path.exists(LAST_HARVEST_FILE):
        with open(LAST_HARVEST_FILE) as f:
            date_str = f.read().strip()
        if date_str:
            # Extract just the date portion (YYYY-MM-DD) for the API filter
            return date_str[:10]
    return None


def run_harvest(since_date=None, test_mode=False, batch_size=None, force_full=False):
    """Run a full or incremental harvest."""
    if batch_size is None:
        batch_size = DEFAULT_BATCH_SIZE

    # Auto-detect incremental mode from .last_harvest if no explicit date
    if not since_date and not force_full and not test_mode:
        since_date = _read_last_harvest()
        if since_date:
            print(f"Auto-detected last harvest: {since_date}")

    mode = "TEST" if test_mode else "INCREMENTAL" if since_date else "FULL"

    print(f"{'='*60}")
    print(f"Clinical Trials Harvest")
    print(f"Mode: {mode}")
    if since_date:
        print(f"Since: {since_date}")
    print(f"Batch size: {batch_size}")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"{'='*60}\n")

    # Initialize database
    init_db()

    # Configure harvest
    max_pages = 1 if test_mode else None
    page_size = 5 if test_mode else 1000

    # Harvest, transform, and load in batches
    batch = []
    total_loaded = 0
    errors = 0
    start_time = time.time()

    for raw_study in harvest_studies(
        max_pages=max_pages,
        page_size=page_size,
        since_date=since_date,
    ):
        try:
            clean = transform_study(raw_study)
            batch.append(clean)
        except Exception as e:
            errors += 1
            nct = raw_study.get("protocolSection", {}).get("identificationModule", {}).get("nctId", "UNKNOWN")
            print(f"  Error transforming {nct}: {e}")
            continue

        # Upsert batch when full
        if len(batch) >= batch_size:
            try:
                upsert_batch(batch)
                total_loaded += len(batch)
                elapsed = time.time() - start_time
                rate = total_loaded / elapsed if elapsed > 0 else 0
                print(f"  Loaded {total_loaded} studies ({rate:.0f}/sec)")
            except Exception as e:
                print(f"  Error upserting batch: {e}")
                errors += len(batch)
            batch = []
            # Sleep between batches to avoid overwhelming cloud DB
            if SLEEP_BETWEEN_BATCHES > 0:
                time.sleep(SLEEP_BETWEEN_BATCHES)

    # Upsert remaining
    if batch:
        try:
            upsert_batch(batch)
            total_loaded += len(batch)
        except Exception as e:
            print(f"  Error upserting final batch: {e}")
            errors += len(batch)

    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print(f"Harvest Complete")
    print(f"  Studies loaded: {total_loaded}")
    print(f"  Errors: {errors}")
    print(f"  Duration: {elapsed:.1f}s")
    print(f"  Finished: {datetime.now().isoformat()}")
    print(f"{'='*60}\n")

    # Show current database stats
    try:
        stats = get_stats()
        print("Database stats:")
        print(json.dumps(stats, indent=2))
    except Exception as e:
        print(f"Could not fetch stats: {e}")

    # Save last harvest time for incremental runs
    with open(LAST_HARVEST_FILE, "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Harvest clinical trials data")
    parser.add_argument("--since", help="Incremental: only studies updated since this date (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Force full harvest (ignore .last_harvest)")
    parser.add_argument("--test", action="store_true", help="Test mode: harvest only 5 studies")
    parser.add_argument("--batch-size", type=int, help=f"Batch size for DB writes (default: {DEFAULT_BATCH_SIZE})")
    args = parser.parse_args()

    run_harvest(
        since_date=args.since,
        test_mode=args.test,
        batch_size=args.batch_size,
        force_full=args.full,
    )
