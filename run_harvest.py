"""
Main harvest runner. 
Pulls data from ClinicalTrials.gov, transforms it, and loads into the database.

Usage:
    python run_harvest.py              # Full harvest (all studies)
    python run_harvest.py --since 2026-03-08   # Incremental (since date)
    python run_harvest.py --test       # Test mode (5 studies)
"""
import argparse
import json
import time
from datetime import datetime

from harvester import harvest_studies
from transformer import transform_study
from database import init_db, upsert_batch, get_stats


BATCH_SIZE = 1000  # Upsert to DB in batches of 100


def run_harvest(since_date=None, test_mode=False):
    """Run a full or incremental harvest."""
    
    print(f"{'='*60}")
    print(f"Clinical Trials Harvest")
    print(f"Mode: {'TEST' if test_mode else 'INCREMENTAL' if since_date else 'FULL'}")
    if since_date:
        print(f"Since: {since_date}")
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
        if len(batch) >= BATCH_SIZE:
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
    stats = get_stats()
    print("Database stats:")
    print(json.dumps(stats, indent=2))
    
    # Save last harvest time for incremental runs
    with open(".last_harvest", "w") as f:
        f.write(datetime.now().isoformat())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Harvest clinical trials data")
    parser.add_argument("--since", help="Incremental: only studies updated since this date (YYYY-MM-DD)")
    parser.add_argument("--test", action="store_true", help="Test mode: harvest only 5 studies")
    args = parser.parse_args()
    
    run_harvest(since_date=args.since, test_mode=args.test)
