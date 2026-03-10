"""
Harvester for ClinicalTrials.gov API v2
Pulls clinical trial data and returns raw JSON studies.
"""
import requests
import time
import json
from datetime import datetime


BASE_URL = "https://clinicaltrials.gov/api/v2/studies"


def harvest_studies(max_pages=None, page_size=10, since_date=None):
    """
    Generator that yields raw study dicts from ClinicalTrials.gov.
    
    Args:
        max_pages: Limit number of pages (None = all pages)
        page_size: Studies per page (max 1000)
        since_date: Only get studies updated after this date (ISO format string)
    """
    page_token = None
    page_count = 0
    total_fetched = 0

    while True:
        # Build request params
        params = {
            "pageSize": page_size,
            "format": "json",
            "countTotal": "true",
        }
        
        if page_token:
            params["pageToken"] = page_token
            
        # For incremental daily updates - only fetch recently modified studies
        if since_date:
            params["filter.lastUpdatePostDate"] = since_date
        
        # Make the request with retry logic
        study_data = _fetch_with_retry(params)
        
        if study_data is None:
            print("Failed to fetch page, stopping.")
            break
        
        studies = study_data.get("studies", [])
        total_count = study_data.get("totalCount", "unknown")
        
        if page_count == 0:
            print(f"Total studies matching query: {total_count}")
        
        for study in studies:
            yield study
            total_fetched += 1
        
        page_count += 1
        print(f"Page {page_count}: fetched {len(studies)} studies ({total_fetched} total)")
        
        # Check if there are more pages
        page_token = study_data.get("nextPageToken")
        if not page_token:
            print("No more pages.")
            break
            
        if max_pages and page_count >= max_pages:
            print(f"Reached max_pages limit ({max_pages}).")
            break
        
        # Respect rate limit (~50 req/min)
        time.sleep(1.2)
    
    print(f"Harvest complete. Total studies fetched: {total_fetched}")


def _fetch_with_retry(params, max_retries=3):
    """Fetch a page from the API with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limited - back off
                wait_time = 2 ** (attempt + 1)
                print(f"Rate limited. Waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"Error {response.status_code}: {response.text[:200]}")
                time.sleep(2 ** attempt)
                
        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
    
    return None


# ---- Quick test ----
if __name__ == "__main__":
    print("Testing harvester - pulling 5 studies...\n")
    
    studies = []
    for study in harvest_studies(max_pages=1, page_size=5):
        studies.append(study)
    
    # Save raw output so we can inspect the structure
    with open("sample_raw.json", "w") as f:
        json.dump(studies, f, indent=2)
    
    print(f"\nSaved {len(studies)} studies to sample_raw.json")
    
    # Print the structure of the first study so we can see what we're working with
    if studies:
        first = studies[0]
        print("\n--- First study top-level keys ---")
        print(json.dumps(list(first.keys()), indent=2))
        
        protocol = first.get("protocolSection", {})
        print("\n--- protocolSection keys ---")
        print(json.dumps(list(protocol.keys()), indent=2))
        
        # Show the identification module
        ident = protocol.get("identificationModule", {})
        print("\n--- identificationModule ---")
        print(json.dumps(ident, indent=2))
        
        # Show the status module
        status = protocol.get("statusModule", {})
        print("\n--- statusModule ---")
        print(json.dumps(status, indent=2))
        
        # Show references if they exist
        refs = protocol.get("referencesModule", {})
        if refs:
            print("\n--- referencesModule ---")
            print(json.dumps(refs, indent=2)[:1000])
        else:
            print("\n--- No references module in this study ---")
