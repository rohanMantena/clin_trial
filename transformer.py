"""
Transformer: converts raw ClinicalTrials.gov API responses 
into our source-agnostic clinical trials schema.
"""
import json
import re
from datetime import datetime


def transform_study(raw_study):
    """
    Take a single raw study dict from ClinicalTrials.gov API v2
    and return a clean, flat dict matching our schema.
    """
    protocol = raw_study.get("protocolSection", {})
    
    # Pull out each module (safely — any could be missing)
    ident = protocol.get("identificationModule", {})
    status = protocol.get("statusModule", {})
    desc = protocol.get("descriptionModule", {})
    conditions = protocol.get("conditionsModule", {})
    design = protocol.get("designModule", {})
    interventions = protocol.get("armsInterventionsModule", {})
    locations = protocol.get("contactsLocationsModule", {})
    sponsor = protocol.get("sponsorCollaboratorsModule", {})
    refs = protocol.get("referencesModule", {})

    return {
        "source": "clinicaltrials.gov",
        "source_id": ident.get("nctId"),
        "source_url": f"https://clinicaltrials.gov/study/{ident.get('nctId')}",
        "title": ident.get("briefTitle"),
        "official_title": ident.get("officialTitle"),
        "brief_summary": desc.get("briefSummary"),
        "status": status.get("overallStatus"),
        "phase": _normalize_phase(design.get("phases", [])),
        "study_type": design.get("studyType"),
        "enrollment": _extract_enrollment(design.get("enrollmentInfo")),
        "start_date": _extract_date(status.get("startDateStruct")),
        "completion_date": _extract_date(status.get("completionDateStruct")),
        "conditions": conditions.get("conditions", []),
        "interventions": _extract_interventions(interventions.get("interventions", [])),
        "sponsor": _extract_sponsor(sponsor),
        "locations": _extract_locations(locations.get("locations", [])),
        "linked_publications": _extract_publications(refs.get("references", [])),
        "source_updated_at": _extract_date(status.get("lastUpdatePostDateStruct")),
    }


def _normalize_phase(phases):
    """
    ClinicalTrials.gov can list multiple phases (e.g., ["PHASE1", "PHASE2"] 
    for a Phase 1/2 study). We join them into a single string.
    Observational studies have an empty list — we return "NA".
    """
    if not phases:
        return "NA"
    return "/".join(phases)


def _extract_date(date_struct):
    """
    Dates come as {"date": "2020-04-29", "type": "ACTUAL"} or 
    {"date": "2023-06"} (month-only precision). 
    We return the date string as-is to preserve precision.
    """
    if not date_struct:
        return None
    return date_struct.get("date")


def _extract_enrollment(enrollment_info):
    """Pull the enrollment count, return None if missing."""
    if not enrollment_info:
        return None
    return enrollment_info.get("count")


def _extract_interventions(raw_interventions):
    """
    Extract intervention name and type into clean objects.
    Input:  [{"type": "DRUG", "name": "Metformin", ...extra fields...}]
    Output: [{"name": "Metformin", "type": "DRUG"}]
    """
    result = []
    for intervention in raw_interventions:
        result.append({
            "name": intervention.get("name"),
            "type": intervention.get("type"),
        })
    return result


def _extract_sponsor(sponsor_module):
    """Pull the lead sponsor name."""
    lead = sponsor_module.get("leadSponsor", {})
    return lead.get("name")


def _extract_locations(raw_locations):
    """
    Simplify locations to country + city.
    We deduplicate countries for a cleaner representation.
    """
    result = []
    for loc in raw_locations:
        result.append({
            "country": loc.get("country"),
            "city": loc.get("city"),
        })
    return result


def _extract_publications(raw_refs):
    """
    Extract linked publications with PMIDs and any DOIs we can parse
    from the citation text.
    """
    result = []
    for ref in raw_refs:
        pub = {
            "pmid": ref.get("pmid"),
            "type": ref.get("type"),  # RESULT, BACKGROUND, DERIVED
        }
        
        # Try to extract DOI from citation text
        citation = ref.get("citation", "")
        doi_match = re.search(r'doi[:\s]*(10\.\S+)', citation, re.IGNORECASE)
        if doi_match:
            # Clean up trailing punctuation
            doi = doi_match.group(1).rstrip('.,;)')
            pub["doi"] = doi
        
        result.append(pub)
    return result


# ---- Test with our sample data ----
if __name__ == "__main__":
    with open("sample_raw.json") as f:
        raw_studies = json.load(f)
    
    print(f"Transforming {len(raw_studies)} studies...\n")
    
    for raw in raw_studies:
        clean = transform_study(raw)
        print(f"--- {clean['source_id']}: {clean['title'][:60]}... ---")
        print(f"  Status:       {clean['status']}")
        print(f"  Phase:        {clean['phase']}")
        print(f"  Type:         {clean['study_type']}")
        print(f"  Enrollment:   {clean['enrollment']}")
        print(f"  Conditions:   {clean['conditions']}")
        print(f"  Interventions:{[i['name'] for i in clean['interventions']]}")
        print(f"  Sponsor:      {clean['sponsor']}")
        print(f"  Locations:    {[l['country'] for l in clean['locations']]}")
        print(f"  Publications: {len(clean['linked_publications'])}")
        for pub in clean['linked_publications']:
            doi_str = f", DOI: {pub.get('doi')}" if pub.get('doi') else ""
            print(f"    - PMID: {pub['pmid']} ({pub['type']}{doi_str})")
        print(f"  Source updated: {clean['source_updated_at']}")
        print()
