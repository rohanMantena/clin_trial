"""
Transformer: converts raw ClinicalTrials.gov API responses
into our source-agnostic clinical trials schema.
"""
import json
import re


def transform_study(raw_study):
    """
    Take a single raw study dict from ClinicalTrials.gov API v2
    and return a clean, flat dict matching our schema.
    """
    protocol = raw_study.get("protocolSection", {})
    derived = raw_study.get("derivedSection", {})

    # Pull out each module (safely — any could be missing)
    ident = protocol.get("identificationModule", {})
    status = protocol.get("statusModule", {})
    desc = protocol.get("descriptionModule", {})
    conditions_mod = protocol.get("conditionsModule", {})
    design = protocol.get("designModule", {})
    interventions_mod = protocol.get("armsInterventionsModule", {})
    contacts = protocol.get("contactsLocationsModule", {})
    sponsor_mod = protocol.get("sponsorCollaboratorsModule", {})
    refs = protocol.get("referencesModule", {})
    eligibility = protocol.get("eligibilityModule", {})

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
        "registry_date": _extract_date(status.get("studyFirstPostDateStruct")),
        "conditions": conditions_mod.get("conditions", []),
        "mesh_terms": _extract_mesh_terms(derived),
        "interventions": _extract_interventions(interventions_mod.get("interventions", [])),
        "sponsor": _extract_sponsor(sponsor_mod),
        "investigators": _extract_investigators(contacts),
        "locations": _extract_locations(contacts.get("locations", [])),
        "linked_publications": _extract_publications(refs.get("references", [])),
        "secondary_ids": _extract_secondary_ids(ident),
        "eligibility": _extract_eligibility(eligibility),
        "has_results": raw_study.get("hasResults", False),
        "source_updated_at": _extract_date(status.get("lastUpdatePostDateStruct")),
    }


def _normalize_phase(phases):
    if not phases:
        return "NA"
    return "/".join(phases)


def _extract_date(date_struct):
    if not date_struct:
        return None
    return date_struct.get("date")


def _extract_enrollment(enrollment_info):
    if not enrollment_info:
        return None
    return enrollment_info.get("count")


def _extract_interventions(raw_interventions):
    result = []
    for intervention in raw_interventions:
        result.append({
            "name": intervention.get("name"),
            "type": intervention.get("type"),
        })
    return result


def _extract_sponsor(sponsor_module):
    lead = sponsor_module.get("leadSponsor", {})
    return lead.get("name")


def _extract_investigators(contacts_module):
    """
    Extract principal investigators — maps to Authors in OpenAlex.
    Returns [{name, affiliation, role}].
    """
    result = []
    for official in contacts_module.get("overallOfficials", []):
        result.append({
            "name": official.get("name"),
            "affiliation": official.get("affiliation"),
            "role": official.get("role"),
        })
    return result


def _extract_locations(raw_locations):
    result = []
    for loc in raw_locations:
        result.append({
            "country": loc.get("country"),
            "city": loc.get("city"),
        })
    return result


def _extract_publications(raw_refs):
    result = []
    for ref in raw_refs:
        pub = {
            "pmid": ref.get("pmid"),
            "type": ref.get("type"),
        }
        citation = ref.get("citation", "")
        doi_match = re.search(r'doi[:\s]*(10\.\S+)', citation, re.IGNORECASE)
        if doi_match:
            doi = doi_match.group(1).rstrip('.,;)')
            pub["doi"] = doi
        result.append(pub)
    return result


def _extract_mesh_terms(derived_section):
    """
    Extract MeSH terms from the derived section — maps to Topics/Concepts in OpenAlex.
    Combines condition and intervention MeSH terms.
    """
    terms = []
    for module_key in ["conditionBrowseModule", "interventionBrowseModule"]:
        module = derived_section.get(module_key, {})
        for mesh in module.get("meshes", []):
            terms.append({
                "id": mesh.get("id"),
                "term": mesh.get("term"),
            })
    return terms


def _extract_secondary_ids(ident_module):
    """Extract secondary/registry IDs for cross-registry deduplication."""
    result = []
    for sid in ident_module.get("secondaryIdInfos", []):
        entry = {"id": sid.get("id")}
        if sid.get("type"):
            entry["type"] = sid["type"]
        if sid.get("domain"):
            entry["domain"] = sid["domain"]
        result.append(entry)
    return result


def _extract_eligibility(elig_module):
    """Extract structured eligibility criteria (sex, age range, healthy volunteers)."""
    if not elig_module:
        return {}
    result = {}
    if elig_module.get("sex"):
        result["sex"] = elig_module["sex"]
    if elig_module.get("minimumAge"):
        result["min_age"] = elig_module["minimumAge"]
    if elig_module.get("maximumAge"):
        result["max_age"] = elig_module["maximumAge"]
    if elig_module.get("healthyVolunteers") is not None:
        result["healthy_volunteers"] = elig_module["healthyVolunteers"]
    return result


# ---- Test with our sample data ----
if __name__ == "__main__":
    with open("sample_raw.json") as f:
        raw_studies = json.load(f)

    print(f"Transforming {len(raw_studies)} studies...\n")

    for raw in raw_studies:
        clean = transform_study(raw)
        print(f"--- {clean['source_id']}: {clean['title'][:60]}... ---")
        print(f"  Status:         {clean['status']}")
        print(f"  Phase:          {clean['phase']}")
        print(f"  Type:           {clean['study_type']}")
        print(f"  Enrollment:     {clean['enrollment']}")
        print(f"  Conditions:     {clean['conditions']}")
        print(f"  MeSH terms:     {len(clean['mesh_terms'])} terms")
        print(f"  Investigators:  {clean['investigators']}")
        print(f"  Secondary IDs:  {clean['secondary_ids'][:2]}")
        print(f"  Has results:    {clean['has_results']}")
        print(f"  Registry date:  {clean['registry_date']}")
        print()
