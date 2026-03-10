# Clinical Trials Aggregator

A source-agnostic API for clinical trial data, built as an abstraction layer for OpenAlex. Currently ingests from ClinicalTrials.gov, designed to support multiple registries under one unified schema.

## Quick Start

```bash
pip install -r requirements.txt

# Initialize database and harvest (first run gets everything)
python run_harvest.py

# Start the API
uvicorn api:app --reload --port 8000

# Visit http://localhost:8000/docs for interactive API docs
```

## Architecture

```
ClinicalTrials.gov API
        │
   harvester.py      ← Extract: paginate through source API
        │
   transformer.py    ← Transform: flatten nested JSON to clean schema
        │
   database.py       ← Load: upsert into database
        │
   api.py            ← Serve: FastAPI endpoints for querying
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /studies` | List/filter studies with pagination |
| `GET /studies/{source}/{source_id}` | Get a single study |
| `GET /stats` | Database summary statistics |

### Key query parameters for `/studies`:
- `status` — RECRUITING, COMPLETED, TERMINATED, etc.
- `phase` — PHASE1, PHASE2, PHASE3, NA, etc.
- `study_type` — INTERVENTIONAL or OBSERVATIONAL
- `condition` — text search in conditions
- `updated_since` — ISO datetime, returns studies updated after this time
- `page`, `page_size` — pagination

### OpenAlex Integration
OpenAlex polls `GET /studies?updated_since=<last_poll_time>` daily to discover new and modified clinical trials. Each study includes `linked_publications` with PMIDs and DOIs that map directly to OpenAlex Work entities.

## Schema Design

Source-agnostic by design. The `source` + `source_id` pair uniquely identifies each study, allowing multiple registries to coexist:

| Field | Type | Description |
|-------|------|-------------|
| source | string | Registry identifier (e.g., "clinicaltrials.gov") |
| source_id | string | ID within that registry (e.g., "NCT04368728") |
| title | string | Brief human-readable title |
| status | string | Normalized trial status |
| phase | string | Trial phase(s) |
| conditions | array | Diseases/conditions studied |
| interventions | array | What's being tested (name + type) |
| linked_publications | array | PMIDs and DOIs bridging to OpenAlex |
| updated_at | timestamp | When this record was last modified |

## Daily Updates

The harvester supports incremental mode using ClinicalTrials.gov's `lastUpdatePostDate` filter. Only studies modified since the last run are fetched and upserted.

```bash
# Manual incremental run
python run_harvest.py --since 2026-03-08

# The API also runs a scheduled daily update automatically
```

## Design Decisions

- **Source-agnostic schema**: Fields are generic, not ClinicalTrials.gov-specific. Adding EU Clinical Trials Register or ISRCTN means writing a new harvester + transformer, not changing the schema.
- **Flat table with JSON arrays**: Conditions, interventions, and locations stored as JSON for simplicity. In production, these would be normalized into separate tables with GIN indexes.
- **DOI extraction from citations**: References often embed DOIs in free text. We parse these out to maximize linkage to OpenAlex.
- **Upsert-based loading**: Idempotent — running the same harvest twice produces the same result.

## What I'd do with more time

- **Postgres with JSONB + GIN indexes** for production-grade querying into JSON fields
- **Elasticsearch** for full-text search across titles and summaries
- **Additional sources**: EU Clinical Trials Register, ISRCTN, WHO ICTRP
- **OpenAlex DOI resolution**: resolve linked DOIs against the OpenAlex API to enrich with work metadata
- **Docker containerization** for consistent deployment
- **Comprehensive tests** for the transformer (edge cases in date parsing, missing modules)
- **Monitoring dashboard** for ingestion health (volume per run, freshness per source)
