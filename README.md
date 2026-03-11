# Clinical Trials Aggregator API

A source-agnostic REST API that harvests clinical trial data from public registries, normalizes it into a common schema, and exposes it for downstream consumers like [OpenAlex](https://openalex.org).

**Live API:** [https://clin-trial.onrender.com](https://clin-trial.onrender.com)

## Architecture

```
ClinicalTrials.gov API v2 ──> Harvester ──> Transformer ──> Postgres (Neon)
                                                                  |
                            OpenAlex  <── FastAPI <───────────────┘
```

| Component | File | Role |
|---|---|---|
| Harvester | `harvester.py` | Paginated fetcher with rate limiting and retry logic |
| Transformer | `transformer.py` | Converts registry-specific JSON to common schema |
| Database | `database.py` | Postgres layer with bulk upserts (ON CONFLICT) |
| API | `api.py` | FastAPI endpoints for querying and polling |
| Orchestrator | `run_harvest.py` | Ties harvest + transform + load into a single pipeline |

### Why source-agnostic?

The schema uses `source` + `source_id` as a composite key instead of `nct_id`. Adding a new registry (EU Clinical Trials Register, ISRCTN, etc.) requires only a new harvester and transformer -- the database and API remain unchanged.

## Schema

27 columns mapping clinical trial data to concepts OpenAlex already understands:

| Field | Type | Maps to OpenAlex |
|---|---|---|
| `investigators` | JSONB | **Authors** (name, affiliation, role) |
| `sponsor` | TEXT | **Institutions** |
| `mesh_terms` | JSONB | **Topics / Concepts** (MeSH taxonomy) |
| `linked_publications` | JSONB | **Works** (PMIDs and DOIs for cross-linking) |
| `conditions` | JSONB | **Concepts** (disease classification) |
| `secondary_ids` | JSONB | Cross-registry deduplication |
| `source` + `source_id` | VARCHAR | Source-agnostic identity |

Additional fields: `title`, `official_title`, `brief_summary`, `status`, `phase`, `study_type`, `enrollment`, `start_date`, `completion_date`, `registry_date`, `interventions`, `locations`, `eligibility`, `has_results`, `source_url`, `source_updated_at`, `created_at`, `updated_at`.

### What's excluded and why

ClinicalTrials.gov returns 50+ fields per study. We deliberately excluded fields that don't serve an aggregator's purpose:

| Excluded field | Reason |
|---|---|
| `keywords` | Redundant with `conditions` + `mesh_terms`. MeSH is the controlled vocabulary; keywords are free-text duplicates |
| `collaborators` | Lead `sponsor` is sufficient for institution mapping. Collaborators add noise without clear entity resolution |
| `eligibility_criteria_text` | Free-text blob often 500+ words per study. We extract only the structured fields (sex, age range, healthy volunteers) into `eligibility` JSONB — keeps rows ~200 bytes vs ~2KB while preserving queryable attributes |
| `detailed_description` | Often duplicates `brief_summary` at greater length; not useful for discovery |
| `arms/groups` | Intervention-arm mappings are protocol-level detail beyond what a registry aggregator needs |
| `outcome_measures` | Clinical endpoint detail, not relevant for study discovery or linking |
| `study_design_info` | Allocation, masking, etc. — protocol design metadata not needed for OpenAlex's entity model |
| `ipd_sharing` | Individual patient data sharing plans — regulatory detail |

### Non-obvious inclusion decisions

- **`eligibility`** — Structured JSONB (sex, min/max age, healthy volunteers) instead of the raw criteria text. Preserves the most useful filter dimensions at a fraction of the storage cost.
- **`secondary_ids`** — Enables cross-registry deduplication when adding EU Clinical Trials Register or ISRCTN as future sources.
- **`mesh_terms`** — Controlled MeSH vocabulary maps directly to OpenAlex Topics/Concepts, unlike free-text `conditions`.
- **`linked_publications`** with regex DOI extraction — ClinicalTrials.gov buries DOIs in citation free-text. We extract them for direct cross-linking with OpenAlex Works.
- **`enrollment`** — Useful for filtering by study size; a common research dimension.

## API Endpoints

### List / Filter studies
```
GET /studies?status=RECRUITING&phase=PHASE3&condition=cancer&page=1&page_size=25
```

| Parameter | Description |
|---|---|
| `status` | Filter by status (RECRUITING, COMPLETED, etc.) |
| `phase` | Filter by phase (PHASE1, PHASE2, PHASE3, etc.) |
| `study_type` | INTERVENTIONAL or OBSERVATIONAL |
| `condition` | Substring search across conditions (e.g., "cancer" matches "Breast Cancer") |
| `has_results` | Boolean -- filter for trials with posted results |
| `updated_since` | ISO datetime -- **primary polling endpoint for OpenAlex** |
| `page` / `page_size` | Pagination (max 100 per page) |

### Get single study
```
GET /studies/clinicaltrials.gov/NCT04368728
```

### Stats
```
GET /stats
```
Returns total count, breakdown by status/phase/type, count with results, and last update time.

## Daily Update Cycle

1. **6 AM UTC** -- GitHub Actions triggers `run_harvest.py --since yesterday`
2. Harvester pulls only studies modified in the last 24 hours (~1,000-2,000 studies)
3. Transformer normalizes to the common schema
4. Database upserts via `ON CONFLICT(source, source_id) DO UPDATE` -- existing studies are updated in place, new studies are inserted
5. OpenAlex polls `GET /studies?updated_since=<timestamp>` to retrieve changes

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Set your Postgres connection string
export DATABASE_URL="postgresql://user:pass@host/db?sslmode=require"

# Test harvest (5 studies)
python run_harvest.py --test

# Full harvest
python run_harvest.py --full

# Incremental (auto-reads .last_harvest or specify date)
python run_harvest.py --since 2026-03-09

# Start API server
uvicorn api:app --reload
```

### Environment Variables

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | Postgres connection string | (required) |
| `BATCH_SIZE` | Studies per DB write batch | 200 |
| `BATCH_SLEEP` | Seconds between batch writes | 0.5 |

## Design Decisions

**Generator-based harvester** -- Streams studies one at a time instead of loading all 575K into memory. The orchestrator batches them for efficient bulk DB writes.

**Bulk upserts with `execute_values`** -- Sends entire batches in a single SQL statement instead of individual INSERTs. ~10x faster for large harvests.

**3 targeted indexes** -- Only `status`, `source`, and `updated_at`. We cut 7 other indexes (including 3 GIN indexes on JSONB columns) because our ILIKE-based condition search can't use GIN indexes (GIN requires `@>` containment), and low-cardinality columns like `phase` (6 values) and `study_type` (2 values) don't benefit from B-tree indexes.

**DOI extraction from citations** -- ClinicalTrials.gov doesn't provide DOIs as a structured field. We regex-extract them from citation text to enable cross-linking with OpenAlex's publication graph.

**Incremental harvesting** -- Uses `filter.advanced=AREA[LastUpdatePostDate]RANGE[date,MAX]` (the v2 API's advanced filter syntax) to fetch only recently modified studies. The simpler `filter.lastUpdatePostDate` parameter returns HTTP 400.

**Upsert-based loading** -- Idempotent. Running the same harvest twice produces the same result. No duplicates, no data loss.

## What I'd do with more time

- **Additional registries**: EU Clinical Trials Register, ISRCTN, WHO ICTRP -- each just needs a harvester + transformer
- **Full-text search**: Elasticsearch or Postgres `tsvector` for searching titles and summaries
- **OpenAlex DOI resolution**: Enrich linked publications by resolving DOIs against the OpenAlex API
- **Connection pooling**: Replace per-request connections with a pool for higher throughput
- **Comprehensive tests**: Edge cases in date parsing, missing modules, malformed API responses
- **Docker containerization**: Consistent local dev and deployment
- **Monitoring dashboard**: Ingestion health metrics -- volume per run, freshness per source, error rates

## Deployment

- **Database**: [Neon](https://neon.tech) Postgres (serverless, scales to zero when idle)
- **API**: [Render](https://render.com) (auto-deploys from GitHub on push)
- **Daily harvest**: GitHub Actions cron job (`.github/workflows/daily_harvest.yml`)

## Tech Stack

- Python 3.12
- FastAPI + Uvicorn
- PostgreSQL (Neon) with JSONB
- psycopg2 with `execute_values` for bulk operations
- ClinicalTrials.gov API v2
