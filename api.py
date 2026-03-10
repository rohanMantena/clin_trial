"""
FastAPI application serving clinical trials data.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import database


@asynccontextmanager
async def lifespan(app):
    """Initialize database on startup."""
    database.init_db()
    yield


app = FastAPI(
    title="Clinical Trials Aggregator",
    description="A source-agnostic API for clinical trial data. "
                "Currently ingesting from ClinicalTrials.gov, designed to support "
                "multiple registries. Built as an abstraction layer for OpenAlex.",
    version="0.1.0",
    lifespan=lifespan,
)

# Allow OpenAlex and other consumers to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    """Health check and API info."""
    return {
        "service": "Clinical Trials Aggregator",
        "version": "0.1.0",
        "endpoints": {
            "studies": "/studies",
            "study_detail": "/studies/{source}/{source_id}",
            "stats": "/stats",
        }
    }


@app.get("/studies")
def list_studies(
    status: Optional[str] = Query(None, description="Filter by status (e.g., RECRUITING, COMPLETED)"),
    phase: Optional[str] = Query(None, description="Filter by phase (e.g., PHASE3)"),
    study_type: Optional[str] = Query(None, description="Filter by type (INTERVENTIONAL or OBSERVATIONAL)"),
    condition: Optional[str] = Query(None, description="Search conditions (e.g., 'cancer')"),
    updated_since: Optional[str] = Query(None, description="ISO datetime — return studies updated after this time. This is the field OpenAlex uses to poll for new/changed studies."),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(25, ge=1, le=100, description="Results per page"),
):
    """
    List and filter clinical trials.

    OpenAlex integration: Use `updated_since` to poll for new/changed studies daily.
    Example: /studies?updated_since=2026-03-08T00:00:00
    """
    results, total = database.search_studies(
        status=status,
        phase=phase,
        study_type=study_type,
        condition=condition,
        updated_since=updated_since,
        page=page,
        page_size=page_size,
    )

    return {
        "meta": {
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": (total + page_size - 1) // page_size if total > 0 else 0,
        },
        "studies": results,
    }


@app.get("/studies/{source}/{source_id}")
def get_study(source: str, source_id: str):
    """
    Get a single study by source and source_id.

    Example: /studies/clinicaltrials.gov/NCT04368728
    """
    study = database.get_study_by_source_id(source, source_id)
    if not study:
        raise HTTPException(status_code=404, detail=f"Study not found: {source}/{source_id}")
    return study


@app.get("/stats")
def stats():
    """
    Summary statistics for the database.
    Useful for monitoring ingestion health.
    """
    return database.get_stats()
