"""
Database layer for clinical trials — Postgres version for production.
Uses DATABASE_URL environment variable (provided by Railway automatically).
"""
import os
import json
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values


DATABASE_URL = os.environ.get("DATABASE_URL")

# Cloud-friendly batch size (Railway free tier crashes at 1000)
BATCH_DB_SIZE = int(os.environ.get("BATCH_DB_SIZE", "200"))


def _clean_url(url):
    """Clean DB URL for psycopg2 compatibility (Neon endpoint ID, channel_binding)."""
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    # psycopg2 doesn't recognize channel_binding
    params.pop("channel_binding", None)
    # Neon requires endpoint ID when libpq lacks SNI support
    hostname = parsed.hostname or ""
    if "neon.tech" in hostname and "options" not in params:
        endpoint_id = hostname.split(".")[0]  # e.g. ep-wispy-bird-abc123-pooler
        params["options"] = [f"endpoint={endpoint_id}"]
    clean_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=clean_query))


def get_connection(retries=3):
    """Get a database connection with retry logic for cloud Postgres."""
    url = DATABASE_URL
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    url = _clean_url(url)

    for attempt in range(retries):
        try:
            conn = psycopg2.connect(url, cursor_factory=RealDictCursor)
            return conn
        except psycopg2.OperationalError as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"  DB connection failed (attempt {attempt + 1}/{retries}), retrying in {wait}s: {e}")
                time.sleep(wait)
            else:
                raise


def init_db():
    """Create the studies table if it doesn't exist."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS studies (
            id SERIAL PRIMARY KEY,
            source VARCHAR(100) NOT NULL,
            source_id VARCHAR(100) NOT NULL,
            source_url TEXT,
            title TEXT,
            official_title TEXT,
            brief_summary TEXT,
            status VARCHAR(50),
            phase VARCHAR(50),
            study_type VARCHAR(50),
            enrollment INTEGER,
            start_date VARCHAR(20),
            completion_date VARCHAR(20),
            conditions JSONB DEFAULT '[]'::jsonb,
            interventions JSONB DEFAULT '[]'::jsonb,
            sponsor TEXT,
            locations JSONB DEFAULT '[]'::jsonb,
            linked_publications JSONB DEFAULT '[]'::jsonb,
            source_updated_at VARCHAR(30),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(source, source_id)
        )
    """)

    # Create indexes for common queries
    cur.execute("CREATE INDEX IF NOT EXISTS idx_status ON studies(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_phase ON studies(phase)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_study_type ON studies(study_type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_source ON studies(source)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_updated ON studies(updated_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_source_updated ON studies(source_updated_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_conditions ON studies USING GIN(conditions)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_interventions ON studies USING GIN(interventions)")

    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized.")


def upsert_study(cur, study):
    """
    Insert or update a study. Uses source + source_id as the unique key.
    """
    cur.execute("""
        INSERT INTO studies (
            source, source_id, source_url, title, official_title,
            brief_summary, status, phase, study_type, enrollment,
            start_date, completion_date, conditions, interventions,
            sponsor, locations, linked_publications,
            source_updated_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, NOW()
        )
        ON CONFLICT(source, source_id) DO UPDATE SET
            title = EXCLUDED.title,
            official_title = EXCLUDED.official_title,
            brief_summary = EXCLUDED.brief_summary,
            status = EXCLUDED.status,
            phase = EXCLUDED.phase,
            study_type = EXCLUDED.study_type,
            enrollment = EXCLUDED.enrollment,
            start_date = EXCLUDED.start_date,
            completion_date = EXCLUDED.completion_date,
            conditions = EXCLUDED.conditions,
            interventions = EXCLUDED.interventions,
            sponsor = EXCLUDED.sponsor,
            locations = EXCLUDED.locations,
            linked_publications = EXCLUDED.linked_publications,
            source_updated_at = EXCLUDED.source_updated_at,
            updated_at = NOW()
    """, (
        study["source"],
        study["source_id"],
        study["source_url"],
        study["title"],
        study["official_title"],
        study["brief_summary"],
        study["status"],
        study["phase"],
        study["study_type"],
        study["enrollment"],
        study["start_date"],
        study["completion_date"],
        json.dumps(study["conditions"]),
        json.dumps(study["interventions"]),
        study["sponsor"],
        json.dumps(study["locations"]),
        json.dumps(study["linked_publications"]),
        study["source_updated_at"],
    ))


def upsert_batch(studies, retries=3):
    """Insert or update a batch of studies using bulk insert with retry logic."""
    for attempt in range(retries):
        conn = get_connection()
        cur = conn.cursor()
        try:
            values = []
            for s in studies:
                values.append((
                    s["source"], s["source_id"], s["source_url"],
                    s["title"], s["official_title"], s["brief_summary"],
                    s["status"], s["phase"], s["study_type"], s["enrollment"],
                    s["start_date"], s["completion_date"],
                    json.dumps(s["conditions"]), json.dumps(s["interventions"]),
                    s["sponsor"], json.dumps(s["locations"]),
                    json.dumps(s["linked_publications"]), s["source_updated_at"],
                ))

            execute_values(cur, """
                INSERT INTO studies (
                    source, source_id, source_url, title, official_title,
                    brief_summary, status, phase, study_type, enrollment,
                    start_date, completion_date, conditions, interventions,
                    sponsor, locations, linked_publications, source_updated_at,
                    updated_at
                ) VALUES %s
                ON CONFLICT(source, source_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    official_title = EXCLUDED.official_title,
                    brief_summary = EXCLUDED.brief_summary,
                    status = EXCLUDED.status,
                    phase = EXCLUDED.phase,
                    study_type = EXCLUDED.study_type,
                    enrollment = EXCLUDED.enrollment,
                    start_date = EXCLUDED.start_date,
                    completion_date = EXCLUDED.completion_date,
                    conditions = EXCLUDED.conditions,
                    interventions = EXCLUDED.interventions,
                    sponsor = EXCLUDED.sponsor,
                    locations = EXCLUDED.locations,
                    linked_publications = EXCLUDED.linked_publications,
                    source_updated_at = EXCLUDED.source_updated_at,
                    updated_at = NOW()
            """, values, template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())")

            conn.commit()
            return len(studies)
        except psycopg2.OperationalError as e:
            conn.rollback()
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"  DB write failed (attempt {attempt + 1}/{retries}), retrying in {wait}s: {e}")
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()


def get_study_by_source_id(source, source_id):
    """Fetch a single study by source + source_id."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM studies WHERE source = %s AND source_id = %s",
        (source, source_id)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row:
        return _row_to_dict(row)
    return None


def search_studies(status=None, phase=None, study_type=None, condition=None,
                   updated_since=None, page=1, page_size=25):
    """
    Search studies with optional filters.
    Returns (studies_list, total_count).
    """
    conn = get_connection()
    cur = conn.cursor()

    where_clauses = []
    params = []

    if status:
        where_clauses.append("status = %s")
        params.append(status)
    if phase:
        where_clauses.append("phase = %s")
        params.append(phase)
    if study_type:
        where_clauses.append("study_type = %s")
        params.append(study_type)
    if condition:
        # JSONB containment — checks if the array contains a matching string
        where_clauses.append("conditions @> %s::jsonb")
        params.append(json.dumps([condition]))
    if updated_since:
        where_clauses.append("updated_at >= %s")
        params.append(updated_since)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    # Get total count
    cur.execute(f"SELECT COUNT(*) as cnt FROM studies {where_sql}", params)
    total = cur.fetchone()["cnt"]

    # Get page of results
    offset = (page - 1) * page_size
    cur.execute(
        f"SELECT * FROM studies {where_sql} ORDER BY updated_at DESC LIMIT %s OFFSET %s",
        params + [page_size, offset]
    )
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return [_row_to_dict(r) for r in rows], total


def get_stats():
    """Get summary statistics about the database."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) as cnt FROM studies")
    total = cur.fetchone()["cnt"]

    cur.execute(
        "SELECT status, COUNT(*) as cnt FROM studies GROUP BY status ORDER BY cnt DESC"
    )
    by_status = cur.fetchall()

    cur.execute(
        "SELECT phase, COUNT(*) as cnt FROM studies GROUP BY phase ORDER BY cnt DESC"
    )
    by_phase = cur.fetchall()

    cur.execute(
        "SELECT study_type, COUNT(*) as cnt FROM studies GROUP BY study_type ORDER BY cnt DESC"
    )
    by_type = cur.fetchall()

    cur.execute("SELECT MAX(updated_at) as latest FROM studies")
    last_updated = cur.fetchone()["latest"]

    cur.close()
    conn.close()

    return {
        "total_studies": total,
        "by_status": {r["status"]: r["cnt"] for r in by_status},
        "by_phase": {r["phase"]: r["cnt"] for r in by_phase},
        "by_type": {r["study_type"]: r["cnt"] for r in by_type},
        "last_updated": str(last_updated) if last_updated else None,
    }


def _row_to_dict(row):
    """
    Convert a database row to a clean dict.
    Postgres with RealDictCursor already returns dicts.
    JSONB columns are automatically parsed by psycopg2.
    We just need to handle datetime serialization.
    """
    d = dict(row)
    # Convert datetime objects to ISO strings for JSON serialization
    for field in ["created_at", "updated_at"]:
        if d.get(field) and hasattr(d[field], "isoformat"):
            d[field] = d[field].isoformat()
    return d
