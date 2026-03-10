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
    """Create the studies table and indexes if they don't already exist."""
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
            registry_date VARCHAR(20),
            conditions JSONB DEFAULT '[]'::jsonb,
            mesh_terms JSONB DEFAULT '[]'::jsonb,
            interventions JSONB DEFAULT '[]'::jsonb,
            sponsor TEXT,
            investigators JSONB DEFAULT '[]'::jsonb,
            locations JSONB DEFAULT '[]'::jsonb,
            linked_publications JSONB DEFAULT '[]'::jsonb,
            secondary_ids JSONB DEFAULT '[]'::jsonb,
            eligibility JSONB DEFAULT '{}'::jsonb,
            has_results BOOLEAN DEFAULT FALSE,
            source_updated_at VARCHAR(30),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(source, source_id)
        )
    """)

    # Create indexes for common queries (skip gracefully if storage is full)
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_status ON studies(status)",
        "CREATE INDEX IF NOT EXISTS idx_source ON studies(source)",
        "CREATE INDEX IF NOT EXISTS idx_updated ON studies(updated_at)",
    ]
    for idx_sql in indexes:
        try:
            cur.execute(idx_sql)
        except psycopg2.errors.DiskFull:
            conn.rollback()
            print(f"  Skipping index (storage full): {idx_sql.split('idx_')[1].split(' ')[0]}")
            break

    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized.")


# Column list used by both upsert_study and upsert_batch
_COLUMNS = [
    "source", "source_id", "source_url", "title", "official_title",
    "brief_summary", "status", "phase", "study_type", "enrollment",
    "start_date", "completion_date", "registry_date",
    "conditions", "mesh_terms", "interventions", "sponsor",
    "investigators", "locations", "linked_publications",
    "secondary_ids", "eligibility", "has_results", "source_updated_at",
]

_JSONB_FIELDS = {
    "conditions", "mesh_terms", "interventions",
    "investigators", "locations",
    "linked_publications", "secondary_ids", "eligibility",
}


def _study_to_values(s):
    """Convert a study dict to a tuple of values matching _COLUMNS."""
    vals = []
    for col in _COLUMNS:
        v = s.get(col)
        if col in _JSONB_FIELDS:
            v = json.dumps(v if v is not None else ({} if col == "eligibility" else []))
        vals.append(v)
    return tuple(vals)


def upsert_study(cur, study):
    """Insert or update a study. Uses source + source_id as the unique key."""
    cols = ", ".join(_COLUMNS + ["updated_at"])
    placeholders = ", ".join(["%s"] * len(_COLUMNS) + ["NOW()"])
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in _COLUMNS if c not in ("source", "source_id")
    ) + ", updated_at = NOW()"

    cur.execute(f"""
        INSERT INTO studies ({cols}) VALUES ({placeholders})
        ON CONFLICT(source, source_id) DO UPDATE SET {update_set}
    """, _study_to_values(study))


def upsert_batch(studies, retries=3):
    """Insert or update a batch of studies using bulk insert with retry logic."""
    cols = ", ".join(_COLUMNS + ["updated_at"])
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in _COLUMNS if c not in ("source", "source_id")
    ) + ", updated_at = NOW()"
    n_cols = len(_COLUMNS)
    template = "(" + ", ".join(["%s"] * n_cols) + ", NOW())"

    for attempt in range(retries):
        conn = get_connection()
        cur = conn.cursor()
        try:
            values = [_study_to_values(s) for s in studies]

            execute_values(cur, f"""
                INSERT INTO studies ({cols}) VALUES %s
                ON CONFLICT(source, source_id) DO UPDATE SET {update_set}
            """, values, template=template)

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
                   has_results=None, updated_since=None, page=1, page_size=25):
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
        where_clauses.append(
            "EXISTS (SELECT 1 FROM jsonb_array_elements_text(conditions) AS c WHERE c ILIKE %s)"
        )
        params.append(f"%{condition}%")
    if has_results is not None:
        where_clauses.append("has_results = %s")
        params.append(has_results)
    if updated_since:
        where_clauses.append("updated_at >= %s")
        params.append(updated_since)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    cur.execute(f"SELECT COUNT(*) as cnt FROM studies {where_sql}", params)
    total = cur.fetchone()["cnt"]

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

    cur.execute("SELECT COUNT(*) as cnt FROM studies WHERE has_results = TRUE")
    with_results = cur.fetchone()["cnt"]

    cur.execute("SELECT MAX(updated_at) as latest FROM studies")
    last_updated = cur.fetchone()["latest"]

    cur.close()
    conn.close()

    return {
        "total_studies": total,
        "with_results": with_results,
        "by_status": {r["status"]: r["cnt"] for r in by_status},
        "by_phase": {r["phase"]: r["cnt"] for r in by_phase},
        "by_type": {r["study_type"]: r["cnt"] for r in by_type},
        "last_updated": str(last_updated) if last_updated else None,
    }


def _row_to_dict(row):
    """Convert a database row to a clean dict."""
    d = dict(row)
    for field in ["created_at", "updated_at"]:
        if d.get(field) and hasattr(d[field], "isoformat"):
            d[field] = d[field].isoformat()
    return d
