"""
Rights Angel — SQLite Database Schema
Architecture Brief v1.2 Section 4 — Exact Implementation
6 tables as specified + audit_log + review_queue
"""
import sqlite3
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_PATH = Path(os.getenv("DB_PATH", "rights_angel.db"))


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables. Idempotent — safe to call multiple times."""
    conn = get_db()
    conn.executescript("""

    -- ── 1. source_documents ──────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS source_documents (
        doc_id           TEXT PRIMARY KEY,
        title            TEXT NOT NULL,
        publisher        TEXT NOT NULL,
        publication_date TEXT NOT NULL,
        url              TEXT,
        file_hash        TEXT NOT NULL UNIQUE,
        ingested_at      TEXT NOT NULL,
        ingested_by      TEXT NOT NULL,
        status           TEXT NOT NULL DEFAULT 'ACTIVE'
                         CHECK(status IN ('ACTIVE','SUPERSEDED'))
    );

    -- ── 2. engine_versions ───────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS engine_versions (
        engine_id        TEXT PRIMARY KEY,
        law_version      TEXT NOT NULL,
        clause_set_hash  TEXT NOT NULL,
        rules_hash       TEXT NOT NULL,
        published_at     TEXT,
        published_by     TEXT,
        status           TEXT NOT NULL DEFAULT 'STAGING'
                         CHECK(status IN ('STAGING','ACTIVE','ARCHIVED')),
        notes            TEXT
    );

    -- ── 3. clauses ───────────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS clauses (
        clause_id         TEXT PRIMARY KEY,
        source_doc_id     TEXT NOT NULL REFERENCES source_documents(doc_id),
        section_ref       TEXT NOT NULL,
        text              TEXT NOT NULL,
        clause_type       TEXT NOT NULL
                          CHECK(clause_type IN ('ELIGIBILITY','EXCLUSION','DEFINITION','PROCEDURE')),
        extraction_method TEXT NOT NULL
                          CHECK(extraction_method IN ('HUMAN','AI_REVIEWED')),
        version           TEXT NOT NULL DEFAULT '1.0',
        is_current        INTEGER NOT NULL DEFAULT 1 CHECK(is_current IN (0,1)),
        created_at        TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_clauses_doc     ON clauses(source_doc_id);
    CREATE INDEX IF NOT EXISTS idx_clauses_type    ON clauses(clause_type);
    CREATE INDEX IF NOT EXISTS idx_clauses_current ON clauses(is_current);

    -- ── 4. rights ────────────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS rights (
        catalog_id       TEXT PRIMARY KEY,
        name             TEXT NOT NULL,
        category_tag     TEXT NOT NULL,
        subcategory_tag  TEXT,
        discount_value   REAL NOT NULL,
        discount_unit    TEXT NOT NULL CHECK(discount_unit IN ('PERCENT','ABSOLUTE','FLAT')),
        friction_score   INTEGER NOT NULL CHECK(friction_score BETWEEN 1 AND 10),
        effective_from   TEXT NOT NULL,
        effective_to     TEXT,
        law_version_hash TEXT,
        status           TEXT NOT NULL DEFAULT 'DRAFT'
                         CHECK(status IN ('ACTIVE','DEPRECATED','DRAFT')),
        created_at       TEXT NOT NULL,
        updated_at       TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_rights_category ON rights(category_tag);
    CREATE INDEX IF NOT EXISTS idx_rights_status   ON rights(status);

    -- ── 5. rights_clauses_map ────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS rights_clauses_map (
        map_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        catalog_id   TEXT NOT NULL REFERENCES rights(catalog_id),
        clause_id    TEXT NOT NULL REFERENCES clauses(clause_id),
        mapping_role TEXT NOT NULL
                     CHECK(mapping_role IN ('GRANTS','CONDITIONS','EXCLUDES','PROCEDURE')),
        created_at   TEXT NOT NULL,
        UNIQUE(catalog_id, clause_id, mapping_role)
    );
    CREATE INDEX IF NOT EXISTS idx_rcm_catalog ON rights_clauses_map(catalog_id);
    CREATE INDEX IF NOT EXISTS idx_rcm_clause  ON rights_clauses_map(clause_id);

    -- ── 6. facts ─────────────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS facts (
        fact_id      TEXT PRIMARY KEY,
        session_id   TEXT NOT NULL,
        fact_type    TEXT NOT NULL,
        value        TEXT NOT NULL,
        provenance   TEXT NOT NULL CHECK(provenance IN ('USER_DECLARED','DOCUMENT','COMPUTED')),
        confidence   REAL NOT NULL DEFAULT 1.0 CHECK(confidence BETWEEN 0.0 AND 1.0),
        collected_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_facts_session ON facts(session_id);

    -- ── Audit log (append-only) ───────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS audit_log (
        log_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT NOT NULL,
        engine_id  TEXT,
        session_id TEXT,
        clause_ids TEXT,
        details    TEXT,
        created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_audit_event   ON audit_log(event_type);
    CREATE INDEX IF NOT EXISTS idx_audit_session ON audit_log(session_id);

    -- ── Human review queue (AI clauses wait here before clause store) ─────────
    CREATE TABLE IF NOT EXISTS review_queue (
        queue_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        clause_id     TEXT NOT NULL UNIQUE,
        source_doc_id TEXT NOT NULL,
        section_ref   TEXT NOT NULL,
        text          TEXT NOT NULL,
        clause_type   TEXT NOT NULL,
        status        TEXT NOT NULL DEFAULT 'PENDING'
                      CHECK(status IN ('PENDING','APPROVED','REJECTED')),
        reviewed_by   TEXT,
        review_note   TEXT,
        submitted_at  TEXT NOT NULL,
        reviewed_at   TEXT
    );

    """)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print(f"✅ Database initialized: {DB_PATH}")
