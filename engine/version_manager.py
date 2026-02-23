"""
Rights Angel — Engine Version Manager
Architecture Brief v1.2 §6.1 Steps 4-6

Controlled Update Workflow:
Step 4: Automated validation (schema, integrity, regression)
Step 5: Human review gate — NO publish without explicit approval
Step 6: Version publish — computes clause_set_hash + rules_hash, archives previous
"""
import hashlib
import json
from datetime import datetime, timezone
from typing import Optional

from database.schema import get_db
from ingestion.pipeline import validate_clause_integrity


def _compute_clause_set_hash(conn) -> str:
    """
    Compute SHA-256 of all current active clauses.
    Deterministic: sorted by clause_id before hashing.
    """
    rows = conn.execute("""
        SELECT clause_id, text, clause_type, source_doc_id, section_ref
        FROM clauses WHERE is_current=1
        ORDER BY clause_id
    """).fetchall()

    payload = json.dumps([dict(r) for r in rows], ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _compute_rules_hash(conn) -> str:
    """
    Compute SHA-256 of the compiled rule set.
    Includes: active rights + rights_clauses_map + discount values + friction scores.
    This hash changes if any eligibility rule changes.
    """
    rights_rows = conn.execute("""
        SELECT catalog_id, discount_value, discount_unit, friction_score,
               category_tag, effective_from, effective_to, status
        FROM rights WHERE status='ACTIVE'
        ORDER BY catalog_id
    """).fetchall()

    map_rows = conn.execute("""
        SELECT catalog_id, clause_id, mapping_role
        FROM rights_clauses_map
        ORDER BY catalog_id, clause_id
    """).fetchall()

    payload = json.dumps({
        "rights": [dict(r) for r in rights_rows],
        "mappings": [dict(r) for r in map_rows],
    }, ensure_ascii=False, sort_keys=True)

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def create_staging_version(law_version: str, notes: Optional[str] = None) -> dict:
    """
    Create a new STAGING engine version.
    Step 4: Runs automated validation suite before staging.
    Will not create if validation fails.
    """
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()

    try:
        # Step 4: Automated validation
        validation = validate_clause_integrity()
        if not validation["passed"]:
            return {
                "status": "BLOCKED",
                "reason": "Automated validation failed — cannot stage new version",
                "errors": validation["errors"],
            }

        clause_set_hash = _compute_clause_set_hash(conn)
        rules_hash = _compute_rules_hash(conn)

        # Generate engine_id: ENGINE-{YEAR}-v{sequence}
        existing_count = conn.execute("SELECT COUNT(*) as c FROM engine_versions").fetchone()["c"]
        year = datetime.now().year
        engine_id = f"ENGINE-{year}-v1.{existing_count}.0"

        conn.execute("""
            INSERT INTO engine_versions
                (engine_id, law_version, clause_set_hash, rules_hash,
                 published_at, published_by, status, notes)
            VALUES (?,?,?,?,NULL,NULL,'STAGING',?)
        """, (engine_id, law_version, clause_set_hash, rules_hash, notes))

        conn.execute("""
            INSERT INTO audit_log (event_type, engine_id, details, created_at)
            VALUES ('ENGINE_STAGED', ?, ?, ?)
        """, (engine_id, json.dumps({
            "law_version": law_version,
            "clause_set_hash": clause_set_hash,
            "rules_hash": rules_hash,
        }, ensure_ascii=False), now))

        conn.commit()

        return {
            "status": "STAGED",
            "engine_id": engine_id,
            "clause_set_hash": clause_set_hash,
            "rules_hash": rules_hash,
            "law_version": law_version,
            "message": "Staged. Human review and approval required before activation.",
        }

    finally:
        conn.close()


def publish_version(engine_id: str, published_by: str) -> dict:
    """
    Step 5+6: Human approval → publish engine version to ACTIVE.
    Archives previous ACTIVE version.
    No version reaches production without this explicit call.
    """
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()

    try:
        version = conn.execute(
            "SELECT * FROM engine_versions WHERE engine_id=?", (engine_id,)
        ).fetchone()

        if not version:
            raise ValueError(f"Engine version not found: {engine_id}")

        if version["status"] != "STAGING":
            raise ValueError(
                f"Only STAGING versions can be published. "
                f"Current status: {version['status']}"
            )

        # Archive all currently ACTIVE versions
        conn.execute(
            "UPDATE engine_versions SET status='ARCHIVED' WHERE status='ACTIVE'"
        )

        # Publish this version
        conn.execute("""
            UPDATE engine_versions
            SET status='ACTIVE', published_at=?, published_by=?
            WHERE engine_id=?
        """, (now, published_by, engine_id))

        conn.execute("""
            INSERT INTO audit_log (event_type, engine_id, details, created_at)
            VALUES ('ENGINE_PUBLISHED', ?, ?, ?)
        """, (engine_id, json.dumps({
            "published_by": published_by,
            "law_version": version["law_version"],
            "clause_set_hash": version["clause_set_hash"],
        }, ensure_ascii=False), now))

        conn.commit()

        return {
            "status": "PUBLISHED",
            "engine_id": engine_id,
            "published_by": published_by,
            "published_at": now,
            "clause_set_hash": version["clause_set_hash"],
            "rules_hash": version["rules_hash"],
        }

    finally:
        conn.close()


def reject_version(engine_id: str, rejected_by: str, reason: str) -> dict:
    """Reject a staged version — blocks it from being published."""
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    try:
        result = conn.execute("""
            UPDATE engine_versions SET status='ARCHIVED', notes=?
            WHERE engine_id=? AND status='STAGING'
        """, (f"REJECTED by {rejected_by}: {reason}", engine_id))

        if result.rowcount == 0:
            raise ValueError(f"No staging version found: {engine_id}")

        conn.execute("""
            INSERT INTO audit_log (event_type, engine_id, details, created_at)
            VALUES ('ENGINE_REJECTED', ?, ?, ?)
        """, (engine_id, json.dumps({
            "rejected_by": rejected_by, "reason": reason
        }, ensure_ascii=False), now))

        conn.commit()
        return {"status": "REJECTED", "engine_id": engine_id}
    finally:
        conn.close()


def get_active_version() -> Optional[dict]:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM engine_versions WHERE status='ACTIVE' ORDER BY published_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def list_versions() -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM engine_versions ORDER BY published_at DESC NULLS LAST"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_audit_log(limit: int = 50) -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT * FROM audit_log
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
