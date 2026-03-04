"""
Rights Angel — FastAPI API Routes
All REST endpoints for Milestone 1

Endpoints:
  POST /api/ingest              — Upload + ingest a legal document
  GET  /api/documents           — List all source documents
  GET  /api/sources             — List approved source IDs
  GET  /api/review/pending      — Get clauses awaiting human review
  POST /api/review/approve      — Approve a clause → enters clause store
  POST /api/review/reject       — Reject a clause
  GET  /api/clauses             — Query clause store
  GET  /api/clauses/{id}/chain  — Full traceability chain for a clause
  GET  /api/rights              — List rights catalog
  POST /api/rights              — Create/update a right
  GET  /api/rights/{id}         — Get right with linked clauses
  POST /api/engine/stage        — Create staging engine version
  POST /api/engine/publish      — Publish (human approval gate)
  POST /api/engine/reject       — Reject staging version
  GET  /api/engine/versions     — List all engine versions
  GET  /api/engine/active       — Get active engine version
  GET  /api/validate            — Run integrity validation
  GET  /api/audit               — Audit log (last 50 entries)
"""
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
from pydantic import BaseModel

from ingestion.pipeline import (
    ingest_document, list_documents, get_approved_sources,
    get_pending_review, approve_clause, reject_clause,
    get_clause_store, validate_clause_integrity, get_traceability_chain,
    SourceNotApprovedError, DocumentUnchangedError,
)
from engine.rights_catalog import (
    get_all_rights, get_right, upsert_right, get_rights_with_clauses,
)
from engine.version_manager import (
    create_staging_version, publish_version, reject_version,
    get_active_version, list_versions, get_audit_log,
)

router = APIRouter()


# ─── Pydantic models ──────────────────────────────────────────────────────────

class ApproveRequest(BaseModel):
    clause_id: str
    reviewed_by: str
    review_note: Optional[str] = None
    override_type: Optional[str] = None

class RejectRequest(BaseModel):
    clause_id: str
    reviewed_by: str
    reason: str

class RightUpsertRequest(BaseModel):
    catalog_id: str
    name: str
    category_tag: str
    subcategory_tag: Optional[str] = None
    discount_value: float
    discount_unit: str
    friction_score: int
    effective_from: str
    effective_to: Optional[str] = None
    status: str = "DRAFT"

class StageVersionRequest(BaseModel):
    law_version: str
    notes: Optional[str] = None

class PublishVersionRequest(BaseModel):
    engine_id: str
    published_by: str

class RejectVersionRequest(BaseModel):
    engine_id: str
    rejected_by: str
    reason: str


# ─── Document Ingestion ───────────────────────────────────────────────────────

@router.post("/ingest")
async def api_ingest(
    file: UploadFile = File(...),
    doc_id: str = Form(...),
    ingested_by: str = Form(...),
    publication_date: Optional[str] = Form(None),
    url: Optional[str] = Form(None),
):
    """
    L1+L2: Ingest a legal document.
    1. Checks approved source list
    2. SHA-256 hash check
    3. Extracts text (PDF/DOCX/TXT)
    4. OpenAI GPT-4o clause extraction
    5. Places clauses in review queue
    """
    try:
        file_bytes = await file.read()
        if len(file_bytes) == 0:
            raise HTTPException(400, "Uploaded file is empty")

        result = ingest_document(
            file_bytes=file_bytes,
            filename=file.filename,
            doc_id=doc_id,
            ingested_by=ingested_by,
            publication_date=publication_date,
            url=url,
        )
        return JSONResponse(content=result, status_code=200)

    except SourceNotApprovedError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except DocumentUnchangedError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")


@router.get("/documents")
def api_list_documents():
    return list_documents()


@router.get("/sources")
def api_approved_sources():
    """List the approved source list with doc_ids and metadata."""
    return get_approved_sources()


# ─── Human Review Queue ───────────────────────────────────────────────────────

@router.get("/review/pending")
def api_pending_review():
    """Get all clauses awaiting human review."""
    return get_pending_review()


@router.post("/review/approve")
def api_approve_clause(req: ApproveRequest):
    """Human approves a clause → enters clause store."""
    try:
        return approve_clause(
            clause_id=req.clause_id,
            reviewed_by=req.reviewed_by,
            review_note=req.review_note,
            override_type=req.override_type,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/review/reject")
def api_reject_clause(req: RejectRequest):
    """Human rejects a clause — it will not enter clause store."""
    try:
        return reject_clause(
            clause_id=req.clause_id,
            reviewed_by=req.reviewed_by,
            reason=req.reason,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


# ─── Clause Store ─────────────────────────────────────────────────────────────

@router.get("/clauses")
def api_clause_store(
    clause_id: Optional[str] = Query(None),
    source_doc_id: Optional[str] = Query(None),
    clause_type: Optional[str] = Query(None),
    include_superseded: bool = Query(False),
):
    """Query the clause store. Per §12.1: queryable by clause_id, source_doc_id, clause_type."""
    return get_clause_store(
        clause_id=clause_id,
        source_doc_id=source_doc_id,
        clause_type=clause_type,
        is_current=not include_superseded,
    )


@router.get("/clauses/{clause_id}/chain")
def api_traceability_chain(clause_id: str):
    """Full traceability chain: clause → source doc → file hash. Milestone 1 requirement."""
    result = get_traceability_chain(clause_id)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


# ─── Rights Catalog ───────────────────────────────────────────────────────────

@router.get("/rights")
def api_list_rights(status: str = Query("ACTIVE")):
    """List all rights in the catalog."""
    return get_all_rights(status=status)


@router.post("/rights")
def api_upsert_right(req: RightUpsertRequest):
    """Create or update a right in the catalog."""
    try:
        return upsert_right(req.dict())
    except Exception as e:
        raise HTTPException(400, str(e))


@router.get("/rights/{catalog_id}")
def api_get_right(catalog_id: str):
    """Get a right with all linked clauses and source documents."""
    result = get_rights_with_clauses(catalog_id)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


# ─── Engine Version Management ────────────────────────────────────────────────

@router.post("/engine/stage")
def api_stage_version(req: StageVersionRequest):
    """
    Step 4: Create staging engine version.
    Runs automated validation — blocked if validation fails.
    """
    return create_staging_version(
        law_version=req.law_version,
        notes=req.notes,
    )


@router.post("/engine/publish")
def api_publish_version(req: PublishVersionRequest):
    """
    Step 5+6: Human approval gate → publish engine to ACTIVE.
    Archives previous active version.
    """
    try:
        return publish_version(
            engine_id=req.engine_id,
            published_by=req.published_by,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/engine/reject")
def api_reject_version(req: RejectVersionRequest):
    """Reject a staging version — blocks publication."""
    try:
        return reject_version(
            engine_id=req.engine_id,
            rejected_by=req.rejected_by,
            reason=req.reason,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.get("/engine/versions")
def api_list_versions():
    """List all engine versions with status."""
    return list_versions()


@router.get("/engine/active")
def api_active_version():
    """Get the currently active engine version."""
    version = get_active_version()
    if not version:
        return {"status": "NO_ACTIVE_VERSION", "message": "No engine version has been published yet."}
    return version


# ─── Validation & Audit ───────────────────────────────────────────────────────

@router.get("/validate")
def api_validate():
    """
    Run full integrity validation on clause store.
    Milestone 1: basic traceability + clause integrity check.
    """
    return validate_clause_integrity()


@router.get("/audit")
def api_audit_log(limit: int = Query(50, ge=1, le=500)):
    """Get audit log entries (append-only, write-only in normal operation)."""
    return get_audit_log(limit=limit)


# ─── Un-approve (client feedback: clause undo) ────────────────────────────────

class UnapproveRequest(BaseModel):
    clause_id: str
    reviewed_by: str
    reason: str

@router.post("/review/unapprove")
def api_unapprove_clause(req: UnapproveRequest):
    """Move an approved clause back to PENDING review queue."""
    try:
        from ingestion.pipeline import unapprove_clause
        return unapprove_clause(
            clause_id=req.clause_id,
            reviewed_by=req.reviewed_by,
            reason=req.reason,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


# ─── Reset DB (temp, for testing only) ───────────────────────────────────────

@router.get("/reset-db-temp")
def reset_db_temp():
    from database.schema import get_db, init_db
    conn = get_db()
    conn.execute("DELETE FROM rights_clauses_map")
    conn.execute("DELETE FROM review_queue")
    conn.execute("DELETE FROM clauses")
    conn.execute("DELETE FROM source_documents")
    conn.execute("DELETE FROM audit_log")
    conn.execute("DELETE FROM engine_versions")
    conn.execute("DELETE FROM facts")
    conn.commit()
    conn.close()
    init_db()
    # Re-seed rights catalog
    from engine.rights_catalog import seed_rights_catalog
    seed_rights_catalog()
    return {"status": "done", "message": "Database reset and rights catalog re-seeded"}
