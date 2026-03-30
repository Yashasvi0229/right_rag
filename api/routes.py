"""
Rights Angel — FastAPI API Routes
Milestone 1 + Milestone 2B endpoints

Milestone 1 endpoints:
  POST /api/ingest                      — Upload + ingest a legal document
  GET  /api/documents                   — List all source documents
  GET  /api/sources                     — List approved source IDs
  GET  /api/review/pending              — Clauses awaiting human review
  POST /api/review/approve              — Approve a clause
  POST /api/review/reject               — Reject a clause
  POST /api/review/unapprove            — Revert approved clause to pending
  GET  /api/clauses                     — Query clause store
  GET  /api/clauses/{id}/chain          — Full traceability chain
  GET  /api/rights                      — List rights catalog
  POST /api/rights                      — Create/update a right
  GET  /api/rights/{id}                 — Get right with linked clauses
  POST /api/engine/stage                — Create staging engine version
  POST /api/engine/publish              — Publish (human approval gate)
  POST /api/engine/reject               — Reject staging version
  GET  /api/engine/versions             — List all engine versions
  GET  /api/engine/active               — Get active engine version
  GET  /api/validate                    — Run integrity validation
  GET  /api/audit                       — Audit log
  POST /api/contact                     — Contact form submission (QA #7)

Milestone 2B endpoints (L4 / L5 / L6):
  POST /api/session/start               — Create new citizen session
  POST /api/facts                       — Submit typed fact objects
  GET  /api/facts/{session_id}          — Get all facts for a session
  DELETE /api/facts/{session_id}        — Clear facts for a session
  POST /api/evaluate/{session_id}       — Run full eligibility evaluation
  GET  /api/evaluate/{session_id}/calculate — Compute discount + installment amounts
"""
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional, List
from pydantic import BaseModel
import uuid
import json
from datetime import datetime

from ingestion.pipeline import (
    ingest_document, list_documents, get_approved_sources,
    get_pending_review, approve_clause, reject_clause,
    get_clause_store, validate_clause_integrity, get_traceability_chain,
    detect_discount_in_text,
    SourceNotApprovedError, DocumentUnchangedError,
)
from engine.rights_catalog import (
    get_all_rights, get_right, upsert_right, get_rights_with_clauses,
)
from engine.version_manager import (
    create_staging_version, publish_version, reject_version,
    get_active_version, list_versions, get_audit_log,
)
from database.schema import get_db

router = APIRouter()


# ═══════════════════════════════════════════════════════════════════════════════
# Pydantic models
# ═══════════════════════════════════════════════════════════════════════════════

class ApproveRequest(BaseModel):
    clause_id:                str
    reviewed_by:              str
    review_note:              Optional[str]   = None
    override_type:            Optional[str]   = None
    section_ref:              Optional[str]   = None   # reviewer can update weak section_ref
    suggested_discount_value: Optional[float] = None   # ★ NEW: unified approval
    suggested_catalog_id:     Optional[str]   = None   # ★ NEW: which right to update

class RejectRequest(BaseModel):
    clause_id:   str
    reviewed_by: str
    reason:      str

class UnapproveRequest(BaseModel):
    clause_id:   str
    reviewed_by: str
    reason:      str

class RightUpsertRequest(BaseModel):
    catalog_id:      str
    name:            str
    category_tag:    str
    subcategory_tag: Optional[str] = None
    discount_value:  float
    discount_unit:   str
    friction_score:  int
    effective_from:  str
    effective_to:    Optional[str] = None
    status:          str = "DRAFT"

class StageVersionRequest(BaseModel):
    law_version: str
    notes:       Optional[str] = None

class PublishVersionRequest(BaseModel):
    engine_id:    str
    published_by: str
    note:         Optional[str] = None

class RejectVersionRequest(BaseModel):
    engine_id:   str
    rejected_by: str
    reason:      str
    note:        Optional[str] = None

class ContactRequest(BaseModel):
    name:    str
    email:   str
    message: str

# ── M2B: Fact submission ──────────────────────────────────────────────────────
class FactItem(BaseModel):
    fact_type:  str
    value:      str           # JSON-encoded value
    provenance: str = "USER_DECLARED"
    confidence: float = 1.0

class FactsSubmitRequest(BaseModel):
    session_id: str
    facts:      List[FactItem]

# ── M2B: Evaluate request ─────────────────────────────────────────────────────
class EvaluateRequest(BaseModel):
    engine_id: Optional[str] = None   # if None → use active engine


# ═══════════════════════════════════════════════════════════════════════════════
# MILESTONE 1 — Document Ingestion
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/ingest")
async def api_ingest(
    file:             UploadFile      = File(...),
    doc_id:           str             = Form(...),
    ingested_by:      str             = Form(...),
    publication_date: Optional[str]   = Form(None),
    doc_type:         Optional[str]   = Form(None),
    url:              Optional[str]   = Form(None),
):
    """L1+L2: Ingest a legal document."""
    # QA #1 backend guard — validate 4-digit year
    if publication_date:
        try:
            parsed = datetime.strptime(publication_date, "%Y-%m-%d")
            if parsed.year < 1900 or parsed.year > datetime.now().year + 1:
                raise HTTPException(
                    400,
                    f"תאריך פרסום לא תקין: {publication_date}. "
                    f"השנה חייבת להיות בין 1900 ל-{datetime.now().year + 1}."
                )
        except ValueError:
            raise HTTPException(
                400,
                f"פורמט תאריך לא תקין: {publication_date}. נדרש: YYYY-MM-DD"
            )

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
        # BUG FIX [object Object]: ensure result is fully JSON-serializable
        return JSONResponse(content=_safe_json(result), status_code=200)

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
    # BUG FIX [object Object]: convert Row objects → plain dicts
    return [_row_to_dict(d) for d in list_documents()]


@router.get("/sources")
def api_approved_sources():
    return get_approved_sources()


# ═══════════════════════════════════════════════════════════════════════════════
# MILESTONE 1 — Human Review Queue
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/review/pending")
def api_pending_review():
    # BUG FIX [object Object]: ensure list of dicts returned
    return [_row_to_dict(c) for c in get_pending_review()]


@router.post("/review/approve")
def api_approve_clause(req: ApproveRequest):
    try:
        result = approve_clause(
            clause_id=req.clause_id,
            reviewed_by=req.reviewed_by,
            review_note=req.review_note,
            override_type=req.override_type,
            section_ref=req.section_ref,
            suggested_discount_value=req.suggested_discount_value,  # ★ NEW
            suggested_catalog_id=req.suggested_catalog_id,          # ★ NEW
        )
        return _safe_json(result)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.get("/review/detect-discount/{clause_id}")
def api_detect_discount(clause_id: str):
    """★ NEW: Detect % value in a pending clause text for unified approval UI."""
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT text, source_doc_id FROM review_queue WHERE clause_id=?",
            (clause_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, f"Clause not found: {clause_id}")
        detected = detect_discount_in_text(row["text"])
        return {
            "clause_id": clause_id,
            "detected_discount_pct": detected,
            "has_suggestion": detected is not None,
        }
    finally:
        conn.close()


@router.post("/review/reject")
def api_reject_clause(req: RejectRequest):
    try:
        result = reject_clause(
            clause_id=req.clause_id,
            reviewed_by=req.reviewed_by,
            reason=req.reason,
        )
        return _safe_json(result)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/review/unapprove")
def api_unapprove_clause(req: UnapproveRequest):
    """Move an approved clause back to PENDING review queue."""
    try:
        from ingestion.pipeline import unapprove_clause
        result = unapprove_clause(
            clause_id=req.clause_id,
            reviewed_by=req.reviewed_by,
            reason=req.reason,
        )
        return _safe_json(result)
    except ValueError as e:
        raise HTTPException(400, str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MILESTONE 1 — Clause Store
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/clauses")
def api_clause_store(
    clause_id:          Optional[str]  = Query(None),
    source_doc_id:      Optional[str]  = Query(None),
    clause_type:        Optional[str]  = Query(None),
    include_superseded: bool           = Query(False),
):
    result = get_clause_store(
        clause_id=clause_id,
        source_doc_id=source_doc_id,
        clause_type=clause_type,
        is_current=not include_superseded,
    )
    # BUG FIX [object Object]: ensure list of plain dicts
    return [_row_to_dict(c) for c in result] if result else []


@router.get("/clauses/{clause_id}/chain")
def api_traceability_chain(clause_id: str):
    result = get_traceability_chain(clause_id)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return _safe_json(result)


# ═══════════════════════════════════════════════════════════════════════════════
# MILESTONE 1 — Rights Catalog
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/rights")
def api_list_rights(status: str = Query("ACTIVE")):
    result = get_all_rights(status=status)
    return [_row_to_dict(r) for r in result] if result else []


@router.post("/rights")
def api_upsert_right(req: RightUpsertRequest):
    try:
        return _safe_json(upsert_right(req.dict()))
    except Exception as e:
        raise HTTPException(400, str(e))


@router.get("/rights/{catalog_id}")
def api_get_right(catalog_id: str):
    result = get_rights_with_clauses(catalog_id)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return _safe_json(result)


# ═══════════════════════════════════════════════════════════════════════════════
# MILESTONE 1 — Engine Version Management
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/engine/stage")
def api_stage_version(req: StageVersionRequest):
    result = create_staging_version(law_version=req.law_version, notes=req.notes)
    return _safe_json(result)


@router.post("/engine/publish")
def api_publish_version(req: PublishVersionRequest):
    try:
        result = publish_version(
            engine_id=req.engine_id,
            published_by=req.published_by,
            notes=req.note,
        )
        return _safe_json(result)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/engine/reject")
def api_reject_version(req: RejectVersionRequest):
    try:
        reason = req.reason or req.note or "Rejected by expert"
        result = reject_version(
            engine_id=req.engine_id,
            rejected_by=req.rejected_by,
            reason=reason,
        )
        return _safe_json(result)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.get("/engine/versions")
def api_list_versions():
    result = list_versions()
    return [_row_to_dict(v) for v in result] if result else []


@router.get("/engine/active")
def api_active_version():
    version = get_active_version()
    if not version:
        return {"status": "NO_ACTIVE_VERSION", "message": "No engine version published yet."}
    return _row_to_dict(version)


# ═══════════════════════════════════════════════════════════════════════════════
# MILESTONE 1 — Validation & Audit
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/validate")
def api_validate():
    result = validate_clause_integrity()
    return _safe_json(result)


@router.get("/audit")
def api_audit_log(limit: int = Query(50, ge=1, le=500)):
    rows = get_audit_log(limit=limit)
    # BUG FIX [object Object]: rows may be sqlite3.Row objects
    if not rows:
        return []
    if hasattr(rows[0], 'keys'):
        return [dict(r) for r in rows]
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
# QA #7 — Contact Form
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/contact")
def api_contact(req: ContactRequest):
    """Receive contact form submissions. Logs to audit_log."""
    if not req.name.strip() or not req.email.strip() or not req.message.strip():
        raise HTTPException(400, "כל השדות חובה")
    if "@" not in req.email or "." not in req.email:
        raise HTTPException(400, "כתובת דוא\"ל לא תקינה")

    # Log to audit_log so it's traceable
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO audit_log (event_type, details, created_at)
               VALUES (?, ?, ?)""",
            ("CONTACT_FORM",
             json.dumps({"name": req.name, "email": req.email, "message": req.message[:500]}, ensure_ascii=False),
             datetime.utcnow().isoformat())
        )
        conn.commit()
    finally:
        conn.close()

    return {"status": "ok", "message": "ההודעה התקבלה בהצלחה"}


# ═══════════════════════════════════════════════════════════════════════════════
# MILESTONE 2B — L4: Fact Normalization
# ═══════════════════════════════════════════════════════════════════════════════

# Valid fact types per M2A architecture doc Section 4.2
VALID_FACT_TYPES = {
    "RESERVE_TYPE", "SERVICE_DAYS_3Y", "SERVICE_START_DATE", "SERVICE_END_DATE",
    "IS_PROPERTY_HOLDER", "PROPERTY_SIZE_SQM", "ANNUAL_TAX_ILS", "DISCOUNT_RATE_PCT",
    "MUNICIPALITY_GRANTS", "FAMILY_SIZE", "ANNUAL_INCOME_ILS", "IS_SENIOR",
    "PAYMENT_UPFRONT", "INSTALLMENT_COUNT",
    # Service period audit trail facts (up to 10 periods)
    "SERVICE_PERIOD_1", "SERVICE_PERIOD_2", "SERVICE_PERIOD_3",
    "SERVICE_PERIOD_4", "SERVICE_PERIOD_5", "SERVICE_PERIOD_6",
    "SERVICE_PERIOD_7", "SERVICE_PERIOD_8", "SERVICE_PERIOD_9", "SERVICE_PERIOD_10",
}


@router.post("/session/start")
def api_session_start():
    """Create a new citizen session. Returns session_id."""
    session_id = str(uuid.uuid4())
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO audit_log (event_type, session_id, details, created_at)
               VALUES (?, ?, ?, ?)""",
            ("SESSION_START", session_id, "{}", datetime.utcnow().isoformat())
        )
        conn.commit()
    finally:
        conn.close()
    return {"session_id": session_id, "status": "created", "created_at": datetime.utcnow().isoformat()}


@router.post("/facts")
def api_submit_facts(req: FactsSubmitRequest):
    """
    L4: Submit typed fact objects for a session.
    Validates fact_type against allowed vocabulary.
    """
    if not req.session_id:
        raise HTTPException(400, "session_id is required")
    if not req.facts:
        raise HTTPException(400, "At least one fact is required")

    # Validate all fact types before inserting any
    invalid = [f.fact_type for f in req.facts if f.fact_type not in VALID_FACT_TYPES]
    if invalid:
        raise HTTPException(400, f"Invalid fact_type(s): {invalid}. Allowed: {sorted(VALID_FACT_TYPES)}")

    conn = get_db()
    inserted = []
    try:
        for fact in req.facts:
            fact_id = str(uuid.uuid4())
            conn.execute(
                """INSERT OR REPLACE INTO facts
                   (fact_id, session_id, fact_type, value, provenance, confidence, collected_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (fact_id, req.session_id, fact.fact_type,
                 fact.value, fact.provenance, fact.confidence,
                 datetime.utcnow().isoformat())
            )
            inserted.append({"fact_id": fact_id, "fact_type": fact.fact_type})
        conn.commit()
    finally:
        conn.close()

    return {
        "status": "ok",
        "session_id": req.session_id,
        "inserted": len(inserted),
        "facts": inserted,
    }


@router.get("/facts/{session_id}")
def api_get_facts(session_id: str):
    """Get all facts collected for a session."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM facts WHERE session_id = ? ORDER BY collected_at",
            (session_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.delete("/facts/{session_id}")
def api_clear_facts(session_id: str):
    """Clear all facts for a session (allows re-submission)."""
    conn = get_db()
    try:
        deleted = conn.execute(
            "DELETE FROM facts WHERE session_id = ?", (session_id,)
        ).rowcount
        conn.commit()
        return {"status": "ok", "session_id": session_id, "deleted": deleted}
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# MILESTONE 2B — L5 + L6: Rule Evaluation + Explainability
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/evaluate/{session_id}")
def api_evaluate(session_id: str, req: EvaluateRequest = EvaluateRequest()):
    """
    L5+L6: Run full eligibility evaluation for a session.
    1. Load facts from facts table
    2. Load rights catalog + linked clauses
    3. Run rule engine (deterministic, no LLM)
    4. Serialize result with Hebrew explanation + evidence chain
    5. Write to audit_log
    Returns: EligibilityResult with evidence chain and explanation_he
    """
    try:
        from engine.rule_engine import evaluate_eligibility
        from engine.decision_serializer import serialize_result

        # Load facts for this session
        conn = get_db()
        try:
            fact_rows = conn.execute(
                "SELECT fact_type, value FROM facts WHERE session_id = ?",
                (session_id,)
            ).fetchall()
        finally:
            conn.close()

        if not fact_rows:
            raise HTTPException(404, f"No facts found for session {session_id}. Call POST /api/facts first.")

        # Build fact dict: {fact_type: parsed_value}
        facts = {}
        for row in fact_rows:
            try:
                facts[row["fact_type"]] = json.loads(row["value"])
            except (json.JSONDecodeError, TypeError):
                facts[row["fact_type"]] = row["value"]

        # Get engine version
        engine_version = get_active_version()
        if not engine_version:
            raise HTTPException(503, "No active engine version. Stage and publish an engine version first.")
        engine_id = dict(engine_version).get("engine_id", "unknown")

        # Get rights catalog with linked clauses
        rights = get_all_rights(status="ACTIVE")
        rights_with_clauses = []
        for r in rights:
            r_dict = _row_to_dict(r)
            full = get_rights_with_clauses(r_dict["catalog_id"])
            if "error" not in full:
                rights_with_clauses.append(full)

        # Run L5 rule engine
        evaluation = evaluate_eligibility(facts, rights_with_clauses)

        # Run L6 serializer
        result = serialize_result(
            session_id=session_id,
            engine_id=engine_id,
            facts=facts,
            evaluation=evaluation,
        )

        # Write to audit_log
        _log_event(
            event_type="EVALUATE",
            session_id=session_id,
            engine_id=engine_id,
            details=json.dumps({"eligible_count": len(result.get("eligible_rights", []))})
        )

        return result

    except HTTPException:
        raise
    except ImportError as e:
        raise HTTPException(501, f"Rule engine not yet implemented: {str(e)}")
    except Exception as e:
        raise HTTPException(500, f"Evaluation failed: {str(e)}")


@router.get("/evaluate/{session_id}/calculate")
def api_calculate(session_id: str):
    """
    Compute discount amount + amount_after_discount + per-installment amounts.
    Uses PAYMENT_UPFRONT and INSTALLMENT_COUNT facts from the session.

    Formula (Soldier — תקנה 3ו):
      discount = annual_tax * (discount_rate_pct / 100)
      discount = min(discount, annual_tax)
      result   = annual_tax - discount

    Formula (Commander — תקנה 3ז):
      taxable_sqm    = min(property_sqm, 100)
      tariff_per_sqm = annual_tax / property_sqm
      tax_on_100sqm  = taxable_sqm * tariff_per_sqm
      discount       = tax_on_100sqm * (discount_rate_pct / 100)
      result         = annual_tax - discount
    """
    # Load facts
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT fact_type, value FROM facts WHERE session_id = ?",
            (session_id,)
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        raise HTTPException(404, f"No facts for session {session_id}")

    facts = {}
    for row in rows:
        try:
            facts[row["fact_type"]] = json.loads(row["value"])
        except Exception:
            facts[row["fact_type"]] = row["value"]

    # Extract needed facts
    reserve_type       = facts.get("RESERVE_TYPE", "SOLDIER")
    annual_tax         = float(facts.get("ANNUAL_TAX_ILS", 0))
    discount_rate_pct  = float(facts.get("DISCOUNT_RATE_PCT", 0))
    property_sqm       = float(facts.get("PROPERTY_SIZE_SQM", 0))
    payment_upfront    = facts.get("PAYMENT_UPFRONT", False)
    installment_count  = int(facts.get("INSTALLMENT_COUNT", 1))

    if annual_tax <= 0:
        raise HTTPException(400, "ANNUAL_TAX_ILS fact is required and must be > 0")

    # ── Soldier (תקנה 3ו) ────────────────────────────────────────────────────
    if reserve_type == "SOLDIER":
        discount = annual_tax * (discount_rate_pct / 100)
        discount = min(discount, annual_tax)          # cap at full tax

    # ── Commander (תקנה 3ז) ──────────────────────────────────────────────────
    elif reserve_type == "COMMANDER":
        if property_sqm <= 0:
            raise HTTPException(400, "PROPERTY_SIZE_SQM is required for commander calculation")
        taxable_sqm    = min(property_sqm, 100.0)
        tariff_per_sqm = annual_tax / property_sqm
        tax_on_100sqm  = taxable_sqm * tariff_per_sqm
        discount       = tax_on_100sqm * (discount_rate_pct / 100)
        discount       = min(discount, annual_tax)

    else:
        # Generic fallback for other right types
        discount = annual_tax * (discount_rate_pct / 100)
        discount = min(discount, annual_tax)

    amount_after_discount = annual_tax - discount

    # Installment breakdown
    if payment_upfront or installment_count <= 1:
        installment_count       = 1
        installment_gross       = annual_tax
        installment_discount    = discount
        installment_net         = amount_after_discount
    else:
        installment_gross       = round(annual_tax / installment_count, 2)
        installment_discount    = round(discount / installment_count, 2)
        installment_net         = round(amount_after_discount / installment_count, 2)

    return {
        "session_id":              session_id,
        "reserve_type":            reserve_type,
        "annual_tax_ils":          round(annual_tax, 2),
        "discount_rate_pct":       round(discount_rate_pct, 2),
        "discount_ils":            round(discount, 2),
        "amount_after_discount_ils": round(amount_after_discount, 2),
        "payment_upfront":         payment_upfront,
        "installment_count":       installment_count,
        "installment_gross_ils":   round(installment_gross, 2),
        "installment_discount_ils":round(installment_discount, 2),
        "installment_net_ils":     round(installment_net, 2),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Dev / Reset
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/reset-db-temp")
def reset_db(secret: Optional[str] = Query(None)):
    """⚠️ DEVELOPMENT ONLY — guarded by RESET_SECRET env var."""
    import os
    expected = os.getenv("RESET_SECRET", "")
    if not expected or secret != expected:
        raise HTTPException(403, "Forbidden. Set RESET_SECRET env var.")
    from database.schema import init_db
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
    from engine.rights_catalog import seed_rights_catalog
    seed_rights_catalog()
    return {"status": "done", "message": "Database reset and rights catalog re-seeded"}


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers — BUG FIX [object Object]
# ═══════════════════════════════════════════════════════════════════════════════

def _row_to_dict(row) -> dict:
    """
    BUG FIX: sqlite3.Row objects are not JSON-serializable by FastAPI.
    When JS receives them as [object Object], it displays that string.
    This converts any Row (or already-dict) to a plain Python dict.
    """
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except Exception:
        return {}


def _safe_json(obj):
    """
    Recursively convert sqlite3.Row objects and other non-serializable
    types to plain Python dicts/lists so FastAPI can serialize them.
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {k: _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_json(i) for i in obj]
    # sqlite3.Row → dict
    try:
        return {k: _safe_json(obj[k]) for k in obj.keys()}
    except Exception:
        return str(obj)


def _log_event(event_type: str, session_id: str = None,
               engine_id: str = None, details: str = "{}"):
    """Append an event to the audit_log table."""
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO audit_log (event_type, engine_id, session_id, details, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (event_type, engine_id, session_id, details, datetime.utcnow().isoformat())
        )
        conn.commit()
    except Exception:
        pass  # audit log failure must never break main flow
    finally:
        conn.close()
