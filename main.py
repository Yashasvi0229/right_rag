"""
Rights Angel — FastAPI Application
Milestone 1 + Milestone 2B Screens + Screen13/14 (Legal Dictionary + Expert Questions)
"""
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from pathlib import Path
import uvicorn

from database.schema import init_db, get_db
from engine.rights_catalog import seed_rights_catalog, get_all_rights
from api.routes import router
from ingestion.pipeline import (
    list_documents, get_approved_sources,
    get_pending_review, get_clause_store, validate_clause_integrity
)
from engine.version_manager import list_versions, get_audit_log

app = FastAPI(title="Rights Angel", version="2.0.0")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

static_dir = BASE_DIR / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

app.include_router(router, prefix="/api")


def get_rights_with_linked_clauses(status: str = "ACTIVE") -> list:
    conn = get_db()
    try:
        rights = conn.execute(
            "SELECT * FROM rights WHERE status=? ORDER BY category_tag, catalog_id",
            (status,)
        ).fetchall()
        result = []
        for r in rights:
            right_dict = dict(r)
            linked = conn.execute("""
                SELECT c.clause_id, c.section_ref, c.clause_type, m.mapping_role,
                       s.title as doc_title, s.doc_id as source_doc_id
                FROM rights_clauses_map m
                JOIN clauses c ON m.clause_id = c.clause_id
                JOIN source_documents s ON c.source_doc_id = s.doc_id
                WHERE m.catalog_id = ? AND c.is_current = 1
                ORDER BY c.created_at ASC
            """, (r["catalog_id"],)).fetchall()
            right_dict["linked_clauses"] = [dict(cl) for cl in linked]
            result.append(right_dict)
        return result
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Root
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/")
async def root():
    return RedirectResponse(url="/screen0")


# ═══════════════════════════════════════════════════════════════════════════════
# Milestone 1 Screens (screen0 – screen9)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/screen0")
async def screen0(request: Request):
    return templates.TemplateResponse("screen0_home.html", {
        "request":       request,
        "rights_count":  len(get_all_rights()),
        "docs_count":    len(list_documents()),
        "clauses_count": len(get_clause_store()),
    })


@app.get("/screen1")
async def screen1(request: Request):
    return templates.TemplateResponse("screen1_upload.html", {
        "request":          request,
        "documents":        list_documents(),
        "approved_sources": get_approved_sources(),
    })


@app.get("/screen2")
async def screen2(request: Request):
    approved = [c for c in get_clause_store() if c.get("is_current")]
    return templates.TemplateResponse("screen2_decomp.html", {
        "request":         request,
        "pending_clauses": get_pending_review(),
        "approved_count":  len(approved),
    })


@app.get("/screen3")
async def screen3(request: Request):
    return templates.TemplateResponse("screen3_engine.html", {
        "request":  request,
        "versions": list_versions(),
        "rights":   get_rights_with_linked_clauses(),
    })


@app.get("/screen4")
async def screen4(request: Request):
    return templates.TemplateResponse("screen4_validation.html", {
        "request":    request,
        "validation": validate_clause_integrity(),
    })


@app.get("/screen5")
async def screen5(request: Request):
    return templates.TemplateResponse("screen5_explain.html", {
        "request": request,
        "rights":  get_all_rights(),
    })


@app.get("/screen6")
async def screen6(request: Request):
    return templates.TemplateResponse("screen6_approval.html", {
        "request":  request,
        "versions": list_versions(),
    })


@app.get("/screen7")
async def screen7(request: Request, type: str = "reserve"):
    return templates.TemplateResponse("screen7_calculator.html", {
        "request":   request,
        "calc_type": type,
        "rights":    get_all_rights(),
    })


@app.get("/screen8")
async def screen8(request: Request):
    return templates.TemplateResponse("screen8_appeal.html", {
        "request": request,
    })


@app.get("/screen9")
async def screen9(request: Request):
    return templates.TemplateResponse("screen9_audit.html", {
        "request": request,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Milestone 2B Screens (screen10 – screen12)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/screen10")
async def screen10(request: Request, type: str = "reserve"):
    from engine.fact_normalizer import get_fact_schema
    return templates.TemplateResponse("screen10_fact_collector.html", {
        "request":     request,
        "calc_type":   type,
        "fact_schema": get_fact_schema(),
        "rights":      get_all_rights(),
    })


@app.get("/screen11")
async def screen11(request: Request, session_id: str = ""):
    return templates.TemplateResponse("screen11_eligibility_result.html", {
        "request":    request,
        "session_id": session_id,
    })


@app.get("/screen12")
async def screen12(request: Request):
    return templates.TemplateResponse("screen12_pipeline_diagram.html", {
        "request":       request,
        "rights_count":  len(get_all_rights()),
        "docs_count":    len(list_documents()),
        "clauses_count": len(get_clause_store()),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# NEW — Screen 13: Legal Dictionary (client file 01 format)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/screen13")
async def screen13(request: Request, doc_id: str = ""):
    """
    מילון משפטי — Legal Dictionary screen.
    Shows all approved clauses with enrichment fields:
    plain_explanation, practical_meaning, evidence_needed,
    approving_authority, confidence_level, notes.
    Matches client file 01 (מילון משפטי) format exactly.
    doc_id query param pre-filters to a specific document.
    Data loaded via JS from /api/clauses.
    """
    doc_title = ""
    if doc_id:
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT title FROM source_documents WHERE doc_id=?", (doc_id,)
            ).fetchone()
            if row:
                doc_title = row["title"]
        finally:
            conn.close()

    return templates.TemplateResponse("screen13_legal_dictionary.html", {
        "request":   request,
        "doc_id":    doc_id,
        "doc_title": doc_title,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# NEW — Screen 14: Expert Questions (client file 04 format)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/screen14")
async def screen14(request: Request, doc_id: str = ""):
    """
    שאלות למומחה משפטי — Expert Questions screen.
    Shows ambiguities and decision points that require human legal expert judgment.
    Matches client file 04 (שאלות למומחה) format exactly.
    doc_id query param pre-filters to a specific document.
    Supports: view questions, generate new questions, record expert answers.
    Data loaded via JS from /api/expert-questions and
    /api/documents/{doc_id}/generate-expert-questions.
    """
    doc_title = ""
    if doc_id:
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT title FROM source_documents WHERE doc_id=?", (doc_id,)
            ).fetchone()
            if row:
                doc_title = row["title"]
        finally:
            conn.close()

    return templates.TemplateResponse("screen14_expert_questions.html", {
        "request":   request,
        "doc_id":    doc_id,
        "doc_title": doc_title,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Startup
# ═══════════════════════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    init_db()
    seed_rights_catalog()

    from engine.version_manager import get_active_version, create_staging_version, publish_version

    if not get_active_version():
        try:
            staged = create_staging_version(
                law_version="2026-seed-v1",
                notes="Auto-staged on first startup — no manual publish needed"
            )
            if staged.get("status") == "STAGED":
                publish_version(
                    engine_id=staged["engine_id"],
                    published_by="system-auto-startup",
                )
                print(f"✅ Engine version auto-published: {staged['engine_id']}")
            else:
                print(f"⚠️  Engine auto-stage blocked: {staged.get('reason', 'unknown')}")
        except Exception as e:
            print(f"⚠️  Engine auto-publish failed (non-fatal): {e}")

    print("✅ Rights Angel v2.0 started")
    print("   Home            : http://localhost:8000/screen0")
    print("   Upload          : http://localhost:8000/screen1")
    print("   Decomp          : http://localhost:8000/screen2")
    print("   Fact collector  : http://localhost:8000/screen10")
    print("   Result          : http://localhost:8000/screen11")
    print("   Pipeline        : http://localhost:8000/screen12")
    print("   Legal dictionary: http://localhost:8000/screen13")
    print("   Expert questions: http://localhost:8000/screen14")
    print("   API docs        : http://localhost:8000/docs")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
