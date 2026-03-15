"""
Rights Angel — FastAPI Application
Milestone 1 + Milestone 2B Screens
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
    """
    Returns all rights with their linked clauses attached.
    Used by screen3 to show legal sources per right.
    """
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
    return templates.TemplateResponse("screen2_decomp.html", {
        "request":        request,
        "pending_clauses": get_pending_review(),
        "approved_count": len(get_clause_store()),
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
    # screen9 now loads data via JS/API — no server-side data needed
    return templates.TemplateResponse("screen9_audit.html", {
        "request": request,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Milestone 2B Screens (screen10 – screen12) — NEW
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/screen10")
async def screen10(request: Request, type: str = "reserve"):
    """
    L4: Citizen fact collection form.
    Citizen fills in their details — submitted to POST /api/facts.
    type param pre-selects soldier / commander / senior / lowincome.
    """
    from engine.fact_normalizer import get_fact_schema
    return templates.TemplateResponse("screen10_fact_collector.html", {
        "request":     request,
        "calc_type":   type,
        "fact_schema": get_fact_schema(),
        "rights":      get_all_rights(),
    })


@app.get("/screen11")
async def screen11(request: Request, session_id: str = ""):
    """
    L5 + L6: Eligibility result screen.
    Calls /api/evaluate/{session_id} via JS on load.
    session_id passed as query param from screen10 after facts submitted.
    """
    return templates.TemplateResponse("screen11_eligibility_result.html", {
        "request":    request,
        "session_id": session_id,
    })


@app.get("/screen12")
async def screen12(request: Request):
    """
    Pipeline diagram screen.
    Visual flow: Legal Text → Atoms → Rules → Eligibility Result.
    Static screen — no data needed from backend.
    """
    return templates.TemplateResponse("screen12_pipeline_diagram.html", {
        "request":       request,
        "rights_count":  len(get_all_rights()),
        "docs_count":    len(list_documents()),
        "clauses_count": len(get_clause_store()),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Startup
# ═══════════════════════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    init_db()
    seed_rights_catalog()
    print("✅ Rights Angel v2.0 started")
    print("   Home       : http://localhost:8000/screen0")
    print("   Check right: http://localhost:8000/screen10")
    print("   Result     : http://localhost:8000/screen11")
    print("   Pipeline   : http://localhost:8000/screen12")
    print("   API docs   : http://localhost:8000/docs")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
