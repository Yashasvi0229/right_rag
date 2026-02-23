"""
Rights Angel — FastAPI Application
Phase 1 Screens + Milestone 1 Backend API
"""
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from pathlib import Path
import uvicorn

from database.schema import init_db
from engine.rights_catalog import seed_rights_catalog, get_all_rights
from api.routes import router
from ingestion.pipeline import (
    list_documents, get_approved_sources,
    get_pending_review, get_clause_store, validate_clause_integrity
)
from engine.version_manager import list_versions, get_audit_log

app = FastAPI(title="Rights Angel", version="1.0.0")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

static_dir = BASE_DIR / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

app.include_router(router, prefix="/api")


@app.get("/")
async def root():
    return RedirectResponse(url="/screen0")

@app.get("/screen0")
async def screen0(request: Request):
    return templates.TemplateResponse("screen0_home.html", {
        "request": request,
        "rights_count": len(get_all_rights()),
        "docs_count": len(list_documents()),
        "clauses_count": len(get_clause_store()),
    })

@app.get("/screen1")
async def screen1(request: Request):
    return templates.TemplateResponse("screen1_upload.html", {
        "request": request,
        "documents": list_documents(),
        "approved_sources": get_approved_sources(),
    })

@app.get("/screen2")
async def screen2(request: Request):
    return templates.TemplateResponse("screen2_decomp.html", {
        "request": request,
        "pending_clauses": get_pending_review(),
        "approved_count": len(get_clause_store()),
    })

@app.get("/screen3")
async def screen3(request: Request):
    return templates.TemplateResponse("screen3_engine.html", {
        "request": request,
        "versions": list_versions(),
        "rights": get_all_rights(),
    })

@app.get("/screen4")
async def screen4(request: Request):
    return templates.TemplateResponse("screen4_validation.html", {
        "request": request,
        "validation": validate_clause_integrity(),
    })

@app.get("/screen5")
async def screen5(request: Request):
    return templates.TemplateResponse("screen5_explain.html", {
        "request": request,
        "rights": get_all_rights(),
    })

@app.get("/screen6")
async def screen6(request: Request):
    return templates.TemplateResponse("screen6_approval.html", {
        "request": request,
        "versions": list_versions(),
    })

@app.get("/screen7")
async def screen7(request: Request, type: str = "reserve"):
    return templates.TemplateResponse("screen7_calculator.html", {
        "request": request,
        "calc_type": type,
        "rights": get_all_rights(),
    })

@app.get("/screen8")
async def screen8(request: Request):
    return templates.TemplateResponse("screen8_appeal.html", {"request": request})

@app.get("/screen9")
async def screen9(request: Request):
    return templates.TemplateResponse("screen9_audit.html", {
        "request": request,
        "audit_log": get_audit_log(limit=50),
        "versions": list_versions(),
    })


@app.on_event("startup")
async def startup():
    init_db()
    seed_rights_catalog()
    print("✅ Rights Angel started")
    print("   Screens : http://localhost:8000/screen0")
    print("   API docs : http://localhost:8000/docs")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)