"""
Rights Angel — FastAPI Application
Milestone 1 Backend — Production Ready

Run: uvicorn main:app --reload --port 8000
"""
import os
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv()

from database.schema import init_db
from engine.rights_catalog import seed_rights_catalog
from api.routes import router

# ── App setup ─────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Rights Angel — Milestone 1 API",
    description="Deterministic eligibility engine for Israeli arnona discounts",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).parent

# Templates
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Static files
static_dir = BASE_DIR / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# API routes
app.include_router(router, prefix="/api")


# ── Startup ───────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    init_db()
    seed_rights_catalog()
    print("✅ Rights Angel Milestone 1 started")
    print(f"   DB: {os.getenv('DB_PATH', 'rights_angel.db')}")
    has_key = os.getenv('OPENAI_API_KEY','').startswith('sk-') and not os.getenv('OPENAI_API_KEY','').startswith('sk-your')
    print(f"   OpenAI: {'✅ key found' if has_key else '❌ KEY NOT SET'}")


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"service": "Rights Angel — Milestone 1", "status": "running"}

@app.get("/health")
def health():
    return {"status": "ok"}


# ── Screen Routes ─────────────────────────────────────────────────────────────

@app.get("/screen1", response_class=HTMLResponse)
async def screen1(request: Request):
    from ingestion.pipeline import list_documents, get_approved_sources
    return templates.TemplateResponse("screen1_upload.html", {
        "request": request,
        "documents": list_documents(),
        "approved_sources": get_approved_sources(),
    })


@app.get("/screen2", response_class=HTMLResponse)
async def screen2(request: Request):
    from ingestion.pipeline import get_pending_review, get_clause_store
    from database.schema import get_db
    conn = get_db()
    try:
        approved_count = conn.execute(
            "SELECT COUNT(*) as c FROM clauses WHERE is_current=1"
        ).fetchone()["c"]
    finally:
        conn.close()

    return templates.TemplateResponse("screen2_decomp.html", {
        "request": request,
        "pending_clauses": get_pending_review(),
        "approved_count": approved_count,
    })


@app.get("/screen4", response_class=HTMLResponse)
async def screen4(request: Request):
    from ingestion.pipeline import validate_clause_integrity
    validation = validate_clause_integrity()
    return templates.TemplateResponse("screen4_validation.html", {
        "request": request,
        "validation": validation,
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("APP_PORT", 8000)),
        reload=os.getenv("DEBUG", "true").lower() == "true",
    )
