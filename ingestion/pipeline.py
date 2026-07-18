"""
Rights Angel — Ingestion Pipeline (L1 + L2)
Architecture Brief v1.2 §6.1 Steps 1-3

L1: Document Loader + SHA-256 Hash Registry
    - APPROVED_SOURCES acts as a metadata registry, not a hard gate
    - Any doc_id is accepted if caller supplies title + publisher
    - Hash check → unchanged = exit, changed = proceed
    - PDF/DOCX/TXT text extraction

L2: OpenAI GPT-4o Atomic Clause Extractor
    - temperature=0 (deterministic, critical for legal accuracy)
    - Domain-agnostic prompt covering any Israeli statutory/administrative law
    - Extracted clauses → human review queue (NOT clause store directly)
    - Human must approve before clause enters store (§2.1 gate)
    - Structured failure reasons surfaced via audit_log for admin UI
"""
import hashlib
import json
import uuid
import os
import io
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")
from database.schema import get_db

# ─────────────────────────────────────────────────────────────────────────────
# APPROVED_SOURCES — metadata registry for known legal documents.
# NOT a hard gate. New sources may be ingested by supplying title + publisher.
# ─────────────────────────────────────────────────────────────────────────────
APPROVED_SOURCES = {
    "GOV-IL-RESERVE-ARNONA-2026": {
        "title": "זכות ההנחה בארנונה לחיילי מילואים בישראל — ריכוז חומר משפטי מקיף",
        "publisher": "ריכוז משפטי | עודכן פברואר 2026",
        "category": "reserve_soldiers",
        "publication_date": "2026-02-01",
    },
    "GOV-IL-LOWINCOME-ARNONA-2026": {
        "title": "הנחה בארנונה למעוטי יכולת בישראל — חוברת בסיס הידע המשפטי",
        "publisher": "מערכת Angel Rights | פברואר 2026",
        "category": "low_income",
        "publication_date": "2026-02-19",
    },
    "GOV-IL-ARNONA-REGULATIONS-1993": {
        "title": "תקנות הסדרים במשק המדינה (הנחה מארנונה), תשנ\"ג-1993",
        "publisher": "כנסת ישראל / משרד הפנים",
        "category": "general",
        "publication_date": "1993-01-01",
    },
    "GOV-IL-RESERVE-REGULATION-3VAV": {
        "title": "תקנה 3ו — הנחה לחיילי מילואים פעילים (תיקון 3, תשע\"ח-2018)",
        "publisher": "משרד הפנים",
        "category": "reserve_soldiers",
        "publication_date": "2018-03-27",
    },
    "GOV-IL-RESERVE-COMMANDER-2022": {
        "title": "תיקון תקנות ארנונה — מפקד מילואים פעיל 25%, תשפ\"ג-2022",
        "publisher": "עיריית נתניה / משרד הפנים",
        "category": "reserve_soldiers",
        "publication_date": "2022-11-02",
    },
    "GOV-IL-HORA-AT-SHA-A-2024": {
        "title": "הוראת שעה — תגמולי מילואים לא ייחשבו כהכנסה, תשפ\"ה-2024",
        "publisher": "שר הפנים משה ארבל",
        "category": "reserve_soldiers",
        "publication_date": "2024-10-15",
    },
    "GOV-IL-ARNONA-AMENDMENT-43-2026": {
        "title": "תקנות הסדרים במשק המדינה (הנחה מארנונה), תיקון מס' 43, התשפ\"ו-2026",
        "publisher": "משרד הפנים",
        "category": "reserve_soldiers",
        "publication_date": "2026-04-04",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Domain-agnostic extraction prompt.
# Handles any Israeli statutory/administrative law, not only arnona.
# temperature=0 for determinism (architecture brief §3.3)
# ─────────────────────────────────────────────────────────────────────────────
EXTRACTION_SYSTEM_PROMPT = """You are a legal analyst specializing in Israeli administrative and statutory law across any domain: property tax (arnona), welfare, employment, family law, criminal procedure, education, social security, tenancy, consumer protection, and others.

Your task: extract ATOMIC LEGAL CLAUSES from Israeli legal documents.

ATOMIC CLAUSE = one single, indivisible legal unit. A condition, exclusion, definition, or procedure that stands alone.

CLAUSE TYPES:
- ELIGIBILITY: grants a right or states a qualifying condition ("זכאי", "רשאי", "entitled to", conditions that must be met)
- EXCLUSION: removes or denies a right ("אינו זכאי", "לא יינתן", "except", "provided that not")
- DEFINITION: defines a legal term ("...משמעו", "...כהגדרתו", "for purposes of this regulation means...")
- PROCEDURE: steps to apply, appeal deadlines, required documents ("יגיש", "submit", "within X days", "required documents")

CRITICAL RULES — NO EXCEPTIONS:
1. VERBATIM HEBREW TEXT ONLY — copy exact text from document, NEVER paraphrase, summarize, or translate
2. section_ref MUST be the EXACT reference found in the document text. Examples across legal domains:
   - "תקנה 3ו(א)", "תקנה 3ז", "סעיף 2(א)(8)"
   - "סעיף 5(ב) לחוק החוזים", "תקנה 12 לתקנות התעבורה"
   - "סעיף 24 לחוק הביטוח הלאומי", "סעיף 8 לחוק הורים יחידים"
   - If no section ref found in the text, use "לא צוין" — NEVER invent or hallucinate a reference
3. Each clause must be independently meaningful
4. Minimum clause length: 50 characters
5. Return ONLY valid JSON — no preamble, no explanation, no markdown
6. NEVER extract incomplete sentences — each clause must be a complete legal statement
7. If a sentence ends abruptly, seems cut off, or contains only a fragment — SKIP IT
8. Each clause must contain at least one complete legal predicate (subject + verb + object)

RETURN FORMAT:
{"clauses": [{"section_ref": "סעיף 5(ב)", "text": "verbatim Hebrew text here", "clause_type": "ELIGIBILITY"}]}"""

EXTRACTION_USER_PROMPT = """Document ID: {doc_id}
Title: {title}
Publisher: {publisher}

Extract all atomic legal clauses from this text:

{text}"""


# ─────────────────────────────────────────────────────────────────────────────
class SourceNotApprovedError(Exception):
    pass

class DocumentUnchangedError(Exception):
    pass


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def extract_text_from_bytes(file_bytes: bytes, filename: str) -> str:
    """Route to correct text extractor based on file extension."""
    ext = Path(filename).suffix.lower()

    if ext == ".pdf":
        try:
            import pdfplumber
            pages = []
            with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
                for page in pdf.pages:
                    t = page.extract_text()
                    if t and t.strip():
                        pages.append(t.strip())
            if not pages:
                raise ValueError("PDF appears to have no extractable text")
            return "\n\n".join(pages)
        except ImportError:
            raise RuntimeError("pdfplumber not installed. Run: pip install pdfplumber")

    elif ext in (".docx", ".doc"):
        try:
            from docx import Document
            doc = Document(io.BytesIO(file_bytes))
            text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
            if not text:
                raise ValueError("DOCX has no extractable text")
            return text
        except ImportError:
            raise RuntimeError("python-docx not installed. Run: pip install python-docx")

    elif ext == ".txt":
        return file_bytes.decode("utf-8", errors="replace")

    else:
        raise ValueError(f"Unsupported file type '{ext}'. Supported: .pdf .docx .txt")


def _chunk_text(text: str, max_chars: int = 5000) -> list[str]:
    """
    Split text into chunks at paragraph boundaries for OpenAI calls.
    Respects token limits while keeping legal context intact.
    """
    if len(text) <= max_chars:
        return [text]

    chunks = []
    paragraphs = text.split("\n\n")
    current = ""

    for para in paragraphs:
        if len(current) + len(para) + 2 < max_chars:
            current += ("\n\n" if current else "") + para
        else:
            if current.strip():
                chunks.append(current.strip())
            current = para

    if current.strip():
        chunks.append(current.strip())

    return chunks or [text[:max_chars]]


def _call_openai(text: str, doc_id: str, meta: dict) -> tuple[list[dict], Optional[dict]]:
    """
    Single OpenAI call for one text chunk.
    Returns (clauses, error_info).
    error_info is None on success, or a dict with 'reason' and 'detail' on failure.
    """
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key or api_key.startswith("sk-your"):
        return [], {
            "reason": "OPENAI_API_KEY_MISSING",
            "detail": "OPENAI_API_KEY not set. Open .env file and add your key.",
        }

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        model = os.getenv("OPENAI_MODEL", "gpt-4o")

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                {"role": "user", "content": EXTRACTION_USER_PROMPT.format(
                    doc_id=doc_id,
                    title=meta.get("title", ""),
                    publisher=meta.get("publisher", ""),
                    text=text,
                )},
            ],
            temperature=0,
            response_format={"type": "json_object"},
            max_tokens=4000,
        )
    except Exception as e:
        return [], {
            "reason": "OPENAI_API_ERROR",
            "error_type": type(e).__name__,
            "detail": str(e)[:500],
        }

    raw = response.choices[0].message.content or ""
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed, None
        for v in parsed.values():
            if isinstance(v, list):
                return v, None
        return [], {
            "reason": "OPENAI_RETURNED_EMPTY",
            "detail": "Response contained no clause list",
        }
    except (json.JSONDecodeError, AttributeError):
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group()), None
            except json.JSONDecodeError:
                pass
        return [], {
            "reason": "OPENAI_PARSE_ERROR",
            "detail": raw[:200],
        }


def _generate_clause_id(doc_id: str, section_ref: str, index: int) -> str:
    """
    Generate deterministic clause_id.
    Format: CL-{DOCSHORT}-{SECTIONSLUG}-{INDEX:03d}
    Example: CL-RESERVE3V-tkn3vav-001
    """
    doc_short = re.sub(r"[^A-Z0-9]", "", doc_id.replace("GOV-IL-", "").upper())[:10]
    section_slug = re.sub(r"[^\w]", "", section_ref.replace(" ", ""))[:8]
    return f"CL-{doc_short}-{section_slug}-{index:03d}"


def _audit(conn, event_type: str, details: dict, clause_ids: list = None):
    """Write to append-only audit log. Never raises — must not block pipeline."""
    try:
        conn.execute(
            "INSERT INTO audit_log (event_type, clause_ids, details, created_at) VALUES (?,?,?,?)",
            (event_type,
             json.dumps(clause_ids or [], ensure_ascii=False),
             json.dumps(details, ensure_ascii=False),
             datetime.now(timezone.utc).isoformat())
        )
    except Exception:
        pass  # Audit failures must never block pipeline


# ═════════════════════════════════════════════════════════════════════════════
#  PUBLIC API
# ═════════════════════════════════════════════════════════════════════════════

def validate_date(date_str: str) -> str:
    """Validate date string — prevents 244444 type bugs. Returns ISO date or today."""
    if not date_str:
        return datetime.now().strftime("%Y-%m-%d")
    clean = re.sub(r"[^\d\-\/]", "", str(date_str))
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(clean, fmt)
            if 1900 <= dt.year <= 2100:
                return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return datetime.now().strftime("%Y-%m-%d")


def ingest_document(
    file_bytes: bytes,
    filename: str,
    doc_id: str,
    ingested_by: str,
    publication_date: Optional[str] = None,
    url: Optional[str] = None,
    title: Optional[str] = None,
    publisher: Optional[str] = None,
    category: Optional[str] = None,
) -> dict:
    """
    L1 + L2: Full ingestion pipeline for a legal document.

    APPROVED_SOURCES is a metadata registry, not a hard gate.
    - If doc_id is registered → registry metadata is used; caller values override.
    - If doc_id is new → caller MUST supply title + publisher.
    Never fails silently. On zero clauses or extraction failure, returns a
    structured result with failure_reason and writes it to audit_log.
    """
    if doc_id in APPROVED_SOURCES:
        registry_meta = APPROVED_SOURCES[doc_id]
        meta = {
            "title": (title or registry_meta["title"]),
            "publisher": (publisher or registry_meta["publisher"]),
            "category": (category or registry_meta.get("category", "general")),
            "publication_date": registry_meta.get("publication_date", ""),
        }
    else:
        if not title or not str(title).strip() or not publisher or not str(publisher).strip():
            raise SourceNotApprovedError(
                f"doc_id '{doc_id}' is not in the metadata registry. "
                f"For new sources, both 'title' and 'publisher' must be provided."
            )
        meta = {
            "title": str(title).strip(),
            "publisher": str(publisher).strip(),
            "category": (category or "general").strip() or "general",
            "publication_date": publication_date or "",
        }

    pub_date = validate_date(publication_date or meta.get("publication_date", ""))
    file_hash = sha256_of(file_bytes)
    now = datetime.now(timezone.utc).isoformat()

    conn = get_db()
    try:
        existing = conn.execute(
            "SELECT file_hash, status FROM source_documents WHERE doc_id=?",
            (doc_id,)
        ).fetchone()

        # ── Hash check ───────────────────────────────────────────────────────
        if existing and existing["file_hash"] == file_hash:
            _audit(conn, "INGEST_UNCHANGED", {
                "doc_id": doc_id, "file_hash": file_hash,
                "message": "Hash matches — no change recorded."
            })
            conn.commit()
            raise DocumentUnchangedError(
                f"Document '{doc_id}' is unchanged (hash matches). "
                "No update recorded per §6.1 Step 2."
            )

        # ── Text extraction ──────────────────────────────────────────────────
        try:
            extracted_text = extract_text_from_bytes(file_bytes, filename)
        except (ValueError, RuntimeError) as e:
            _audit(conn, "INGEST_ZERO_CLAUSES", {
                "doc_id": doc_id, "filename": filename,
                "reason": "TEXT_EXTRACTION_FAILED",
                "detail": str(e)[:500],
            })
            conn.commit()
            return {
                "doc_id": doc_id,
                "title": meta["title"],
                "publisher": meta["publisher"],
                "file_hash": file_hash,
                "clause_count": 0,
                "status": "NO_CLAUSES_EXTRACTED",
                "failure_reason": "TEXT_EXTRACTION_FAILED",
                "failure_detail": str(e)[:500],
                "message": f"Text extraction failed: {e}",
            }

        # ── Upsert source_documents row ──────────────────────────────────────
        if existing:
            conn.execute("""
                UPDATE source_documents 
                SET status='SUPERSEDED', file_hash=?, ingested_at=?, ingested_by=?
                WHERE doc_id=?
            """, (file_hash, now, ingested_by, doc_id))
        else:
            conn.execute("""
                INSERT INTO source_documents
                    (doc_id, title, publisher, publication_date, url,
                     file_hash, ingested_at, ingested_by, status)
                VALUES (?,?,?,?,?,?,?,?,'ACTIVE')
            """, (
                doc_id, meta["title"], meta["publisher"],
                pub_date, url, file_hash, now, ingested_by,
            ))
            conn.commit()

        # ── Pre-check: reject too-short input before spending API tokens ─────
        if len(extracted_text.strip()) < 200:
            _audit(conn, "INGEST_ZERO_CLAUSES", {
                "doc_id": doc_id, "filename": filename,
                "reason": "TEXT_TOO_SHORT",
                "detail": f"Extracted text is only {len(extracted_text.strip())} chars (min 200)",
                "text_length": len(extracted_text.strip()),
            })
            conn.commit()
            return {
                "doc_id": doc_id,
                "title": meta["title"],
                "publisher": meta["publisher"],
                "file_hash": file_hash,
                "clause_count": 0,
                "status": "NO_CLAUSES_EXTRACTED",
                "failure_reason": "TEXT_TOO_SHORT",
                "failure_detail": f"Extracted text is only {len(extracted_text.strip())} chars",
                "message": "Document contains too little text to extract clauses.",
            }

        # ── OpenAI clause extraction ─────────────────────────────────────────
        chunks = _chunk_text(extracted_text, max_chars=5000)
        all_clauses = []
        extraction_errors = []
        for chunk in chunks:
            extracted, err = _call_openai(chunk, doc_id, meta)
            if err:
                extraction_errors.append(err)
            all_clauses.extend(extracted)

        if not all_clauses and extraction_errors:
            first_err = extraction_errors[0]
            _audit(conn, "INGEST_ZERO_CLAUSES", {
                "doc_id": doc_id, "filename": filename,
                "reason": first_err.get("reason", "OPENAI_UNKNOWN_ERROR"),
                "detail": first_err.get("detail", ""),
                "error_type": first_err.get("error_type"),
                "chunks_attempted": len(chunks),
                "chunks_failed": len(extraction_errors),
            })
            conn.commit()
            return {
                "doc_id": doc_id,
                "title": meta["title"],
                "publisher": meta["publisher"],
                "file_hash": file_hash,
                "clause_count": 0,
                "status": "NO_CLAUSES_EXTRACTED",
                "failure_reason": first_err.get("reason", "OPENAI_UNKNOWN_ERROR"),
                "failure_detail": first_err.get("detail", ""),
                "message": f"Clause extraction failed: {first_err.get('reason')}",
            }

        # ── Place in review_queue ────────────────────────────────────────────
        queued_ids = []
        skipped_short = 0
        for i, c in enumerate(all_clauses, 1):
            section_ref = str(c.get("section_ref", "לא צוין"))[:128]
            text = str(c.get("text", "")).strip()
            clause_type = c.get("clause_type", "ELIGIBILITY")

            if len(text) < 50:
                skipped_short += 1
                continue

            if clause_type not in ("ELIGIBILITY", "EXCLUSION", "DEFINITION", "PROCEDURE"):
                clause_type = "ELIGIBILITY"

            clause_id = _generate_clause_id(doc_id, section_ref, i)

            conn.execute("""
                INSERT OR IGNORE INTO review_queue
                    (clause_id, source_doc_id, section_ref, text,
                     clause_type, status, submitted_at)
                VALUES (?,?,?,?,?,'PENDING',?)
            """, (clause_id, doc_id, section_ref, text, clause_type, now))

            queued_ids.append(clause_id)

        if not queued_ids:
            reason = "ALL_CLAUSES_TOO_SHORT" if skipped_short > 0 else "OPENAI_RETURNED_EMPTY"
            _audit(conn, "INGEST_ZERO_CLAUSES", {
                "doc_id": doc_id, "filename": filename,
                "reason": reason,
                "detail": f"OpenAI returned {len(all_clauses)} raw clauses, {skipped_short} skipped as too short (<50 chars)",
                "raw_clause_count": len(all_clauses),
                "skipped_short": skipped_short,
                "extraction_errors": extraction_errors,
            })
            conn.commit()
            return {
                "doc_id": doc_id,
                "title": meta["title"],
                "publisher": meta["publisher"],
                "file_hash": file_hash,
                "clause_count": 0,
                "status": "NO_CLAUSES_EXTRACTED",
                "failure_reason": reason,
                "failure_detail": f"OpenAI returned {len(all_clauses)} raw clauses, {skipped_short} skipped as too short",
                "raw_clause_count": len(all_clauses),
                "skipped_short": skipped_short,
                "message": "No usable clauses extracted from document.",
            }

        _audit(conn, "INGEST_SUCCESS", {
            "doc_id": doc_id, "file_hash": file_hash,
            "filename": filename, "ingested_by": ingested_by,
            "clauses_queued": len(queued_ids),
            "is_update": existing is not None,
            "category": meta.get("category"),
            "raw_clause_count": len(all_clauses),
            "skipped_short": skipped_short,
            "partial_extraction_errors": len(extraction_errors),
        }, clause_ids=queued_ids)

        conn.commit()

        result = {
            "doc_id": doc_id,
            "title": meta["title"],
            "publisher": meta["publisher"],
            "file_hash": file_hash,
            "clause_count": len(queued_ids),
            "status": "UPDATED" if existing else "NEW",
            "message": f"{len(queued_ids)} clauses extracted and placed in human review queue.",
        }
        if extraction_errors:
            result["partial_extraction_errors"] = extraction_errors
            result["message"] += f" ({len(extraction_errors)} chunk(s) failed)"
        return result

    finally:
        conn.close()


def list_documents() -> list[dict]:
    """List all source documents ordered by ingestion date."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM source_documents ORDER BY ingested_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_documents_with_status() -> list[dict]:
    """
    List all source documents with their most recent ingestion outcome attached.
    Used by admin UI (screen1) to show zero-clause reasons per document.
    Adds fields: latest_ingestion_event, latest_ingestion_reason,
    latest_ingestion_detail, latest_clauses_queued.
    """
    docs = list_documents()
    if not docs:
        return docs
    conn = get_db()
    try:
        for doc in docs:
            row = conn.execute("""
                SELECT event_type, details
                FROM audit_log
                WHERE event_type IN ('INGEST_SUCCESS', 'INGEST_ZERO_CLAUSES', 'INGEST_UNCHANGED')
                  AND json_extract(details, '$.doc_id') = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (doc["doc_id"],)).fetchone()
            if not row:
                doc["latest_ingestion_event"] = None
                doc["latest_ingestion_reason"] = None
                doc["latest_ingestion_detail"] = None
                doc["latest_clauses_queued"] = None
                continue
            try:
                details = json.loads(row["details"])
            except (json.JSONDecodeError, TypeError):
                details = {}
            doc["latest_ingestion_event"] = row["event_type"]
            doc["latest_ingestion_reason"] = details.get("reason")
            doc["latest_ingestion_detail"] = details.get("detail")
            doc["latest_clauses_queued"] = details.get("clauses_queued", 0)
        return docs
    finally:
        conn.close()


def get_ingestion_status(doc_id: str) -> Optional[dict]:
    """
    Return the most recent ingestion outcome for a specific doc_id.
    Used by admin UI to display why an upload produced zero clauses.
    Returns None if no ingestion events were logged for the document.
    """
    conn = get_db()
    try:
        row = conn.execute("""
            SELECT event_type, details, created_at
            FROM audit_log
            WHERE event_type IN ('INGEST_SUCCESS', 'INGEST_ZERO_CLAUSES', 'INGEST_UNCHANGED')
              AND json_extract(details, '$.doc_id') = ?
            ORDER BY created_at DESC
            LIMIT 1
        """, (doc_id,)).fetchone()

        if not row:
            return None

        try:
            details = json.loads(row["details"])
        except (json.JSONDecodeError, TypeError):
            details = {}

        return {
            "doc_id": doc_id,
            "event_type": row["event_type"],
            "created_at": row["created_at"],
            "success": row["event_type"] == "INGEST_SUCCESS",
            "reason": details.get("reason"),
            "detail": details.get("detail"),
            "clauses_queued": details.get("clauses_queued", 0),
            "raw_clause_count": details.get("raw_clause_count"),
            "skipped_short": details.get("skipped_short"),
            "chunks_attempted": details.get("chunks_attempted"),
            "chunks_failed": details.get("chunks_failed"),
            "error_type": details.get("error_type"),
        }
    finally:
        conn.close()


def get_approved_sources() -> list[dict]:
    """Return the curated approved source list."""
    return [
        {"doc_id": k, **v}
        for k, v in APPROVED_SOURCES.items()
    ]


def get_pending_review() -> list[dict]:
    """Get all clauses waiting in human review queue."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT q.*, s.title as doc_title, s.publisher
            FROM review_queue q
            JOIN source_documents s ON q.source_doc_id = s.doc_id
            WHERE q.status = 'PENDING'
            ORDER BY q.submitted_at ASC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def detect_discount_in_text(text: str) -> Optional[float]:
    """
    Scan clause text for a percentage value.
    Returns the first % value found, or None.
    Examples: "הנחה בשיעור של עד 10%" → 10.0
              "50% מסכום הארנונה" → 50.0
    """
    matches = re.findall(r'(\d+(?:\.\d+)?)\s*%', text)
    if matches:
        val = float(matches[0])
        # Only return sensible discount values (1–100%)
        if 1.0 <= val <= 100.0:
            return val
    return None


def approve_clause(
    clause_id: str,
    reviewed_by: str,
    review_note: Optional[str] = None,
    override_type: Optional[str] = None,
    section_ref: Optional[str] = None,   # ✅ FIX: allow reviewer to fix weak section_ref
    suggested_discount_value: Optional[float] = None,   # ★ NEW: unified approval
    suggested_catalog_id: Optional[str] = None,         # ★ NEW: which right to update
) -> dict:
    """
    Human reviewer approves a clause → enters clause store.
    Architecture Brief §2.1 Human Gate — no clause enters store without this.

    ── BUG FIX (approve after unapprove) ────────────────────────────────────
    BEFORE: SELECT WHERE status='PENDING' → after unapprove+re-approve flow,
            HTTP 400 returned → frontend buttons appear broken.
    AFTER:  SELECT without status filter → only block REJECTED clauses.
            INSERT OR REPLACE handles re-approval cleanly.
    ─────────────────────────────────────────────────────────────────────────
    """
    conn = get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()

        # ── FIX: No status='PENDING' filter — allow re-approval after unapprove
        item = conn.execute(
            "SELECT * FROM review_queue WHERE clause_id=?",
            (clause_id,)
        ).fetchone()

        if not item:
            raise ValueError(f"Clause not found in review queue: {clause_id}")

        # Only block explicitly REJECTED clauses
        if item["status"] == "REJECTED":
            raise ValueError(
                f"Clause '{clause_id}' was rejected and cannot be re-approved."
            )

        final_type = override_type if override_type else item["clause_type"]
        valid_types = ("ELIGIBILITY", "EXCLUSION", "DEFINITION", "PROCEDURE")
        if final_type not in valid_types:
            raise ValueError(f"Invalid clause_type '{final_type}'. Must be one of: {valid_types}")

        # Supersede any existing version of this clause
        conn.execute("UPDATE clauses SET is_current=0 WHERE clause_id=?", (clause_id,))

        # ── FIX: INSERT OR REPLACE handles re-approval cleanly ───────────────
        # ✅ FIX: Use updated section_ref if reviewer provided one
        final_section_ref = section_ref.strip() if section_ref and section_ref.strip() else item["section_ref"]

        conn.execute("""
            INSERT OR REPLACE INTO clauses
                (clause_id, source_doc_id, section_ref, text, clause_type,
                 extraction_method, version, is_current, created_at)
            VALUES (?,?,?,?,?,'AI_REVIEWED','1.0',1,?)
        """, (clause_id, item["source_doc_id"], final_section_ref,
              item["text"], final_type, now))
        
        # Also update section_ref in review_queue for consistency
        if section_ref and section_ref.strip():
            conn.execute(
                "UPDATE review_queue SET section_ref=? WHERE clause_id=?",
                (final_section_ref, clause_id)
            )

        # Mark queue item approved
        conn.execute("""
            UPDATE review_queue
            SET status='APPROVED', reviewed_by=?, review_note=?, reviewed_at=?
            WHERE clause_id=?
        """, (reviewed_by, review_note, now, clause_id))

        # Auto-link to rights catalog
        _auto_link_clause_to_rights(conn, clause_id, item["source_doc_id"], final_type, now)

        # ★ NEW: Unified approval — update discount_value in rights catalog if confirmed
        discount_update_result = None
        if suggested_discount_value is not None and suggested_catalog_id:
            try:
                # Fetch existing right to preserve all other fields
                existing_right = conn.execute(
                    "SELECT * FROM rights WHERE catalog_id=?",
                    (suggested_catalog_id,)
                ).fetchone()
                if existing_right:
                    old_value = existing_right["discount_value"]
                    conn.execute(
                        "UPDATE rights SET discount_value=?, updated_at=? WHERE catalog_id=?",
                        (suggested_discount_value, now, suggested_catalog_id)
                    )
                    discount_update_result = {
                        "catalog_id": suggested_catalog_id,
                        "old_value": old_value,
                        "new_value": suggested_discount_value,
                        "updated": True,
                    }
                    _audit(conn, "DISCOUNT_VALUE_UPDATED", {
                        "reviewed_by": reviewed_by,
                        "catalog_id": suggested_catalog_id,
                        "old_value": old_value,
                        "new_value": suggested_discount_value,
                        "triggered_by_clause": clause_id,
                        "unified_approval": True,
                    }, clause_ids=[clause_id])
            except Exception as e:
                # Discount update failure must never block clause approval
                discount_update_result = {"error": str(e), "updated": False}

        _audit(conn, "CLAUSE_APPROVED",
               {"reviewed_by": reviewed_by, "clause_type": final_type,
                "review_note": review_note,
                "discount_updated": discount_update_result is not None},
               clause_ids=[clause_id])

        conn.commit()
        result = {"clause_id": clause_id, "status": "APPROVED", "clause_type": final_type}
        if discount_update_result:
            result["discount_update"] = discount_update_result
        return result

    finally:
        conn.close()


def reject_clause(clause_id: str, reviewed_by: str, reason: str) -> dict:
    """Human reviewer rejects a clause — it will not enter clause store."""
    conn = get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        result = conn.execute("""
            UPDATE review_queue
            SET status='REJECTED', reviewed_by=?, review_note=?, reviewed_at=?
            WHERE clause_id=? AND status='PENDING'
        """, (reviewed_by, reason, now, clause_id))

        if result.rowcount == 0:
            raise ValueError(f"No pending clause found: {clause_id}")

        _audit(conn, "CLAUSE_REJECTED",
               {"reviewed_by": reviewed_by, "reason": reason},
               clause_ids=[clause_id])

        conn.commit()
        return {"clause_id": clause_id, "status": "REJECTED"}
    finally:
        conn.close()


def unapprove_clause(clause_id: str, reviewed_by: str, reason: str) -> dict:
    """
    Un-approve a clause — moves back to PENDING in review queue.
    Client feedback: allows reviewer to undo an approval mistake.
    Removes from clause store + removes rights_clauses_map links.

    ── BUG FIX ──────────────────────────────────────────────────────────────
    BEFORE: Raised ValueError if clause not in clause store.
            Edge case: called twice → second call fails → UI stuck.
    AFTER:  If not in store → still reset review_queue to PENDING (no error).
            UI always restores correctly regardless of DB state.
    ─────────────────────────────────────────────────────────────────────────
    """
    conn = get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()

        # Try clause store — but don't fail if not found
        clause = conn.execute(
            "SELECT * FROM clauses WHERE clause_id=? AND is_current=1", (clause_id,)
        ).fetchone()

        if clause:
            # Remove from clause store
            conn.execute("DELETE FROM clauses WHERE clause_id=?", (clause_id,))
            # Remove any rights_clauses_map links
            conn.execute("DELETE FROM rights_clauses_map WHERE clause_id=?", (clause_id,))
        # else: not in store — still reset queue below (no error raised)

        # ── FIX: Always move to PENDING regardless of current state ──────────
        conn.execute("""
            UPDATE review_queue
            SET status='PENDING', reviewed_by=NULL, review_note=?, reviewed_at=NULL
            WHERE clause_id=?
        """, (f"Un-approved by {reviewed_by}: {reason}", clause_id))

        _audit(conn, "CLAUSE_UNAPPROVED",
               {"reviewed_by": reviewed_by, "reason": reason,
                "was_in_store": clause is not None},
               clause_ids=[clause_id])
        conn.commit()
        return {"clause_id": clause_id, "status": "PENDING", "message": "Moved back to review queue"}
    finally:
        conn.close()


def _auto_link_clause_to_rights(conn, clause_id: str, source_doc_id: str, clause_type: str, now: str):
    """
    Auto-link an approved clause to relevant rights based on the source doc domain.
    - RESERVE in doc_id → all Reserve* rights
    - LOWINCOME in doc_id → Low_Income + Senior rights
    - ARNONA / HORA-AT-SHA in doc_id → all Municipal_Tax rights
    - Otherwise → skip auto-link and log to audit (reviewer must link manually)
    """
    role_map = {"ELIGIBILITY": "CONDITIONS", "EXCLUSION": "EXCLUDES",
                "DEFINITION": "CONDITIONS", "PROCEDURE": "PROCEDURE"}
    mapping_role = role_map.get(clause_type, "CONDITIONS")

    src_upper = source_doc_id.upper()

    if "RESERVE" in src_upper:
        rights = conn.execute(
            "SELECT catalog_id FROM rights WHERE subcategory_tag LIKE 'Reserve%' AND status='ACTIVE'"
        ).fetchall()
    elif "LOWINCOME" in src_upper:
        rights = conn.execute(
            "SELECT catalog_id FROM rights WHERE (subcategory_tag LIKE '%Income%' OR subcategory_tag LIKE 'Senior%') AND status='ACTIVE'"
        ).fetchall()
    elif "ARNONA" in src_upper or "HORA-AT-SHA" in src_upper:
        rights = conn.execute(
            "SELECT catalog_id FROM rights WHERE category_tag='Municipal_Tax' AND status='ACTIVE'"
        ).fetchall()
    else:
        _audit(conn, "AUTO_LINK_SKIPPED_UNKNOWN_DOMAIN", {
            "clause_id": clause_id,
            "source_doc_id": source_doc_id,
            "reason": "Source doc does not match a known legal domain in the catalog; auto-link skipped. Reviewer must link manually if a matching right exists.",
        }, clause_ids=[clause_id])
        return

    for right in rights:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO rights_clauses_map
                    (catalog_id, clause_id, mapping_role, created_at)
                VALUES (?,?,?,?)
            """, (right["catalog_id"], clause_id, mapping_role, now))
        except Exception:
            pass


def get_clause_store(
    clause_id: Optional[str] = None,
    source_doc_id: Optional[str] = None,
    clause_type: Optional[str] = None,
    is_current: bool = True,
) -> list[dict]:
    """
    Query the clause store.
    Per §12.1: queryable by clause_id, source_doc_id, section_ref, clause_type.
    """
    conn = get_db()
    try:
        q = """
            SELECT c.*, s.title as doc_title, s.publisher, s.publication_date as doc_date
            FROM clauses c
            JOIN source_documents s ON c.source_doc_id = s.doc_id
            WHERE 1=1
        """
        params = []
        if clause_id:
            q += " AND c.clause_id=?"; params.append(clause_id)
        if source_doc_id:
            q += " AND c.source_doc_id=?"; params.append(source_doc_id)
        if clause_type:
            q += " AND c.clause_type=?"; params.append(clause_type)
        if is_current:
            q += " AND c.is_current=1"
        q += " ORDER BY c.created_at DESC"
        return [dict(r) for r in conn.execute(q, params).fetchall()]
    finally:
        conn.close()


def detect_contradictions() -> list[dict]:
    """
    Domain-agnostic contradiction / ambiguity detection over the active clause store.
    Two deterministic heuristics:
      1. CONTRADICTION_SAME_SECTION_DIFFERENT_TYPE — same (doc, section_ref) has both
         ELIGIBILITY and EXCLUSION clauses. Might be complementary but needs review.
      2. CONTRADICTION_DIFFERENT_DISCOUNT — same (doc, section_ref) has multiple
         ELIGIBILITY clauses that mention different discount percentages.
    Returns a list of contradiction records with clause_ids for admin UI.
    """
    conn = get_db()
    contradictions = []
    try:
        rows = conn.execute("""
            SELECT clause_id, source_doc_id, section_ref, clause_type, text
            FROM clauses WHERE is_current=1
            ORDER BY source_doc_id, section_ref
        """).fetchall()

        by_section: dict = {}
        for r in rows:
            key = (r["source_doc_id"], r["section_ref"])
            by_section.setdefault(key, []).append(dict(r))

        for (doc_id, sec_ref), clauses in by_section.items():
            if len(clauses) < 2:
                continue

            types_present = {c["clause_type"] for c in clauses}
            if "ELIGIBILITY" in types_present and "EXCLUSION" in types_present:
                elig_ids = [c["clause_id"] for c in clauses if c["clause_type"] == "ELIGIBILITY"]
                excl_ids = [c["clause_id"] for c in clauses if c["clause_type"] == "EXCLUSION"]
                contradictions.append({
                    "code": "CONTRADICTION_SAME_SECTION_DIFFERENT_TYPE",
                    "severity": "WARNING",
                    "source_doc_id": doc_id,
                    "section_ref": sec_ref,
                    "clause_ids": elig_ids + excl_ids,
                    "msg": (
                        f"Section '{sec_ref}' has both ELIGIBILITY ({len(elig_ids)}) "
                        f"and EXCLUSION ({len(excl_ids)}) clauses — verify these are "
                        f"complementary, not contradictory."
                    ),
                })

            elig_clauses = [c for c in clauses if c["clause_type"] == "ELIGIBILITY"]
            if len(elig_clauses) >= 2:
                discounts_seen: dict = {}
                for c in elig_clauses:
                    d = detect_discount_in_text(c["text"])
                    if d is not None:
                        discounts_seen.setdefault(d, []).append(c["clause_id"])
                if len(discounts_seen) >= 2:
                    all_ids = [cid for ids in discounts_seen.values() for cid in ids]
                    contradictions.append({
                        "code": "CONTRADICTION_DIFFERENT_DISCOUNT",
                        "severity": "WARNING",
                        "source_doc_id": doc_id,
                        "section_ref": sec_ref,
                        "clause_ids": all_ids,
                        "discounts_found": sorted(discounts_seen.keys()),
                        "msg": (
                            f"Section '{sec_ref}' contains multiple ELIGIBILITY clauses "
                            f"with different discount values: {sorted(discounts_seen.keys())} — "
                            f"potential contradiction, needs human review."
                        ),
                    })
        return contradictions
    finally:
        conn.close()


def validate_clause_integrity() -> dict:
    """
    Milestone 1: Clause integrity validation + basic traceability.
    Architecture Brief §12.1-§12.4.
    Checks: orphaned clauses, missing fields, invalid types, broken FK links,
            and cross-clause contradictions/ambiguities.
    """
    conn = get_db()
    errors = []
    warnings = []

    try:
        clauses = conn.execute("SELECT * FROM clauses WHERE is_current=1").fetchall()
        active_docs = {
            r["doc_id"] for r in
            conn.execute("SELECT doc_id FROM source_documents WHERE status='ACTIVE'").fetchall()
        }
        map_rows = conn.execute("SELECT * FROM rights_clauses_map").fetchall()
        active_clause_ids = {c["clause_id"] for c in clauses}
        active_rights = {
            r["catalog_id"] for r in
            conn.execute("SELECT catalog_id FROM rights WHERE status='ACTIVE'").fetchall()
        }

        for c in clauses:
            # Check 1: traceability — clause links to active source doc
            if c["source_doc_id"] not in active_docs:
                errors.append({
                    "code": "ORPHANED_CLAUSE",
                    "severity": "CRITICAL",
                    "clause_id": c["clause_id"],
                    "msg": f"Source doc not found or superseded: {c['source_doc_id']}"
                })

            # Check 2: required fields
            for field in ("clause_id", "source_doc_id", "section_ref", "text", "clause_type"):
                if not c[field]:
                    errors.append({
                        "code": "MISSING_FIELD",
                        "severity": "CRITICAL",
                        "clause_id": c["clause_id"],
                        "msg": f"Missing required field: {field}"
                    })

            # Check 3: text length
            if len(c["text"].strip()) < 50:
                warnings.append({
                    "code": "SHORT_TEXT",
                    "severity": "WARNING",
                    "clause_id": c["clause_id"],
                    "msg": f"Clause text is suspiciously short ({len(c['text'])} chars) — may be incomplete"
                })

            # Check 4: weak section ref — client feedback: must have real ref
            if c["section_ref"] in ("לא צוין", "") or len(c["section_ref"]) < 3:
                warnings.append({
                    "code": "WEAK_SECTION_REF",
                    "severity": "WARNING",
                    "clause_id": c["clause_id"],
                    "msg": f"Section ref not specific: '{c['section_ref']}' — needs human review"
                })

        # Check 5: rights_clauses_map referential integrity
        for m in map_rows:
            if m["clause_id"] not in active_clause_ids:
                errors.append({
                    "code": "MAP_DEAD_CLAUSE",
                    "severity": "CRITICAL",
                    "clause_id": m["clause_id"],
                    "msg": "rights_clauses_map references non-current clause"
                })
            if m["catalog_id"] not in active_rights:
                warnings.append({
                    "code": "MAP_INACTIVE_RIGHT",
                    "severity": "WARNING",
                    "clause_id": m["clause_id"],
                    "msg": f"rights_clauses_map references non-active right: {m['catalog_id']}"
                })

        # Check 6: every active right should have at least 1 linked clause
        for right_id in active_rights:
            linked = conn.execute(
                "SELECT COUNT(*) as c FROM rights_clauses_map WHERE catalog_id=?",
                (right_id,)
            ).fetchone()["c"]
            if linked == 0:
                warnings.append({
                    "code": "RIGHT_NO_CLAUSES",
                    "severity": "WARNING",
                    "clause_id": None,
                    "msg": f"Right '{right_id}' has no linked clauses yet"
                })

        # Check 7: contradictions / ambiguities (M1 acceptance criterion)
        contradictions = detect_contradictions()
        for c in contradictions:
            warnings.append({
                "code": c["code"],
                "severity": c["severity"],
                "clause_id": (c.get("clause_ids") or [None])[0],
                "clause_ids": c.get("clause_ids", []),
                "source_doc_id": c.get("source_doc_id"),
                "section_ref": c.get("section_ref"),
                "msg": c["msg"],
            })

        # Traceability summary
        traceable = sum(1 for c in clauses if c["source_doc_id"] in active_docs)

        # Count rights and mappings
        total_rights = conn.execute("SELECT COUNT(*) as c FROM rights WHERE status='ACTIVE'").fetchone()["c"]
        total_mappings = conn.execute("SELECT COUNT(*) as c FROM rights_clauses_map").fetchone()["c"]

        report = {
            "passed": len(errors) == 0,
            "total_clauses": len(clauses),
            "total_source_docs": len(active_docs),
            "total_rights": total_rights,
            "total_mappings": total_mappings,
            "critical_errors": len(errors),
            "warnings": len(warnings),
            "errors": errors,
            "warnings_list": warnings,
            "contradictions": contradictions,
            "contradictions_count": len(contradictions),
            "traceability": {
                "traceable_clauses": traceable,
                "total_clauses": len(clauses),
                "percent": round((traceable / len(clauses) * 100) if clauses else 0, 1),
                "all_clauses_traceable": len(errors) == 0,
            },
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }

        _audit(conn, "INTEGRITY_VALIDATION", {
            "passed": report["passed"],
            "critical_errors": len(errors),
            "warnings": len(warnings),
            "contradictions": len(contradictions),
        })
        conn.commit()
        return report

    finally:
        conn.close()


def get_traceability_chain(clause_id: str) -> dict:
    """
    Return full traceability chain for a clause.
    Milestone 1 requirement: clause_id → source_doc → section_ref → file_hash
    """
    conn = get_db()
    try:
        clause = conn.execute(
            "SELECT * FROM clauses WHERE clause_id=?", (clause_id,)
        ).fetchone()

        if not clause:
            return {"error": f"Clause not found: {clause_id}"}

        doc = conn.execute(
            "SELECT * FROM source_documents WHERE doc_id=?",
            (clause["source_doc_id"],)
        ).fetchone()

        rights_using = conn.execute("""
            SELECT r.catalog_id, r.name, m.mapping_role
            FROM rights_clauses_map m
            JOIN rights r ON m.catalog_id = r.catalog_id
            WHERE m.clause_id=?
        """, (clause_id,)).fetchall()

        text = clause["text"]
        return {
            "clause_id": clause["clause_id"],
            "section_ref": clause["section_ref"],
            "clause_type": clause["clause_type"],
            "extraction_method": clause["extraction_method"],
            "text_snippet": text[:300] + "..." if len(text) > 300 else text,
            "is_current": bool(clause["is_current"]),
            "source_document": {
                "doc_id": doc["doc_id"] if doc else None,
                "title": doc["title"] if doc else "NOT FOUND",
                "publisher": doc["publisher"] if doc else None,
                "publication_date": doc["publication_date"] if doc else None,
                "file_hash": doc["file_hash"] if doc else None,
                "status": doc["status"] if doc else "NOT_FOUND",
            },
            "used_by_rights": [
                {"catalog_id": r["catalog_id"], "name": r["name"],
                 "role": r["mapping_role"]}
                for r in rights_using
            ],
            "is_traceable": doc is not None and doc["status"] == "ACTIVE",
        }
    finally:
        conn.close()
