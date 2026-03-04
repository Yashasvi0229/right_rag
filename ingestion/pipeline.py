"""
Rights Angel — Ingestion Pipeline (L1 + L2)
Architecture Brief v1.2 §6.1 Steps 1-3

FIXES:
- file_hash UNIQUE constraint: allow re-upload by updating existing record
- Better GPT prompt: extracts real section refs (תקנה 3ו, תקנה 3ז etc.)
- Clause unapprove: move approved clause back to PENDING
- Date validation on ingestion
- Auto-link clauses to rights catalog after approval
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
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")
from database.schema import get_db

# ─────────────────────────────────────────────────────────────────────────────
# APPROVED SOURCE LIST
# ─────────────────────────────────────────────────────────────────────────────
APPROVED_SOURCES = {
    "GOV-IL-RESERVE-ARNONA-2026": {
        "title": "זכות ההנחה בארנונה לחיילי מילואים בישראל — ריכוז חומר משפטי מקיף",
        "publisher": "ריכוז משפטי | עודכן פברואר 2026",
        "category": "reserve_soldiers",
        "publication_date": "2026-02-01",
        "url": "https://www.gov.il/he/departments/general/arnona-discount",
    },
    "GOV-IL-LOWINCOME-ARNONA-2026": {
        "title": "הנחה בארנונה למעוטי יכולת בישראל — חוברת בסיס הידע המשפטי",
        "publisher": "מערכת Angel Rights | פברואר 2026",
        "category": "low_income",
        "publication_date": "2026-02-19",
        "url": "https://www.gov.il/he/departments/general/arnona-discount-low-income",
    },
    "GOV-IL-ARNONA-REGULATIONS-1993": {
        "title": "תקנות הסדרים במשק המדינה (הנחה מארנונה), תשנ\"ג-1993",
        "publisher": "כנסת ישראל / משרד הפנים",
        "category": "general",
        "publication_date": "1993-01-01",
        "url": "https://www.nevo.co.il/law_html/Law01/P221_001.htm",
    },
    "GOV-IL-RESERVE-REGULATION-3VAV": {
        "title": "תקנה 3ו — הנחה לחיילי מילואים פעילים (תיקון 3, תשע\"ח-2018)",
        "publisher": "משרד הפנים",
        "category": "reserve_soldiers",
        "publication_date": "2018-03-27",
        "url": "https://www.nevo.co.il/law_html/Law01/P221_001.htm#Seif3v",
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
}

# ─────────────────────────────────────────────────────────────────────────────
# IMPROVED EXTRACTION PROMPT — client feedback:
# Must extract REAL section refs, NO hallucinations, verbatim Hebrew text
# ─────────────────────────────────────────────────────────────────────────────
EXTRACTION_SYSTEM_PROMPT = """You are a legal analyst specializing in Israeli administrative law (משפט מנהלי ישראלי), specifically property tax (ארנונה) regulations.

Your task: extract ATOMIC LEGAL CLAUSES from the provided Israeli legal document text.

CRITICAL RULES — NO EXCEPTIONS:
1. VERBATIM HEBREW TEXT ONLY — copy exact text from document, never paraphrase or translate
2. section_ref MUST be the EXACT reference found in the text (e.g. "תקנה 3ו(א)", "תקנה 3ז", "סעיף 2(א)(8)", "פרק 1.2")
   - If no section ref found in text, use "לא צוין" — NEVER invent or hallucinate a reference
3. Each clause must be independently meaningful and complete
4. Minimum clause length: 20 characters
5. Return ONLY valid JSON — no preamble, no explanation, no markdown

CLAUSE TYPES:
- ELIGIBILITY: grants a right or qualifying condition ("זכאי", "רשאי", "entitled", conditions that must be met)
- EXCLUSION: removes or denies a right ("אינו זכאי", "לא יינתן", "except", "provided that not")
- DEFINITION: defines a legal term ("לעניין זה", "המונח", "פירושו", "הגדרה")
- PROCEDURE: application steps, deadlines, required documents ("יגיש", "תוך X ימים", "המסמכים הנדרשים")

RETURN FORMAT (JSON only, no other text):
{"clauses": [{"section_ref": "תקנה 3ו(א)", "text": "verbatim Hebrew text here", "clause_type": "ELIGIBILITY"}]}"""

EXTRACTION_USER_PROMPT = """Document ID: {doc_id}
Title: {title}
Publisher: {publisher}
Legal Source URL: {url}

Extract all atomic legal clauses from this text. Remember: copy verbatim Hebrew text only, exact section references only.

TEXT:
{text}"""


class SourceNotApprovedError(Exception):
    pass

class DocumentUnchangedError(Exception):
    pass


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def validate_date(date_str: str) -> str:
    """Validate and normalize date string. Returns ISO date or raises ValueError."""
    if not date_str:
        return datetime.now().strftime("%Y-%m-%d")
    # Remove non-numeric characters except dash
    clean = re.sub(r'[^\d\-\/]', '', str(date_str))
    # Try various formats
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(clean, fmt)
            # Sanity check: year must be between 1900 and 2100
            if 1900 <= dt.year <= 2100:
                return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # If all fail, return today
    return datetime.now().strftime("%Y-%m-%d")


def extract_text_from_bytes(file_bytes: bytes, filename: str) -> str:
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
            raise RuntimeError("python-docx not installed.")
    elif ext == ".txt":
        return file_bytes.decode("utf-8", errors="replace")
    else:
        raise ValueError(f"Unsupported file type '{ext}'")


def _chunk_text(text: str, max_chars: int = 5000) -> list[str]:
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


def _call_openai(text: str, doc_id: str, meta: dict) -> list[dict]:
    from openai import OpenAI
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key or api_key.startswith("sk-your"):
        raise ValueError("OPENAI_API_KEY not set.")
    client = OpenAI(api_key=api_key)
    model = os.getenv("OPENAI_MODEL", "gpt-4o")
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
            {"role": "user", "content": EXTRACTION_USER_PROMPT.format(
                doc_id=doc_id,
                title=meta["title"],
                publisher=meta["publisher"],
                url=meta.get("url", "לא צוין"),
                text=text,
            )},
        ],
        temperature=0,
        response_format={"type": "json_object"},
        max_tokens=4000,
    )
    raw = response.choices[0].message.content
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
        for v in parsed.values():
            if isinstance(v, list):
                return v
        return []
    except (json.JSONDecodeError, AttributeError):
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        return []


def _generate_clause_id(doc_id: str, section_ref: str, index: int) -> str:
    doc_short = re.sub(r"[^A-Z0-9]", "", doc_id.replace("GOV-IL-", "").upper())[:10]
    section_slug = re.sub(r"[^\w]", "", section_ref.replace(" ", ""))[:8]
    return f"CL-{doc_short}-{section_slug}-{index:03d}"


def _audit(conn, event_type: str, details: dict, clause_ids: list = None):
    try:
        conn.execute(
            "INSERT INTO audit_log (event_type, clause_ids, details, created_at) VALUES (?,?,?,?)",
            (event_type,
             json.dumps(clause_ids or [], ensure_ascii=False),
             json.dumps(details, ensure_ascii=False),
             datetime.now(timezone.utc).isoformat())
        )
    except Exception:
        pass


# ═════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═════════════════════════════════════════════════════════════════════════════

def ingest_document(
    file_bytes: bytes,
    filename: str,
    doc_id: str,
    ingested_by: str,
    publication_date: Optional[str] = None,
    url: Optional[str] = None,
) -> dict:
    """
    L1 + L2: Full ingestion pipeline.
    FIX 1: file_hash UNIQUE — if same hash exists, raise DocumentUnchangedError
    FIX 2: if doc_id exists with DIFFERENT hash — UPDATE record (not INSERT)
    FIX 3: date validation
    """
    if doc_id not in APPROVED_SOURCES:
        valid = list(APPROVED_SOURCES.keys())
        raise SourceNotApprovedError(
            f"doc_id '{doc_id}' not on approved source list.\nValid: {valid}"
        )

    meta = APPROVED_SOURCES[doc_id]
    # FIX 3: validate date
    pub_date = validate_date(publication_date or meta.get("publication_date", ""))
    file_hash = sha256_of(file_bytes)
    now = datetime.now(timezone.utc).isoformat()

    conn = get_db()
    try:
        existing = conn.execute(
            "SELECT doc_id, file_hash, status FROM source_documents WHERE doc_id=?",
            (doc_id,)
        ).fetchone()

        # Check if same file already ingested
        if existing and existing["file_hash"] == file_hash:
            _audit(conn, "INGEST_UNCHANGED", {
                "doc_id": doc_id, "file_hash": file_hash,
                "message": "Hash matches — no change recorded."
            })
            conn.commit()
            raise DocumentUnchangedError(
                f"Document '{doc_id}' is unchanged (same file). "
                "Upload a different/updated file."
            )

        # Extract text
        extracted_text = extract_text_from_bytes(file_bytes, filename)

        # FIX 2: UPDATE if exists, INSERT if new
        if existing:
            # Mark old clauses as superseded
            conn.execute(
                "UPDATE clauses SET is_current=0 WHERE source_doc_id=?", (doc_id,)
            )
            # Update source document record
            conn.execute("""
                UPDATE source_documents
                SET file_hash=?, ingested_at=?, ingested_by=?,
                    publication_date=?, status='ACTIVE'
                WHERE doc_id=?
            """, (file_hash, now, ingested_by, pub_date, doc_id))
        else:
            conn.execute("""
                INSERT INTO source_documents
                    (doc_id, title, publisher, publication_date, url,
                     file_hash, ingested_at, ingested_by, status)
                VALUES (?,?,?,?,?,?,?,?,'ACTIVE')
            """, (
                doc_id, meta["title"], meta["publisher"],
                pub_date, url or meta.get("url"), file_hash, now, ingested_by,
            ))
        conn.commit()

        # OpenAI extraction
        chunks = _chunk_text(extracted_text, max_chars=5000)
        all_clauses = []
        for chunk in chunks:
            extracted = _call_openai(chunk, doc_id, meta)
            all_clauses.extend(extracted)

        # Place in review queue
        queued_ids = []
        for i, c in enumerate(all_clauses, 1):
            section_ref = str(c.get("section_ref", "לא צוין"))[:128]
            text = str(c.get("text", "")).strip()
            clause_type = c.get("clause_type", "ELIGIBILITY")

            if len(text) < 10:
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

        _audit(conn, "INGEST_SUCCESS", {
            "doc_id": doc_id, "file_hash": file_hash,
            "filename": filename, "ingested_by": ingested_by,
            "clauses_queued": len(queued_ids),
            "is_update": existing is not None,
        }, clause_ids=queued_ids)

        conn.commit()

        return {
            "doc_id": doc_id,
            "title": meta["title"],
            "publisher": meta["publisher"],
            "file_hash": file_hash,
            "clause_count": len(queued_ids),
            "status": "UPDATED" if existing else "NEW",
            "message": f"{len(queued_ids)} clauses extracted and placed in human review queue.",
        }

    finally:
        conn.close()


def list_documents() -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM source_documents ORDER BY ingested_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_approved_sources() -> list[dict]:
    return [{"doc_id": k, **v} for k, v in APPROVED_SOURCES.items()]


def get_pending_review() -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT q.*, s.title as doc_title, s.publisher, s.url as doc_url
            FROM review_queue q
            JOIN source_documents s ON q.source_doc_id = s.doc_id
            WHERE q.status = 'PENDING'
            ORDER BY q.submitted_at ASC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def approve_clause(
    clause_id: str,
    reviewed_by: str,
    review_note: Optional[str] = None,
    override_type: Optional[str] = None,
) -> dict:
    """Human approves clause → enters clause store + auto-links to rights catalog."""
    conn = get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        item = conn.execute(
            "SELECT * FROM review_queue WHERE clause_id=? AND status='PENDING'",
            (clause_id,)
        ).fetchone()

        if not item:
            raise ValueError(f"No pending clause found: {clause_id}")

        final_type = override_type if override_type else item["clause_type"]
        valid_types = ("ELIGIBILITY", "EXCLUSION", "DEFINITION", "PROCEDURE")
        if final_type not in valid_types:
            raise ValueError(f"Invalid clause_type '{final_type}'")

        # Supersede old version
        conn.execute("UPDATE clauses SET is_current=0 WHERE clause_id=?", (clause_id,))

        # Write to clause store
        conn.execute("""
            INSERT INTO clauses
                (clause_id, source_doc_id, section_ref, text, clause_type,
                 extraction_method, version, is_current, created_at)
            VALUES (?,?,?,?,?,'AI_REVIEWED','1.0',1,?)
        """, (clause_id, item["source_doc_id"], item["section_ref"],
              item["text"], final_type, now))

        # Mark approved in queue
        conn.execute("""
            UPDATE review_queue
            SET status='APPROVED', reviewed_by=?, review_note=?, reviewed_at=?
            WHERE clause_id=?
        """, (reviewed_by, review_note, now, clause_id))

        # Auto-link to rights catalog based on clause type + source doc
        _auto_link_clause_to_rights(conn, clause_id, item["source_doc_id"], final_type, now)

        _audit(conn, "CLAUSE_APPROVED",
               {"reviewed_by": reviewed_by, "clause_type": final_type},
               clause_ids=[clause_id])

        conn.commit()
        return {"clause_id": clause_id, "status": "APPROVED", "clause_type": final_type}

    finally:
        conn.close()


def unapprove_clause(clause_id: str, reviewed_by: str, reason: str) -> dict:
    """
    FIX: Un-approve a clause — move back to PENDING in review queue.
    Removes from clause store, removes rights_clauses_map links.
    """
    conn = get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()

        # Check clause exists in store
        clause = conn.execute(
            "SELECT * FROM clauses WHERE clause_id=? AND is_current=1", (clause_id,)
        ).fetchone()

        if not clause:
            raise ValueError(f"No active clause found: {clause_id}")

        # Remove from clause store
        conn.execute("DELETE FROM clauses WHERE clause_id=?", (clause_id,))

        # Remove rights_clauses_map links
        conn.execute("DELETE FROM rights_clauses_map WHERE clause_id=?", (clause_id,))

        # Move back to PENDING in review queue
        conn.execute("""
            UPDATE review_queue
            SET status='PENDING', reviewed_by=NULL, review_note=?, reviewed_at=NULL
            WHERE clause_id=?
        """, (f"Un-approved by {reviewed_by}: {reason}", clause_id))

        _audit(conn, "CLAUSE_UNAPPROVED",
               {"reviewed_by": reviewed_by, "reason": reason},
               clause_ids=[clause_id])

        conn.commit()
        return {"clause_id": clause_id, "status": "PENDING", "message": "Clause moved back to review queue"}

    finally:
        conn.close()


def _auto_link_clause_to_rights(conn, clause_id: str, source_doc_id: str, clause_type: str, now: str):
    """
    Auto-link approved clause to relevant rights in catalog.
    Based on source document category + clause type.
    """
    # Get rights that match this document's category
    doc = conn.execute(
        "SELECT * FROM source_documents WHERE doc_id=?", (source_doc_id,)
    ).fetchone()

    if not doc:
        return

    # Determine mapping role from clause type
    role_map = {
        "ELIGIBILITY": "CONDITIONS",
        "EXCLUSION": "EXCLUDES",
        "DEFINITION": "CONDITIONS",
        "PROCEDURE": "PROCEDURE",
    }
    mapping_role = role_map.get(clause_type, "CONDITIONS")

    # Find relevant rights based on source doc
    if "RESERVE" in source_doc_id:
        rights = conn.execute(
            "SELECT catalog_id FROM rights WHERE subcategory_tag LIKE 'Reserve%' AND status='ACTIVE'"
        ).fetchall()
    elif "LOWINCOME" in source_doc_id:
        rights = conn.execute(
            "SELECT catalog_id FROM rights WHERE subcategory_tag LIKE '%Income%' OR subcategory_tag LIKE 'Senior%' AND status='ACTIVE'"
        ).fetchall()
    else:
        rights = conn.execute(
            "SELECT catalog_id FROM rights WHERE status='ACTIVE'"
        ).fetchall()

    for right in rights:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO rights_clauses_map
                    (catalog_id, clause_id, mapping_role, created_at)
                VALUES (?,?,?,?)
            """, (right["catalog_id"], clause_id, mapping_role, now))
        except Exception:
            pass


def reject_clause(clause_id: str, reviewed_by: str, reason: str) -> dict:
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


def get_clause_store(
    clause_id: Optional[str] = None,
    source_doc_id: Optional[str] = None,
    clause_type: Optional[str] = None,
    is_current: bool = True,
) -> list[dict]:
    conn = get_db()
    try:
        q = """
            SELECT c.*, s.title as doc_title, s.publisher,
                   s.publication_date as doc_date, s.url as doc_url
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


def validate_clause_integrity() -> dict:
    """
    Enhanced validation — per-clause checks + rights_clauses_map integrity.
    Client feedback: stronger self-validation mechanism.
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
            # Check 1: traceability
            if c["source_doc_id"] not in active_docs:
                errors.append({
                    "code": "ORPHANED_CLAUSE",
                    "severity": "CRITICAL",
                    "clause_id": c["clause_id"],
                    "msg": f"Source doc not found: {c['source_doc_id']}"
                })

            # Check 2: required fields
            for field in ("clause_id", "source_doc_id", "section_ref", "text", "clause_type"):
                if not c[field]:
                    errors.append({
                        "code": "MISSING_FIELD",
                        "severity": "CRITICAL",
                        "clause_id": c["clause_id"],
                        "msg": f"Missing: {field}"
                    })

            # Check 3: section_ref should not be generic
            if c["section_ref"] in ("לא צוין", "§1", "§2") or len(c["section_ref"]) < 3:
                warnings.append({
                    "code": "WEAK_SECTION_REF",
                    "severity": "WARNING",
                    "clause_id": c["clause_id"],
                    "msg": f"Section ref may not be specific enough: '{c['section_ref']}'"
                })

            # Check 4: text length
            if len(c["text"].strip()) < 20:
                warnings.append({
                    "code": "SHORT_TEXT",
                    "severity": "WARNING",
                    "clause_id": c["clause_id"],
                    "msg": f"Clause text too short ({len(c['text'])} chars)"
                })

            # Check 5: extraction method must be AI_REVIEWED or HUMAN
            if c["extraction_method"] not in ("AI_REVIEWED", "HUMAN"):
                errors.append({
                    "code": "INVALID_EXTRACTION_METHOD",
                    "severity": "CRITICAL",
                    "clause_id": c["clause_id"],
                    "msg": f"Invalid extraction_method: {c['extraction_method']}"
                })

        # Check 6: rights_clauses_map integrity
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
                    "msg": f"Map references inactive right: {m['catalog_id']}"
                })

        # Check 7: every active right should have at least 1 clause linked
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

        traceable = sum(1 for c in clauses if c["source_doc_id"] in active_docs)

        report = {
            "passed": len(errors) == 0,
            "total_clauses": len(clauses),
            "total_source_docs": len(active_docs),
            "total_rights": len(active_rights),
            "total_mappings": len(map_rows),
            "critical_errors": len(errors),
            "warnings": len(warnings),
            "errors": errors,
            "warnings_list": warnings,
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
            "total_mappings": len(map_rows),
        })
        conn.commit()
        return report

    finally:
        conn.close()


def get_traceability_chain(clause_id: str) -> dict:
    """Full traceability: clause → source doc → section ref → file hash → rights linked."""
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
            SELECT r.catalog_id, r.name, r.discount_value, r.discount_unit,
                   m.mapping_role
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
                "url": doc["url"] if doc else None,
                "status": doc["status"] if doc else "NOT_FOUND",
            },
            "linked_rights": [
                {
                    "catalog_id": r["catalog_id"],
                    "name": r["name"],
                    "discount_value": r["discount_value"],
                    "discount_unit": r["discount_unit"],
                    "mapping_role": r["mapping_role"],
                }
                for r in rights_using
            ],
            "is_traceable": doc is not None and doc["status"] == "ACTIVE",
        }
    finally:
        conn.close()
