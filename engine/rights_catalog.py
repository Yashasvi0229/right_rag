"""
Rights Angel — Rights Catalog
Seeded with REAL data from client's two PDFs:
1. Reserve soldiers discount (תקנה 3ו) 
2. Low income discount (תקנה 2(א)(8))

All discount_value and friction_score are human-defined per §5.3.
friction_score = procedural difficulty 1-10 (1=easy, 10=very hard)
"""
import json
from datetime import datetime, timezone
from typing import Optional
from database.schema import get_db


# ─────────────────────────────────────────────────────────────────────────────
# Real rights data extracted from client PDFs
# ─────────────────────────────────────────────────────────────────────────────
SEED_RIGHTS = [

    # ═══ RESERVE SOLDIERS — PDF 1 ════════════════════════════════════════════

    {
        "catalog_id": "ARNONA-RESERVE-STANDARD-001",
        "name": "הנחה בארנונה לחייל מילואים פעיל — 5%",
        "category_tag": "Municipal_Tax",
        "subcategory_tag": "Reserve_Soldiers",
        "discount_value": 5.0,
        "discount_unit": "PERCENT",
        "friction_score": 5,
        # Friction: needs active certificate + form + local authority approval
        # Not automatic in most authorities → medium friction
        "effective_from": "2018-03-27",
        "effective_to": None,
        "status": "ACTIVE",
        "source": "תקנה 3ו(א), תיקון מס' 3, תשע\"ח-2018",
    },
    {
        "catalog_id": "ARNONA-RESERVE-COMMANDER-002",
        "name": "הנחה בארנונה למפקד מילואים פעיל — 25% עד 100 מ\"ר",
        "category_tag": "Municipal_Tax",
        "subcategory_tag": "Reserve_Commander",
        "discount_value": 25.0,
        "discount_unit": "PERCENT",
        "friction_score": 7,
        # Friction: needs commander certificate + command role proof + form
        # Only some municipalities adopted this amendment
        "effective_from": "2022-11-02",
        "effective_to": None,
        "status": "ACTIVE",
        "source": "תקנות ההסדרים תשפ\"ג-2022, מפקד מילואים פעיל",
    },
    {
        "catalog_id": "ARNONA-RESERVE-FAMILY5-003",
        "name": "הנחה 100% עד 90 מ\"ר — חייל מילואים עם משפחה 5+ נפשות",
        "category_tag": "Municipal_Tax",
        "subcategory_tag": "Reserve_Large_Family",
        "discount_value": 100.0,
        "discount_unit": "PERCENT",
        "friction_score": 6,
        # Friction: needs IDF certificate + family status proof (5+ people)
        "effective_from": "2018-03-27",
        "effective_to": None,
        "status": "ACTIVE",
        "source": "עיריית נתניה — חייל עם 5 נפשות ומעלה",
    },

    # ═══ LOW INCOME — PDF 2 ══════════════════════════════════════════════════

    {
        "catalog_id": "ARNONA-LOWINCOME-SENIOR100-004",
        "name": "הנחה 100% לאזרח ותיק מקבל השלמת הכנסה",
        "category_tag": "Municipal_Tax",
        "subcategory_tag": "Senior_Income_Support",
        "discount_value": 100.0,
        "discount_unit": "PERCENT",
        "friction_score": 4,
        # Friction: needs Bituach Leumi certificate + automatic renewal over 70
        "effective_from": "2026-01-01",
        "effective_to": None,
        "status": "ACTIVE",
        "source": "תקנות הנחה מארנונה 2026 — אזרח ותיק, השלמת הכנסה",
    },
    {
        "catalog_id": "ARNONA-LOWINCOME-SENIOR25-005",
        "name": "הנחה 25% לאזרח ותיק מקבל קצבת זיקנה / שאירים / נכות עבודה",
        "category_tag": "Municipal_Tax",
        "subcategory_tag": "Senior_Pension",
        "discount_value": 25.0,
        "discount_unit": "PERCENT",
        "friction_score": 3,
        # Friction: low — pension auto-verified, standard form
        "effective_from": "2026-01-01",
        "effective_to": None,
        "status": "ACTIVE",
        "source": "תקנות הנחה מארנונה 2026 — קצבת זיקנה/שאירים",
    },
    {
        "catalog_id": "ARNONA-LOWINCOME-INCOME70-006",
        "name": "הנחה עד 70% לפי מבחן הכנסה — הכנסה נמוכה",
        "category_tag": "Municipal_Tax",
        "subcategory_tag": "Low_Income",
        "discount_value": 70.0,
        "discount_unit": "PERCENT",
        "friction_score": 8,
        # Friction: high — income declaration + proofs + discretion of authority
        "effective_from": "2026-01-01",
        "effective_to": None,
        "status": "ACTIVE",
        "source": "תקנה 2(א)(8) — הנחה לפי מבחן הכנסה",
    },
    {
        "catalog_id": "ARNONA-LOWINCOME-WOMEN6267-007",
        "name": "הנחה 30% לנשים גיל 62-67 בשנות 2022-2025",
        "category_tag": "Municipal_Tax",
        "subcategory_tag": "Women_Retirement_Age",
        "discount_value": 30.0,
        "discount_unit": "PERCENT",
        "friction_score": 5,
        "effective_from": "2022-01-01",
        "effective_to": "2025-12-31",
        "status": "ACTIVE",
        "source": "תקנות הנחה — גיל פרישה נשים 62-67, הוראת שעה",
    },
    {
        "catalog_id": "ARNONA-LOWINCOME-SPECIAL70-008",
        "name": "הנחה נזקקות מיוחדת עד 70% — הוצאות רפואיות חריגות",
        "category_tag": "Municipal_Tax",
        "subcategory_tag": "Special_Hardship",
        "discount_value": 70.0,
        "discount_unit": "PERCENT",
        "friction_score": 9,
        # Friction: very high — ועדת חריגים, medical expense receipts required
        "effective_from": "2026-01-01",
        "effective_to": None,
        "status": "ACTIVE",
        "source": "ועדת חריגים — נזקקות מיוחדת עם קבלות הוצאות",
    },
]


def seed_rights_catalog():
    """
    Populate rights table with seed data from client PDFs.
    Idempotent — uses INSERT OR IGNORE.
    """
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()

    try:
        for r in SEED_RIGHTS:
            conn.execute("""
                INSERT OR IGNORE INTO rights
                    (catalog_id, name, category_tag, subcategory_tag,
                     discount_value, discount_unit, friction_score,
                     effective_from, effective_to, status, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                r["catalog_id"], r["name"], r["category_tag"],
                r.get("subcategory_tag"), r["discount_value"],
                r["discount_unit"], r["friction_score"],
                r["effective_from"], r.get("effective_to"),
                r["status"], now, now,
            ))

        conn.commit()
        return {"seeded": len(SEED_RIGHTS), "status": "OK"}

    finally:
        conn.close()


def get_all_rights(status: str = "ACTIVE") -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM rights WHERE status=? ORDER BY category_tag, discount_value DESC",
            (status,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_right(catalog_id: str) -> Optional[dict]:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM rights WHERE catalog_id=?", (catalog_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def upsert_right(data: dict) -> dict:
    """Create or update a rights record."""
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    try:
        existing = conn.execute(
            "SELECT catalog_id FROM rights WHERE catalog_id=?",
            (data["catalog_id"],)
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE rights SET
                    name=?, category_tag=?, subcategory_tag=?,
                    discount_value=?, discount_unit=?, friction_score=?,
                    effective_from=?, effective_to=?, status=?, updated_at=?
                WHERE catalog_id=?
            """, (
                data["name"], data["category_tag"], data.get("subcategory_tag"),
                data["discount_value"], data["discount_unit"], data["friction_score"],
                data["effective_from"], data.get("effective_to"),
                data.get("status", "DRAFT"), now, data["catalog_id"],
            ))
        else:
            conn.execute("""
                INSERT INTO rights
                    (catalog_id, name, category_tag, subcategory_tag,
                     discount_value, discount_unit, friction_score,
                     effective_from, effective_to, status, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                data["catalog_id"], data["name"], data["category_tag"],
                data.get("subcategory_tag"), data["discount_value"],
                data["discount_unit"], data["friction_score"],
                data["effective_from"], data.get("effective_to"),
                data.get("status", "DRAFT"), now, now,
            ))

        conn.commit()
        return {"catalog_id": data["catalog_id"], "action": "UPDATED" if existing else "CREATED"}
    finally:
        conn.close()


def get_rights_with_clauses(catalog_id: str) -> dict:
    """Return a right + all linked clauses with their source documents."""
    conn = get_db()
    try:
        right = conn.execute(
            "SELECT * FROM rights WHERE catalog_id=?", (catalog_id,)
        ).fetchone()

        if not right:
            return {"error": f"Right not found: {catalog_id}"}

        linked = conn.execute("""
            SELECT m.mapping_role, c.clause_id, c.section_ref, c.text,
                   c.clause_type, s.doc_id, s.title as doc_title,
                   s.publisher, s.file_hash, s.publication_date
            FROM rights_clauses_map m
            JOIN clauses c ON m.clause_id = c.clause_id
            JOIN source_documents s ON c.source_doc_id = s.doc_id
            WHERE m.catalog_id=? AND c.is_current=1
            ORDER BY m.mapping_role, c.section_ref
        """, (catalog_id,)).fetchall()

        return {
            **dict(right),
            "clauses": [dict(r) for r in linked],
            "clause_count": len(linked),
        }
    finally:
        conn.close()
