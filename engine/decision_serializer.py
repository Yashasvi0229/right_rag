"""
Rights Angel — L6: Decision Serializer + Explainability
engine/decision_serializer.py

Converts L5 evaluation output into a structured EligibilityResult
with full evidence chain and Hebrew plain-language explanation.

Per M2A Architecture Doc Section 6.1:
  Every determination must include:
  - clause_id + source_doc_id + section_ref + engine_id (evidence chain)
  - explanation_he: Hebrew plain-language explanation of the calculation
  - amount_after_discount_ils: what citizen actually pays
  - installment_amount_ils: per-installment payment if applicable

Resolution status is forwarded from L5 and mapped to a Hebrew top-level
message so the citizen always sees a clear response:
  ELIGIBLE              — summary + per-right details
  INELIGIBLE            — conditions that failed
  INSUFFICIENT_EVIDENCE — missing facts / invalid input
  DOMAIN_NOT_INGESTED   — legal domain not loaded in catalog yet

This module contains NO LLM calls — all explanations are
deterministic template strings filled with computed values.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ═══════════════════════════════════════════════════════════════════════════════
# Hebrew labels for facts and domains (used in top-level explanation_he)
# ═══════════════════════════════════════════════════════════════════════════════

_FACT_LABELS_HE = {
    "RESERVE_TYPE":          "סוג שירות מילואים",
    "SERVICE_DAYS_3Y":       "ימי שירות בשלוש שנים אחרונות",
    "SERVICE_START_DATE":    "תאריך תחילת שירות",
    "SERVICE_END_DATE":      "תאריך סיום שירות",
    "IS_PROPERTY_HOLDER":    "האם מחזיק בנכס",
    "PROPERTY_SIZE_SQM":     "גודל הנכס במ\"ר",
    "ANNUAL_TAX_ILS":        "ארנונה שנתית",
    "DISCOUNT_RATE_PCT":     "שיעור ההנחה של הרשות",
    "MUNICIPALITY_GRANTS":   "האם הרשות המקומית מעניקה הנחה",
    "FAMILY_SIZE":           "גודל משפחה",
    "ANNUAL_INCOME_ILS":     "הכנסה שנתית",
    "IS_SENIOR":             "האם בגיל פרישה",
    "PAYMENT_UPFRONT":       "האם תשלום מראש",
    "INSTALLMENT_COUNT":     "מספר תשלומים",
    "GENDER":                "מגדר",
    "AGE":                   "גיל",
    "SERVICE_YEAR":          "שנת השירות",
    "CONSECUTIVE_DAYS":      "ימי מילואים רצופים",
    "IS_PREGNANCY_BED_REST": "שמירת הריון",
}

_DOMAIN_LABELS_HE = {
    "RESERVE":   "מילואים",
    "SENIOR":    "גיל פרישה",
    "LOWINCOME": "מעוטי יכולת",
    "TUITION":   "שכר לימוד למילואים",
    "PREGNANCY": "שמירת הריון למילואים",
    "UNKNOWN":   "לא זוהה",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Hebrew explanation templates
# ═══════════════════════════════════════════════════════════════════════════════

def _build_explanation_he(
    right:    dict,
    facts:    dict,
    calc:     dict,
) -> str:
    """
    Build a Hebrew plain-language explanation for a single eligible right.
    Pure template — no LLM.
    """
    catalog_id    = right.get("catalog_id", "")
    name          = right.get("name", "הנחה")
    discount_rate = float(facts.get("DISCOUNT_RATE_PCT", right.get("discount_value", 0)))
    annual_tax    = float(facts.get("ANNUAL_TAX_ILS", 0))
    discount_ils  = float(calc.get("discount_ils", 0))
    after_ils     = float(calc.get("amount_after_discount_ils", 0))
    installments  = int(calc.get("installment_count", 1))
    inst_net      = float(calc.get("installment_net_ils", after_ils))
    upfront       = calc.get("payment_upfront", True)
    property_sqm  = float(facts.get("PROPERTY_SIZE_SQM", 0))

    # ── Soldier (תקנה 3ו) ────────────────────────────────────────────────────
    if "RESERVE" in catalog_id and "COMMANDER" not in catalog_id and "FAMILY5" not in catalog_id:
        explanation = (
            f"הנחה בארנונה לחייל מילואים פעיל — עד {discount_rate:.0f}% על פי תקנה 3ו.\n"
            f"ארנונה שנתית לפני הנחה: {annual_tax:,.0f}₪\n"
            f"שיעור הנחה שאושר: {discount_rate:.0f}%\n"
            f"סכום ההנחה: {discount_ils:,.0f}₪\n"
            f"לתשלום לאחר הנחה: {after_ils:,.0f}₪"
        )
        if not upfront and installments > 1:
            explanation += (
                f"\nתשלום לכל תשלום ({installments} תשלומים): {inst_net:,.0f}₪"
            )
        explanation += "\n\nהערה: הנחת רשות — הרשות המקומית אינה חייבת להעניקה. יש להגיש בקשה."

    # ── Commander (תקנה 3ז) ──────────────────────────────────────────────────
    elif "COMMANDER" in catalog_id:
        taxable_sqm = min(property_sqm, 100.0) if property_sqm > 0 else 100.0
        explanation = (
            f"הנחה בארנונה למפקד מילואים פעיל — עד {discount_rate:.0f}% על פי תקנה 3ז.\n"
        )
        if property_sqm > 100:
            explanation += (
                f"שטח הנכס: {property_sqm:.0f} מ\"ר | ההנחה חלה על {taxable_sqm:.0f} מ\"ר ראשונים בלבד (תקרה חוקית).\n"
            )
        else:
            explanation += f"שטח הנכס: {property_sqm:.0f} מ\"ר (מתחת לתקרה — כל השטח זכאי להנחה).\n"

        explanation += (
            f"ארנונה שנתית לפני הנחה: {annual_tax:,.0f}₪\n"
            f"שיעור הנחה שאושר: {discount_rate:.0f}%\n"
            f"סכום ההנחה: {discount_ils:,.0f}₪\n"
            f"לתשלום לאחר הנחה: {after_ils:,.0f}₪"
        )
        if not upfront and installments > 1:
            explanation += (
                f"\nתשלום לכל תשלום ({installments} תשלומים): {inst_net:,.0f}₪"
            )
        explanation += "\n\nהערה: הנחת רשות — הרשות המקומית אינה חייבת להעניקה. יש להגיש בקשה."

    # ── Family 5+ ────────────────────────────────────────────────────────────
    elif "FAMILY5" in catalog_id:
        explanation = (
            f"הנחה מלאה של 100% לחייל מילואים פעיל עם משפחה בת 5 נפשות ומעלה.\n"
            f"ארנונה שנתית לפני הנחה: {annual_tax:,.0f}₪\n"
            f"סכום ההנחה: {discount_ils:,.0f}₪ (100%)\n"
            f"לתשלום לאחר הנחה: {after_ils:,.0f}₪\n\n"
            f"הערה: חלה על נכסים עד 90 מ\"ר. יש לפנות לרשות המקומית."
        )

    # ── Senior / Low Income ──────────────────────────────────────────────────
    elif "SENIOR" in catalog_id or "LOWINCOME" in catalog_id:
        explanation = (
            f"{name} — {discount_rate:.0f}% הנחה בארנונה.\n"
            f"ארנונה שנתית לפני הנחה: {annual_tax:,.0f}₪\n"
            f"סכום ההנחה: {discount_ils:,.0f}₪\n"
            f"לתשלום לאחר הנחה: {after_ils:,.0f}₪"
        )
        if not upfront and installments > 1:
            explanation += (
                f"\nתשלום לכל תשלום ({installments} תשלומים): {inst_net:,.0f}₪"
            )

    # ── Generic fallback ─────────────────────────────────────────────────────
    else:
        explanation = (
            f"{name}\n"
            f"שיעור הנחה: {discount_rate:.0f}%\n"
            f"ארנונה שנתית: {annual_tax:,.0f}₪\n"
            f"סכום ההנחה: {discount_ils:,.0f}₪\n"
            f"לתשלום: {after_ils:,.0f}₪"
        )

    return explanation


# ═══════════════════════════════════════════════════════════════════════════════
# Top-level Hebrew explanation — one per resolution_status
# ═══════════════════════════════════════════════════════════════════════════════

def _format_errors(errors: list) -> str:
    """Format fact-validation errors as a readable Hebrew-friendly string."""
    parts = []
    for e in (errors or [])[:5]:
        if isinstance(e, dict):
            ft = e.get("fact_type", "") or ""
            er = e.get("error", "") or e.get("message", "") or str(e)
            parts.append(f"{ft}: {er}" if ft else er)
        else:
            parts.append(str(e))
    return "; ".join(parts)


def _build_top_level_explanation_he(evaluation: dict) -> str:
    """
    Build a Hebrew plain-language message describing the overall outcome.
    Called once per evaluation regardless of resolution_status — guarantees
    the response always has an explanation_he field (M2 acceptance criterion).
    """
    resolution_status = evaluation.get("resolution_status")

    # Backward compatibility: derive from winning_rights if status missing
    if not resolution_status:
        resolution_status = "ELIGIBLE" if evaluation.get("winning_rights") else "INELIGIBLE"

    if resolution_status == "ELIGIBLE":
        winning = evaluation.get("winning_rights", []) or []
        total = float(evaluation.get("total_discount_ils", 0.0) or 0.0)
        return (
            f"נמצאו {len(winning)} זכות/זכויות שאתה זכאי להן. "
            f"סה\"כ הנחה שנתית מקסימלית: {total:,.0f}₪. "
            f"פירוט מלא ושרשרת ראיות לכל זכות מופיעים למטה."
        )

    if resolution_status == "DOMAIN_NOT_INGESTED":
        supported = evaluation.get("supported_domains", []) or []
        supported_he = [_DOMAIN_LABELS_HE.get(d, d) for d in supported]
        supported_str = ", ".join(supported_he) if supported_he else "אין כרגע תחומים נתמכים"
        inferred = _DOMAIN_LABELS_HE.get(
            evaluation.get("inferred_domain", "UNKNOWN"),
            "התחום המבוקש"
        )
        return (
            f"תחום החקיקה \"{inferred}\" עדיין לא נטען למערכת. "
            f"תחומים נתמכים כרגע: {supported_str}. "
            f"פנה למנהל המערכת לצירוף החקיקה המבוקשת."
        )

    if resolution_status == "INSUFFICIENT_EVIDENCE":
        errors = evaluation.get("errors") or []
        if errors:
            err_str = _format_errors(errors)
            return (
                f"לא ניתן להכריע — הנתונים שהוזנו אינם תקינים. "
                f"שגיאות: {err_str}. "
                f"יש לתקן את הנתונים ולנסות שוב."
            )

        missing = evaluation.get("missing_facts", []) or []
        if missing:
            missing_he = [_FACT_LABELS_HE.get(f, f) for f in missing]
            missing_str = ", ".join(missing_he)
            return (
                f"לא ניתן להכריע — חסרים נתונים לבדיקת הזכאות. "
                f"נתונים חסרים: {missing_str}. "
                f"יש להשלים את הפרטים ולנסות שוב."
            )

        return (
            "לא ניתן להכריע — חסרים נתונים לבדיקת הזכאות. "
            "יש להשלים את הפרטים ולנסות שוב."
        )

    # ── INELIGIBLE (default) ──────────────────────────────────────────────────
    per_right = evaluation.get("per_right_results", []) or []
    all_reasons = []
    for r in per_right:
        all_reasons.extend(r.get("failed_conditions", []) or [])
        all_reasons.extend(r.get("triggered_exclusions", []) or [])
    unique_reasons = list(dict.fromkeys(all_reasons))[:5]

    if unique_reasons:
        reasons_str = "; ".join(unique_reasons)
        return (
            f"על פי הנתונים שהוזנו, לא עמדת בתנאי הזכאות של אף זכות במערכת. "
            f"תנאים שלא מתקיימים: {reasons_str}."
        )

    return (
        "על פי הנתונים שהוזנו, לא עמדת בתנאי הזכאות של אף זכות במערכת. "
        "יתכן שאין חוק המתאים למצבך."
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Evidence chain builder
# ═══════════════════════════════════════════════════════════════════════════════

def _build_evidence_chain(right: dict) -> List[dict]:
    """
    Build evidence chain from right's linked_clauses.
    Each item references: clause_id, section_ref, source_doc_id, mapping_role.
    """
    chain = []
    linked_clauses = right.get("linked_clauses", [])

    for clause in linked_clauses:
        if not clause:
            continue
        chain.append({
            "clause_id":       clause.get("clause_id", ""),
            "section_ref":     clause.get("section_ref", ""),
            "source_doc_id":   clause.get("source_doc_id", ""),
            "doc_title":       clause.get("doc_title", ""),
            "mapping_role":    clause.get("mapping_role", ""),
            "text_snippet":    (clause.get("text") or "")[:120] + "..."
                               if len(clause.get("text", "")) > 120
                               else clause.get("text", ""),
        })

    return chain


# ═══════════════════════════════════════════════════════════════════════════════
# Main serializer
# ═══════════════════════════════════════════════════════════════════════════════

def serialize_result(
    session_id:  str,
    engine_id:   str,
    facts:       Dict[str, Any],
    evaluation:  dict,
) -> dict:
    """
    L6: Serialize full EligibilityResult.

    Args:
        session_id:  citizen session UUID
        engine_id:   active engine version ID
        facts:       normalized fact dict from L4
        evaluation:  output from L5 evaluate_eligibility()

    Returns:
        Complete EligibilityResult dict with Hebrew explanation + evidence chain.
    """
    decision_id  = str(uuid.uuid4())
    evaluated_at = datetime.now(timezone.utc).isoformat()

    winning_rights = evaluation.get("winning_rights", [])
    flagged        = evaluation.get("flagged_for_review", False)
    total_discount = evaluation.get("total_discount_ils", 0.0)

    # Build per-right result entries
    eligible_entries = []
    for right in winning_rights:
        calc = {
            "discount_ils":              right.get("discount_ils", 0.0),
            "amount_after_discount_ils": right.get("amount_after_discount_ils", 0.0),
            "installment_count":         right.get("installment_count", 1),
            "installment_net_ils":       right.get("installment_net_ils", 0.0),
            "installment_gross_ils":     right.get("installment_gross_ils", 0.0),
            "installment_discount_ils":  right.get("installment_discount_ils", 0.0),
            "payment_upfront":           right.get("payment_upfront", True),
        }

        explanation_he = _build_explanation_he(right, facts, calc)
        evidence_chain = _build_evidence_chain(right)

        eligible_entries.append({
            # Right identity
            "catalog_id":                right.get("catalog_id"),
            "name":                      right.get("name"),
            "category_tag":              right.get("category_tag"),
            "discount_value":            right.get("discount_value"),
            "discount_unit":             right.get("discount_unit", "PERCENT"),
            "friction_score":            right.get("friction_score"),

            # Calculation results
            "discount_ils":              round(float(calc["discount_ils"]), 2),
            "amount_after_discount_ils": round(float(calc["amount_after_discount_ils"]), 2),
            "installment_count":         calc["installment_count"],
            "installment_net_ils":       round(float(calc["installment_net_ils"]), 2),
            "installment_gross_ils":     round(float(calc["installment_gross_ils"]), 2),
            "installment_discount_ils":  round(float(calc["installment_discount_ils"]), 2),
            "payment_upfront":           calc["payment_upfront"],

            # L6: Hebrew explanation (mandatory per M2A Section 10)
            "explanation_he":            explanation_he,

            # L6: Evidence chain
            "evidence_chain":            evidence_chain,

            # Overlap resolution metadata
            "overlap_resolved":          right.get("overlap_resolved", False),
            "teiku_flagged":             right.get("teiku_flagged", False),
        })

    # Build ineligible summary (for UI — show why other rights didn't apply)
    ineligible_entries = []
    for result in evaluation.get("per_right_results", []):
        if not result.get("eligible"):
            ineligible_entries.append({
                "catalog_id":           result.get("catalog_id"),
                "failed_conditions":    result.get("failed_conditions", []),
                "triggered_exclusions": result.get("triggered_exclusions", []),
            })

    # ── Resolution status + top-level Hebrew explanation ────────────────────
    resolution_status = evaluation.get("resolution_status")
    if not resolution_status:
        resolution_status = "ELIGIBLE" if eligible_entries else "INELIGIBLE"

    top_level_explanation_he = _build_top_level_explanation_he(evaluation)

    return {
        # Decision metadata
        "decision_id":        decision_id,
        "session_id":         session_id,
        "engine_id":          engine_id,
        "evaluated_at":       evaluated_at,

        # Results
        "is_eligible":        len(eligible_entries) > 0,
        "eligible_rights":    eligible_entries,
        "ineligible_summary": ineligible_entries,
        "per_right_results":  evaluation.get("per_right_results", []),

        # Summary
        "total_discount_ils": round(total_discount, 2),
        "flagged_for_review": flagged,

        # M2 fix: never silent — resolution + Hebrew top-level message
        "resolution_status":  resolution_status,
        "explanation_he":     top_level_explanation_he,
        "inferred_domain":    evaluation.get("inferred_domain"),
        "supported_domains":  evaluation.get("supported_domains", []),
        "missing_facts":      evaluation.get("missing_facts", []),

        # Forward validation errors when INSUFFICIENT_EVIDENCE from bad input
        "validation_errors":  evaluation.get("errors", []),

        # Fact snapshot (for audit trail)
        "facts_used": {
            k: v for k, v in facts.items()
            if k in {
                "RESERVE_TYPE", "ANNUAL_TAX_ILS", "DISCOUNT_RATE_PCT",
                "PROPERTY_SIZE_SQM", "PAYMENT_UPFRONT", "INSTALLMENT_COUNT",
                "IS_PROPERTY_HOLDER", "MUNICIPALITY_GRANTS",
            }
        },
    }
