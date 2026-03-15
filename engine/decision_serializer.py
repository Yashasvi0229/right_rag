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

This module contains NO LLM calls — all explanations are
deterministic template strings filled with computed values.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


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

        # Summary
        "total_discount_ils": round(total_discount, 2),
        "flagged_for_review": flagged,

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
