"""
Rights Angel — L5: Rule Evaluation Engine
engine/rule_engine.py

Deterministic eligibility evaluator.
No LLM at query time — pure Python logic only.

Algorithm (per M2A Architecture Doc Section 5.2):
  Step 1: Per-right eligibility check
          ALL CONDITIONS clauses satisfied AND NO EXCLUSION triggered
  Step 2: Group eligible rights by category_tag
  Step 3: Select MAX(discount_value) per category
  Step 4: Teiku tie-break → lowest friction_score wins
          If still equal → lexicographic catalog_id, flag for human review
  Step 5: Calculator — correct discount formula (no service days proration)

Formula per QA Feedback + Client Excel (March 2026):
  Soldier (תקנה 3ו):
    discount = annual_tax * (discount_rate_pct / 100)
    discount = min(discount, annual_tax)

  Commander (תקנה 3ז):
    taxable_sqm    = min(property_sqm, 100)
    tariff_per_sqm = annual_tax / property_sqm
    tax_on_100sqm  = taxable_sqm * tariff_per_sqm
    discount       = tax_on_100sqm * (discount_rate_pct / 100)
    discount       = min(discount, annual_tax)
"""

from typing import Any, Dict, List, Optional
from engine.fact_normalizer import normalize_facts


# ═══════════════════════════════════════════════════════════════════════════════
# Step 1: Per-right eligibility check
# ═══════════════════════════════════════════════════════════════════════════════

def _check_right_eligibility(facts: Dict[str, Any], right: dict) -> dict:
    """
    Evaluate a single right against the fact set.

    Returns:
        {
          "eligible": bool,
          "reasons":  [str],   — why eligible or not
          "failed_conditions": [str],
          "triggered_exclusions": [str],
        }
    """
    catalog_id = right.get("catalog_id", "")
    reasons    = []
    failed     = []
    excluded   = []

    # ── Step 1a: Check CONDITIONS ────────────────────────────────────────────
    # Map each right's catalog_id to required fact checks
    eligibility_checks = _get_eligibility_checks(catalog_id)

    for check_fn, description in eligibility_checks:
        if not check_fn(facts):
            failed.append(description)

    # ── Step 1b: Check EXCLUSIONS ────────────────────────────────────────────
    exclusion_checks = _get_exclusion_checks(catalog_id)

    for check_fn, description in exclusion_checks:
        if check_fn(facts):
            excluded.append(description)

    eligible = len(failed) == 0 and len(excluded) == 0

    if eligible:
        reasons.append("כל תנאי הזכאות מתקיימים")
    else:
        reasons.extend([f"תנאי לא מתקיים: {f}" for f in failed])
        reasons.extend([f"חריג פעיל: {e}" for e in excluded])

    return {
        "eligible":             eligible,
        "catalog_id":           catalog_id,
        "reasons":              reasons,
        "failed_conditions":    failed,
        "triggered_exclusions": excluded,
    }


def _get_eligibility_checks(catalog_id: str) -> List[tuple]:
    """
    Returns list of (check_function, description) for a right's CONDITIONS.
    Each check_function takes facts dict and returns bool.
    """
    checks = []

    # ── Common: property holder ───────────────────────────────────────────────
    checks.append((
        lambda f: f.get("IS_PROPERTY_HOLDER") is True,
        "מחזיק בנכס (שוכר או בעלים)"
    ))

    # ── Common: municipality grants the discount ──────────────────────────────
    checks.append((
        lambda f: f.get("MUNICIPALITY_GRANTS") is not False,
        "רשות מקומית אישרה את ההנחה"
    ))

    # ── Reserve soldier rights ────────────────────────────────────────────────
    if "RESERVE" in catalog_id:
        if "COMMANDER" in catalog_id:
            # Amendment 43 commander — gender specific
            if "FEMALE" in catalog_id:
                checks.append((
                    lambda f: f.get("RESERVE_TYPE") == "COMMANDER",
                    "מפקד מילואים פעיל (תקנה 2ב)"
                ))
                checks.append((
                    lambda f: f.get("GENDER") == "FEMALE",
                    "מפקדת מילואים — נקבה (תוספת 10% מגדר, תיקון 43)"
                ))
            elif "MALE" in catalog_id:
                checks.append((
                    lambda f: f.get("RESERVE_TYPE") == "COMMANDER",
                    "מפקד מילואים פעיל (תקנה 2ב)"
                ))
                checks.append((
                    lambda f: f.get("GENDER") in ("MALE", None),
                    "מפקד מילואים — זכר (תיקון 43)"
                ))
            else:
                # Legacy commander
                checks.append((
                    lambda f: f.get("RESERVE_TYPE") == "COMMANDER",
                    "מפקד מילואים פעיל (תקנה 3ז)"
                ))
        elif "FAMILY5" in catalog_id:
            checks.append((
                lambda f: f.get("RESERVE_TYPE") in ("SOLDIER", "COMMANDER"),
                "חייל מילואים פעיל"
            ))
            checks.append((
                lambda f: int(f.get("FAMILY_SIZE", 0)) >= 5,
                "משפחה בת 5 נפשות ומעלה"
            ))
        elif "SOLDIER" in catalog_id and "FEMALE" in catalog_id:
            # Amendment 43 female soldier
            checks.append((
                lambda f: f.get("RESERVE_TYPE") in ("SOLDIER", "COMMANDER"),
                "חיילת מילואים פעילה (תקנה 2א)"
            ))
            checks.append((
                lambda f: f.get("GENDER") == "FEMALE",
                "חיילת מילואים — נקבה (תוספת 10% מגדר, תיקון 43)"
            ))
        elif "SOLDIER" in catalog_id and "MALE" in catalog_id:
            # Amendment 43 male soldier
            checks.append((
                lambda f: f.get("RESERVE_TYPE") in ("SOLDIER", "COMMANDER"),
                "חייל מילואים פעיל (תקנה 2א)"
            ))
            checks.append((
                lambda f: f.get("GENDER") in ("MALE", None),
                "חייל מילואים — זכר (תיקון 43)"
            ))
        else:
            # Legacy standard soldier
            checks.append((
                lambda f: f.get("RESERVE_TYPE") in ("SOLDIER", "COMMANDER"),
                "חייל מילואים פעיל (תקנה 3ו)"
            ))

        # Min 20 days in 3 years — all reserve rights
        checks.append((
            lambda f: int(f.get("SERVICE_DAYS_3Y", 0)) >= 20,
            "לפחות 20 ימי שמ\"פ ב-3 שנים אחרונות"
        ))

    # ── Low income / senior rights ────────────────────────────────────────────
    if "LOWINCOME" in catalog_id or "SENIOR" in catalog_id:
        if "SENIOR" in catalog_id:
            checks.append((
                lambda f: f.get("IS_SENIOR") is True,
                "גיל פרישה (67+ גברים, 62+ נשים)"
            ))
        # Income check — basic presence check
        checks.append((
            lambda f: f.get("ANNUAL_INCOME_ILS") is not None,
            "הכנסה שנתית ידועה"
        ))

    if "WOMEN6267" in catalog_id:
        # Women 62-67 specific — we check IS_SENIOR as proxy
        checks.append((
            lambda f: f.get("IS_SENIOR") is True,
            "אישה בגיל 62-67"
        ))

    return checks


def _get_exclusion_checks(catalog_id: str) -> List[tuple]:
    """
    Returns list of (check_function, description) for EXCLUSION clauses.
    If any returns True → citizen is EXCLUDED from this right.
    """
    exclusions = []

    # Reserve rights: NONE type is excluded
    if "RESERVE" in catalog_id and "FAMILY5" not in catalog_id:
        exclusions.append((
            lambda f: f.get("RESERVE_TYPE") == "NONE",
            "לא משרת מילואים"
        ))

    # Commander right: less than 100 sqm is not excluded but changes calc
    # No hard exclusions for sqm — handled in calculator

    return exclusions


# ═══════════════════════════════════════════════════════════════════════════════
# Step 3-4: Overlap resolution — MAX discount + Teiku tie-break
# ═══════════════════════════════════════════════════════════════════════════════

def _resolve_overlap(eligible_rights: List[dict]) -> List[dict]:
    """
    Per architecture brief:
    - Group by category_tag
    - Select MAX(discount_value) per category
    - Tie-break: lowest friction_score
    - Still tied: lexicographic catalog_id, flag for human review

    Returns list of one winning right per category.
    """
    from collections import defaultdict
    by_category = defaultdict(list)

    for right in eligible_rights:
        tag = right.get("category_tag", "UNKNOWN")
        by_category[tag].append(right)

    winners = []
    flagged = False

    for category, rights_in_cat in by_category.items():
        if len(rights_in_cat) == 1:
            winners.append({**rights_in_cat[0], "overlap_resolved": False})
            continue

        # Sort: highest discount first, then lowest friction, then catalog_id
        sorted_rights = sorted(
            rights_in_cat,
            key=lambda r: (
                -float(r.get("discount_value", 0)),
                int(r.get("friction_score", 10)),
                str(r.get("catalog_id", ""))
            )
        )

        winner = sorted_rights[0]
        runner_up = sorted_rights[1]

        # Check if tie on discount AND friction
        tie = (
            float(winner.get("discount_value", 0)) == float(runner_up.get("discount_value", 0)) and
            int(winner.get("friction_score", 10)) == int(runner_up.get("friction_score", 10))
        )

        if tie:
            flagged = True

        winners.append({
            **winner,
            "overlap_resolved": len(rights_in_cat) > 1,
            "overlap_count":    len(rights_in_cat),
            "teiku_flagged":    tie,
        })

    return winners, flagged


# ═══════════════════════════════════════════════════════════════════════════════
# Step 5: Calculator — correct formulas (NO service days)
# ═══════════════════════════════════════════════════════════════════════════════

def _calculate_discount(facts: Dict[str, Any], right: dict) -> dict:
    """
    Compute discount amount for a winning right.

    Returns dict with all computed amounts.
    """
    catalog_id        = right.get("catalog_id", "")
    annual_tax        = float(facts.get("ANNUAL_TAX_ILS", 0))
    discount_rate_pct = float(facts.get("DISCOUNT_RATE_PCT",
                               right.get("discount_value", 0)))
    property_sqm      = float(facts.get("PROPERTY_SIZE_SQM", 0))
    payment_upfront   = facts.get("PAYMENT_UPFRONT", True)
    installment_count = int(facts.get("INSTALLMENT_COUNT", 1))
    gender            = facts.get("GENDER", None)
    age               = facts.get("AGE", None)
    service_year      = facts.get("SERVICE_YEAR", None)

    if annual_tax <= 0:
        return {
            "discount_ils":              0.0,
            "amount_after_discount_ils": 0.0,
            "installment_net_ils":       0.0,
            "formula_used":              "N/A — annual_tax is 0",
        }

    # ── Amendment 43: Age < 30 → 100% discount (תקנה 4) ─────────────────────
    if age is not None and int(age) < 30 and "RESERVE" in catalog_id:
        discount_rate_pct = 100.0

    # ── Commander (תקנה 2ב / 3ז): 120 sqm cap ────────────────────────────────
    if "COMMANDER" in catalog_id and property_sqm > 0:
        taxable_sqm    = min(property_sqm, 120.0)
        tariff_per_sqm = annual_tax / property_sqm
        tax_on_sqm     = taxable_sqm * tariff_per_sqm
        discount       = tax_on_sqm * (discount_rate_pct / 100)
        formula        = (f"MIN({property_sqm},120)={taxable_sqm}sqm × "
                          f"{tariff_per_sqm:.2f}₪/sqm × {discount_rate_pct}%")

    # ── All other rights (Soldier, Low Income, Senior, etc.) ─────────────────
    else:
        discount = annual_tax * (discount_rate_pct / 100)
        formula  = f"{annual_tax}₪ × {discount_rate_pct}%"

    # Cap: discount cannot exceed the total tax
    discount = min(discount, annual_tax)

    # ── Amendment 43: Pro-rata 272/365 for year 2026 ──────────────────────────
    pro_rata_applied = False
    if service_year == 2026 and "43" in catalog_id:
        PRO_RATA_2026 = 272 / 365  # = 0.7452 — days remaining from 4/4/2026
        discount      = round(discount * PRO_RATA_2026, 2)
        annual_tax_prorated = round(annual_tax * PRO_RATA_2026, 2)
        formula      += f" × 272/365 (יחסי 2026)"
        pro_rata_applied = True
    else:
        annual_tax_prorated = annual_tax

    amount_after_discount = annual_tax_prorated - discount

    # Installment breakdown
    if payment_upfront or installment_count <= 1:
        installment_count    = 1
        installment_gross    = annual_tax
        installment_discount = discount
        installment_net      = amount_after_discount
    else:
        installment_gross    = round(annual_tax / installment_count, 2)
        installment_discount = round(discount / installment_count, 2)
        installment_net      = round(amount_after_discount / installment_count, 2)

    return {
        "discount_ils":              round(discount, 2),
        "amount_after_discount_ils": round(amount_after_discount, 2),
        "payment_upfront":           bool(payment_upfront),
        "installment_count":         installment_count,
        "installment_gross_ils":     round(installment_gross, 2),
        "installment_discount_ils":  round(installment_discount, 2),
        "installment_net_ils":       round(installment_net, 2),
        "formula_used":              formula,
        "pro_rata_applied":          pro_rata_applied,
        "pro_rata_ratio":            round(272/365, 4) if pro_rata_applied else None,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_eligibility(
    raw_facts: Dict[str, Any],
    rights_with_clauses: List[dict],
) -> dict:
    """
    Full L5 evaluation pipeline.

    Args:
        raw_facts:            {fact_type: value} — from facts table
        rights_with_clauses:  list of right dicts with linked_clauses attached

    Returns:
        {
          "fact_validation":    {...},
          "per_right_results":  [{catalog_id, eligible, reasons, ...}],
          "eligible_rights":    [{...right + calc}],
          "winning_rights":     [{...after overlap resolution}],
          "flagged_for_review": bool,
          "total_discount_ils": float,
        }
    """
    # ── L4: Normalize facts ───────────────────────────────────────────────────
    fact_result = normalize_facts(raw_facts)
    if not fact_result["valid"]:
        return {
            "fact_validation":    fact_result,
            "per_right_results":  [],
            "eligible_rights":    [],
            "winning_rights":     [],
            "flagged_for_review": False,
            "total_discount_ils": 0.0,
            "error":              "Fact validation failed",
            "errors":             fact_result["errors"],
        }

    facts = fact_result["normalized"]

    # ── Step 1: Check each right ──────────────────────────────────────────────
    per_right_results = []
    eligible_rights   = []

    for right in rights_with_clauses:
        result = _check_right_eligibility(facts, right)
        per_right_results.append(result)

        if result["eligible"]:
            # Attach calculation
            calc = _calculate_discount(facts, right)
            eligible_rights.append({**right, **result, **calc})

    # ── Steps 2-4: Overlap resolution ────────────────────────────────────────
    if eligible_rights:
        winning_rights, flagged = _resolve_overlap(eligible_rights)
    else:
        winning_rights, flagged = [], False

    # Total discount across all winning rights
    total_discount = sum(
        float(r.get("discount_ils", 0)) for r in winning_rights
    )

    return {
        "fact_validation":    fact_result,
        "per_right_results":  per_right_results,
        "eligible_rights":    eligible_rights,
        "winning_rights":     winning_rights,
        "flagged_for_review": flagged,
        "total_discount_ils": round(total_discount, 2),
    }
