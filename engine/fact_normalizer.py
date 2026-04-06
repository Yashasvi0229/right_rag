"""
Rights Angel — L4: Fact Normalization Layer
engine/fact_normalizer.py

Validates, normalizes and type-checks all citizen-submitted facts
before they enter the L5 rule engine.

Fact types (14 total per M2A Architecture Doc Section 4.2):
  RESERVE_TYPE        — SOLDIER | COMMANDER | NONE
  SERVICE_DAYS_3Y     — Integer 0-1095
  SERVICE_START_DATE  — ISO date YYYY-MM-DD
  SERVICE_END_DATE    — ISO date YYYY-MM-DD
  IS_PROPERTY_HOLDER  — bool
  PROPERTY_SIZE_SQM   — float > 0
  ANNUAL_TAX_ILS      — float > 0
  DISCOUNT_RATE_PCT   — float 0-100
  MUNICIPALITY_GRANTS — bool
  FAMILY_SIZE         — int 1-20
  ANNUAL_INCOME_ILS   — float >= 0
  IS_SENIOR           — bool
  PAYMENT_UPFRONT     — bool
  INSTALLMENT_COUNT   — int: 1 | 6 | 12
"""

from datetime import datetime
from typing import Any, Dict, List, Tuple


# ═══════════════════════════════════════════════════════════════════════════════
# Schema: allowed values and types per fact_type
# ═══════════════════════════════════════════════════════════════════════════════

FACT_SCHEMA: Dict[str, dict] = {
    "RESERVE_TYPE": {
        "type": "enum",
        "allowed": ["SOLDIER", "COMMANDER", "NONE"],
        "description": "סוג השירות — חייל / מפקד / לא רלוונטי",
    },
    "SERVICE_DAYS_3Y": {
        "type": "int",
        "min": 0,
        "max": 1095,   # 3 years × 365
        "description": "מספר ימי מילואים מצטבר ב-3 שנים אחרונות",
    },
    "SERVICE_START_DATE": {
        "type": "date",
        "description": "תאריך תחילת תקופת השירות (YYYY-MM-DD)",
    },
    "SERVICE_END_DATE": {
        "type": "date",
        "description": "תאריך סיום תקופת השירות (YYYY-MM-DD)",
    },
    "IS_PROPERTY_HOLDER": {
        "type": "bool",
        "description": "האם הפונה רשום כבעל/שוכר הנכס?",
    },
    "PROPERTY_SIZE_SQM": {
        "type": "float",
        "min": 1.0,
        "max": 10000.0,
        "description": "שטח הנכס במטרים רבועים",
    },
    "ANNUAL_TAX_ILS": {
        "type": "float",
        "min": 0.01,
        "max": 10_000_000.0,
        "description": "סכום הארנונה השנתי לפני הנחה (₪)",
    },
    "DISCOUNT_RATE_PCT": {
        "type": "float",
        "min": 0.0,
        "max": 100.0,
        "description": "שיעור ההנחה שאישרה הרשות המקומית (%)",
    },
    "MUNICIPALITY_GRANTS": {
        "type": "bool",
        "description": "האם הרשות המקומית אישרה את ההנחה הזו?",
    },
    "FAMILY_SIZE": {
        "type": "int",
        "min": 1,
        "max": 20,
        "description": "מספר נפשות במשפחה (לזכאות 100% משפחה 5+)",
    },
    "ANNUAL_INCOME_ILS": {
        "type": "float",
        "min": 0.0,
        "max": 100_000_000.0,
        "description": "הכנסה שנתית (₪) — לבדיקת זכאות מעוטי יכולת",
    },
    "IS_SENIOR": {
        "type": "bool",
        "description": "האם הפונה מעל גיל הפרישה? (67+ גברים, 62+ נשים)",
    },
    "PAYMENT_UPFRONT": {
        "type": "bool",
        "description": "האם שילם את כל הארנונה השנתית מראש?",
    },
    "INSTALLMENT_COUNT": {
        "type": "enum",
        "allowed": [1, 6, 12],
        "description": "מספר התשלומים (1 / 6 / 12)",
    },
    # ★ NEW: Amendment 43 — gender bonus (+10% for women)
    "GENDER": {
        "type": "enum",
        "allowed": ["MALE", "FEMALE"],
        "description": "מגדר המשרת — MALE / FEMALE (לתוספת 10% לנשים לפי תיקון 43)",
    },
    # ★ NEW: Amendment 43 — age for 100% discount rule (תקנה 4)
    "AGE": {
        "type": "int",
        "min": 18,
        "max": 120,
        "description": "גיל המשרת — לבדיקת זכאות 100% לגיל מתחת ל-30 (תקנה 4)",
    },
    # ★ NEW: Amendment 46 — Takkana 6: 100 consecutive days for tuition
    "CONSECUTIVE_DAYS": {
        "type": "int",
        "min": 0,
        "max": 1500,
        "description": "ימי שירות מילואים רצופים (מ-7.10.2023) — 100+ ימים זכאות לשנת לימוד חינם (תקנה 6)",
    },
    # ★ NEW: Amendment 46 — Takkana 7: pregnancy bed rest
    "IS_PREGNANCY_BED_REST": {
        "type": "bool",
        "description": "האם קיבלה הוראת רופא לשמירת הריון החופפת ימי מילואים? (תקנה 7)",
    },
    # ★ NEW: Amendment 46 — effective date for pro-rata 269/365
    "SERVICE_YEAR": {
        "type": "int",
        "min": 2020,
        "max": 2030,
        "description": "שנת המס לחישוב — 2026 מפעיל חישוב יחסי 269/365 (תיקון 46, מ-6.4.2026)",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════════

def validate_fact(fact_type: str, raw_value: Any) -> Tuple[Any, List[str]]:
    """
    Validate and normalize a single fact.

    Returns:
        (normalized_value, errors)
        errors is empty list if valid.
    """
    if fact_type not in FACT_SCHEMA:
        return None, [f"Unknown fact_type: '{fact_type}'. "
                      f"Allowed: {sorted(FACT_SCHEMA.keys())}"]

    schema = FACT_SCHEMA[fact_type]
    errors = []
    value  = raw_value

    ftype = schema["type"]

    # ── enum ─────────────────────────────────────────────────────────────────
    if ftype == "enum":
        allowed = schema["allowed"]
        # Try to cast to int if allowed values are ints
        if allowed and isinstance(allowed[0], int):
            try:
                value = int(value)
            except (TypeError, ValueError):
                errors.append(f"{fact_type}: cannot convert '{value}' to integer.")
                return None, errors
        else:
            value = str(value).strip().upper() if isinstance(value, str) else value

        if value not in allowed:
            errors.append(
                f"{fact_type}: '{value}' is not allowed. "
                f"Allowed values: {allowed}"
            )
            return None, errors
        return value, []

    # ── bool ─────────────────────────────────────────────────────────────────
    if ftype == "bool":
        if isinstance(value, bool):
            return value, []
        if isinstance(value, str):
            if value.lower() in ("true", "1", "yes", "כן"):
                return True, []
            if value.lower() in ("false", "0", "no", "לא"):
                return False, []
        if isinstance(value, int):
            return bool(value), []
        errors.append(f"{fact_type}: '{value}' is not a valid boolean. Use true/false.")
        return None, errors

    # ── int ──────────────────────────────────────────────────────────────────
    if ftype == "int":
        try:
            value = int(float(str(value)))
        except (TypeError, ValueError):
            errors.append(f"{fact_type}: '{value}' is not a valid integer.")
            return None, errors
        if "min" in schema and value < schema["min"]:
            errors.append(f"{fact_type}: {value} is below minimum {schema['min']}.")
        if "max" in schema and value > schema["max"]:
            errors.append(f"{fact_type}: {value} exceeds maximum {schema['max']}.")
        return value, errors

    # ── float ────────────────────────────────────────────────────────────────
    if ftype == "float":
        try:
            value = float(str(value))
        except (TypeError, ValueError):
            errors.append(f"{fact_type}: '{value}' is not a valid number.")
            return None, errors
        if "min" in schema and value < schema["min"]:
            errors.append(f"{fact_type}: {value} is below minimum {schema['min']}.")
        if "max" in schema and value > schema["max"]:
            errors.append(f"{fact_type}: {value} exceeds maximum {schema['max']}.")
        return value, errors

    # ── date ─────────────────────────────────────────────────────────────────
    if ftype == "date":
        # Accept DD/MM/YYYY (from UI date picker) or YYYY-MM-DD
        if isinstance(value, str):
            value = value.strip()
            # Try DD/MM/YYYY first
            if "/" in value and len(value) == 10:
                try:
                    dt = datetime.strptime(value, "%d/%m/%Y")
                    return dt.strftime("%Y-%m-%d"), []
                except ValueError:
                    pass
            # Try YYYY-MM-DD
            try:
                dt = datetime.strptime(value, "%Y-%m-%d")
                if dt.year < 1900 or dt.year > 2100:
                    errors.append(f"{fact_type}: year {dt.year} out of valid range 1900-2100.")
                    return None, errors
                return value, []
            except ValueError:
                pass
        errors.append(
            f"{fact_type}: '{value}' is not a valid date. "
            f"Use YYYY-MM-DD or DD/MM/YYYY format."
        )
        return None, errors

    errors.append(f"Unknown schema type '{ftype}' for {fact_type}.")
    return None, errors


def validate_fact_set(facts: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
    """
    Validate an entire fact set.

    Args:
        facts: {fact_type: raw_value}

    Returns:
        (normalized_facts, all_errors)
        all_errors is empty dict if everything valid.
    """
    normalized = {}
    all_errors: Dict[str, List[str]] = {}

    for fact_type, raw_value in facts.items():
        norm, errs = validate_fact(fact_type, raw_value)
        if errs:
            all_errors[fact_type] = errs
        else:
            normalized[fact_type] = norm

    return normalized, all_errors


# ═══════════════════════════════════════════════════════════════════════════════
# Business rule cross-checks
# ═══════════════════════════════════════════════════════════════════════════════

def check_business_rules(facts: Dict[str, Any]) -> List[str]:
    """
    Cross-fact business rule checks AFTER individual validation passes.
    Returns list of warning/error strings (non-fatal — returned as warnings).
    """
    warnings = []

    # Date order check
    start = facts.get("SERVICE_START_DATE")
    end   = facts.get("SERVICE_END_DATE")
    if start and end:
        try:
            dt_start = datetime.strptime(start, "%Y-%m-%d")
            dt_end   = datetime.strptime(end,   "%Y-%m-%d")
            if dt_end < dt_start:
                warnings.append(
                    "SERVICE_END_DATE is before SERVICE_START_DATE. "
                    "Please check the dates."
                )
        except ValueError:
            pass

    # Commander requires PROPERTY_SIZE_SQM
    if facts.get("RESERVE_TYPE") == "COMMANDER" and not facts.get("PROPERTY_SIZE_SQM"):
        warnings.append(
            "RESERVE_TYPE is COMMANDER but PROPERTY_SIZE_SQM is missing. "
            "The 100 sqm cap cannot be applied without property size."
        )

    # Installment count consistency
    payment_upfront   = facts.get("PAYMENT_UPFRONT")
    installment_count = facts.get("INSTALLMENT_COUNT")
    if payment_upfront is True and installment_count and installment_count > 1:
        warnings.append(
            "PAYMENT_UPFRONT is true but INSTALLMENT_COUNT > 1. "
            "Installment count will be ignored — treating as upfront payment."
        )

    # Minimum service days check for basic eligibility
    service_days = facts.get("SERVICE_DAYS_3Y")
    reserve_type = facts.get("RESERVE_TYPE")
    if reserve_type in ("SOLDIER", "COMMANDER") and service_days is not None:
        if service_days < 20:
            warnings.append(
                f"SERVICE_DAYS_3Y is {service_days}, which is below the minimum "
                f"threshold of 20 days required for basic reserve soldier eligibility (תקנה 3ו)."
            )

    return warnings


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_facts(raw_facts: Dict[str, Any]) -> dict:
    """
    Main entry point for L4.

    Args:
        raw_facts: {fact_type: raw_value} — from API or UI

    Returns:
        {
          "valid": bool,
          "normalized": {fact_type: typed_value},
          "errors": {fact_type: [error_msg]},
          "warnings": [warning_msg],
        }
    """
    normalized, errors = validate_fact_set(raw_facts)

    warnings = []
    if not errors:
        warnings = check_business_rules(normalized)

    return {
        "valid":      len(errors) == 0,
        "normalized": normalized,
        "errors":     errors,
        "warnings":   warnings,
    }


def get_fact_schema() -> dict:
    """Return the full fact schema — used by UI to build dynamic forms."""
    return {
        fact_type: {
            "type":        schema["type"],
            "description": schema["description"],
            **({"allowed": schema["allowed"]} if "allowed" in schema else {}),
            **({"min": schema["min"]} if "min" in schema else {}),
            **({"max": schema["max"]} if "max" in schema else {}),
        }
        for fact_type, schema in FACT_SCHEMA.items()
    }
