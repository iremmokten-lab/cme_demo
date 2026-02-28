from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def _to_float(x: Any) -> float:
    try:
        if pd.isna(x):
            return 0.0
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return 0.0


def _months_present(df: pd.DataFrame) -> List[str]:
    if df is None or df.empty or "month" not in df.columns:
        return []
    return sorted({str(x) for x in df["month"].dropna().tolist()})


def completeness_checks(
    *,
    energy_df: pd.DataFrame | None,
    production_df: pd.DataFrame | None,
) -> Dict[str, Any]:
    """Completeness kontrolleri: missing months/products/fuels."""
    checks: List[Dict[str, Any]] = []

    energy_months = _months_present(energy_df) if isinstance(energy_df, pd.DataFrame) else []
    prod_months = _months_present(production_df) if isinstance(production_df, pd.DataFrame) else []

    missing_energy_months = []
    missing_prod_months = []
    # Eğer iki dataset varsa ay farklarını yakala
    if energy_months and prod_months:
        missing_energy_months = sorted(set(prod_months) - set(energy_months))
        missing_prod_months = sorted(set(energy_months) - set(prod_months))

    checks.append(
        {
            "check_id": "DQ.COMP.MONTHS.MATCH",
            "status": "FAIL" if (missing_energy_months or missing_prod_months) else "PASS",
            "details": {
                "missing_in_energy": missing_energy_months,
                "missing_in_production": missing_prod_months,
            },
            "severity": "major" if (missing_energy_months or missing_prod_months) else "info",
        }
    )

    # Fuel completeness
    missing_fuels = []
    if isinstance(energy_df, pd.DataFrame) and not energy_df.empty:
        if "fuel_type" in energy_df.columns:
            fuels = sorted({_norm(x) for x in energy_df["fuel_type"].dropna().tolist()})
            if not fuels:
                missing_fuels = ["fuel_type"]
        else:
            missing_fuels = ["fuel_type"]

    checks.append(
        {
            "check_id": "DQ.COMP.FUELS.PRESENT",
            "status": "FAIL" if missing_fuels else "PASS",
            "details": {"missing_fields": missing_fuels},
            "severity": "major" if missing_fuels else "info",
        }
    )

    # Products completeness
    missing_products = []
    if isinstance(production_df, pd.DataFrame) and not production_df.empty:
        cols = [c.lower().strip() for c in production_df.columns]
        df = production_df.copy()
        df.columns = cols
        if "product_code" not in df.columns and "sku" not in df.columns:
            missing_products = ["product_code"]
        else:
            codes = df["product_code"] if "product_code" in df.columns else df["sku"]
            if codes.dropna().empty:
                missing_products = ["product_code"]

    checks.append(
        {
            "check_id": "DQ.COMP.PRODUCTS.PRESENT",
            "status": "FAIL" if missing_products else "PASS",
            "details": {"missing_fields": missing_products},
            "severity": "major" if missing_products else "info",
        }
    )

    return {"checks": checks}


def anomaly_checks(
    *,
    energy_df: pd.DataFrame | None,
    production_df: pd.DataFrame | None,
) -> Dict[str, Any]:
    """Anomali kontrolleri: intensity spikes, numeric outliers (basit)."""
    flags: List[Dict[str, Any]] = []

    # Energy spike (quantity)
    if isinstance(energy_df, pd.DataFrame) and not energy_df.empty and "quantity" in energy_df.columns:
        q = energy_df["quantity"].apply(_to_float)
        if len(q) >= 6:
            med = float(q.median())
            maxv = float(q.max())
            if med > 0 and maxv / med > 5:
                flags.append(
                    {
                        "flag_id": "DQ.ANOM.ENERGY.SPIKE",
                        "severity": "major",
                        "details": {"median_quantity": med, "max_quantity": maxv, "ratio": (maxv / med)},
                    }
                )

    # Production spike
    if isinstance(production_df, pd.DataFrame) and not production_df.empty and "quantity" in production_df.columns:
        q = production_df["quantity"].apply(_to_float)
        if len(q) >= 6:
            med = float(q.median())
            maxv = float(q.max())
            if med > 0 and maxv / med > 5:
                flags.append(
                    {
                        "flag_id": "DQ.ANOM.PRODUCTION.SPIKE",
                        "severity": "major",
                        "details": {"median_quantity": med, "max_quantity": maxv, "ratio": (maxv / med)},
                    }
                )

    return {"qa_flags": flags}


def cross_checks(
    *,
    energy_df: pd.DataFrame | None,
    production_df: pd.DataFrame | None,
) -> Dict[str, Any]:
    """Basit cross-check: production vs energy (toplam trend uyumu)."""
    checks: List[Dict[str, Any]] = []

    if (
        isinstance(energy_df, pd.DataFrame)
        and isinstance(production_df, pd.DataFrame)
        and (not energy_df.empty)
        and (not production_df.empty)
        and ("quantity" in energy_df.columns)
        and ("quantity" in production_df.columns)
    ):
        e_sum = float(energy_df["quantity"].apply(_to_float).sum())
        p_sum = float(production_df["quantity"].apply(_to_float).sum())
        # Çok kaba kontrol
        if p_sum <= 0 and e_sum > 0:
            checks.append(
                {
                    "check_id": "DQ.CROSS.PRODUCTION.ZERO_ENERGY.NONZERO",
                    "status": "FAIL",
                    "severity": "major",
                    "details": {"energy_total": e_sum, "production_total": p_sum},
                }
            )
        else:
            checks.append(
                {
                    "check_id": "DQ.CROSS.PRODUCTION.ENERGY.BASIC",
                    "status": "PASS",
                    "severity": "info",
                    "details": {"energy_total": e_sum, "production_total": p_sum},
                }
            )
    else:
        checks.append(
            {
                "check_id": "DQ.CROSS.PRODUCTION.ENERGY.BASIC",
                "status": "WARN",
                "severity": "minor",
                "details": {"note": "Cross-check için energy_df ve production_df gerekli alanlara sahip değil."},
            }
        )

    return {"checks": checks}


def run_data_quality_engine(
    *,
    energy_df: pd.DataFrame | None,
    production_df: pd.DataFrame | None,
) -> Dict[str, Any]:
    """DQ engine birleşik çıktısı."""
    out = {"checks": [], "qa_flags": []}
    out["checks"].extend((completeness_checks(energy_df=energy_df, production_df=production_df).get("checks") or []))
    out["qa_flags"].extend((anomaly_checks(energy_df=energy_df, production_df=production_df).get("qa_flags") or []))
    out["checks"].extend((cross_checks(energy_df=energy_df, production_df=production_df).get("checks") or []))
    return out
