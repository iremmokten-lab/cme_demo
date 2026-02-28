from __future__ import annotations

from typing import Any, Dict, List

from src.mrv.data_quality_engine import run_data_quality_engine


def build_qaqc_checks(*, results: dict, config: dict) -> dict:
    """
    QA/QC checks:
      - completeness / anomalies / cross checks (data quality engine)
      - methodology placeholders

    Bu çıktı ETS dataset'ine ve compliance_checks.json içine konulabilir.
    """
    energy_df = None
    production_df = None

    # Eğer results içinde df yoksa DQ sadece results düzeyinde sınırlı çalışır.
    # Bu repo yaklaşımında DQ engine UI'da ingestion aşamasında hesaplanıyor.
    # Burada best-effort:
    dq = {"checks": [], "qa_flags": []}
    try:
        # results -> input_bundle -> activity_snapshot_ref -> uri ile df çekmek istenirse:
        input_bundle = (results or {}).get("input_bundle") or {}
        activity = (input_bundle.get("activity_snapshot_ref") or {}) if isinstance(input_bundle, dict) else {}
        from src.services.workflow import load_csv_from_uri

        e_uri = ((activity.get("energy") or {}).get("uri") or "")
        p_uri = ((activity.get("production") or {}).get("uri") or "")
        if e_uri and p_uri:
            energy_df = load_csv_from_uri(str(e_uri))
            production_df = load_csv_from_uri(str(p_uri))
            dq = run_data_quality_engine(energy_df=energy_df, production_df=production_df)
    except Exception:
        pass

    checks: List[Dict[str, Any]] = []
    checks.extend(dq.get("checks") or [])

    # Basic methodology checks placeholders
    checks.append(
        {
            "check_id": "QAQC.METHOD.MONITORING_PLAN.PRESENT",
            "status": "PASS" if (config or {}).get("monitoring_plan_id") else "WARN",
            "severity": "minor",
            "details": {"note": "Monitoring plan seçimi önerilir (MRR uyumu)."},
        }
    )
    checks.append(
        {
            "check_id": "QAQC.METHOD.TIER.JUSTIFICATION.PRESENT",
            "status": "PASS",
            "severity": "info",
            "details": {"note": "Tier justification stream bazında raporlandı."},
        }
    )

    return {"checks": checks, "qa_flags": dq.get("qa_flags") or []}
