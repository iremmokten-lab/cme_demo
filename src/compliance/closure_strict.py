from __future__ import annotations

from typing import Any, Dict, List, Tuple

from src.mrv.compliance import evaluate_compliance


def require_pass(compliance: dict) -> None:
    # compliance format: {"status":"PASS|FAIL", "checks":[...]}
    if not isinstance(compliance, dict):
        raise ValueError("Compliance çıktısı okunamadı.")
    if str(compliance.get("status") or "").upper() != "PASS":
        # Provide short summary
        checks = compliance.get("checks") or []
        missing = []
        for c in checks:
            if isinstance(c, dict) and str(c.get("status") or "").upper() == "FAIL":
                missing.append(str(c.get("code") or c.get("message") or "FAIL"))
        msg = "Compliance FAIL: " + ", ".join(missing[:12])
        raise ValueError(msg)


def evaluate_and_require(project_id: int, context: dict) -> dict:
    comp = evaluate_compliance(project_id=int(project_id), context=context or {})
    require_pass(comp)
    return comp
