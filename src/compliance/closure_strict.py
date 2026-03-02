from __future__ import annotations

from src.mrv.compliance import evaluate_compliance

def require_pass(compliance: dict) -> None:
    if str(compliance.get("status","")).upper() != "PASS":
        fails=[]
        for c in (compliance.get("checks") or []):
            if isinstance(c, dict) and str(c.get("status","")).upper()=="FAIL":
                fails.append(str(c.get("code") or c.get("message") or "FAIL"))
        raise ValueError("Compliance FAIL: " + ", ".join(fails[:20]))

def evaluate_and_require(project_id: int, context: dict) -> dict:
    comp = evaluate_compliance(project_id=int(project_id), context=context or {})
    require_pass(comp)
    return comp
