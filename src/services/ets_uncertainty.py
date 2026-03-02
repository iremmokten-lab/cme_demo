from __future__ import annotations

import json, math
from datetime import datetime, timezone
from typing import List, Dict, Any

from src.db.session import db
from src.db.ets_compliance_models import ETSUncertaintyAssessment

def sqrt_sum_squares(errors: List[float]) -> float:
    return math.sqrt(sum((float(e) ** 2) for e in (errors or [])))

def record_uncertainty(company_id: int, year: int, errors: List[float], meta: Dict[str, Any] | None = None) -> ETSUncertaintyAssessment:
    val = sqrt_sum_squares(errors)
    percent = float(val)
    payload = {"errors": [float(e) for e in (errors or [])], "meta": meta or {}, "computed": percent}
    with db() as s:
        row = ETSUncertaintyAssessment(company_id=int(company_id), year=int(year), method="sqrt_sum_squares", assessment_json=json.dumps(payload, ensure_ascii=False), result_percent=percent, created_at=datetime.now(timezone.utc))
        s.add(row)
        s.commit()
        return row
