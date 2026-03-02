from __future__ import annotations

import json
from hashlib import sha256
from datetime import datetime, timezone
from typing import Dict, Any

from src.db.session import db
from src.db.ets_compliance_models import ETSMonitoringPlan

def _sha(obj: Dict[str, Any]) -> str:
    b = json.dumps(obj or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(b).hexdigest()

def upsert_monitoring_plan(company_id: int, year: int, plan: Dict[str, Any], *, facility_id: int | None = None) -> ETSMonitoringPlan:
    h = _sha(plan)
    with db() as s:
        latest = (
            s.query(ETSMonitoringPlan)
            .filter(ETSMonitoringPlan.company_id==int(company_id), ETSMonitoringPlan.year==int(year))
            .order_by(ETSMonitoringPlan.version.desc())
            .first()
        )
        ver = 1 if not latest else int(latest.version) + 1
        if latest:
            latest.status = "superseded"
        row = ETSMonitoringPlan(company_id=int(company_id), year=int(year), version=ver, facility_id=facility_id, plan_json=json.dumps(plan, ensure_ascii=False), plan_hash=h, created_at=datetime.now(timezone.utc))
        s.add(row)
        s.commit()
        return row

def get_active_monitoring_plan(company_id: int, year: int) -> ETSMonitoringPlan | None:
    with db() as s:
        return (
            s.query(ETSMonitoringPlan)
            .filter(ETSMonitoringPlan.company_id==int(company_id), ETSMonitoringPlan.year==int(year), ETSMonitoringPlan.status=="active")
            .order_by(ETSMonitoringPlan.version.desc())
            .first()
        )
