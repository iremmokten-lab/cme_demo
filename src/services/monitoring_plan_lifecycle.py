from __future__ import annotations
import json
from typing import Any, Dict
from sqlalchemy import select
from src.db.session import db
from src.db.phase_ab_models import MonitoringPlanVersion
from src.mrv.lineage import sha256_json

def create_new_version(project_id:int, period_year:int, plan:Dict[str,Any], *, facility_id:int|None=None, user_id:int|None=None) -> MonitoringPlanVersion:
    with db() as s:
        latest = s.execute(select(MonitoringPlanVersion).where(MonitoringPlanVersion.project_id==int(project_id), MonitoringPlanVersion.period_year==int(period_year)).order_by(MonitoringPlanVersion.version.desc())).scalars().first()
        new_v = int(latest.version)+1 if latest else 1
        mp = MonitoringPlanVersion(project_id=int(project_id), facility_id=(int(facility_id) if facility_id else None),
                                   period_year=int(period_year), version=int(new_v), status="draft",
                                   plan_json=json.dumps(plan or {}, ensure_ascii=False),
                                   created_by_user_id=(int(user_id) if user_id else None))
        s.add(mp); s.commit(); s.refresh(mp); return mp

def approve(project_id:int, period_year:int, version:int) -> None:
    with db() as s:
        mp = s.execute(select(MonitoringPlanVersion).where(MonitoringPlanVersion.project_id==int(project_id), MonitoringPlanVersion.period_year==int(period_year), MonitoringPlanVersion.version==int(version))).scalars().first()
        if not mp: raise ValueError("Monitoring plan bulunamadı.")
        mp.status="approved"; s.commit()

def lock(project_id:int, period_year:int, version:int) -> None:
    with db() as s:
        mp = s.execute(select(MonitoringPlanVersion).where(MonitoringPlanVersion.project_id==int(project_id), MonitoringPlanVersion.period_year==int(period_year), MonitoringPlanVersion.version==int(version))).scalars().first()
        if not mp: raise ValueError("Monitoring plan bulunamadı.")
        mp.status="locked"; s.commit()

def get_active_plan_ref(project_id:int, period_year:int) -> dict | None:
    with db() as s:
        mp = s.execute(select(MonitoringPlanVersion).where(MonitoringPlanVersion.project_id==int(project_id), MonitoringPlanVersion.period_year==int(period_year), MonitoringPlanVersion.status.in_(["approved","locked"])).order_by(MonitoringPlanVersion.version.desc())).scalars().first()
        if not mp: return None
        plan={}
        try: plan=json.loads(mp.plan_json or "{}")
        except Exception: plan={}
        return {"monitoring_plan_version_id": int(mp.id), "period_year": int(period_year), "version": int(mp.version), "status": str(mp.status), "hash": sha256_json(plan)}
