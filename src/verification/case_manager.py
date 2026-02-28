from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from sqlalchemy import select

from src.db.models import VerificationCase, VerificationFinding, CalculationSnapshot, Project
from src.db.session import db


def _safe_json_loads(s: str, default):
    try:
        return json.loads(s or "")
    except Exception:
        return default


def create_case(
    *,
    project_id: int,
    facility_id: int | None,
    period_year: int,
    snapshot_id: int | None,
    title: str,
    description: str,
    created_by_user_id: int | None = None,
) -> VerificationCase:
    with db() as s:
        case = VerificationCase(
            project_id=int(project_id),
            facility_id=int(facility_id) if facility_id is not None else None,
            period_year=int(period_year),
            snapshot_id=int(snapshot_id) if snapshot_id is not None else None,
            status="open",
            title=str(title or "Verification Case"),
            description=str(description or ""),
            sampling_json=json.dumps({}, ensure_ascii=False),
            created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
        )
        s.add(case)
        s.commit()
        s.refresh(case)
        return case


def list_cases(project_id: int) -> list[VerificationCase]:
    with db() as s:
        return (
            s.execute(select(VerificationCase).where(VerificationCase.project_id == int(project_id)).order_by(VerificationCase.created_at.desc()))
            .scalars()
            .all()
        )


def add_sampling_plan(case_id: int, sampling_plan: dict, user_id: int | None = None) -> VerificationCase:
    with db() as s:
        c = s.get(VerificationCase, int(case_id))
        if not c:
            raise ValueError("Case bulunamadı.")
        c.sampling_json = json.dumps(sampling_plan or {}, ensure_ascii=False)
        s.commit()
        s.refresh(c)
        return c


def add_finding(
    *,
    case_id: int,
    severity: str,
    title: str,
    description: str,
    evidence_ref: str = "",
    corrective_action: str = "",
    action_due_date: str = "",
    created_by_user_id: int | None = None,
) -> VerificationFinding:
    with db() as s:
        f = VerificationFinding(
            case_id=int(case_id),
            severity=str(severity or "major"),
            title=str(title or "Finding"),
            description=str(description or ""),
            evidence_ref=str(evidence_ref or ""),
            corrective_action=str(corrective_action or ""),
            action_due_date=str(action_due_date or ""),
            status="open",
            created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
        )
        s.add(f)
        s.commit()
        s.refresh(f)
        return f


def close_case(case_id: int, user_id: int | None = None) -> VerificationCase:
    with db() as s:
        c = s.get(VerificationCase, int(case_id))
        if not c:
            raise ValueError("Case bulunamadı.")
        c.status = "closed"
        c.closed_at = datetime.now(timezone.utc)
        c.closed_by_user_id = int(user_id) if user_id is not None else None
        s.commit()
        s.refresh(c)
        return c


def export_case_json(case_id: int) -> dict:
    with db() as s:
        c = s.get(VerificationCase, int(case_id))
        if not c:
            raise ValueError("Case bulunamadı.")
        findings = (
            s.execute(select(VerificationFinding).where(VerificationFinding.case_id == int(case_id)).order_by(VerificationFinding.created_at.asc()))
            .scalars()
            .all()
        )

    return {
        "case": {
            "id": int(c.id),
            "project_id": int(c.project_id),
            "facility_id": int(c.facility_id) if c.facility_id is not None else None,
            "period_year": int(c.period_year) if c.period_year is not None else None,
            "snapshot_id": int(c.snapshot_id) if c.snapshot_id is not None else None,
            "status": str(c.status or ""),
            "title": str(c.title or ""),
            "description": str(c.description or ""),
            "sampling_plan": _safe_json_loads(c.sampling_json, {}),
            "created_at": str(c.created_at),
            "closed_at": str(c.closed_at) if c.closed_at else None,
        },
        "findings": [
            {
                "id": int(f.id),
                "severity": str(f.severity or ""),
                "title": str(f.title or ""),
                "description": str(f.description or ""),
                "evidence_ref": str(f.evidence_ref or ""),
                "corrective_action": str(f.corrective_action or ""),
                "action_due_date": str(f.action_due_date or ""),
                "status": str(f.status or ""),
                "created_at": str(f.created_at),
                "resolved_at": str(f.resolved_at) if f.resolved_at else None,
            }
            for f in findings
        ],
    }
