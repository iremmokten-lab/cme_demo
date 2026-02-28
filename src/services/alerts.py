from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, List

from sqlalchemy import desc, select

from src.db.models import Alert, CalculationSnapshot, Project
from src.db.session import db
from src.services.projects import is_consultant, require_company_id


def _utcnow():
    return datetime.now(timezone.utc)


def _safe_json(d: dict) -> str:
    try:
        return json.dumps(d, ensure_ascii=False)
    except Exception:
        return "{}"


def _read_results_json(snapshot: CalculationSnapshot) -> dict:
    try:
        return json.loads(snapshot.results_json or "{}")
    except Exception:
        return {}


def _extract_compliance_status(results: dict) -> tuple[str, int, int]:
    checks = (results.get("compliance_checks") or []) if isinstance(results, dict) else []
    if not isinstance(checks, list):
        checks = []
    fail = 0
    warn = 0
    for c in checks:
        st = str((c or {}).get("status", "") or "").lower()
        if st == "fail":
            fail += 1
        elif st in ("warn", "warning"):
            warn += 1
    status = str((results.get("compliance") or {}).get("status", "") or "").lower().strip() or (
        "fail" if fail else ("warn" if warn else "pass")
    )
    return status, int(fail), int(warn)


def generate_alerts_for_snapshot(company_id: int, snapshot: CalculationSnapshot) -> List[dict]:
    results = _read_results_json(snapshot)
    out: List[dict] = []

    _stt, fail, warn = _extract_compliance_status(results)
    if fail > 0:
        out.append(
            {
                "alert_type": "compliance_fail",
                "severity": "critical",
                "title": "Uyum Kontrolleri: FAIL",
                "message": f"Snapshot #{snapshot.id} için {fail} adet FAIL kontrolü var.",
                "meta": {"snapshot_id": snapshot.id, "fail": fail, "warn": warn},
            }
        )
    elif warn > 0:
        out.append(
            {
                "alert_type": "compliance_warn",
                "severity": "warn",
                "title": "Uyum Kontrolleri: WARN",
                "message": f"Snapshot #{snapshot.id} için {warn} adet WARN kontrolü var.",
                "meta": {"snapshot_id": snapshot.id, "fail": fail, "warn": warn},
            }
        )

    return out


def upsert_alerts(company_id: int, snapshot: CalculationSnapshot) -> int:
    alerts = generate_alerts_for_snapshot(company_id, snapshot)
    if not alerts:
        return 0

    with db() as s:
        created = 0
        for a in alerts:
            atype = str(a.get("alert_type", ""))
            existing = (
                s.execute(
                    select(Alert)
                    .where(
                        Alert.company_id == int(company_id),
                        Alert.snapshot_id == int(snapshot.id),
                        Alert.alert_type == atype,
                        Alert.status == "open",
                    )
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if existing:
                existing.severity = str(a.get("severity", existing.severity))
                existing.title = str(a.get("title", existing.title))
                existing.message = str(a.get("message", existing.message))
                existing.meta_json = _safe_json(a.get("meta", {}) or {})
                s.add(existing)
            else:
                obj = Alert(
                    company_id=int(company_id),
                    project_id=int(snapshot.project_id) if getattr(snapshot, "project_id", None) else None,
                    snapshot_id=int(snapshot.id),
                    alert_type=atype,
                    severity=str(a.get("severity", "warn")),
                    title=str(a.get("title", "")),
                    message=str(a.get("message", "")),
                    status="open",
                    created_at=_utcnow(),
                    meta_json=_safe_json(a.get("meta", {}) or {}),
                )
                s.add(obj)
                created += 1

        s.commit()

    return int(created)


def list_open_alerts_for_user(user: Any, limit: int = 200) -> List[Alert]:
    cid = require_company_id(user)
    with db() as s:
        q = select(Alert).where(Alert.company_id == int(cid), Alert.status == "open")
        if not is_consultant(user):
            shared_snap_ids = (
                s.execute(
                    select(CalculationSnapshot.id)
                    .join(Project, Project.id == CalculationSnapshot.project_id)
                    .where(Project.company_id == int(cid), CalculationSnapshot.shared_with_client == True)  # noqa: E712
                )
                .scalars()
                .all()
            )
            if not shared_snap_ids:
                return []
            q = q.where(Alert.snapshot_id.in_(shared_snap_ids))
        return s.execute(q.order_by(desc(Alert.created_at)).limit(int(limit))).scalars().all()


def resolve_alert(user: Any, alert_id: int) -> Alert:
    cid = require_company_id(user)
    if not is_consultant(user):
        raise PermissionError("Alert kapatma sadece danışman rolünde açıktır.")
    with db() as s:
        a = s.get(Alert, int(alert_id))
        if not a or int(a.company_id) != int(cid):
            raise PermissionError("Alert bulunamadı veya erişim yok.")
        a.status = "resolved"
        a.resolved_at = _utcnow()
        try:
            a.resolved_by_user_id = int(getattr(user, "id", None) or 0) or None
        except Exception:
            a.resolved_by_user_id = None
        s.add(a)
        s.commit()
        s.refresh(a)
        return a
