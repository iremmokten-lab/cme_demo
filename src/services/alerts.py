from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, List

from sqlalchemy import desc, select

from src.db.models import Alert, CalculationSnapshot, Project
from src.db.session import db
from src.services.projects import require_company_id


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


def _extract_compliance_counts(results: dict) -> tuple[int, int]:
    checks = results.get("compliance_checks") if isinstance(results, dict) else []
    if not isinstance(checks, list):
        return 0, 0
    fail = 0
    warn = 0
    for c in checks:
        stt = str((c or {}).get("status", "") or "").lower().strip()
        if stt == "fail":
            fail += 1
        elif stt in ("warn", "warning"):
            warn += 1
    return int(fail), int(warn)


def generate_alerts_for_snapshot(company_id: int, snapshot: CalculationSnapshot) -> List[dict]:
    results = _read_results_json(snapshot)
    fail, warn = _extract_compliance_counts(results)

    out: List[dict] = []

    if fail > 0:
        out.append(
            {
                "alert_type": "compliance_fail",
                "severity": "critical",
                "title": "Uyum Kontrolleri: FAIL",
                "message": f"Snapshot #{snapshot.id} için {fail} adet FAIL kontrolü var.",
                "meta": {"snapshot_id": int(snapshot.id), "fail": fail, "warn": warn},
            }
        )
    elif warn > 0:
        out.append(
            {
                "alert_type": "compliance_warn",
                "severity": "warn",
                "title": "Uyum Kontrolleri: WARN",
                "message": f"Snapshot #{snapshot.id} için {warn} adet WARN kontrolü var.",
                "meta": {"snapshot_id": int(snapshot.id), "fail": fail, "warn": warn},
            }
        )

    qa_flags = results.get("qa_flags") if isinstance(results, dict) else []
    if isinstance(qa_flags, list) and len(qa_flags) > 0:
        out.append(
            {
                "alert_type": "data_quality",
                "severity": "warn",
                "title": "Veri Kalitesi: QA Bayrakları",
                "message": f"Snapshot #{snapshot.id} için {len(qa_flags)} adet QA flag var.",
                "meta": {"snapshot_id": int(snapshot.id), "qa_flags": qa_flags[:50]},
            }
        )

    return out


def upsert_alerts(company_id: int, snapshot: CalculationSnapshot) -> int:
    alerts = generate_alerts_for_snapshot(company_id, snapshot)
    if not alerts:
        return 0

    created = 0
    with db() as s:
        for a in alerts:
            atype = str(a.get("alert_type", "generic"))
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
                    project_id=int(getattr(snapshot, "project_id", 0) or 0),
                    snapshot_id=int(snapshot.id),
                    alert_type=atype,
                    severity=str(a.get("severity", "warn")),
                    title=str(a.get("title", "")),
                    message=str(a.get("message", "")),
                    status="open",
                    meta_json=_safe_json(a.get("meta", {}) or {}),
                    created_at=_utcnow(),
                )
                s.add(obj)
                created += 1
        s.commit()
    return int(created)


def list_open_alerts_for_user(user: Any, limit: int = 200) -> List[Alert]:
    cid = require_company_id(user)
    role = str(getattr(user, "role", "") or "").lower()

    with db() as s:
        q = select(Alert).where(Alert.company_id == int(cid), Alert.status == "open")
        if role.startswith("client") or role.startswith("verifier"):
            proj_ids = s.execute(select(Project.id).where(Project.company_id == int(cid))).scalars().all()
            if not proj_ids:
                return []
            shared_snap_ids = (
                s.execute(
                    select(CalculationSnapshot.id).where(
                        CalculationSnapshot.project_id.in_(proj_ids),
                        CalculationSnapshot.shared_with_client == True,  # noqa: E712
                    )
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
    role = str(getattr(user, "role", "") or "").lower()
    if not role.startswith("consult"):
        raise PermissionError("Alert kapatma sadece danışman rolünde açıktır.")

    with db() as s:
        a = s.get(Alert, int(alert_id))
        if not a or int(a.company_id) != int(cid):
            raise PermissionError("Alert bulunamadı veya erişim yok.")
        a.status = "resolved"
        a.resolved_at = _utcnow()
        a.resolved_by_user_id = int(getattr(user, "id", 0) or 0) or None
        s.add(a)
        s.commit()
        s.refresh(a)
        return a
