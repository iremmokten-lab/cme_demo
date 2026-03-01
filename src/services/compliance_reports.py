from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import select

from src.db.models import CalculationSnapshot, Report
from src.db.session import db
from src.mrv.lineage import sha256_bytes
from src.services.storage import REPORT_DIR, write_bytes


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj or {}, ensure_ascii=False, sort_keys=True, indent=2, default=str).encode("utf-8")


def save_compliance_checks_report(
    *,
    project_id: int,
    snapshot_id: int,
    compliance_obj: Dict[str, Any],
    created_by_user_id: Optional[int] = None,
) -> Report:
    """
    Writes storage/reports/<snapshot_id>/compliance_checks.json and records a Report row.
    """
    data = _json_bytes(compliance_obj)
    sha = sha256_bytes(data)

    path = REPORT_DIR / str(int(snapshot_id)) / "compliance_checks.json"
    write_bytes(path, data)

    with db() as s:
        existing = (
            s.execute(
                select(Report)
                .where(Report.snapshot_id == int(snapshot_id))
                .where(Report.report_type == "compliance_checks")
                .order_by(Report.created_at.desc())
            )
            .scalars()
            .first()
        )
        if existing:
            existing.file_path = str(path)
            existing.file_hash = sha
            existing.meta_json = json.dumps(
                {
                    "schema": str(compliance_obj.get("schema") or "compliance_checks.v1"),
                    "overall_status": str(compliance_obj.get("overall_status") or ""),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            if created_by_user_id is not None:
                existing.created_by_user_id = int(created_by_user_id)
            s.add(existing)
            s.commit()
            s.refresh(existing)
            return existing

        r = Report(
            project_id=int(project_id),
            snapshot_id=int(snapshot_id),
            report_type="compliance_checks",
            file_path=str(path),
            file_hash=sha,
            meta_json=json.dumps(
                {
                    "schema": str(compliance_obj.get("schema") or "compliance_checks.v1"),
                    "overall_status": str(compliance_obj.get("overall_status") or ""),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
        )
        s.add(r)
        s.commit()
        s.refresh(r)
        return r


def get_latest_compliance_checks(project_id: int) -> Optional[Dict[str, Any]]:
    with db() as s:
        rep = (
            s.execute(
                select(Report)
                .where(Report.project_id == int(project_id))
                .where(Report.report_type == "compliance_checks")
                .order_by(Report.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        if not rep:
            return None
        p = Path(str(rep.file_path or ""))
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None


def get_compliance_checks_for_snapshot(snapshot_id: int) -> Optional[Dict[str, Any]]:
    with db() as s:
        rep = (
            s.execute(
                select(Report)
                .where(Report.snapshot_id == int(snapshot_id))
                .where(Report.report_type == "compliance_checks")
                .order_by(Report.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        if not rep:
            return None
        p = Path(str(rep.file_path or ""))
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None
