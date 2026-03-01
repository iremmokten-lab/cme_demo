from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yaml
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from src.db.models import DatasetUpload, Facility, MonitoringPlan, Project
from src.services.storage_backend import LocalStorageBackend

SPEC_DIR = Path("./spec")


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def _safe_read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def load_yaml(path: Path) -> dict:
    return yaml.safe_load(_safe_read_text(path)) or {}


def load_json(path: Path) -> dict:
    return json.loads(_safe_read_text(path))


def list_spec_files() -> List[Path]:
    if not SPEC_DIR.exists():
        return []
    return sorted([p for p in SPEC_DIR.glob("*.yaml") if p.is_file()])


def list_specs() -> List[dict]:
    specs: List[dict] = []
    for p in list_spec_files():
        try:
            specs.append(load_yaml(p))
        except Exception:
            continue
    # stable order
    specs.sort(key=lambda s: str(s.get("spec_id", "")))
    return specs


def get_spec(spec_id: str) -> dict:
    sid = (spec_id or "").strip()
    for s in list_specs():
        if str(s.get("spec_id", "")).strip() == sid:
            return s
    raise KeyError(f"Spec bulunamadı: {spec_id}")


def load_data_dictionary() -> dict:
    p_json = SPEC_DIR / "data_dictionary.json"
    if p_json.exists():
        return load_json(p_json)
    p_md = SPEC_DIR / "data_dictionary.md"
    if p_md.exists():
        return {"version": "unknown", "markdown": _safe_read_text(p_md)}
    return {"version": "missing", "datasets": [], "config_keys": []}


def _load_upload_bytes(u: DatasetUpload) -> bytes:
    # Prefer DB bytes, else storage uri (local backend by default)
    if getattr(u, "content_bytes", None):
        return u.content_bytes or b""
    if getattr(u, "content_b64", None):
        try:
            import base64
            return base64.b64decode(u.content_b64 or "")
        except Exception:
            return b""
    uri = getattr(u, "storage_uri", "") or ""
    if uri:
        backend = LocalStorageBackend()
        try:
            return backend.get_bytes(uri)
        except Exception:
            return b""
    return b""


def _read_csv_columns(data: bytes) -> Tuple[set[str], Optional[str]]:
    if not data:
        return set(), "Dosya içeriği okunamadı."
    try:
        df = pd.read_csv(pd.io.common.BytesIO(data))
        cols = {_norm(c) for c in df.columns}
        return cols, None
    except Exception as e:
        return set(), f"CSV okunamadı: {e}"


def get_latest_uploads(session: Session, project_id: int) -> Dict[str, DatasetUpload]:
    rows = session.execute(
        select(DatasetUpload)
        .where(DatasetUpload.project_id == project_id)
        .order_by(desc(DatasetUpload.id))
    ).scalars().all()

    latest: Dict[str, DatasetUpload] = {}
    for r in rows:
        dtype = _norm(getattr(r, "dataset_type", ""))
        if dtype and dtype not in latest:
            latest[dtype] = r
    return latest


def _get_facility(session: Session, project_id: int) -> Optional[Facility]:
    p = session.get(Project, project_id)
    if not p:
        return None
    if getattr(p, "facility_id", None):
        return session.get(Facility, p.facility_id)
    return None


def _get_monitoring_plan(session: Session, project_id: int) -> Optional[MonitoringPlan]:
    # Monitoring plan may be stored per project; take latest by id
    rows = session.execute(
        select(MonitoringPlan)
        .where(MonitoringPlan.project_id == project_id)
        .order_by(desc(MonitoringPlan.id))
        .limit(1)
    ).scalars().all()
    return rows[0] if rows else None


def _resolve_dataset_column_from_internal_path(internal_path: str) -> Optional[str]:
    # "energy.fuel_type" -> "fuel_type"
    s = (internal_path or "").strip()
    if not s:
        return None
    if "." in s:
        return s.split(".")[-1].strip()
    return s.strip()


@dataclass
class MappingCheck:
    spec_id: str
    field_key: str
    label_tr: str
    required: str
    source: str
    internal_path: str
    status: str  # OK / MISSING / UNKNOWN
    reason_tr: str = ""


def assess_project_against_spec(session: Session, project_id: int, spec_id: str) -> List[MappingCheck]:
    spec = get_spec(spec_id)
    fields = spec.get("fields", []) or []
    uploads = get_latest_uploads(session, project_id)
    facility = _get_facility(session, project_id)
    mp = _get_monitoring_plan(session, project_id)

    # Load dataset columns lazily
    dataset_cols: Dict[str, Tuple[set[str], Optional[str]]] = {}

    checks: List[MappingCheck] = []

    for f in fields:
        field_key = str(f.get("key", "")).strip()
        label_tr = str(f.get("label_tr", field_key)).strip()
        required = str(f.get("required", "")).strip() or "SHOULD"
        source = str(f.get("source", "")).strip()
        internal_path = str(f.get("internal_path", "")).strip()
        status = "UNKNOWN"
        reason = ""

        # Facility/config/monitoring plan presence checks (Step-1: only structural)
        if source in ("facility", "config", "methodology", "snapshot", "calculation", "dq_engine", "qa_qc", "uncertainty", "factor_registry"):
            # We don't compute values in Step-1; we just verify that a container exists.
            # Facility exists?
            if source == "facility":
                if facility:
                    status = "OK"
                else:
                    status = "MISSING"
                    reason = "Bu proje için tesis (facility) kaydı bulunamadı."
            elif source == "monitoring_plan":
                if mp:
                    status = "OK"
                else:
                    status = "MISSING"
                    reason = "Monitoring plan kaydı bulunamadı."
            else:
                # Unknown at step-1; will be validated in Step-2/3 (strict validators + calc)
                status = "UNKNOWN"
                reason = "Bu alan, hesap/rapor üretimi sırasında doğrulanır (Adım-2/3)."
        # Dataset checks
        if source.startswith("dataset."):
            dtype = _norm(source.split(".", 1)[1])
            u = uploads.get(dtype)
            if not u:
                status = "MISSING"
                reason = f"'{dtype}' dataset'i bu projeye yüklenmemiş."
            else:
                if dtype not in dataset_cols:
                    data = _load_upload_bytes(u)
                    cols, err = _read_csv_columns(data)
                    dataset_cols[dtype] = (cols, err)
                cols, err = dataset_cols[dtype]
                if err:
                    status = "UNKNOWN"
                    reason = err
                else:
                    col = _resolve_dataset_column_from_internal_path(internal_path)
                    if col is None:
                        status = "OK"
                    else:
                        if _norm(col) in cols:
                            status = "OK"
                        else:
                            status = "MISSING"
                            reason = f"CSV içinde '{col}' kolonu bulunamadı."
        checks.append(
            MappingCheck(
                spec_id=spec_id,
                field_key=field_key,
                label_tr=label_tr,
                required=required,
                source=source,
                internal_path=internal_path,
                status=status,
                reason_tr=reason,
            )
        )

    # Stable order: MUST first, then CONDITIONAL/DEFINTIVE_ONLY/SHOULD
    prio = {"MUST": 0, "CONDITIONAL": 1, "DEFINTIVE_ONLY": 2, "SHOULD": 3}
    checks.sort(key=lambda c: (prio.get(c.required, 9), c.status != "MISSING", c.field_key))
    return checks
