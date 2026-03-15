# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy.orm import Session

from src.connectors.erp_connector import http_fetch_json, read_csv_bytes, read_json_bytes
from src.db.erp_models import ERPConnection, ERPJobRun, ERPMapping
from src.services.erp_ingestion_service import apply_mapping_and_ingest


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SyncResult:
    job_run_id: int
    datasetuploads: List[Dict[str, Any]]
    status: str
    error: str = ""


def _env_secret(secret_ref: str) -> str:
    ref = (secret_ref or "").strip()
    return os.getenv(f"ERP_SECRET_{ref}", "") if ref else ""


def _auth_headers(auth_type: str, secret: str) -> Dict[str, str]:
    t = (auth_type or "none").strip().lower()
    if t == "none" or not secret:
        return {}
    if t == "api_key":
        if ":" in secret:
            h, v = secret.split(":", 1)
            return {h.strip(): v.strip()}
        return {"X-API-Key": secret}
    if t == "bearer":
        return {"Authorization": f"Bearer {secret}"}
    if t == "basic":
        import base64

        token = base64.b64encode(secret.encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}
    return {}


class ERPSyncService:
    def __init__(self, s: Session, *, company_id: int, user_id: Optional[int] = None):
        self.s = s
        self.company_id = int(company_id)
        self.user_id = int(user_id) if user_id is not None else None

    def _decorate_connection(self, c: ERPConnection | None) -> ERPConnection | None:
        if c is None:
            return None
        auth = {}
        cfg = {}
        try:
            auth = json.loads(getattr(c, "auth_json", "{}") or "{}")
        except Exception:
            auth = {}
        try:
            cfg = json.loads(getattr(c, "config_json", "{}") or "{}")
        except Exception:
            cfg = {}
        setattr(c, "vendor", getattr(c, "vendor", str(getattr(c, "kind", "CUSTOM") or "CUSTOM").upper()))
        default_mode = "rest" if str(getattr(c, "base_url", "") or "").strip() else "csv_upload"
        setattr(c, "mode", getattr(c, "mode", str(cfg.get("mode") or default_mode)))
        setattr(c, "auth_type", getattr(c, "auth_type", str(auth.get("auth_type") or "none")))
        setattr(c, "secret_ref", getattr(c, "secret_ref", str(auth.get("secret_ref") or getattr(c, "token_secret", "") or "")))
        setattr(c, "description", getattr(c, "description", str(cfg.get("description") or "")))
        setattr(c, "is_active", getattr(c, "is_active", str(getattr(c, "status", "active")).lower() != "inactive"))
        return c

    def _query_mapping(self, connection_id: int, dataset_type: str):
        dtype = str(dataset_type).strip().lower()
        q = self.s.query(ERPMapping).filter(ERPMapping.dataset_type == dtype)
        if hasattr(ERPMapping, "connection_id"):
            q = q.filter(ERPMapping.connection_id == int(connection_id))
        elif hasattr(ERPMapping, "project_id"):
            q = q.filter(ERPMapping.project_id == int(connection_id))
        return q

    def _decorate_mapping(self, m: ERPMapping | None) -> ERPMapping | None:
        if m is None:
            return None
        extra = {}
        try:
            extra = json.loads(getattr(m, "notes", "{}") or "{}")
        except Exception:
            extra = {}
        setattr(m, "transform_json", getattr(m, "transform_json", json.dumps(extra.get("transform") or {}, ensure_ascii=False)))
        setattr(m, "enabled", getattr(m, "enabled", str(getattr(m, "status", "approved")).lower() != "disabled"))
        return m

    def list_connections(self) -> List[ERPConnection]:
        rows = self.s.query(ERPConnection).order_by(ERPConnection.created_at.desc()).all()
        return [self._decorate_connection(c) for c in rows]

    def get_connection(self, connection_id: int) -> Optional[ERPConnection]:
        return self._decorate_connection(self.s.get(ERPConnection, int(connection_id)))

    def upsert_connection(
        self,
        *,
        connection_id: Optional[int],
        name: str,
        vendor: str,
        mode: str,
        base_url: str,
        auth_type: str,
        secret_ref: str,
        description: str,
        is_active: bool,
    ) -> ERPConnection:
        name = (name or "").strip()
        if not name:
            raise ValueError("Bağlantı adı boş olamaz.")

        if connection_id:
            c = self.s.get(ERPConnection, int(connection_id))
            if not c:
                raise ValueError("Bağlantı bulunamadı.")
        else:
            kwargs = {}
            if hasattr(ERPConnection, "company_id"):
                kwargs["company_id"] = self.company_id
            elif hasattr(ERPConnection, "project_id"):
                kwargs["project_id"] = 0
            c = ERPConnection(**kwargs)
            self.s.add(c)

        c.name = name
        if hasattr(c, "vendor"):
            c.vendor = (vendor or "CUSTOM").strip().upper()
        if hasattr(c, "mode"):
            c.mode = (mode or "csv_upload").strip().lower()
        if hasattr(c, "kind"):
            c.kind = (vendor or "custom").strip().lower()
        c.base_url = (base_url or "").strip()
        if hasattr(c, "auth_type"):
            c.auth_type = (auth_type or "none").strip().lower()
        if hasattr(c, "secret_ref"):
            c.secret_ref = (secret_ref or "").strip()
        if hasattr(c, "token_secret"):
            c.token_secret = (secret_ref or "").strip()
        if hasattr(c, "description"):
            c.description = (description or "").strip()
        if hasattr(c, "is_active"):
            c.is_active = bool(is_active)
        if hasattr(c, "status"):
            c.status = "active" if is_active else "inactive"
        if hasattr(c, "updated_at"):
            c.updated_at = utcnow()
        if hasattr(c, "auth_json"):
            c.auth_json = json.dumps({"auth_type": auth_type, "secret_ref": secret_ref}, ensure_ascii=False)
        if hasattr(c, "config_json"):
            c.config_json = json.dumps({"mode": mode, "description": description}, ensure_ascii=False)
        self.s.flush()
        return self._decorate_connection(c)

    def get_mapping(self, connection_id: int, dataset_type: str) -> Optional[ERPMapping]:
        return self._decorate_mapping(self._query_mapping(connection_id, dataset_type).first())

    def upsert_mapping(
        self,
        *,
        connection_id: int,
        dataset_type: str,
        mapping_json: str,
        transform_json: str,
        enabled: bool,
    ) -> ERPMapping:
        dtype = str(dataset_type).strip().lower()
        m = self._query_mapping(connection_id, dtype).first()
        if not m:
            kwargs = {"dataset_type": dtype}
            if hasattr(ERPMapping, "connection_id"):
                kwargs["connection_id"] = int(connection_id)
            elif hasattr(ERPMapping, "project_id"):
                kwargs["project_id"] = int(connection_id)
            m = ERPMapping(**kwargs)
            self.s.add(m)

        try:
            mapping_obj = json.loads(mapping_json or "{}")
            transform_obj = json.loads(transform_json or "{}")
        except Exception:
            raise ValueError("Mapping/Transform JSON geçersiz.")

        m.mapping_json = json.dumps(mapping_obj, ensure_ascii=False)
        if hasattr(m, "transform_json"):
            m.transform_json = json.dumps(transform_obj, ensure_ascii=False)
        if hasattr(m, "enabled"):
            m.enabled = bool(enabled)
        if hasattr(m, "status"):
            m.status = "approved" if enabled else "disabled"
        if hasattr(m, "notes"):
            m.notes = json.dumps({"transform": transform_obj}, ensure_ascii=False)
        if hasattr(m, "updated_at"):
            m.updated_at = utcnow()
        self.s.flush()
        return self._decorate_mapping(m)

    def sync_from_upload(self, *, connection_id: int, project_id: int, dataset_type: str, file_name: str, file_bytes: bytes, file_format: str) -> SyncResult:
        c = self.get_connection(int(connection_id))
        if not c or not bool(getattr(c, "is_active", True)):
            raise ValueError("Bağlantı bulunamadı veya pasif.")

        job = ERPJobRun(company_id=self.company_id, connection_id=c.id, project_id=int(project_id), status="running")
        self.s.add(job)
        self.s.flush()

        try:
            fmt = (file_format or "csv").strip().lower()
            if fmt == "csv":
                df = read_csv_bytes(file_bytes)
            elif fmt == "json":
                df = read_json_bytes(file_bytes)
            else:
                raise ValueError("Desteklenmeyen format. (csv/json)")

            m = self.get_mapping(c.id, dataset_type)
            mapping = json.loads(getattr(m, "mapping_json", "{}") or "{}") if m else {}
            transform = json.loads(getattr(m, "transform_json", "{}") or "{}") if m else {}
            enabled = bool(getattr(m, "enabled", True)) if m else True
            if not enabled:
                raise ValueError("Bu dataset_type için mapping devre dışı.")

            res = apply_mapping_and_ingest(
                project_id=int(project_id),
                dataset_type=str(dataset_type),
                df=df,
                mapping=mapping,
                transform=transform,
                source_name=file_name,
                meta={"erp": {"connection_id": int(c.id), "connection_name": c.name, "vendor": getattr(c, "vendor", "CUSTOM"), "job_run_id": int(job.id), "mode": "upload", "file_format": fmt}},
                uploaded_by_user_id=self.user_id,
            )
            job.status = "success"
            job.summary_json = json.dumps({"datasetuploads": [res]}, ensure_ascii=False)
            job.finished_at = utcnow()
            self.s.flush()
            return SyncResult(job_run_id=int(job.id), datasetuploads=[res], status="success")
        except Exception as e:
            job.status = "failed"
            job.error_text = str(e)
            job.finished_at = utcnow()
            self.s.flush()
            return SyncResult(job_run_id=int(job.id), datasetuploads=[], status="failed", error=str(e))

    def sync_from_rest(self, *, connection_id: int, project_id: int, dataset_type: str, endpoint_path: str, params_json: str) -> SyncResult:
        c = self.get_connection(int(connection_id))
        if not c or not bool(getattr(c, "is_active", True)):
            raise ValueError("Bağlantı bulunamadı veya pasif.")
        if str(getattr(c, "mode", "csv_upload")).lower() != "rest":
            raise ValueError("Bu bağlantı REST modunda değil.")
        if not c.base_url:
            raise ValueError("base_url boş olamaz.")

        job = ERPJobRun(company_id=self.company_id, connection_id=c.id, project_id=int(project_id), status="running")
        self.s.add(job)
        self.s.flush()

        try:
            params = json.loads(params_json or "{}")
            if not isinstance(params, dict):
                raise ValueError("Params JSON geçersiz (nesne olmalı).")
            secret = _env_secret(str(getattr(c, "secret_ref", "") or ""))
            headers = _auth_headers(str(getattr(c, "auth_type", "none") or "none"), secret)
            url = c.base_url.rstrip("/") + "/" + (endpoint_path or "").lstrip("/")
            payload = http_fetch_json(url=url, headers=headers, params=params)
            df = pd.DataFrame(payload.get("value", payload)) if isinstance(payload, dict) else pd.DataFrame(payload)

            m = self.get_mapping(c.id, dataset_type)
            mapping = json.loads(getattr(m, "mapping_json", "{}") or "{}") if m else {}
            transform = json.loads(getattr(m, "transform_json", "{}") or "{}") if m else {}
            enabled = bool(getattr(m, "enabled", True)) if m else True
            if not enabled:
                raise ValueError("Bu dataset_type için mapping devre dışı.")

            res = apply_mapping_and_ingest(
                project_id=int(project_id),
                dataset_type=str(dataset_type),
                df=df,
                mapping=mapping,
                transform=transform,
                source_name=f"rest_{c.name}_{dataset_type}",
                meta={"erp": {"connection_id": int(c.id), "connection_name": c.name, "vendor": getattr(c, "vendor", "CUSTOM"), "job_run_id": int(job.id), "mode": "rest", "url": url, "params": params}},
                uploaded_by_user_id=self.user_id,
            )
            job.status = "success"
            job.summary_json = json.dumps({"datasetuploads": [res]}, ensure_ascii=False)
            job.finished_at = utcnow()
            self.s.flush()
            return SyncResult(job_run_id=int(job.id), datasetuploads=[res], status="success")
        except Exception as e:
            job.status = "failed"
            job.error_text = str(e)
            job.finished_at = utcnow()
            self.s.flush()
            return SyncResult(job_run_id=int(job.id), datasetuploads=[], status="failed", error=str(e))

    def list_job_runs(self, connection_id: Optional[int] = None, limit: int = 100) -> List[ERPJobRun]:
        q = self.s.query(ERPJobRun).filter(ERPJobRun.company_id == self.company_id)
        if connection_id is not None:
            q = q.filter(ERPJobRun.connection_id == int(connection_id))
        return q.order_by(ERPJobRun.started_at.desc()).limit(int(limit)).all()
