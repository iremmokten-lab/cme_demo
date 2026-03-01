# -*- coding: utf-8 -*-
"""Faz-3 ERP Integration Service.

Bu servis şunları yapar:
  - ERPConnection / ERPMapping kayıtlarını yönetir
  - "Sync" çalıştırır (CSV/JSON upload veya REST fetch)
  - Her çalıştırma için ERPJobRun kaydı açar (audit)
  - Sonuç olarak DatasetUpload oluşturur

Güvenlik yaklaşımı:
  - Secret DB'de saklanmaz.
  - Kullanıcı secret_ref girer.
  - Gerçek secret Streamlit Cloud secrets/env üzerinden okunur:
      ERP_SECRET_<secret_ref>
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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
    if not ref:
        return ""
    key = f"ERP_SECRET_{ref}"
    return os.getenv(key, "")


def _auth_headers(auth_type: str, secret: str) -> Dict[str, str]:
    t = (auth_type or "none").strip().lower()
    if t == "none" or not secret:
        return {}
    if t == "api_key":
        # secret format: "Header-Name: value" or just value (defaults to X-API-Key)
        if ":" in secret:
            h, v = secret.split(":", 1)
            return {h.strip(): v.strip()}
        return {"X-API-Key": secret}
    if t == "bearer":
        return {"Authorization": f"Bearer {secret}"}
    if t == "basic":
        # secret: "username:password" → Authorization: Basic base64
        import base64

        token = base64.b64encode(secret.encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}
    return {}


class ERPSyncService:
    def __init__(self, s: Session, *, company_id: int, user_id: Optional[int] = None):
        self.s = s
        self.company_id = int(company_id)
        self.user_id = int(user_id) if user_id is not None else None

    # ----------------------
    # Connection CRUD
    # ----------------------
    def list_connections(self) -> List[ERPConnection]:
        return (
            self.s.query(ERPConnection)
            .filter(ERPConnection.company_id == self.company_id)
            .order_by(ERPConnection.created_at.desc())
            .all()
        )

    def get_connection(self, connection_id: int) -> Optional[ERPConnection]:
        c = self.s.get(ERPConnection, int(connection_id))
        if not c or int(c.company_id) != self.company_id:
            return None
        return c

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
            c = self.get_connection(int(connection_id))
            if not c:
                raise ValueError("Bağlantı bulunamadı.")
        else:
            c = ERPConnection(company_id=self.company_id)
            self.s.add(c)

        c.name = name
        c.vendor = (vendor or "CUSTOM").strip().upper()
        c.mode = (mode or "csv_upload").strip().lower()
        c.base_url = (base_url or "").strip()
        c.auth_type = (auth_type or "none").strip().lower()
        c.secret_ref = (secret_ref or "").strip()
        c.description = (description or "").strip()
        c.is_active = bool(is_active)
        c.updated_at = utcnow()

        self.s.flush()
        return c

    # ----------------------
    # Mapping CRUD
    # ----------------------
    def get_mapping(self, connection_id: int, dataset_type: str) -> Optional[ERPMapping]:
        return (
            self.s.query(ERPMapping)
            .filter(
                ERPMapping.connection_id == int(connection_id),
                ERPMapping.dataset_type == str(dataset_type).strip().lower(),
            )
            .first()
        )

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
        m = self.get_mapping(int(connection_id), dtype)
        if not m:
            m = ERPMapping(connection_id=int(connection_id), dataset_type=dtype)
            self.s.add(m)

        # validate JSON strings
        try:
            mapping_obj = json.loads(mapping_json or "{}")
        except Exception:
            raise ValueError("Mapping JSON geçersiz.")
        try:
            transform_obj = json.loads(transform_json or "{}")
        except Exception:
            raise ValueError("Transform JSON geçersiz.")

        m.mapping_json = json.dumps(mapping_obj, ensure_ascii=False)
        m.transform_json = json.dumps(transform_obj, ensure_ascii=False)
        m.enabled = bool(enabled)
        m.updated_at = utcnow()
        self.s.flush()
        return m

    # ----------------------
    # Sync execution
    # ----------------------
    def sync_from_upload(
        self,
        *,
        connection_id: int,
        project_id: int,
        dataset_type: str,
        file_name: str,
        file_bytes: bytes,
        file_format: str,
    ) -> SyncResult:
        c = self.get_connection(int(connection_id))
        if not c or not c.is_active:
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
            mapping = json.loads(m.mapping_json) if (m and m.mapping_json) else {}
            transform = json.loads(m.transform_json) if (m and m.transform_json) else {}
            enabled = bool(m.enabled) if m else True
            if not enabled:
                raise ValueError("Bu dataset_type için mapping devre dışı.")

            res = apply_mapping_and_ingest(
                project_id=int(project_id),
                dataset_type=str(dataset_type),
                df=df,
                mapping=mapping,
                transform=transform,
                source_name=file_name,
                meta={
                    "erp": {
                        "connection_id": int(c.id),
                        "connection_name": c.name,
                        "vendor": c.vendor,
                        "job_run_id": int(job.id),
                        "mode": "upload",
                        "file_format": fmt,
                    }
                },
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

    def sync_from_rest(
        self,
        *,
        connection_id: int,
        project_id: int,
        dataset_type: str,
        endpoint_path: str,
        params_json: str,
    ) -> SyncResult:
        c = self.get_connection(int(connection_id))
        if not c or not c.is_active:
            raise ValueError("Bağlantı bulunamadı veya pasif.")
        if (c.mode or "").lower() != "rest":
            raise ValueError("Bu bağlantı REST modunda değil.")
        if not c.base_url:
            raise ValueError("base_url boş olamaz.")

        job = ERPJobRun(company_id=self.company_id, connection_id=c.id, project_id=int(project_id), status="running")
        self.s.add(job)
        self.s.flush()

        try:
            try:
                params = json.loads(params_json or "{}")
                if not isinstance(params, dict):
                    raise ValueError
            except Exception:
                raise ValueError("Params JSON geçersiz (nesne olmalı).")

            secret = _env_secret(c.secret_ref)
            headers = _auth_headers(c.auth_type, secret)

            url = c.base_url.rstrip("/") + "/" + (endpoint_path or "").lstrip("/")
            payload = http_fetch_json(url=url, headers=headers, params=params)

            # normalize payload to df
            df = pd.DataFrame(payload.get("value", payload)) if isinstance(payload, dict) else pd.DataFrame(payload)

            m = self.get_mapping(c.id, dataset_type)
            mapping = json.loads(m.mapping_json) if (m and m.mapping_json) else {}
            transform = json.loads(m.transform_json) if (m and m.transform_json) else {}
            enabled = bool(m.enabled) if m else True
            if not enabled:
                raise ValueError("Bu dataset_type için mapping devre dışı.")

            res = apply_mapping_and_ingest(
                project_id=int(project_id),
                dataset_type=str(dataset_type),
                df=df,
                mapping=mapping,
                transform=transform,
                source_name=f"rest_{c.name}_{dataset_type}",
                meta={
                    "erp": {
                        "connection_id": int(c.id),
                        "connection_name": c.name,
                        "vendor": c.vendor,
                        "job_run_id": int(job.id),
                        "mode": "rest",
                        "url": url,
                        "params": params,
                    }
                },
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
