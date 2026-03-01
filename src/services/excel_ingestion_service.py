# -*- coding: utf-8 -*-
"""Faz-0: Excel Import → Dataset storage + DB kayıt + (opsiyonel) Snapshot üretimi.

Bu servis, Excel Import Center UI tarafından kullanılır.

Akış:
1) Excel (.xlsx) yükle
2) Şema doğrula + deterministik dataset_hash üret
3) Data Quality raporu üret
4) CSV bytes olarak storage backend'e yaz (local/S3)
5) DatasetUpload tablosuna kayıt aç (sha256=file, content_hash=dataset_hash)

Not:
- Engine run_full mevcut orchestration'dır. Bu servis snapshot üretmez; UI isterse run_full çağırır.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.db.models import DatasetUpload
from src.db.session import db
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.mrv.lineage import sha256_bytes
from src.services.ingestion import data_quality_assess
from src.services.storage_backend import get_storage_backend

from src.connectors.excel_connector import compute_dataset_hash


@dataclass(frozen=True)
class ExcelImportResult:
    upload_id: int
    dataset_type: str
    storage_uri: str
    sha256_file: str
    dataset_hash: str
    data_quality_score: int
    data_quality_report: dict


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def save_excel_dataset(
    *,
    project_id: int,
    dataset_type: str,
    df: pd.DataFrame,
    original_filename: str,
    user: Any | None,
    schema_version: str = "excel_v1",
) -> ExcelImportResult:
    dataset_hash = compute_dataset_hash(df)

    score, dq_report = data_quality_assess(dataset_type, df)

    csv_bytes = dataframe_to_csv_bytes(df)
    sha_file = sha256_bytes(csv_bytes)

    backend = get_storage_backend()
    safe_name = (original_filename or f"{dataset_type}.xlsx").replace("/", "_").replace("\\", "_")
    key = f"project_{int(project_id)}/datasets/{dataset_type}/{sha_file[:10]}_{safe_name}.csv"
    loc = backend.put_bytes(key, csv_bytes, content_type="text/csv")

    meta = {
        "source": "excel_import_center",
        "original_filename": str(original_filename or ""),
        "dataset_hash": dataset_hash,
        "file_sha256": sha_file,
        "storage_backend": loc.backend,
    }

    with db() as s:
        du = DatasetUpload(
            project_id=int(project_id),
            dataset_type=str(dataset_type),
            schema_version=str(schema_version),
            original_filename=str(original_filename or f"{dataset_type}.xlsx"),
            storage_uri=str(loc.uri),
            sha256=str(sha_file),
            content_hash=str(dataset_hash),
            validated=True,
            data_quality_score=int(score),
            data_quality_report_json=json.dumps(dq_report, ensure_ascii=False),
            meta_json=json.dumps(meta, ensure_ascii=False),
            uploaded_by_user_id=int(getattr(user, "id", None)) if user is not None and getattr(user, "id", None) is not None else None,
        )
        # Küçük dosyalar için DB kopyası (Streamlit Cloud uyumlu). Büyükse sadece storage_uri.
        du.content_bytes = csv_bytes if len(csv_bytes) <= 2_000_000 else None
        s.add(du)
        s.commit()
        s.refresh(du)

    try:
        append_audit(
            "excel_dataset_imported",
            {
                "project_id": int(project_id),
                "dataset_type": str(dataset_type),
                "upload_id": int(du.id),
                "storage_uri": str(loc.uri),
                "sha256_file": sha_file,
                "dataset_hash": dataset_hash,
                "dq_score": int(score),
            },
            user_id=int(getattr(user, "id", None)) if user is not None and getattr(user, "id", None) is not None else None,
            company_id=infer_company_id_for_user(user) if user is not None else None,
            entity_type="dataset_upload",
            entity_id=str(int(du.id)),
        )
    except Exception:
        pass

    return ExcelImportResult(
        upload_id=int(du.id),
        dataset_type=str(dataset_type),
        storage_uri=str(loc.uri),
        sha256_file=str(sha_file),
        dataset_hash=str(dataset_hash),
        data_quality_score=int(score),
        data_quality_report=dq_report,
    )
