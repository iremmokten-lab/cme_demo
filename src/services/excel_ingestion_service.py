
# -*- coding: utf-8 -*-
"""Faz-0 Excel ingestion → CSV storage + DatasetUpload kaydı + Data Quality.

Amaç:
- Excel şablonlarını kullanıcı yükler
- Sistem kolonları normalize eder ve şema doğrular
- CSV bytes üretir (UTF-8) ve storage/uploads altına kaydeder (sha dedup)
- DatasetUpload tablosuna kayıt açar (audit trail için)

Not:
- Core pipeline CSV ingest ile zaten çalışıyor (src/ui/consultant.py).
- Burada Excel'i CSV'ye çevirip aynı formatı üretiriz; engine değişmeden çalışır.
"""

from __future__ import annotations

import io
import json
from typing import Any, Dict, Tuple

import pandas as pd

from src.connectors.excel_connector import load_excel, normalize_headers, compute_dataset_hash
from src.db.models import DatasetUpload
from src.db.session import db
from src.mrv.lineage import sha256_bytes
from src.services.ingestion import data_quality_assess, validate_csv
from src.services.storage import UPLOAD_DIR, write_bytes


def _safe_name(name: str) -> str:
    name = (name or "").strip().replace(" ", "_")
    keep = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.()"
    out = "".join(ch for ch in name if ch in keep)
    return out or "upload"


def _save_upload_dedup(project_id: int, dataset_type: str, file_name: str, file_bytes: bytes) -> Tuple[str, str]:
    sha = sha256_bytes(file_bytes)
    safe = _safe_name(file_name) or f"{dataset_type}.csv"
    fp = UPLOAD_DIR / f"project_{project_id}" / f"{dataset_type}_{sha[:10]}_{safe}"
    fp.parent.mkdir(parents=True, exist_ok=True)
    write_bytes(fp, file_bytes)
    return str(fp.as_posix()), sha


def _map_to_core_csv(dataset_type: str, df: pd.DataFrame) -> pd.DataFrame:
    """Excel şemasını core CSV şemasına dönüştürür.
    - energy: fuel_quantity/fuel_unit zorunlu
    - production: product_code, quantity, unit
    - facility/cbam_products/bom_precursors: faz-0 master data (core hesapta direkt kullanılmayabilir)
    """
    dtype = (dataset_type or "").strip().lower()

    d = normalize_headers(df)

    if dtype == "energy":
        # legacy support: quantity/unit → fuel_quantity/fuel_unit
        if "fuel_quantity" not in d.columns and "quantity" in d.columns:
            d = d.rename(columns={"quantity": "fuel_quantity"})
        if "fuel_unit" not in d.columns and "unit" in d.columns:
            d = d.rename(columns={"unit": "fuel_unit"})
        # accept 'fuel' column as fuel_type
        if "fuel_type" not in d.columns and "fuel" in d.columns:
            d = d.rename(columns={"fuel": "fuel_type"})
        # keep only relevant columns + extras
        return d

    if dtype == "production":
        if "product_code" not in d.columns and "product" in d.columns:
            d = d.rename(columns={"product": "product_code"})
        if "product_code" not in d.columns and "product_sku" in d.columns:
            d = d.rename(columns={"product_sku": "product_code"})
        return d

    if dtype == "materials":
        # materials already a known core type in consultant panel (precursor) - keep as-is
        return d

    # master datasets
    if dtype == "cbam_products":
        if "product_code" not in d.columns and "product_sku" in d.columns:
            d = d.rename(columns={"product_sku": "product_code"})
        return d

    if dtype == "bom_precursors":
        if "product_code" not in d.columns and "product_sku" in d.columns:
            d = d.rename(columns={"product_sku": "product_code"})
        if "precursor_code" not in d.columns and "precursor_sku" in d.columns:
            d = d.rename(columns={"precursor_sku": "precursor_code"})
        return d

    return d


def ingest_excel_to_datasetupload(
    *,
    project_id: int,
    dataset_type: str,
    xlsx_bytes: bytes,
    original_filename: str,
    uploaded_by_user_id: int | None = None,
) -> Dict[str, Any]:
    # Read + schema validate + deterministic hash
    bio = io.BytesIO(xlsx_bytes)
    result = load_excel(bio, dataset_type)
    df = result["dataframe"]
    df = _map_to_core_csv(dataset_type, df)

    # Convert to CSV bytes
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    # Core validator where applicable
    dtype = dataset_type.strip().lower()
    core_errors = []
    if dtype in ("energy", "production", "materials"):
        core_errors = validate_csv(dtype, df)

    dq_score, dq_report = data_quality_assess(dtype, df)

    storage_uri, sha = _save_upload_dedup(
        project_id=int(project_id),
        dataset_type=str(dtype),
        file_name=original_filename.replace(".xlsx", ".csv"),
        file_bytes=csv_bytes,
    )

    content_hash = compute_dataset_hash(df)

    meta = {
        "source": "excel",
        "xlsx_filename": original_filename,
        "converted_to": "csv",
        "core_validation_errors": core_errors,
    }

    with db() as s:
        du = DatasetUpload(
            project_id=int(project_id),
            dataset_type=str(dtype),
            original_filename=str(original_filename),
            storage_uri=str(storage_uri),
            sha256=str(sha),
            content_hash=str(content_hash),
            schema_version="v1",
            validated=(len(core_errors) == 0),
            data_quality_score=int(dq_score),
            data_quality_report_json=json.dumps(dq_report, ensure_ascii=False),
            meta_json=json.dumps(meta, ensure_ascii=False),
            uploaded_by_user_id=uploaded_by_user_id,
        )
        s.add(du)
        s.commit()
        s.refresh(du)

    return {
        "dataset_upload_id": int(du.id),
        "dataset_type": str(dtype),
        "storage_uri": str(storage_uri),
        "sha256": str(sha),
        "content_hash": str(content_hash),
        "validated": bool(len(core_errors) == 0),
        "core_validation_errors": core_errors,
        "data_quality_score": int(dq_score),
        "data_quality_report": dq_report,
        "rows": int(len(df)),
    }
