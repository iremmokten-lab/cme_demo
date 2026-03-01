# -*- coding: utf-8 -*-
"""Faz-3 ERP → DatasetUpload ingestion.

Amaç:
  - ERP'den gelen veriyi (CSV/JSON/REST) al
  - Platformun beklediği dataset_type formatına dönüştür
  - Deterministik CSV üret
  - storage/uploads altına kaydet
  - DatasetUpload kaydı oluştur (audit trail)

Bu modül Faz-0 Excel ingestion ile aynı determinism ve audit yaklaşımını izler.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

import pandas as pd

from src.connectors.excel_connector import compute_dataset_hash, normalize_headers
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


def _apply_column_mapping(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """Kaynak kolon → hedef kolon eşlemesi."""
    if not mapping:
        return df
    df2 = df.copy()
    cols = {str(k).strip().lower(): str(v).strip().lower() for k, v in mapping.items() if str(k).strip()}
    # normalize_headers zaten lower+underscore yapar; yine de güvenli.
    df2.columns = [str(c).strip().lower() for c in df2.columns]
    rename = {}
    for src, tgt in cols.items():
        if src in df2.columns and tgt:
            rename[src] = tgt
    if rename:
        df2 = df2.rename(columns=rename)
    return df2


def _apply_simple_transforms(df: pd.DataFrame, transform: Dict[str, Any]) -> pd.DataFrame:
    """Basit dönüşümler:
    - set_defaults: {col: value}
    - multiply: {col: factor}

    (İleri seviye transformlar Faz-3 dışı; burada sade ve deterministic kalıyoruz.)
    """
    if not transform:
        return df

    df2 = df.copy()
    set_defaults = transform.get("set_defaults", {}) if isinstance(transform, dict) else {}
    multiply = transform.get("multiply", {}) if isinstance(transform, dict) else {}

    if isinstance(set_defaults, dict):
        for k, v in set_defaults.items():
            col = str(k).strip().lower()
            if col and col not in df2.columns:
                df2[col] = v
            elif col:
                df2[col] = df2[col].fillna(v)

    if isinstance(multiply, dict):
        for k, factor in multiply.items():
            col = str(k).strip().lower()
            if col in df2.columns:
                try:
                    df2[col] = pd.to_numeric(df2[col], errors="coerce") * float(factor)
                except Exception:
                    pass

    return df2


def _map_to_core_csv(dataset_type: str, df: pd.DataFrame) -> pd.DataFrame:
    """ERP'den gelen DF'yi platformun core CSV beklentisine yaklaştır.

    Burada Faz-0 ile aynı mapping mantığı korunur.
    """
    dtype = (dataset_type or "").strip().lower()
    d = normalize_headers(df)

    if dtype == "energy":
        if "fuel_quantity" not in d.columns and "quantity" in d.columns:
            d = d.rename(columns={"quantity": "fuel_quantity"})
        if "fuel_unit" not in d.columns and "unit" in d.columns:
            d = d.rename(columns={"unit": "fuel_unit"})
        if "fuel_type" not in d.columns and "fuel" in d.columns:
            d = d.rename(columns={"fuel": "fuel_type"})
        return d

    if dtype == "production":
        if "product_code" not in d.columns and "product" in d.columns:
            d = d.rename(columns={"product": "product_code"})
        if "product_code" not in d.columns and "product_sku" in d.columns:
            d = d.rename(columns={"product_sku": "product_code"})
        return d

    if dtype == "materials":
        return d

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


def ingest_df_to_datasetupload(
    *,
    project_id: int,
    dataset_type: str,
    df: pd.DataFrame,
    source_name: str,
    meta: Dict[str, Any],
    uploaded_by_user_id: int | None = None,
) -> Dict[str, Any]:
    """DF → CSV bytes → storage + DatasetUpload."""
    dtype = (dataset_type or "").strip().lower()
    df2 = _map_to_core_csv(dtype, df)

    # Deterministik CSV
    csv_bytes = df2.to_csv(index=False).encode("utf-8")

    # core validator
    core_errors: List[Dict[str, Any]] = []
    if dtype in ("energy", "production", "materials"):
        core_errors = validate_csv(dtype, df2)

    dq_score, dq_report = data_quality_assess(dtype, df2)

    storage_uri, sha = _save_upload_dedup(
        project_id=int(project_id),
        dataset_type=str(dtype),
        file_name=f"erp_{_safe_name(source_name)}.csv",
        file_bytes=csv_bytes,
    )

    content_hash = compute_dataset_hash(df2)

    meta2 = {
        "source": "erp",
        "source_name": str(source_name),
        **(meta or {}),
        "core_validation_errors": core_errors,
    }

    with db() as s:
        du = DatasetUpload(
            project_id=int(project_id),
            dataset_type=str(dtype),
            original_filename=f"erp_{source_name}",
            storage_uri=str(storage_uri),
            sha256=str(sha),
            content_hash=str(content_hash),
            schema_version="v1",
            validated=(len(core_errors) == 0),
            data_quality_score=int(dq_score),
            data_quality_report_json=json.dumps(dq_report, ensure_ascii=False),
            meta_json=json.dumps(meta2, ensure_ascii=False),
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
        "rows": int(len(df2)),
    }


def apply_mapping_and_ingest(
    *,
    project_id: int,
    dataset_type: str,
    df: pd.DataFrame,
    mapping: Dict[str, str],
    transform: Dict[str, Any],
    source_name: str,
    meta: Dict[str, Any],
    uploaded_by_user_id: int | None = None,
) -> Dict[str, Any]:
    df2 = normalize_headers(df)
    df2 = _apply_column_mapping(df2, mapping)
    df2 = _apply_simple_transforms(df2, transform)
    return ingest_df_to_datasetupload(
        project_id=project_id,
        dataset_type=dataset_type,
        df=df2,
        source_name=source_name,
        meta=meta,
        uploaded_by_user_id=uploaded_by_user_id,
    )
