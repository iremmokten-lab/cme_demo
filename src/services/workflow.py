from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import select

from src.db.session import db
from src.db.models import CalculationSnapshot, DatasetUpload
from src.mrv.audit import append_audit
from src.mrv.compliance import evaluate_compliance
from src.mrv.lineage import sha256_json


def load_csv_from_uri(uri: str) -> pd.DataFrame:
    return pd.read_csv(uri)


def latest_upload(project_id: int, dataset_type: str) -> DatasetUpload | None:
    with db() as s:
        return (
            s.execute(
                select(DatasetUpload)
                .where(DatasetUpload.project_id == int(project_id), DatasetUpload.dataset_type == str(dataset_type))
                .order_by(DatasetUpload.uploaded_at.desc())
            )
            .scalars()
            .first()
        )


def _input_hashes_payload(project_id: int, energy_u: DatasetUpload, prod_u: DatasetUpload, materials_u: DatasetUpload | None) -> dict:
    # DatasetUpload modelinde sha256 alanı var (content_hash değil)
    payload = {
        "project_id": int(project_id),
        "energy": {
            "upload_id": getattr(energy_u, "id", None),
            "sha256": getattr(energy_u, "sha256", None),
            "uri": str(getattr(energy_u, "storage_uri", "") or ""),
            "original_filename": str(getattr(energy_u, "original_filename", "") or ""),
            "schema_version": str(getattr(energy_u, "schema_version", "") or ""),
        },
        "production": {
            "upload_id": getattr(prod_u, "id", None),
            "sha256": getattr(prod_u, "sha256", None),
            "uri": str(getattr(prod_u, "storage_uri", "") or ""),
            "original_filename": str(getattr(prod_u, "original_filename", "") or ""),
            "schema_version": str(getattr(prod_u, "schema_version", "") or ""),
        },
        "materials": (
            {
                "upload_id": getattr(materials_u, "id", None),
                "sha256": getattr(materials_u, "sha256", None),
                "uri": str(getattr(materials_u, "storage_uri", "") or ""),
                "original_filename": str(getattr(materials_u, "original_filename", "") or ""),
                "schema_version": str(getattr(materials_u, "schema_version", "") or ""),
            }
            if materials_u
            else None
        ),
    }
    return payload


def _compute_result_hash(
    engine_version: str,
    config: dict,
    input_hashes: dict,
    scenario: dict,
    methodology_id: int | None,
    factor_set_ref: list,
    monitoring_plan_ref: dict | None,
) -> str:
    payload = {
        "engine_version": engine_version,
        "config": config or {},
        "input_hashes": input_hashes or {},
        "scenario": scenario or {},
        "methodology_id": methodology_id,
        "factor_set_ref": factor_set_ref or [],
        "monitoring_plan_ref": monitoring_plan_ref,
    }
    return sha256_json(payload)


def _try_reuse_snapshot(project_id: int, result_hash: str) -> CalculationSnapshot | None:
    with db() as s:
        existing = (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == int(project_id), CalculationSnapshot.result_hash == str(result_hash))
                .order_by(CalculationSnapshot.created_at.desc())
            )
            .scalars()
            .first()
        )
        return existing


def list_snapshots(project_id: int) -> list[CalculationSnapshot]:
    with db() as s:
        return (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == int(project_id))
                .order_by(CalculationSnapshot.created_at.desc())
            )
            .scalars()
            .all()
        )


def get_snapshot(snapshot_id: int) -> CalculationSnapshot | None:
    with db() as s:
        return s.execute(select(CalculationSnapshot).where(CalculationSnapshot.id == int(snapshot_id))).scalar_one_or_none()


def _latest_snapshot_hash(project_id: int) -> str | None:
    with db() as s:
        sn = (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == int(project_id))
                .order_by(CalculationSnapshot.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        if not sn:
            return None
        return str(getattr(sn, "result_hash", None) or "") or None


def create_snapshot(
    *,
    project_id: int,
    engine_version: str,
    result_hash: str,
    config: dict,
    input_hashes: dict,
    results_json: dict,
    methodology_id: int | None = None,
    created_by_user_id: int | None = None,
    previous_snapshot_hash: str | None = None,
) -> CalculationSnapshot:
    with db() as s:
        snap = CalculationSnapshot(
            project_id=int(project_id),
            engine_version=str(engine_version),
            result_hash=str(result_hash),
            config_json=json.dumps(config or {}, ensure_ascii=False, sort_keys=True, default=str),
            input_hashes_json=json.dumps(input_hashes or {}, ensure_ascii=False, sort_keys=True, default=str),
            results_json=json.dumps(results_json or {}, ensure_ascii=False, sort_keys=True, default=str),
            methodology_id=int(methodology_id) if methodology_id is not None else None,
            created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
            previous_snapshot_hash=str(previous_snapshot_hash) if previous_snapshot_hash else None,
        )
        s.add(snap)
        s.commit()
        s.refresh(snap)
        return snap


def run_full(
    project_id: int,
    config: dict,
    scenario: dict | None = None,
    methodology_id: int | None = None,
    created_by_user_id: int | None = None,
) -> CalculationSnapshot:
    """
    Paket A:
      - InputBundle/ResultBundle deterministik sözleşme
      - Orchestrator deterministik compute
      - Compliance Rule Engine hesap sonrası otomatik kontroller
      - Sonuçlar snapshot.results_json içine deterministik şekilde yazılır
      - Reuse güvenli: same input+config+scenario+methodology+factor_set+monitoring_plan => same snapshot
    """
    scenario = scenario or {}
    from src.mrv.orchestrator import ENGINE_VERSION_PACKET_A, run_orchestrator

    energy_u = latest_upload(project_id, "energy")
    prod_u = latest_upload(project_id, "production")
    materials_u = latest_upload(project_id, "materials")

    if not energy_u:
        raise ValueError("energy.csv yüklenmemiş.")
    if not prod_u:
        raise ValueError("production.csv yüklenmemiş.")

    input_hashes = _input_hashes_payload(project_id, energy_u, prod_u, materials_u)

    energy_df = load_csv_from_uri(str(getattr(energy_u, "storage_uri", "") or ""))
    prod_df = load_csv_from_uri(str(getattr(prod_u, "storage_uri", "") or ""))
    materials_df = None
    if materials_u and str(getattr(materials_u, "storage_uri", "") or ""):
        materials_df = load_csv_from_uri(str(getattr(materials_u, "storage_uri", "") or ""))

    input_bundle, result_bundle, legacy = run_orchestrator(
        project_id=int(project_id),
        config=config or {},
        scenario=scenario,
        methodology_id=methodology_id,
        activity_snapshot_ref=input_hashes,
        energy_df=energy_df,
        production_df=prod_df,
        materials_df=materials_df,
    )

    # Reuse anahtarı (Paket A)
    legacy_input_bundle = legacy.get("input_bundle", {}) or {}
    factor_set_ref = legacy_input_bundle.get("factor_set_ref", []) or []
    monitoring_plan_ref = legacy_input_bundle.get("monitoring_plan_ref", None)

    candidate_hash = _compute_result_hash(
        ENGINE_VERSION_PACKET_A,
        config or {},
        input_hashes,
        scenario,
        methodology_id,
        factor_set_ref,
        monitoring_plan_ref,
    )

    existing = _try_reuse_snapshot(project_id, candidate_hash)
    if existing:
        try:
            append_audit("snapshot_reused", {"snapshot_id": existing.id, "result_hash": candidate_hash})
        except Exception:
            pass
        return existing

    # Compliance evaluation (A3)
    compliance_checks, qa_flags_extra = evaluate_compliance(
        input_bundle=input_bundle,
        result_bundle=result_bundle,
        legacy_results=legacy,
    )

    results_json = legacy.get("results_json", {}) or {}
    # Paket A standard output
    results_json["compliance_checks"] = [c.to_dict() for c in compliance_checks]
    # mevcut qa_flags + compliance qa_flags merge
    existing_qa = results_json.get("qa_flags", []) or []
    existing_qa = existing_qa if isinstance(existing_qa, list) else []
    results_json["qa_flags"] = existing_qa + [q.to_dict() for q in qa_flags_extra]

    # backward compat: compliance obj
    results_json["compliance"] = {
        "compliance_checks": results_json["compliance_checks"],
        "qa_flags": results_json["qa_flags"],
    }

    # deterministic section güncelle (candidate hash = snapshot hash)
    det = results_json.get("deterministic", {}) or {}
    if not isinstance(det, dict):
        det = {}
    det["snapshot_result_hash"] = candidate_hash
    results_json["deterministic"] = det

    prev_hash = _latest_snapshot_hash(project_id)

    snap = create_snapshot(
        project_id=int(project_id),
        engine_version=ENGINE_VERSION_PACKET_A,
        result_hash=candidate_hash,
        config=config or {},
        input_hashes=input_hashes,
        results_json=results_json,
        methodology_id=methodology_id,
        created_by_user_id=created_by_user_id,
        previous_snapshot_hash=prev_hash,
    )

    try:
        append_audit("snapshot_created", {"snapshot_id": snap.id, "result_hash": candidate_hash})
    except Exception:
        pass

    return snap
