from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import select

from src.db.session import db
from src.db.models import CalculationSnapshot, DatasetUpload, Methodology, MonitoringPlan
from src.mrv.audit import append_audit
from src.mrv.lineage import sha256_json

# Paket A: deterministik orchestrator + compliance
from src.mrv.compliance import evaluate_compliance


def load_csv_from_uri(uri: str) -> pd.DataFrame:
    return pd.read_csv(uri)


def latest_upload(project_id: int, dataset_type: str) -> DatasetUpload | None:
    with db() as s:
        return (
            s.execute(
                select(DatasetUpload)
                .where(DatasetUpload.project_id == project_id, DatasetUpload.dataset_type == dataset_type)
                .order_by(DatasetUpload.uploaded_at.desc())
            )
            .scalars()
            .first()
        )


def _input_hashes_payload(project_id: int, energy_u: DatasetUpload, prod_u: DatasetUpload, materials_u: DatasetUpload | None) -> dict:
    payload = {
        "project_id": project_id,
        "energy_upload_id": getattr(energy_u, "id", None),
        "energy_hash": getattr(energy_u, "content_hash", None),
        "production_upload_id": getattr(prod_u, "id", None),
        "production_hash": getattr(prod_u, "content_hash", None),
        "materials_upload_id": getattr(materials_u, "id", None) if materials_u else None,
        "materials_hash": getattr(materials_u, "content_hash", None) if materials_u else None,
    }
    return payload


def _compute_result_hash(
    engine_version: str,
    config: dict,
    input_hashes: dict,
    scenario: dict,
    methodology_id: int | None,
    factor_set_ref: list,
    monitoring_plan_ref: str | None,
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
                .where(CalculationSnapshot.project_id == project_id, CalculationSnapshot.result_hash == result_hash)
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
                .where(CalculationSnapshot.project_id == project_id)
                .order_by(CalculationSnapshot.created_at.desc())
            )
            .scalars()
            .all()
        )


def get_snapshot(snapshot_id: int) -> CalculationSnapshot | None:
    with db() as s:
        return s.execute(select(CalculationSnapshot).where(CalculationSnapshot.id == snapshot_id)).scalar_one_or_none()


def create_snapshot(
    project_id: int,
    result_hash: str,
    results_json: dict,
    input_ref_json: dict,
    created_by_user_id: int | None = None,
) -> CalculationSnapshot:
    with db() as s:
        snap = CalculationSnapshot(
            project_id=project_id,
            result_hash=result_hash,
            results_json=json.dumps(results_json, ensure_ascii=False, sort_keys=True, default=str),
            input_ref_json=json.dumps(input_ref_json, ensure_ascii=False, sort_keys=True, default=str),
            created_by_user_id=created_by_user_id,
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
    from src.mrv.orchestrator import ENGINE_VERSION_PACKET_A, run_orchestrator  # circular import kırmak için local import

    energy_u = latest_upload(project_id, "energy")
    prod_u = latest_upload(project_id, "production")
    materials_u = latest_upload(project_id, "materials")  # precursor için

    if not energy_u:
        raise ValueError("energy.csv yüklenmemiş.")
    if not prod_u:
        raise ValueError("production.csv yüklenmemiş.")

    # Input hash refs (activity snapshot ref)
    input_hashes = _input_hashes_payload(project_id, energy_u, prod_u, materials_u)

    # Orchestrator compute (deterministik bundles + legacy results)
    input_bundle, result_bundle, legacy_results = run_orchestrator(
        project_id=project_id,
        config=config,
        scenario=scenario,
        methodology_id=methodology_id,
        activity_snapshot_ref=input_hashes,
    )

    # Reuse anahtarı (Paket A genişletilmiş)
    factor_set_ref = legacy_results.get("input_bundle", {}).get("factor_set_ref", [])
    monitoring_plan_ref = legacy_results.get("input_bundle", {}).get("monitoring_plan_ref", None)

    candidate_hash = _compute_result_hash(
        ENGINE_VERSION_PACKET_A,
        config,
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

    # Compliance evaluation
    compliance = evaluate_compliance(result_bundle)

    results_json = legacy_results.get("results_json", {})
    results_json["compliance"] = compliance

    snap = create_snapshot(
        project_id=project_id,
        result_hash=candidate_hash,
        results_json=results_json,
        input_ref_json={
            "input_hashes": input_hashes,
            "config": config,
            "scenario": scenario,
            "methodology_id": methodology_id,
            "factor_set_ref": factor_set_ref,
            "monitoring_plan_ref": monitoring_plan_ref,
            "engine_version": ENGINE_VERSION_PACKET_A,
            "input_bundle": input_bundle,
        },
        created_by_user_id=created_by_user_id,
    )

    try:
        append_audit("snapshot_created", {"snapshot_id": snap.id, "result_hash": candidate_hash})
    except Exception:
        pass

    return snap
