from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import select

from src.db.session import db
from src.db.models import CalculationSnapshot, DatasetUpload, Methodology, MonitoringPlan
from src.mrv.audit import append_audit
from src.mrv.lineage import sha256_json

# Paket A: deterministik orchestrator + compliance
from src.mrv.orchestrator import ENGINE_VERSION_PACKET_A, run_orchestrator
from src.mrv.compliance import evaluate_compliance

# Paket D3: deterministik reuse (input hash + config hash) -> Paket A’da genişletildi


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


def _latest_snapshot(project_id: int) -> CalculationSnapshot | None:
    with db() as s:
        return (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == project_id)
                .order_by(CalculationSnapshot.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )


def _monitoring_plan_for_project(project_id: int) -> dict | None:
    """ Paket D2: MonitoringPlan zorunlu bağ (facility bazlı)
    Varsayılan: project.facility_id varsa o tesise ait en güncel plan.
    """
    try:
        from src.db.models import Project  # local import to avoid circulars

        with db() as s:
            p = s.get(Project, int(project_id))
            if not p or not p.facility_id:
                return None
            mp = (
                s.execute(
                    select(MonitoringPlan)
                    .where(MonitoringPlan.facility_id == int(p.facility_id))
                    .order_by(MonitoringPlan.updated_at.desc(), MonitoringPlan.created_at.desc())
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if not mp:
                return None
            return {
                "id": mp.id,
                "facility_id": mp.facility_id,
                "method": mp.method,
                "tier_level": mp.tier_level,
                "data_source": mp.data_source,
                "qa_procedure": mp.qa_procedure,
                "responsible_person": mp.responsible_person,
                "updated_at": (mp.updated_at.isoformat() if getattr(mp, "updated_at", None) else None),
            }
    except Exception:
        return None


def _input_hashes_payload(project_id: int, energy_u: DatasetUpload, prod_u: DatasetUpload, materials_u: DatasetUpload | None) -> dict:
    payload = {
        "energy": {
            "uri": str(energy_u.storage_uri),
            "sha256": getattr(energy_u, "sha256", ""),
            "original_filename": getattr(energy_u, "original_filename", ""),
            "schema_version": getattr(energy_u, "schema_version", "v1"),
        },
        "production": {
            "uri": str(prod_u.storage_uri),
            "sha256": getattr(prod_u, "sha256", ""),
            "original_filename": getattr(prod_u, "original_filename", ""),
            "schema_version": getattr(prod_u, "schema_version", "v1"),
        },
    }
    if materials_u:
        payload["materials"] = {
            "uri": str(materials_u.storage_uri),
            "sha256": getattr(materials_u, "sha256", ""),
            "original_filename": getattr(materials_u, "original_filename", ""),
            "schema_version": getattr(materials_u, "schema_version", "v1"),
        }
    return payload


def _config_hash(config: dict) -> str:
    # sadece config -> deterministik hash (sıralı JSON)
    return sha256_json({"config": config})


def _inputs_hash(inputs: dict) -> str:
    # URI yerine SHA’ları merkeze al (güvenli deterministik reuse)
    slim = {}
    for k, v in (inputs or {}).items():
        if isinstance(v, dict):
            slim[k] = {"sha256": v.get("sha256") or "", "schema_version": v.get("schema_version") or "v1"}
    return sha256_json({"inputs": slim})


def _deterministic_key(
    engine_version: str,
    config: dict,
    inputs: dict,
    scenario: dict,
    methodology_id: int | None,
    # Paket A ekleri:
    factor_set_ref: list | None,
    monitoring_plan_ref: dict | None,
) -> dict:
    """
    Paket A: same input + same config + same factor_set + same monitoring_plan_ref
            (+ same scenario + same methodology + same engine_version) => reuse
    """
    cfg_h = _config_hash(config)
    in_h = _inputs_hash(inputs)
    sc_h = sha256_json({"scenario": scenario or {}})
    fs_h = sha256_json({"factor_set_ref": factor_set_ref or []})
    mp_h = sha256_json({"monitoring_plan_ref": monitoring_plan_ref or {}})
    return {
        "engine_version": engine_version,
        "config_hash": cfg_h,
        "inputs_hash": in_h,
        "scenario_hash": sc_h,
        "methodology_id": methodology_id,
        "factor_set_hash": fs_h,
        "monitoring_plan_hash": mp_h,
    }


def _compute_result_hash(
    engine_version: str,
    config: dict,
    inputs: dict,
    scenario: dict,
    methodology_id: int | None,
    factor_set_ref: list | None,
    monitoring_plan_ref: dict | None,
) -> str:
    key = _deterministic_key(engine_version, config, inputs, scenario, methodology_id, factor_set_ref, monitoring_plan_ref)
    return sha256_json(key)


def _try_reuse_snapshot(project_id: int, result_hash: str) -> CalculationSnapshot | None:
    with db() as s:
        existing = (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == project_id, CalculationSnapshot.result_hash == result_hash)
                .order_by(CalculationSnapshot.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        return existing


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
            append_audit(
                "snapshot_reused",
                {
                    "project_id": project_id,
                    "snapshot_id": existing.id,
                    "hash": existing.result_hash,
                    "engine_version": existing.engine_version,
                },
            )
        except Exception:
            pass
        return existing

    # Compliance checks (A3) + QA flags merge
    compliance_checks, compliance_qa_flags = evaluate_compliance(
        input_bundle=input_bundle,
        result_bundle=result_bundle,
        legacy_results=legacy_results,
    )

    # deterministik yazım: listeler zaten evaluate içinde deterministik üretildi; burada da stabil sırayla serialize ederiz
    legacy_results["compliance_checks"] = [c.to_dict() for c in compliance_checks]
    # mevcut qa_flags korunur, compliance_qa_flags eklenir (uniq id ile)
    existing_qa = legacy_results.get("qa_flags", []) or []
    seen = set()
    merged = []
    for q in existing_qa:
        if isinstance(q, dict):
            fid = str(q.get("flag_id") or "")
            if fid and fid not in seen:
                seen.add(fid)
                merged.append(q)
    for q in compliance_qa_flags:
        d = q.to_dict()
        fid = str(d.get("flag_id") or "")
        if fid and fid not in seen:
            seen.add(fid)
            merged.append(d)
    merged.sort(key=lambda x: (str(x.get("severity", "")), str(x.get("flag_id", ""))))
    legacy_results["qa_flags"] = merged

    # ResultBundle içine compliance’i yansıt (snapshot results_json için)
    # (ResultBundle hash’i compliance eklenmeden hesaplanmıştı; snapshot determinism için "snapshot_hash" ayrı candidate_hash üzerinden kilitleniyor.)
    legacy_results["result_bundle"]["compliance_checks"] = legacy_results["compliance_checks"]
    legacy_results["result_bundle"]["qa_flags"] = legacy_results["qa_flags"]

    # Hash chain: previous snapshot hash
    prev = _latest_snapshot(project_id)
    prev_hash = prev.result_hash if prev else None

    snap = CalculationSnapshot(
        project_id=project_id,
        engine_version=ENGINE_VERSION_PACKET_A,
        config_json=json.dumps(config, ensure_ascii=False),
        input_hashes_json=json.dumps(input_hashes, ensure_ascii=False),
        results_json=json.dumps(legacy_results, ensure_ascii=False, sort_keys=True),
        result_hash=candidate_hash,
        methodology_id=methodology_id,
        created_by_user_id=created_by_user_id,
        previous_snapshot_hash=prev_hash,
        locked=False,
        shared_with_client=False,
    )

    with db() as s:
        s.add(snap)
        s.commit()
        s.refresh(snap)

    try:
        append_audit(
            "snapshot_created",
            {
                "project_id": project_id,
                "snapshot_id": snap.id,
                "hash": snap.result_hash,
                "prev_hash": prev_hash,
                "engine_version": ENGINE_VERSION_PACKET_A,
                "has_compliance": True,
            },
        )
    except Exception:
        pass

    return snap
