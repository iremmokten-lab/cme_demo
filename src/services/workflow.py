from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import select

from src.db.models import CalculationSnapshot, DatasetUpload, EmissionFactor, SnapshotDatasetLink, SnapshotFactorLink
from src.db.session import db
from src.mrv.audit import append_audit
from src.mrv.compliance import evaluate_compliance
from src.mrv.lineage import sha256_json


def _run_phase3_ai(project_id: int, legacy_results: dict, config: dict) -> dict:
    """Faz 3: Benchmark + Advisor + Optimizer.

    - Deterministik: sadece snapshot sonuçları + config.ai.optimizer_constraints kullanır.
    - Evidence gap: project'e bağlı evidence kategorilerine göre çıkar.
    """

    try:
        from src.engine.advisor import build_reduction_advice
        from src.engine.benchmark import build_benchmark_report
        from src.engine.optimizer import build_optimizer_payload
    except Exception:
        return {}

    results = legacy_results.get("results_json", legacy_results) if isinstance(legacy_results, dict) else {}
    if not isinstance(results, dict):
        results = {}

    input_bundle = results.get("input_bundle") or {}
    facility = (input_bundle.get("facility") or {}) if isinstance(input_bundle, dict) else {}
    kpis = (results.get("kpis") or {}) if isinstance(results, dict) else {}

    # energy breakdown
    breakdown = (results.get("breakdown") or {}) if isinstance(results, dict) else {}
    energy_breakdown = (breakdown.get("energy") or {}) if isinstance(breakdown, dict) else {}

    cbam = (results.get("cbam") or {}) if isinstance(results, dict) else {}
    cbam_table = results.get("cbam_table")

    # Evidence categories present
    categories = []
    try:
        from sqlalchemy import select

        from src.db.models import EvidenceDocument
        from src.db.session import db

        with db() as s:
            cats = (
                s.execute(
                    select(EvidenceDocument.category)
                    .where(EvidenceDocument.project_id == int(project_id))
                    .distinct()
                )
                .scalars()
                .all()
            )
        categories = [str(c or "").strip() for c in cats if c]
    except Exception:
        categories = []

    bench = build_benchmark_report(facility=facility, kpis=kpis, cbam=cbam, cbam_table=cbam_table)
    advice = build_reduction_advice(
        kpis=kpis,
        energy_breakdown=energy_breakdown,
        cbam=cbam,
        evidence_categories_present=categories,
    )

    constraints = {}
    try:
        constraints = ((config or {}).get("ai") or {}).get("optimizer_constraints") or {}
        if not isinstance(constraints, dict):
            constraints = {}
    except Exception:
        constraints = {}

    total_tco2 = 0.0
    try:
        total_tco2 = float(kpis.get("total_tco2", 0.0) or 0.0)
    except Exception:
        total_tco2 = 0.0

    opt = build_optimizer_payload(total_tco2=total_tco2, measures=(advice.get("measures") or []), constraints=constraints)

    return {
        "benchmark": bench,
        "advisor": advice,
        "optimizer": opt,
        "meta": {
            "phase": "faz3",
            "optimizer_constraints": constraints,
            "evidence_categories_present": categories,
        },
    }


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
    """Activity data snapshot ref.
    DatasetUpload modelinde içerik hash alanı `sha256` olarak tutulur.
    """

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


def _try_reuse_snapshot(project_id: int, input_hash: str, result_hash: str) -> CalculationSnapshot | None:
    with db() as s:
        existing = (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == int(project_id), CalculationSnapshot.input_hash == str(input_hash), CalculationSnapshot.result_hash == str(result_hash))
                .order_by(CalculationSnapshot.created_at.desc())
            )
            .scalars()
            .first()
        )
        return existing


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
    # Audit-ready hashes
    input_hash: str,
    result_hash: str,
    config: dict,
    input_hashes: dict,
    results_json: dict,
    methodology_id: int | None = None,
    factor_set_id: int | None = None,
    monitoring_plan_id: int | None = None,
    created_by_user_id: int | None = None,
    previous_snapshot_hash: str | None = None,
    base_snapshot_id: int | None = None,
) -> CalculationSnapshot:
    """Immutable-by-policy snapshot persist.

    Bu fonksiyon *tek doğruluk kaynağı* olan snapshot kaydını oluşturur ve
    denetim için gerekli hash/governance alanlarını deterministik şekilde doldurur.

    Kilitli hale getirme ayrı bir aksiyondur (lock_snapshot).
    """

    # --- derive audit hashes from deterministic bundle
    input_bundle = (results_json or {}).get("input_bundle") or {}
    if not isinstance(input_bundle, dict):
        input_bundle = {}

    factor_set_ref = input_bundle.get("factor_set_ref") or []
    if not isinstance(factor_set_ref, list):
        factor_set_ref = []

    methodology_ref = input_bundle.get("methodology_ref") or None
    if methodology_ref is not None and not isinstance(methodology_ref, dict):
        methodology_ref = None

    dataset_hashes = {
        "energy": ((input_hashes or {}).get("energy") or {}).get("sha256"),
        "production": ((input_hashes or {}).get("production") or {}).get("sha256"),
        "materials": (((input_hashes or {}).get("materials") or {}) if (input_hashes or {}).get("materials") else {}).get("sha256"),
    }

    factor_set_hash = sha256_json(factor_set_ref or [])
    methodology_hash = sha256_json(methodology_ref or {})

    # price evidence must be frozen for replay; store inside snapshot for evidence pack
    price_evidence_list = []
    try:
        pe = (config or {}).get("_price_evidence")
        if isinstance(pe, dict) and pe:
            price_evidence_list.append(pe)
        pe2 = (config or {}).get("_price_evidence_list")
        if isinstance(pe2, list):
            for x in pe2:
                if isinstance(x, dict) and x:
                    price_evidence_list.append(x)
    except Exception:
        price_evidence_list = []

    scenario_meta = input_bundle.get("scenario") if isinstance(input_bundle.get("scenario"), dict) else {}
    if not isinstance(scenario_meta, dict):
        scenario_meta = {}

    with db() as s:
        snap = CalculationSnapshot(
            project_id=int(project_id),
            engine_version=str(engine_version),
            input_hash=str(input_hash),
            result_hash=str(result_hash),
            config_json=json.dumps(config or {}, ensure_ascii=False, sort_keys=True, default=str),
            input_hashes_json=json.dumps(input_hashes or {}, ensure_ascii=False, sort_keys=True, default=str),
            results_json=json.dumps(results_json or {}, ensure_ascii=False, sort_keys=True, default=str),
            dataset_hashes_json=json.dumps(dataset_hashes or {}, ensure_ascii=False, sort_keys=True, default=str),
            factor_set_hash=str(factor_set_hash or ""),
            methodology_hash=str(methodology_hash or ""),
            methodology_id=int(methodology_id) if methodology_id is not None else None,
            factor_set_id=int(factor_set_id) if factor_set_id is not None else None,
            monitoring_plan_id=int(monitoring_plan_id) if monitoring_plan_id is not None else None,
            previous_snapshot_hash=str(previous_snapshot_hash) if previous_snapshot_hash else None,
            base_snapshot_id=int(base_snapshot_id) if base_snapshot_id is not None else None,
            scenario_meta_json=json.dumps(scenario_meta or {}, ensure_ascii=False, sort_keys=True, default=str),
            price_evidence_json=json.dumps(price_evidence_list or [], ensure_ascii=False, sort_keys=True, default=str),
            created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
        )
        s.add(snap)
        s.commit()
        s.refresh(snap)

        # --- Link datasets for DB-level immutability
        for dkey in ["energy", "production", "materials"]:
            info = (input_hashes or {}).get(dkey) or None
            if not isinstance(info, dict):
                continue
            du_id = info.get("upload_id")
            if du_id is None:
                continue
            try:
                link = SnapshotDatasetLink(
                    snapshot_id=int(snap.id),
                    datasetupload_id=int(du_id),
                    dataset_type=str(dkey),
                    sha256=str(info.get("sha256") or ""),
                    storage_uri=str(info.get("uri") or ""),
                )
                s.add(link)
            except Exception:
                continue

        # --- Link factors used in snapshot (used_in_snapshots governance)
        for fr in factor_set_ref or []:
            if not isinstance(fr, dict):
                continue
            fid = fr.get("id")
            if fid is None:
                continue
            try:
                link = SnapshotFactorLink(
                    snapshot_id=int(snap.id),
                    factor_id=int(fid),
                    factor_type=str(fr.get("factor_type") or ""),
                    region=str(fr.get("region") or "TR"),
                    year=(int(fr.get("year")) if fr.get("year") is not None and str(fr.get("year")).strip() != "" else None),
                    version=str(fr.get("version") or "v1"),
                    factor_hash=str(fr.get("factor_hash") or ""),
                )
                s.add(link)
            except Exception:
                continue

        s.commit()

        if append_audit:
            append_audit(
                "snapshot_created",
                {
                    "snapshot_id": snap.id,
                    "input_hash": input_hash,
                    "result_hash": result_hash,
                    "factor_set_hash": factor_set_hash,
                    "methodology_hash": methodology_hash,
                },
                user_id=created_by_user_id,
                company_id=None,
                entity_type="snapshot",
                entity_id=str(snap.id),
            )
        return snap


def run_full(
    project_id: int,
    config: dict,
    scenario: dict | None = None,
    methodology_id: int | None = None,
    created_by_user_id: int | None = None,
) -> CalculationSnapshot:
    """Danışman panelinin kullandığı uçtan uca snapshot üretimi.

    Fix kapsamı:
    - DatasetUpload.content_hash -> sha256 düzeltildi
    - evaluate_compliance(...) doğru imzayla çağrılır
    - results_json.compliance_checks[] / results_json.qa_flags[] standardize edildi
    - Snapshot reuse deterministik candidate_hash ile yapılır
    """
    scenario = scenario or {}

    # --- Carbon price evidence freeze (CBAM / ETS cost)
    # External feeds MUST NOT be used for locked snapshots. We freeze the values here and store in snapshot config.
    from datetime import datetime, timezone

    cfg = dict(config or {})
    try:
        eua = float(cfg.get("eua_price_eur_per_t") or cfg.get("eua_price") or 0.0)
    except Exception:
        eua = 0.0
    try:
        fx = float(cfg.get("fx_tl_per_eur") or cfg.get("eur_try") or 0.0)
    except Exception:
        fx = 0.0
    cfg["_price_evidence"] = {
        "price_type": "EUA_EUR_PER_T",
        "price_value": eua,
        "fx_tl_per_eur": fx,
        "source": "user_config_or_secrets",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "hash": sha256_json({"eua_price_eur_per_t": eua, "fx_tl_per_eur": fx, "source": "user_config_or_secrets"}),
    }
    config = cfg

    from src.mrv.orchestrator import ENGINE_VERSION_PACKET_A, run_orchestrator  # circular import için local import

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

    input_bundle, result_bundle, legacy_results = run_orchestrator(
        project_id=int(project_id),
        config=(config or {}),
        scenario=scenario,
        methodology_id=methodology_id,
        activity_snapshot_ref=input_hashes,
        energy_df=energy_df,
        production_df=prod_df,
        materials_df=materials_df,
    )

    # Reuse key
    ib = legacy_results.get("input_bundle", {}) or {}
    factor_set_ref = ib.get("factor_set_ref", []) or []
    monitoring_plan_ref = ib.get("monitoring_plan_ref", None)

    candidate_hash = _compute_result_hash(
        ENGINE_VERSION_PACKET_A,
        (config or {}),
        input_hashes,
        scenario,
        methodology_id,
        factor_set_ref,
        monitoring_plan_ref,
    )

    existing = _try_reuse_snapshot(project_id, input_hash=candidate_hash, result_hash=result_bundle.result_hash)
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
        legacy_results=legacy_results,
    )

    results_json = legacy_results.get("results_json", {}) or {}

    # Standard outputs
    results_json["compliance_checks"] = [c.to_dict() for c in (compliance_checks or [])]
    results_json["qa_flags"] = [q.to_dict() for q in (qa_flags_extra or [])]

    # Faz 3 AI
    results_json["ai"] = _run_phase3_ai(project_id, legacy_results, config)

    previous_snapshot_hash = _latest_snapshot_hash(project_id)

    snap = create_snapshot(
        project_id=int(project_id),
        engine_version=ENGINE_VERSION_PACKET_A,
        input_hash=str(candidate_hash),
        result_hash=str(result_bundle.result_hash or ""),
        config=(config or {}),
        input_hashes=input_hashes,
        results_json=results_json,
        methodology_id=methodology_id,
        factor_set_id=None,
        monitoring_plan_id=None,
        created_by_user_id=created_by_user_id,
        previous_snapshot_hash=previous_snapshot_hash,
    )

    return snap
