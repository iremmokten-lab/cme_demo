from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import select

from src.db.session import db
from src.db.models import CalculationSnapshot, DatasetUpload, Methodology, MonitoringPlan
from src.engine.cbam import cbam_compute
from src.engine.emissions import energy_emissions
from src.engine.ets import ets_net_and_cost, ets_verification_payload
from src.engine.scenarios import apply_scenarios
from src.mrv.audit import append_audit
from src.mrv.lineage import sha256_json

# Paket D3: deterministik reuse (input hash + config hash)
ENGINE_VERSION = "engine-2.0.0-packetD"


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
    """
    Paket D2: MonitoringPlan zorunlu bağ (facility bazlı)
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


def _deterministic_key(engine_version: str, config: dict, inputs: dict, scenario: dict, methodology_id: int | None) -> dict:
    """
    Paket D3: same input + same config (+ same scenario + same methodology + same engine_version) => reuse
    """
    cfg_h = _config_hash(config)
    in_h = _inputs_hash(inputs)
    sc_h = sha256_json({"scenario": scenario or {}})

    return {
        "engine_version": engine_version,
        "config_hash": cfg_h,
        "inputs_hash": in_h,
        "scenario_hash": sc_h,
        "methodology_id": methodology_id,
    }


def _compute_result_hash(engine_version: str, config: dict, inputs: dict, scenario: dict, methodology_id: int | None) -> str:
    key = _deterministic_key(engine_version, config, inputs, scenario, methodology_id)
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
    scenario = scenario or {}

    energy_u = latest_upload(project_id, "energy")
    prod_u = latest_upload(project_id, "production")
    materials_u = latest_upload(project_id, "materials")  # precursor için

    if not energy_u:
        raise ValueError("energy.csv yüklenmemiş.")
    if not prod_u:
        raise ValueError("production.csv yüklenmemiş.")

    energy_df = load_csv_from_uri(str(energy_u.storage_uri))
    prod_df = load_csv_from_uri(str(prod_u.storage_uri))

    materials_df = None
    if materials_u:
        try:
            materials_df = load_csv_from_uri(str(materials_u.storage_uri))
        except Exception:
            materials_df = None

    # Senaryo uygulama
    if scenario:
        energy_df, prod_df = apply_scenarios(
            energy_df,
            prod_df,
            renewable_share=float(scenario.get("renewable_share", 0.0)),
            energy_reduction_pct=float(scenario.get("energy_reduction_pct", 0.0)),
            supplier_factor_multiplier=float(scenario.get("supplier_factor_multiplier", 1.0)),
            export_mix_multiplier=float(scenario.get("export_mix_multiplier", 1.0)),
        )

    # Deterministik reuse anahtarı
    input_hashes = _input_hashes_payload(project_id, energy_u, prod_u, materials_u)
    candidate_hash = _compute_result_hash(ENGINE_VERSION, config, input_hashes, scenario, methodology_id)

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

    # Emissions (fuel-bazlı direct + electricity-based indirect)
    electricity_method = str(config.get("electricity_method", "location"))
    market_override = config.get("market_grid_factor_override", None)
    if market_override is not None:
        try:
            market_override = float(market_override)
        except Exception:
            market_override = None

    emis = energy_emissions(
        energy_df,
        region=str(config.get("region", "TR")),
        electricity_method=electricity_method,
        market_grid_factor_override=market_override,
    )

    # ETS
    ets_fin = ets_net_and_cost(
        scope1_tco2=float(emis.get("direct_tco2", 0.0)),
        free_alloc_t=float(config.get("free_alloc_t", 0.0)),
        banked_t=float(config.get("banked_t", 0.0)),
        allowance_price_eur_per_t=float(config.get("eua_price_eur", 80.0)),
        fx_tl_per_eur=float(config.get("fx_tl_per_eur", 35.0)),
    )

    monitoring_plan = _monitoring_plan_for_project(project_id)
    ets_verify = ets_verification_payload(
        fuel_rows=list(emis.get("fuel_rows") or []),
        monitoring_plan=monitoring_plan,
        uncertainty_notes=str(config.get("uncertainty_notes", "")),
    )

    # CBAM
    cbam_table, cbam_totals = cbam_compute(
        production_df=prod_df,
        energy_breakdown=emis,
        materials_df=materials_df,
        eua_price_eur_per_t=float(config.get("eua_price_eur", 80.0)),
        allocation_basis=str(config.get("cbam_allocation_basis", "quantity")),
    )

    # Methodology payload (report/evidence)
    meth_payload = None
    if methodology_id:
        with db() as s:
            m = s.get(Methodology, int(methodology_id))
            if m:
                meth_payload = {
                    "id": m.id,
                    "name": m.name,
                    "version": m.version,
                    "scope": m.scope,
                    "description": m.description,
                    "created_at": (m.created_at.isoformat() if getattr(m, "created_at", None) else None),
                }

    # Paket D4: report-ready results structure (CBAM/ETS sections)
    results = {
        "kpis": {
            "direct_tco2": float(emis.get("direct_tco2", 0.0)),
            "indirect_tco2": float(emis.get("indirect_tco2", 0.0)),
            "total_tco2": float(emis.get("total_tco2", 0.0)),
            "ets_net_tco2": float(ets_fin.get("net_tco2", 0.0)),
            "ets_cost_tl": float(ets_fin.get("cost_tl", 0.0)),
            "ets_cost_eur": float(ets_fin.get("cost_eur", 0.0)),
            "cbam_embedded_tco2": float(cbam_totals.get("embedded_tco2", 0.0)),
            "cbam_cost_eur": float(cbam_totals.get("cbam_cost_eur", 0.0)),
        },
        "emissions_detail": {
            "fuel_rows": emis.get("fuel_rows", []),
            "electricity_rows": emis.get("electricity_rows", []),
            "notes": emis.get("notes", []),
            "electricity_method": electricity_method,
        },
        "ets": {
            "financials": ets_fin,
            "verification": ets_verify,
        },
        "cbam": {
            "totals": cbam_totals,
            "allocation_basis": cbam_totals.get("allocation_basis", "quantity"),
        },
        "cbam_table": cbam_table.to_dict(orient="records"),
        "scenario": scenario,
        "methodology": meth_payload,
        "deterministic": _deterministic_key(ENGINE_VERSION, config, input_hashes, scenario, methodology_id),
    }

    # Hash chain: previous snapshot hash
    prev = _latest_snapshot(project_id)
    prev_hash = prev.result_hash if prev else None

    snap = CalculationSnapshot(
        project_id=project_id,
        engine_version=ENGINE_VERSION,
        config_json=json.dumps(config, ensure_ascii=False),
        input_hashes_json=json.dumps(input_hashes, ensure_ascii=False),
        results_json=json.dumps(results, ensure_ascii=False),
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
                "engine_version": ENGINE_VERSION,
            },
        )
    except Exception:
        pass

    return snap
