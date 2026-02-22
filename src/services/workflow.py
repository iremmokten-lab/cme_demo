from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import select

from src.db.session import db
from src.db.models import CalculationSnapshot, DatasetUpload
from src.mrv.audit import append_audit
from src.mrv.lineage import sha256_json
from src.engine.cbam import cbam_cost
from src.engine.emissions import energy_emissions_kg
from src.engine.ets import ets_cost_tl
from src.engine.scenarios import apply_scenarios

ENGINE_VERSION = "engine-0.6.0"


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


def run_full(
    project_id: int,
    config: dict,
    scenario: dict | None = None,
    methodology_id: int | None = None,
    created_by_user_id: int | None = None,
) -> CalculationSnapshot:
    energy_u = latest_upload(project_id, "energy")
    prod_u = latest_upload(project_id, "production")

    if not energy_u:
        raise ValueError("energy.csv yüklenmemiş.")
    if not prod_u:
        raise ValueError("production.csv yüklenmemiş.")

    energy_uri = str(energy_u.storage_uri)
    prod_uri = str(prod_u.storage_uri)

    energy_df = load_csv_from_uri(energy_uri)
    prod_df = load_csv_from_uri(prod_uri)

    # Scenario apply
    scenario = scenario or {}
    if scenario:
        energy_df, prod_df = apply_scenarios(
            energy_df,
            prod_df,
            renewable_share=float(scenario.get("renewable_share", 0.0)),
            energy_reduction_pct=float(scenario.get("energy_reduction_pct", 0.0)),
            supplier_factor_multiplier=float(scenario.get("supplier_factor_multiplier", 1.0)),
            export_mix_multiplier=float(scenario.get("export_mix_multiplier", 1.0)),
        )

    emis = energy_emissions_kg(energy_df)
    total_t = emis["total_kg"] / 1000.0
    scope1_t = emis["scope1_kg"] / 1000.0

    ets = ets_cost_tl(
        scope1_tco2=scope1_t,
        free_alloc_t=float(config.get("free_alloc_t", 0.0)),
        banked_t=float(config.get("banked_t", 0.0)),
        allowance_price_eur_per_t=float(config.get("eua_price_eur", 80.0)),
        fx_tl_per_eur=float(config.get("fx_tl_per_eur", 35.0)),
    )

    cbam_df, cbam_totals = cbam_cost(
        prod_df=prod_df,
        total_energy_kg=emis["total_kg"],
        eua_price_eur_per_t=float(config.get("eua_price_eur", 80.0)),
    )

    results = {
        "kpis": {
            "energy_total_tco2": total_t,
            "energy_scope1_tco2": scope1_t,
            "energy_scope2_tco2": emis["scope2_kg"] / 1000.0,
            "ets_net_tco2": ets["net_tco2"],
            "ets_cost_tl": ets["cost_tl"],
            "cbam_embedded_tco2": cbam_totals["embedded_tco2"],
            "cbam_cost_eur": cbam_totals["cbam_cost_eur"],
        },
        "cbam_table": cbam_df.to_dict(orient="records"),
        "scenario": scenario,
    }

    # MRV hashes (input lineage)
    input_hashes = {
        "energy": {
            "uri": energy_uri,
            "sha256": getattr(energy_u, "sha256", ""),
            "original_filename": getattr(energy_u, "original_filename", ""),
            "schema_version": getattr(energy_u, "schema_version", "v1"),
        },
        "production": {
            "uri": prod_uri,
            "sha256": getattr(prod_u, "sha256", ""),
            "original_filename": getattr(prod_u, "original_filename", ""),
            "schema_version": getattr(prod_u, "schema_version", "v1"),
        },
    }

    result_hash = sha256_json({"engine_version": ENGINE_VERSION, "config": config, "inputs": input_hashes, "results": results})

    snap = CalculationSnapshot(
        project_id=project_id,
        engine_version=ENGINE_VERSION,
        config_json=json.dumps(config, ensure_ascii=False),
        input_hashes_json=json.dumps(input_hashes, ensure_ascii=False),
        results_json=json.dumps(results, ensure_ascii=False),
        result_hash=result_hash,
        methodology_id=methodology_id,
        created_by_user_id=created_by_user_id,
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
            {"project_id": project_id, "snapshot_id": snap.id, "hash": result_hash, "engine_version": ENGINE_VERSION},
        )
    except Exception:
        pass

    return snap
