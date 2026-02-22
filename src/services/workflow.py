import json
import pandas as pd
from sqlalchemy import select

from src.db.session import db
from src.db.models import DatasetUpload, CalculationSnapshot
from src.mrv.lineage import sha256_json
from src.mrv.audit import append_audit
from src.engine.emissions import energy_emissions_kg
from src.engine.ets import ets_cost_tl
from src.engine.cbam import cbam_cost
from src.engine.scenarios import apply_scenarios
from src.services.storage import UPLOAD_DIR
from pathlib import Path

ENGINE_VERSION = "engine-0.5.0"

def load_csv_from_uri(uri: str) -> pd.DataFrame:
    return pd.read_csv(uri)

def latest_upload_uri(project_id: int, dataset_type: str) -> str | None:
    with db() as s:
        u = s.execute(
            select(DatasetUpload)
            .where(DatasetUpload.project_id == project_id, DatasetUpload.dataset_type == dataset_type)
            .order_by(DatasetUpload.uploaded_at.desc())
        ).scalars().first()
        return u.storage_uri if u else None

def run_full(project_id: int, config: dict, scenario: dict | None = None) -> CalculationSnapshot:
    energy_uri = latest_upload_uri(project_id, "energy")
    prod_uri = latest_upload_uri(project_id, "production")

    if not energy_uri:
        raise ValueError("energy.csv yüklenmemiş.")
    if not prod_uri:
        raise ValueError("production.csv yüklenmemiş.")

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

    # MRV hashes
    input_hashes = {"energy_uri": energy_uri, "production_uri": prod_uri}
    result_hash = sha256_json({"config": config, "inputs": input_hashes, "results": results})

    snap = CalculationSnapshot(
        project_id=project_id,
        engine_version=ENGINE_VERSION,
        config_json=json.dumps(config, ensure_ascii=False),
        input_hashes_json=json.dumps(input_hashes, ensure_ascii=False),
        results_json=json.dumps(results, ensure_ascii=False),
        result_hash=result_hash,
    )

    with db() as s:
        s.add(snap)
        s.commit()
        s.refresh(snap)

    append_audit("snapshot_created", {"project_id": project_id, "snapshot_id": snap.id, "hash": result_hash})
    return snap
