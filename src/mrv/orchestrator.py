from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import select

from src.db.models import EmissionFactor, Methodology, Project
from src.db.session import db
from src.engine.cbam import cbam_compute
from src.engine.emissions import energy_emissions, resolve_factor_set_for_energy_df
from src.engine.ets import ets_compute
from src.mrv.compliance import QAFlag, ResultBundle
from src.mrv.lineage import sha256_json

ENGINE_VERSION_PACKET_A = "engine-3.0.0-packetA"


def _norm(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)


def _get_project(project_id: int) -> Project:
    with db() as s:
        proj = s.execute(select(Project).where(Project.id == project_id)).scalar_one_or_none()
        if not proj:
            raise ValueError("Project bulunamadı.")
        return proj


def _load_methodology(methodology_id: Optional[int]) -> Optional[Methodology]:
    if not methodology_id:
        return None
    with db() as s:
        m = s.execute(select(Methodology).where(Methodology.id == methodology_id)).scalar_one_or_none()
        return m


def _load_active_factors() -> List[EmissionFactor]:
    with db() as s:
        return (
            s.execute(select(EmissionFactor).where(EmissionFactor.is_active == True))  # noqa: E712
            .scalars()
            .all()
        )


def apply_scenarios(
    energy_df: pd.DataFrame,
    production_df: pd.DataFrame,
    renewable_share: float = 0.0,
    energy_reduction_pct: float = 0.0,
    supplier_factor_multiplier: float = 1.0,
    export_mix_multiplier: float = 1.0,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Senaryo uygulama: mevcut davranışı korur (basit multipliers)
    """
    e = energy_df.copy()
    p = production_df.copy()

    if energy_reduction_pct and "quantity" in e.columns:
        e["quantity"] = e["quantity"].astype(float) * (1.0 - float(energy_reduction_pct) / 100.0)

    # renewable_share: elektrik emisyon katsayısını düşürme gibi davranışlar
    # burada veri formatına göre sadece placeholder bir dönüşüm yapıyoruz
    if renewable_share and "energy_type" in e.columns and "quantity" in e.columns:
        # elektrik tüketiminin belirli payını "renewable" olarak işaretlemek gibi basit bir işaretleme
        pass

    # supplier_factor_multiplier: precursor/material embedded factors için kullanılabilir (legacy)
    if supplier_factor_multiplier:
        pass

    # export_mix_multiplier: export oranı gibi optimizasyonlar (legacy)
    if export_mix_multiplier:
        pass

    return e, p


def build_input_bundle(
    project_id: int,
    config: dict,
    energy_df: pd.DataFrame,
    production_df: pd.DataFrame,
    materials_df: Optional[pd.DataFrame],
    methodology_id: Optional[int],
) -> Dict[str, Any]:
    """
    InputBundle: deterministik hash için normalize edilmiş input paketini üretir.
    """
    proj = _get_project(project_id)
    methodology = _load_methodology(methodology_id)
    factors = _load_active_factors()

    # Factor set resolve: energy df üstünde yakıt/electricity vs için hangi faktörleri kullanacağız
    factor_set_ref = resolve_factor_set_for_energy_df(energy_df, factors)

    input_bundle = {
        "project": {
            "id": proj.id,
            "name": proj.name,
            "country": proj.country,
            "sector": proj.sector,
        },
        "methodology": {
            "id": getattr(methodology, "id", None),
            "code": getattr(methodology, "code", None),
            "name": getattr(methodology, "name", None),
            "reg_reference": getattr(methodology, "reg_reference", None),
        },
        "config": config or {},
        "datasets": {
            "energy": energy_df.to_dict(orient="records"),
            "production": production_df.to_dict(orient="records"),
            "materials": materials_df.to_dict(orient="records") if materials_df is not None else None,
        },
        "factor_set_ref": factor_set_ref,
        "monitoring_plan_ref": None,
    }

    return input_bundle


def compute_result_bundle(
    config: dict,
    energy_df: pd.DataFrame,
    production_df: pd.DataFrame,
    materials_df: Optional[pd.DataFrame],
    factors: List[EmissionFactor],
) -> ResultBundle:
    """
    ResultBundle: ETS/CBAM emisyon hesapları + QA/Compliance flagleri
    """
    # Direct + Electricity emissions
    energy_result = energy_emissions(energy_df, factors, config=config)

    # ETS compute
    ets_result = ets_compute(
        energy_df=energy_df,
        production_df=production_df,
        factors=factors,
        config=config,
    )

    # CBAM compute
    cbam_result = cbam_compute(
        production_df=production_df,
        materials_df=materials_df,
        factors=factors,
        config=config,
    )

    flags: List[QAFlag] = []

    # Basit QA: negatif değer kontrolü
    for col in ["quantity", "value", "amount", "kwh", "mwh", "ton"]:
        if col in energy_df.columns:
            try:
                if (energy_df[col].astype(float) < 0).any():
                    flags.append(QAFlag(code="NEGATIVE_ENERGY_VALUE", severity="high", message=f"{col} negatif değer içeriyor"))
            except Exception:
                pass

    # ResultBundle sözleşmesi
    bundle = ResultBundle(
        energy=energy_result,
        ets=ets_result,
        cbam=cbam_result,
        qa_flags=flags,
    )
    return bundle


def run_orchestrator(
    project_id: int,
    config: dict,
    scenario: dict | None = None,
    methodology_id: int | None = None,
    activity_snapshot_ref: dict | None = None,
) -> Tuple[Dict[str, Any], ResultBundle, Dict[str, Any]]:
    """
    Orchestrator paket A:
      - Uploadlardan CSV yükler (services.workflow yardımcıları)
      - InputBundle üretir
      - Emissions/ETS/CBAM compute
      - ResultBundle üretir (compliance/qa placeholder ile)
      - result_hash: ResultBundle canonical json üzerinden
    """
    scenario = scenario or {}

    # Load activity datasets
    from src.services.workflow import latest_upload  # mevcut fonksiyon, bozmuyoruz
    ENGINE_VERSION_PACKET_A = "engine-3.0.0"


def run_orchestrator(...):

    from src.services.workflow import load_csv_from_uri
    
    energy_u = latest_upload(project_id, "energy")
    prod_u = latest_upload(project_id, "production")
    materials_u = latest_upload(project_id, "materials")

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

    # Senaryo uygulama (mevcut davranış korunur)
    if scenario:
        energy_df, prod_df = apply_scenarios(
            energy_df,
            prod_df,
            renewable_share=float(scenario.get("renewable_share", 0.0)),
            energy_reduction_pct=float(scenario.get("energy_reduction_pct", 0.0)),
            supplier_factor_multiplier=float(scenario.get("supplier_factor_multiplier", 1.0)),
            export_mix_multiplier=float(scenario.get("export_mix_multiplier", 1.0)),
        )

    # Input bundle (factor_set resolve dahil)
    input_bundle = build_input_bundle(
        project_id=project_id,
        config=config,
        energy_df=energy_df,
        production_df=prod_df,
        materials_df=materials_df,
        methodology_id=methodology_id,
    )

    # Active factors
    factors = _load_active_factors()

    # Result bundle compute
    result_bundle = compute_result_bundle(
        config=config,
        energy_df=energy_df,
        production_df=prod_df,
        materials_df=materials_df,
        factors=factors,
    )

    # legacy_results: UI/raporlama tarafında eski şemayı bozmadan taşımak için
    legacy_results = {
        "engine_version": ENGINE_VERSION_PACKET_A,
        "input_bundle": {
            "factor_set_ref": input_bundle.get("factor_set_ref", []),
            "monitoring_plan_ref": input_bundle.get("monitoring_plan_ref"),
        },
        "activity_snapshot_ref": activity_snapshot_ref or {},
        "results_json": {
            "energy": result_bundle.energy,
            "ets": result_bundle.ets,
            "cbam": result_bundle.cbam,
            "qa_flags": [f.__dict__ for f in result_bundle.qa_flags],
        },
    }

    return input_bundle, result_bundle, legacy_results


def compute_result_hash(
    engine_version: str,
    config: dict,
    input_hashes: dict,
    scenario: dict,
    methodology_id: Optional[int],
    factor_set_ref: list,
    monitoring_plan_ref: Optional[str],
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
