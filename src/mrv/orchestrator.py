from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import select

from src.db.models import EmissionFactor, Methodology, Project
from src.db.session import db
from src.engine.cbam import cbam_compute
from src.engine.emissions import energy_emissions, resolve_factor_set_for_energy_df
from src.engine.ets import ets_net_and_cost, ets_verification_payload
from src.engine.scenarios import apply_scenarios
from src.mrv.bundles import (
    ComplianceCheck,
    FactorRef,
    InputBundle,
    MonitoringPlanRef,
    PriceRef,
    QAFlag,
    ResultBundle,
)
from src.mrv.lineage import sha256_json
from src.services.workflow import load_csv_from_uri  # mevcut yardımcıyı reuse için


ENGINE_VERSION_PACKET_A = "engine-3.0.0-packetA"


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _product_mapping_from_production_df(prod_df: pd.DataFrame) -> List[Dict[str, Any]]:
    if prod_df is None or len(prod_df) == 0:
        return []
    df = prod_df.copy()
    df.columns = [_norm(c) for c in df.columns]

    # En yaygın alanlar (repo cbam motoru ile uyumlu olması için “best effort”):
    cn_col = "cn_code" if "cn_code" in df.columns else ("cn" if "cn" in df.columns else None)
    name_col = "product_name" if "product_name" in df.columns else ("name" if "name" in df.columns else None)
    code_col = "product_code" if "product_code" in df.columns else ("sku" if "sku" in df.columns else None)
    covered_col = "cbam_covered" if "cbam_covered" in df.columns else None

    mapping: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        cn = str(r.get(cn_col) or "").strip() if cn_col else ""
        nm = str(r.get(name_col) or "").strip() if name_col else ""
        pc = str(r.get(code_col) or "").strip() if code_col else ""
        covered = r.get(covered_col) if covered_col else None

        if not (cn or nm or pc):
            continue

        row = {
            "cn_code": cn,
            "product_name": nm,
            "product_code": pc,
        }
        if covered is not None:
            row["cbam_covered"] = bool(covered) if isinstance(covered, (bool, int)) else str(covered)
        mapping.append(row)

    # deterministik: aynı ürün tekrarları varsa uniq + sort
    seen = set()
    out = []
    for m in mapping:
        k = (m.get("cn_code", ""), m.get("product_name", ""), m.get("product_code", ""))
        if k in seen:
            continue
        seen.add(k)
        out.append(m)

    out.sort(key=lambda x: (x.get("cn_code", ""), x.get("product_name", ""), x.get("product_code", "")))
    return out


def _methodology_ref(methodology_id: Optional[int]) -> Optional[Dict[str, Any]]:
    if not methodology_id:
        return None
    with db() as s:
        m = s.get(Methodology, int(methodology_id))
        if not m:
            return None
        return {
            "id": m.id,
            "name": m.name,
            "version": m.version,
            "scope": m.scope,
            "description": m.description,
            "created_at": (m.created_at.isoformat() if getattr(m, "created_at", None) else None),
        }


def _facility_ref_for_project(project_id: int) -> Dict[str, Any]:
    with db() as s:
        p = s.get(Project, int(project_id))
        if not p:
            return {"facility_id": None}
        return {
            "facility_id": getattr(p, "facility_id", None),
            "company_id": getattr(p, "company_id", None),
        }


def _period_ref_for_project(project_id: int) -> Dict[str, Any]:
    # Repo MVP: proje yılı period olarak kabul edilir (UI/CSV ile detaylandırılabilir)
    with db() as s:
        p = s.get(Project, int(project_id))
        if not p:
            return {"year": None}
        return {
            "year": getattr(p, "year", None),
        }


def _monitoring_plan_ref_for_project(project_id: int) -> Optional[MonitoringPlanRef]:
    # Mevcut workflow içindeki helper mantığı ile uyumlu: facility bazlı en güncel plan
    try:
        from src.services.workflow import _monitoring_plan_for_project  # type: ignore
    except Exception:
        return None

    mp = _monitoring_plan_for_project(project_id)
    if not mp:
        return None
    return MonitoringPlanRef(
        id=mp.get("id"),
        facility_id=mp.get("facility_id"),
        method=str(mp.get("method") or "standard"),
        tier_level=str(mp.get("tier_level") or "Tier 2"),
        updated_at=mp.get("updated_at"),
    )


def build_input_bundle(
    *,
    project_id: int,
    config: Dict[str, Any],
    scenario: Optional[Dict[str, Any]],
    methodology_id: Optional[int],
    activity_snapshot_ref: Dict[str, Any],
    energy_df: pd.DataFrame,
    production_df: pd.DataFrame,
) -> InputBundle:
    scenario = scenario or {}

    region = str(config.get("region", "TR")).strip() or "TR"
    electricity_method = str(config.get("electricity_method", "location")).strip() or "location"
    market_override = config.get("market_grid_factor_override", None)
    try:
        market_override_f = float(market_override) if market_override is not None else None
    except Exception:
        market_override_f = None

    # Deterministik config hash
    config_hash = sha256_json({"config": config})

    # Factor set resolve: energy_df üzerinde görünen yakıtlar + elektrik metodu
    factor_refs, factor_lookup = resolve_factor_set_for_energy_df(
        energy_df=energy_df,
        region=region,
        electricity_method=electricity_method,
        market_grid_factor_override=market_override_f,
    )

    # FactorRef dataclass listesine dönüştür
    factor_set_ref = [
        FactorRef(
            id=fr.get("id"),
            factor_type=str(fr.get("factor_type") or ""),
            region=str(fr.get("region") or region),
            year=fr.get("year"),
            version=str(fr.get("version") or ""),
            value=float(fr.get("value") or 0.0),
            unit=str(fr.get("unit") or ""),
            source=str(fr.get("source") or ""),
        )
        for fr in (factor_refs or [])
    ]

    price_ref = PriceRef(
        eua_price_eur_per_t=_safe_float(config.get("eua_price_eur", 80.0), 80.0),
        fx_tl_per_eur=_safe_float(config.get("fx_tl_per_eur", 35.0), 35.0),
    )

    return InputBundle(
        engine_version=ENGINE_VERSION_PACKET_A,
        project_id=int(project_id),
        period=_period_ref_for_project(project_id),
        facility=_facility_ref_for_project(project_id),
        product_mapping=_product_mapping_from_production_df(production_df),
        activity_snapshot_ref=activity_snapshot_ref,
        monitoring_plan_ref=_monitoring_plan_ref_for_project(project_id),
        factor_set_ref=factor_set_ref,
        price_ref=price_ref,
        config=config,
        config_hash=config_hash,
        methodology_ref=_methodology_ref(methodology_id),
        scenario=scenario,
    )


def run_orchestrator(
    *,
    project_id: int,
    config: Dict[str, Any],
    scenario: Optional[Dict[str, Any]],
    methodology_id: Optional[int],
    activity_snapshot_ref: Dict[str, Any],
) -> Tuple[InputBundle, ResultBundle, Dict[str, Any]]:
    """
    Deterministik Orchestrator:
      - InputBundle üretir
      - Emissions/ETS/CBAM compute
      - ResultBundle üretir (compliance/qa placeholder ile)
      - result_hash: ResultBundle canonical json üzerinden
    """
    scenario = scenario or {}

    # Load activity datasets
    from src.services.workflow import latest_upload  # mevcut fonksiyon, bozmuyoruz

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
        scenario=scenario,
        methodology_id=methodology_id,
        activity_snapshot_ref=activity_snapshot_ref,
        energy_df=energy_df,
        production_df=prod_df,
    )

    region = str(config.get("region", "TR")).strip() or "TR"
    electricity_method = str(config.get("electricity_method", "location")).strip() or "location"
    market_override = config.get("market_grid_factor_override", None)
    try:
        market_override_f = float(market_override) if market_override is not None else None
    except Exception:
        market_override_f = None

    # Emissions (factor_set_lock ile deterministik)
    emis = energy_emissions(
        energy_df,
        region=region,
        electricity_method=electricity_method,
        market_grid_factor_override=market_override_f,
        factor_set_lock=[f.to_dict() for f in input_bundle.factor_set_ref],
    )

    # ETS
    ets_fin = ets_net_and_cost(
        scope1_tco2=float(emis.get("direct_tco2", 0.0)),
        free_alloc_t=float(config.get("free_alloc_t", 0.0)),
        banked_t=float(config.get("banked_t", 0.0)),
        allowance_price_eur_per_t=float(config.get("eua_price_eur", 80.0)),
        fx_tl_per_eur=float(config.get("fx_tl_per_eur", 35.0)),
    )
    mp_ref = input_bundle.monitoring_plan_ref.to_dict() if input_bundle.monitoring_plan_ref else None
    ets_verify = ets_verification_payload(
        fuel_rows=list(emis.get("fuel_rows") or []),
        monitoring_plan=mp_ref,
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

    # ResultBundle temel yapı
    input_bundle_hash = input_bundle.input_bundle_hash()

    # QA flags (deterministik, minimal)
    qa_flags: List[QAFlag] = []
    if float(emis.get("total_tco2", 0.0)) <= 0.0:
        qa_flags.append(
            QAFlag(
                flag_id="QA_TOTAL_ZERO",
                severity="warn",
                message_tr="Toplam emisyon 0 görünüyor. Yakıt/elektrik alanlarını ve faktör setini kontrol edin.",
                context={"direct_tco2": emis.get("direct_tco2"), "indirect_tco2": emis.get("indirect_tco2")},
            )
        )

    # Cost outputs (CBAM/ETS)
    cost_outputs = {
        "ets": {
            "net_tco2": float(ets_fin.get("net_tco2", 0.0)),
            "cost_eur": float(ets_fin.get("cost_eur", 0.0)),
            "cost_tl": float(ets_fin.get("cost_tl", 0.0)),
            "price_ref": input_bundle.price_ref.to_dict(),
        },
        "cbam": {
            "embedded_tco2": float(cbam_totals.get("embedded_tco2", 0.0)),
            "cbam_cost_eur": float(cbam_totals.get("cbam_cost_eur", 0.0)),
            "price_ref": input_bundle.price_ref.to_dict(),
        },
    }

    # Breakdown tree (yakıt/proses/ürün): MVP deterministik
    breakdown = {
        "by_fuel": sorted((emis.get("fuel_rows") or []), key=lambda r: (str(r.get("fuel_type", "")), str(r.get("unit", "")))),
        "by_electricity": sorted((emis.get("electricity_rows") or []), key=lambda r: (str(r.get("grid_method", "")), str(r.get("source", "")))),
        "by_product": sorted((cbam_table.to_dict(orient="records") if hasattr(cbam_table, "to_dict") else []), key=lambda r: str(r.get("cn_code", ""))),
    }

    totals = {
        "tco2e": float(emis.get("total_tco2", 0.0)),
        "scope1_tco2e": float(emis.get("direct_tco2", 0.0)),
        "scope2_tco2e": float(emis.get("indirect_tco2", 0.0)),
        "cbam_embedded_tco2e": float(cbam_totals.get("embedded_tco2", 0.0)),
    }

    unit_conversions = {
        "notes": [
            "Bu MVP’de birim dönüşümleri enerji satırlarında (kWh/MWh) ve faktörlerde (kgCO2e/kWh → tCO2e) uygulanır.",
        ]
    }

    source_references = {
        "factor_set_ref": [f.to_dict() for f in input_bundle.factor_set_ref],
        "monitoring_plan_ref": mp_ref,
        "activity_snapshot_ref": activity_snapshot_ref,
        "methodology_ref": input_bundle.methodology_ref,
    }

    # Compliance checks orchestrator dışındaki modülde doldurulacak (A3)
    compliance_checks: List[ComplianceCheck] = []

    # Result hash deterministik: ResultBundle canonical dict üzerinden
    temp_result = ResultBundle(
        engine_version=ENGINE_VERSION_PACKET_A,
        input_bundle_hash=input_bundle_hash,
        result_hash="__placeholder__",
        totals=totals,
        breakdown=breakdown,
        unit_conversions=unit_conversions,
        source_references=source_references,
        qa_flags=qa_flags,
        compliance_checks=compliance_checks,
        cost_outputs=cost_outputs,
    )
    result_hash = sha256_json({"result": temp_result.to_canonical_dict()})

    result_bundle = ResultBundle(
        engine_version=ENGINE_VERSION_PACKET_A,
        input_bundle_hash=input_bundle_hash,
        result_hash=result_hash,
        totals=totals,
        breakdown=breakdown,
        unit_conversions=unit_conversions,
        source_references=source_references,
        qa_flags=qa_flags,
        compliance_checks=compliance_checks,
        cost_outputs=cost_outputs,
    )

    # Workflow’un eski results_json yapısını korumak için “legacy shaped results” da döndürürüz
    legacy_results = {
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
        "ets": {"financials": ets_fin, "verification": ets_verify},
        "cbam": {"totals": cbam_totals, "allocation_basis": cbam_totals.get("allocation_basis", "quantity")},
        "cbam_table": cbam_table.to_dict(orient="records") if hasattr(cbam_table, "to_dict") else [],
        "scenario": scenario,
        "methodology": input_bundle.methodology_ref,
        # Deterministik sözleşme alanları (A1/A2)
        "input_bundle": input_bundle.to_canonical_dict(),
        "result_bundle": result_bundle.to_canonical_dict(),
        "deterministic": {
            "engine_version": ENGINE_VERSION_PACKET_A,
            "input_bundle_hash": input_bundle_hash,
            "result_hash": result_hash,
        },
        # A3 burada doldurulacak
        "compliance_checks": [],
        "qa_flags": [q.to_dict() for q in qa_flags],
    }

    return input_bundle, result_bundle, legacy_results
