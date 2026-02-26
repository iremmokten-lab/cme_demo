from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

import pandas as pd
from sqlalchemy import select

from src.db.models import Methodology, MonitoringPlan, Project
from src.db.session import db
from src.engine.cbam import cbam_compute
from src.engine.emissions import energy_emissions, resolve_factor_set_for_energy_df
from src.engine.ets import ets_net_and_cost, ets_verification_payload
from src.mrv.bundles import FactorRef, InputBundle, MonitoringPlanRef, PriceRef, QAFlag, ResultBundle
from src.mrv.lineage import sha256_json


ENGINE_VERSION_PACKET_A = "engine-3.0.0-packetA"


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _period_from_config(config: Dict[str, Any]) -> Dict[str, Any]:
    period = dict((config or {}).get("period") or {})
    if "year" not in period:
        try:
            period["year"] = int((config or {}).get("year"))
        except Exception:
            period["year"] = None
    return period


def _facility_from_project(project: Project) -> Dict[str, Any]:
    fac = getattr(project, "facility", None)
    if not fac:
        return {"id": None, "name": "", "country": "", "sector": ""}
    return {
        "id": int(getattr(fac, "id", 0) or 0),
        "name": str(getattr(fac, "name", "") or ""),
        "country": str(getattr(fac, "country", "") or ""),
        "sector": str(getattr(fac, "sector", "") or ""),
    }


def _product_mapping_from_production_df(production_df: pd.DataFrame) -> List[Dict[str, Any]]:
    if production_df is None or len(production_df) == 0:
        return []
    df = production_df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    cols = set(df.columns.tolist())

    def colpick(*names: str) -> str:
        for n in names:
            if n in cols:
                return n
        return ""

    cn_c = colpick("cn_code", "cn", "cncode")
    pn_c = colpick("product_name", "product")
    pc_c = colpick("product_code", "sku", "code")

    mapping: List[Dict[str, Any]] = []
    seen = set()
    for _, r in df.iterrows():
        cn = str(r.get(cn_c, "") or "").strip() if cn_c else ""
        pn = str(r.get(pn_c, "") or "").strip() if pn_c else ""
        pc = str(r.get(pc_c, "") or "").strip() if pc_c else ""
        key = (cn, pn, pc)
        if key in seen:
            continue
        seen.add(key)
        if not (cn or pn or pc):
            continue
        mapping.append({"cn_code": cn, "product_name": pn, "product_code": pc})

    mapping.sort(key=lambda x: (x.get("cn_code", ""), x.get("product_name", ""), x.get("product_code", "")))
    return mapping


def _latest_monitoring_plan_ref(facility_id: int | None) -> MonitoringPlanRef | None:
    if not facility_id:
        return None
    with db() as s:
        mp = (
            s.execute(
                select(MonitoringPlan)
                .where(MonitoringPlan.facility_id == int(facility_id))
                .order_by(MonitoringPlan.updated_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        if not mp:
            return None
        return MonitoringPlanRef(
            id=int(mp.id),
            facility_id=int(mp.facility_id),
            method=str(mp.method or ""),
            tier_level=str(mp.tier_level or ""),
            updated_at=(mp.updated_at.isoformat() if getattr(mp, "updated_at", None) else None),
        )


def _methodology_ref(methodology_id: int | None) -> Dict[str, Any] | None:
    if not methodology_id:
        return None
    with db() as s:
        m = s.get(Methodology, int(methodology_id))
        if not m:
            return None
        return {
            "id": int(m.id),
            "name": str(m.name or ""),
            "version": str(m.version or ""),
            "scope": str(m.scope or ""),
        }


def _factor_refs_from_meta(factor_meta_rows: List[Dict[str, Any]]) -> List[FactorRef]:
    refs: List[FactorRef] = []
    for fr in factor_meta_rows or []:
        if not isinstance(fr, dict):
            continue
        refs.append(
            FactorRef(
                id=fr.get("id"),
                factor_type=str(fr.get("factor_type") or ""),
                region=str(fr.get("region") or ""),
                year=fr.get("year"),
                version=str(fr.get("version") or ""),
                value=float(fr.get("value")) if fr.get("value") is not None else 0.0,
                unit=str(fr.get("unit") or ""),
                source=str(fr.get("source") or ""),
            )
        )
    refs.sort(key=lambda x: (x.factor_type, x.region, x.version, str(x.id)))
    return refs


def run_orchestrator(
    *,
    project_id: int,
    config: Dict[str, Any],
    scenario: Dict[str, Any],
    methodology_id: int | None,
    activity_snapshot_ref: Dict[str, Any],
    energy_df: pd.DataFrame,
    production_df: pd.DataFrame,
    materials_df: pd.DataFrame | None = None,
) -> Tuple[InputBundle, ResultBundle, Dict[str, Any]]:
    """Paket A2: Deterministik Orchestrator.

    Aynı input snapshot + aynı config + aynı factor/methodology versiyonu => aynı sonuç.
    """

    scenario = scenario or {}
    config = config or {}

    with db() as s:
        project = s.get(Project, int(project_id))
        if not project:
            raise ValueError("Proje bulunamadı.")

    period = _period_from_config(config)
    facility = _facility_from_project(project)
    product_mapping = _product_mapping_from_production_df(production_df)

    # Factor set lock (deterministik)
    region = str((config or {}).get("region") or facility.get("country") or "TR")
    electricity_method = str((config or {}).get("electricity_method") or "location")
    market_override = (config or {}).get("market_grid_factor_override", None)
    market_override_f = float(market_override) if market_override is not None and str(market_override).strip() != "" else None

    factor_meta_rows, _factor_lookup = resolve_factor_set_for_energy_df(
        energy_df=energy_df,
        region=region or "TR",
        electricity_method=electricity_method,
        market_grid_factor_override=market_override_f,
    )
    factor_refs = _factor_refs_from_meta(factor_meta_rows)

    # Monitoring plan ref
    mp_ref = _latest_monitoring_plan_ref(facility.get("id"))

    # Price ref
    price = PriceRef(
        eua_price_eur_per_t=_to_float((config or {}).get("eua_price_eur_per_t", 75.0), 75.0),
        fx_tl_per_eur=_to_float((config or {}).get("fx_tl_per_eur", 35.0), 35.0),
    )

    config_hash = sha256_json(config)

    input_bundle = InputBundle(
        engine_version=ENGINE_VERSION_PACKET_A,
        project_id=int(project_id),
        period=period,
        facility=facility,
        product_mapping=product_mapping,
        activity_snapshot_ref=activity_snapshot_ref or {},
        monitoring_plan_ref=mp_ref,
        factor_set_ref=factor_refs,
        price_ref=price,
        config=config,
        config_hash=config_hash,
        methodology_ref=_methodology_ref(methodology_id),
        scenario=scenario,
    )

    input_bundle_hash = input_bundle.input_bundle_hash()

    # Core compute
    energy_out = energy_emissions(
        energy_df,
        region=region or "TR",
        electricity_method=electricity_method,
        market_grid_factor_override=market_override_f,
        factor_set_lock=[fr.to_dict() for fr in factor_refs],
    )

    # ETS cost + verification payload
    ets_cfg = (config or {}).get("ets") or {}
    free_alloc = _to_float(ets_cfg.get("free_alloc_t", 0.0), 0.0)
    banked = _to_float(ets_cfg.get("banked_t", 0.0), 0.0)

    ets_cost = ets_net_and_cost(
        scope1_tco2=float(energy_out.get("direct_tco2", 0.0) or 0.0),
        free_alloc_t=free_alloc,
        banked_t=banked,
        allowance_price_eur_per_t=price.eua_price_eur_per_t,
        fx_tl_per_eur=price.fx_tl_per_eur,
    )

    mp_for_payload = mp_ref.to_dict() if mp_ref else None
    ets_verif = ets_verification_payload(
        fuel_rows=list(energy_out.get("fuel_rows", []) or []),
        monitoring_plan=mp_for_payload,
        uncertainty_notes=str((config or {}).get("uncertainty_notes", "") or ""),
    )

    # CBAM compute
    cbam_df, cbam_totals = cbam_compute(
        production_df=production_df,
        energy_breakdown=energy_out,
        materials_df=materials_df,
        eua_price_eur_per_t=price.eua_price_eur_per_t,
        allocation_basis=str(((config or {}).get("cbam") or {}).get("allocation_basis", "quantity") or "quantity"),
    )
    cbam_table = cbam_df.to_dict(orient="records") if cbam_df is not None and len(cbam_df) > 0 else []

    totals = {
        "scope1_tco2": float(energy_out.get("direct_tco2", 0.0) or 0.0),
        "scope2_tco2": float(energy_out.get("indirect_tco2", 0.0) or 0.0),
        "total_tco2": float(energy_out.get("total_tco2", 0.0) or 0.0),
    }

    breakdown = {
        "energy": {
            "direct_tco2": totals["scope1_tco2"],
            "indirect_tco2": totals["scope2_tco2"],
            "fuel_rows": list(energy_out.get("fuel_rows", []) or []),
            "electricity_rows": list(energy_out.get("electricity_rows", []) or []),
        },
        "cbam": {
            "table": cbam_table,
            "totals": cbam_totals or {},
        },
    }

    unit_conversions = {
        "notes": [
            "MVP: enerji hesapları yakıt birimlerini NCV ile GJ'e çevirerek tCO2 hesaplar.",
            "Elektrik için kWh bazında grid factor (kgCO2/kWh) kullanılır.",
        ]
    }

    source_references = {
        "factor_set_ref": [fr.to_dict() for fr in factor_refs],
        "monitoring_plan_ref": (mp_ref.to_dict() if mp_ref else None),
        "methodology_ref": _methodology_ref(methodology_id),
        "price_ref": price.to_dict(),
    }

    qa_flags: List[QAFlag] = []
    if (energy_df is None) or len(energy_df) == 0:
        qa_flags.append(QAFlag(flag_id="QA_EMPTY_ENERGY", severity="fail", message_tr="Energy dataset boş.", context={}))
    if (production_df is None) or len(production_df) == 0:
        qa_flags.append(QAFlag(flag_id="QA_EMPTY_PRODUCTION", severity="fail", message_tr="Production dataset boş.", context={}))

    cost_outputs = {
        "ets": ets_cost,
        "cbam": {
            "eua_price_eur_per_t": price.eua_price_eur_per_t,
            "estimated_cost_eur": float((cbam_totals or {}).get("estimated_cost_eur", 0.0) or 0.0),
            "estimated_cost_tl": float((cbam_totals or {}).get("estimated_cost_tl", 0.0) or 0.0),
        },
    }

    tmp_rb = {
        "engine_version": ENGINE_VERSION_PACKET_A,
        "input_bundle_hash": input_bundle_hash,
        "totals": totals,
        "breakdown": breakdown,
        "unit_conversions": unit_conversions,
        "source_references": source_references,
        "qa_flags": [q.to_dict() for q in qa_flags],
        "compliance_checks": [],
        "cost_outputs": cost_outputs,
    }
    result_hash = sha256_json({"result": tmp_rb})

    result_bundle = ResultBundle(
        engine_version=ENGINE_VERSION_PACKET_A,
        input_bundle_hash=input_bundle_hash,
        result_hash=result_hash,
        totals=totals,
        breakdown=breakdown,
        unit_conversions=unit_conversions,
        source_references=source_references,
        qa_flags=qa_flags,
        compliance_checks=[],
        cost_outputs=cost_outputs,
    )

    legacy_results: Dict[str, Any] = {
        "kpis": {
            "scope1_tco2": totals["scope1_tco2"],
            "scope2_tco2": totals["scope2_tco2"],
            "total_tco2": totals["total_tco2"],
        },
        "cbam_table": cbam_table,
        "cbam": cbam_totals or {},
        "ets": {
            "net_and_cost": ets_cost,
            "verification": ets_verif,
        },
        "input_bundle": input_bundle.to_canonical_dict(),
        "deterministic": {
            "engine_version": ENGINE_VERSION_PACKET_A,
            "input_bundle_hash": input_bundle_hash,
            "result_hash": result_hash,
            "config_hash": config_hash,
        },
        "qa_flags": [q.to_dict() for q in qa_flags],
        "compliance_checks": [],
    }

    legacy_results["results_json"] = dict(legacy_results)

    return input_bundle, result_bundle, legacy_results
