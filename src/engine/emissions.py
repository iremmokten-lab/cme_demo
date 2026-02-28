from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import select

from src.db.models import EmissionFactor, FactorSet
from src.db.session import db


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def _to_float(x: Any) -> float:
    try:
        if pd.isna(x):
            return 0.0
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return 0.0


@dataclass
class FuelFactorPack:
    ncv_gj_per_unit: float
    ef_tco2_per_gj: float
    oxidation_factor: float
    source: str = ""
    meta: Dict[str, Any] | None = None


# ----------------------------
# Factor resolution (versioned + deterministic)
# ----------------------------
def _pick_latest_factor(rows: List[EmissionFactor]) -> Optional[EmissionFactor]:
    if not rows:
        return None
    return sorted(
        rows,
        key=lambda r: (
            int(r.year) if r.year is not None else -1,
            str(r.version or ""),
            int(r.id) if getattr(r, "id", None) is not None else -1,
        ),
        reverse=True,
    )[0]


def _get_active_factor_set_id(project_id: int, region: str = "TR") -> Optional[int]:
    with db() as s:
        fs = (
            s.execute(
                select(FactorSet)
                .where(FactorSet.project_id == project_id, FactorSet.region == region)
                .order_by(FactorSet.year.desc().nullslast(), FactorSet.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        return int(fs.id) if fs else None


def _get_factor_record(
    project_id: int,
    factor_type: str,
    region: str = "TR",
    factor_set_id: Optional[int] = None,
) -> Optional[EmissionFactor]:
    fsid = factor_set_id or _get_active_factor_set_id(project_id, region=region)
    with db() as s:
        q = select(EmissionFactor).where(
            EmissionFactor.project_id == project_id,
            EmissionFactor.factor_type == factor_type,
            EmissionFactor.region == region,
        )
        if fsid is not None:
            q = q.where(EmissionFactor.factor_set_id == fsid)
        rows = s.execute(q).scalars().all()
        return _pick_latest_factor(rows)


def _factor_meta(f: Optional[EmissionFactor], factor_type: str, region: str) -> Dict[str, Any]:
    if not f:
        return {
            "id": None,
            "factor_type": factor_type,
            "region": region,
            "year": None,
            "version": None,
            "unit": None,
            "source": "DEFAULT",
            "reference": "",
            "value": None,
        }
    return {
        "id": int(f.id),
        "factor_type": str(f.factor_type),
        "region": str(f.region),
        "year": int(f.year) if f.year is not None else None,
        "version": str(f.version or ""),
        "unit": str(f.unit or ""),
        "source": str(f.source or ""),
        "reference": str(f.reference or ""),
        "value": float(f.value),
    }


def _default_fuel_pack(fuel_type: str) -> FuelFactorPack:
    ft = _norm(fuel_type)
    if ft in {"natural_gas", "dogalgaz", "ng"}:
        return FuelFactorPack(ncv_gj_per_unit=0.038, ef_tco2_per_gj=0.0561, oxidation_factor=0.995, source="DEFAULT")
    if ft in {"coal", "komur", "hard_coal"}:
        return FuelFactorPack(ncv_gj_per_unit=0.025, ef_tco2_per_gj=0.0946, oxidation_factor=0.99, source="DEFAULT")
    if ft in {"diesel", "motorin"}:
        return FuelFactorPack(ncv_gj_per_unit=0.0359, ef_tco2_per_gj=0.0741, oxidation_factor=0.99, source="DEFAULT")
    if ft in {"fuel_oil", "fueloil"}:
        return FuelFactorPack(ncv_gj_per_unit=0.0404, ef_tco2_per_gj=0.0774, oxidation_factor=0.99, source="DEFAULT")
    return FuelFactorPack(ncv_gj_per_unit=0.03, ef_tco2_per_gj=0.07, oxidation_factor=0.99, source="DEFAULT")


def _lookup_from_factor_set_lock(factor_set_lock: Any) -> Optional[Dict[str, Dict[str, Any]]]:
    if not factor_set_lock:
        return None
    lookup: Dict[str, Dict[str, Any]] = {}
    if isinstance(factor_set_lock, dict):
        for k, v in factor_set_lock.items():
            if isinstance(v, dict) and (v.get("factor_type") or k):
                ft = str(v.get("factor_type") or k)
                lookup[ft] = v
        return lookup or None
    if isinstance(factor_set_lock, list):
        for it in factor_set_lock:
            if isinstance(it, dict) and it.get("factor_type"):
                lookup[str(it["factor_type"])] = it
    return lookup or None


def resolve_factor_set_for_energy_df(
    project_id: int,
    df_energy: pd.DataFrame,
    region: str = "TR",
    factor_set_id: Optional[int] = None,
) -> Dict[str, Any]:
    fuels: List[str] = []
    if isinstance(df_energy, pd.DataFrame) and not df_energy.empty and "fuel_type" in df_energy.columns:
        fuels = sorted({_norm(x) for x in df_energy["fuel_type"].dropna().tolist()})
    factor_types: List[str] = []
    for f in fuels:
        if not f:
            continue
        if "elektr" in f or "electric" in f:
            continue
        factor_types.extend([f"ncv:{f}", f"ef:{f}", f"of:{f}"])
    factor_types.extend(["grid:location", "grid:market"])

    lookup: Dict[str, Dict[str, Any]] = {}
    used_default = False
    for ft in sorted(set(factor_types)):
        rec = _get_factor_record(project_id, ft, region=region, factor_set_id=factor_set_id)
        if rec:
            lookup[ft] = _factor_meta(rec, ft, region)
        else:
            used_default = True
            lookup[ft] = _factor_meta(None, ft, region)
    refs = list(lookup.values())
    refs.sort(key=lambda x: (str(x.get("factor_type", "")), str(x.get("region", ""))))
    return {"refs": refs, "lookup": lookup, "used_default": used_default, "region": region, "factor_set_id": factor_set_id}


def _val_from_lookup(lookup: Dict[str, Dict[str, Any]], factor_type: str, default: float) -> Tuple[float, str, Dict[str, Any]]:
    meta = lookup.get(factor_type) if lookup else None
    if meta and meta.get("value") is not None:
        return float(meta["value"]), str(meta.get("source") or ""), meta
    return float(default), "DEFAULT", (meta or {"factor_type": factor_type, "source": "DEFAULT"})


def _combustion_direct(
    project_id: int,
    df_energy: pd.DataFrame,
    *,
    region: str = "TR",
    factor_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    factor_set_id: Optional[int] = None,
) -> Dict[str, Any]:
    if df_energy is None or df_energy.empty:
        return {"rows": [], "totals": {"tco2": 0.0, "gj": 0.0}, "factor_refs": [], "used_default_factors": False}

    df = df_energy.copy()
    for col in ["month", "fuel_type", "quantity", "unit"]:
        if col not in df.columns:
            df[col] = None

    df["fuel_type_norm"] = df["fuel_type"].apply(_norm)
    df["quantity_num"] = df["quantity"].apply(_to_float)

    bundle = resolve_factor_set_for_energy_df(project_id, df, region=region, factor_set_id=factor_set_id)
    lookup = factor_lookup or bundle["lookup"]

    out_rows: List[Dict[str, Any]] = []
    total_tco2 = 0.0
    total_gj = 0.0

    for _, r in df.iterrows():
        ft = _norm(r.get("fuel_type_norm") or r.get("fuel_type") or "")
        if "elektr" in ft or "electric" in ft:
            continue
        qty = _to_float(r.get("quantity_num") or r.get("quantity") or 0.0)

        default_pack = _default_fuel_pack(ft)
        ncv, ncv_src, ncv_meta = _val_from_lookup(lookup, f"ncv:{ft}", default_pack.ncv_gj_per_unit)
        ef, ef_src, ef_meta = _val_from_lookup(lookup, f"ef:{ft}", default_pack.ef_tco2_per_gj)
        of, of_src, of_meta = _val_from_lookup(lookup, f"of:{ft}", default_pack.oxidation_factor)

        gj = qty * ncv
        tco2 = gj * ef * of

        total_gj += gj
        total_tco2 += tco2

        out_rows.append(
            {
                "month": str(r.get("month") or ""),
                "fuel_type": str(r.get("fuel_type") or ""),
                "unit": str(r.get("unit") or ""),
                "quantity": qty,
                "ncv_gj_per_unit": ncv,
                "ef_tco2_per_gj": ef,
                "oxidation_factor": of,
                "gj": gj,
                "tco2": tco2,
                "factor_sources": {"ncv": ncv_src, "ef": ef_src, "of": of_src},
                "factor_meta": {"ncv": ncv_meta, "ef": ef_meta, "of": of_meta},
            }
        )

    return {
        "rows": out_rows,
        "totals": {"tco2": float(total_tco2), "gj": float(total_gj)},
        "factor_refs": bundle["refs"],
        "used_default_factors": bool(bundle["used_default"]),
        "region": region,
        "factor_set_id": factor_set_id,
    }


def _electricity_indirect(
    project_id: int,
    df_electricity: pd.DataFrame,
    *,
    region: str = "TR",
    method: str = "location",
    market_grid_factor_override: Optional[float] = None,
    factor_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    factor_set_id: Optional[int] = None,
) -> Dict[str, Any]:
    if df_electricity is None or df_electricity.empty:
        return {"rows": [], "totals": {"tco2": 0.0, "mwh": 0.0}, "factor_refs": [], "used_default_factors": False}

    df = df_electricity.copy()
    # Accept either "mwh" or "quantity"+unit forms
    if "mwh" not in df.columns:
        df["mwh"] = None
    if "month" not in df.columns:
        df["month"] = None
    if "quantity" in df.columns and df["mwh"].isna().all():
        # Try convert if unit is kwh or mwh
        units = df.get("unit", pd.Series([""] * len(df))).astype(str).str.lower()
        q = df.get("quantity", pd.Series([0] * len(df))).apply(_to_float)
        mwh = []
        for i in range(len(df)):
            u = units.iloc[i] if i < len(units) else ""
            if "kwh" in u:
                mwh.append(q.iloc[i] / 1000.0)
            else:
                mwh.append(q.iloc[i])
        df["mwh"] = mwh

    df["mwh_num"] = df["mwh"].apply(_to_float)

    dummy_energy = pd.DataFrame({"fuel_type": []})
    bundle = resolve_factor_set_for_energy_df(project_id, dummy_energy, region=region, factor_set_id=factor_set_id)
    lookup = factor_lookup or bundle["lookup"]

    method_norm = _norm(method)
    if method_norm not in {"location", "market"}:
        method_norm = "location"

    default_grid = 0.45  # placeholder
    v, src, meta = _val_from_lookup(lookup, f"grid:{method_norm}", default_grid)

    if method_norm == "market" and market_grid_factor_override is not None:
        v = float(market_grid_factor_override)
        src = "OVERRIDE"
        meta = {**meta, "value": v, "source": src, "override": True}

    out_rows: List[Dict[str, Any]] = []
    total_mwh = 0.0
    total_tco2 = 0.0
    for _, r in df.iterrows():
        mwh = _to_float(r.get("mwh_num") or r.get("mwh") or 0.0)
        tco2 = mwh * v
        total_mwh += mwh
        total_tco2 += tco2
        out_rows.append(
            {
                "month": str(r.get("month") or ""),
                "mwh": mwh,
                "grid_factor_tco2_per_mwh": v,
                "tco2": tco2,
                "factor_source": src,
                "factor_meta": meta,
                "method": method_norm,
            }
        )

    return {
        "rows": out_rows,
        "totals": {"tco2": float(total_tco2), "mwh": float(total_mwh)},
        "factor_refs": bundle["refs"],
        "used_default_factors": bool(bundle["used_default"] or (src == "DEFAULT")),
        "region": region,
        "factor_set_id": factor_set_id,
        "electricity_method": method_norm,
    }


def energy_emissions(
    df_energy: pd.DataFrame,
    *,
    project_id: int,
    region: str = "TR",
    electricity_method: str = "location",
    market_grid_factor_override: float | None = None,
    factor_set_lock: Any = None,
    factor_set_id: int | None = None,
) -> Dict[str, Any]:
    """Orchestrator uyumlu ana API.

    Çıktı (CBAM/ETS uyumlu):
      - direct_rows, indirect_rows
      - direct_tco2, indirect_tco2, total_tco2
      - factor_refs (kilitlenebilir)
      - used_default_factors (compliance flag)
    """
    df_energy = df_energy if isinstance(df_energy, pd.DataFrame) else pd.DataFrame()
    # Heuristic split
    elec_mask = pd.Series([False] * len(df_energy))
    if not df_energy.empty and "fuel_type" in df_energy.columns:
        elec_mask = df_energy["fuel_type"].astype(str).str.lower().str.contains("elektr|electric")
    if not df_energy.empty and "mwh" in df_energy.columns:
        elec_mask = elec_mask | df_energy["mwh"].notna()

    elec_df = df_energy[elec_mask].copy() if not df_energy.empty else pd.DataFrame()
    fuel_df = df_energy[~elec_mask].copy() if not df_energy.empty else pd.DataFrame()

    lock_lookup = _lookup_from_factor_set_lock(factor_set_lock)

    direct = _combustion_direct(project_id, fuel_df, region=region, factor_lookup=lock_lookup, factor_set_id=factor_set_id)
    indirect = _electricity_indirect(
        project_id,
        elec_df,
        region=region,
        method=electricity_method,
        market_grid_factor_override=market_grid_factor_override,
        factor_lookup=lock_lookup,
        factor_set_id=factor_set_id,
    )

    factor_refs = []
    for fr in (direct.get("factor_refs") or []):
        factor_refs.append(fr)
    for fr in (indirect.get("factor_refs") or []):
        factor_refs.append(fr)

    # de-duplicate by factor_type+region
    seen = set()
    uniq = []
    for fr in factor_refs:
        if not isinstance(fr, dict):
            continue
        key = (str(fr.get("factor_type") or ""), str(fr.get("region") or ""))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(fr)
    uniq.sort(key=lambda x: (str(x.get("factor_type", "")), str(x.get("region", ""))))

    direct_t = float(((direct.get("totals") or {}).get("tco2") or 0.0))
    indirect_t = float(((indirect.get("totals") or {}).get("tco2") or 0.0))
    used_default = bool(direct.get("used_default_factors") or indirect.get("used_default_factors"))

    return {
        "direct_rows": direct.get("rows") or [],
        "indirect_rows": indirect.get("rows") or [],
        "direct_tco2": direct_t,
        "indirect_tco2": indirect_t,
        "total_tco2": direct_t + indirect_t,
        "direct_meta": {"gj": float(((direct.get("totals") or {}).get("gj") or 0.0))},
        "indirect_meta": {"mwh": float(((indirect.get("totals") or {}).get("mwh") or 0.0)), "method": electricity_method},
        "factor_refs": uniq,
        "used_default_factors": used_default,
        "region": region,
        "factor_set_id": factor_set_id,
    }
