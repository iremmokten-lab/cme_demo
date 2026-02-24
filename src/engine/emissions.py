from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import select

from src.db.models import EmissionFactor
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
    of: float
    source: str = ""
    meta: Dict[str, Any] = None


def _pick_latest_factor(rows: List[EmissionFactor]) -> EmissionFactor | None:
    if not rows:
        return None
    # year desc, version desc, id desc (deterministik)
    rows_sorted = sorted(
        rows,
        key=lambda r: (
            int(r.year) if r.year is not None else -1,
            str(r.version or ""),
            int(r.id) if getattr(r, "id", None) is not None else -1,
        ),
        reverse=True,
    )
    return rows_sorted[0]


def _get_factor_record(factor_type: str, region: str = "TR") -> Optional[EmissionFactor]:
    with db() as s:
        rows = (
            s.execute(
                select(EmissionFactor).where(
                    EmissionFactor.factor_type == factor_type,
                    EmissionFactor.region == region,
                )
            )
            .scalars()
            .all()
        )
        return _pick_latest_factor(rows)


def _default_fuel_pack(fuel_type: str) -> FuelFactorPack | None:
    ft = _norm(fuel_type)
    if ft in ("natural_gas", "ng", "dogalgaz", "doğalgaz"):
        return FuelFactorPack(ncv_gj_per_unit=0.038, ef_tco2_per_gj=0.0561, of=0.995, source="Demo fallback", meta={"fallback": True})
    if ft in ("diesel", "motorin"):
        return FuelFactorPack(ncv_gj_per_unit=0.036, ef_tco2_per_gj=0.0741, of=0.995, source="Demo fallback", meta={"fallback": True})
    if ft in ("coal", "komur", "kömür", "lignite", "linyit"):
        return FuelFactorPack(ncv_gj_per_unit=0.025, ef_tco2_per_gj=0.0946, of=0.98, source="Demo fallback", meta={"fallback": True})
    return None


def _factor_meta(f: Optional[EmissionFactor], factor_type: str, region: str) -> Dict[str, Any]:
    if not f:
        return {
            "id": None,
            "factor_type": factor_type,
            "region": region,
            "year": None,
            "version": "",
            "value": None,
            "unit": "",
            "source": "",
        }
    return {
        "id": int(f.id),
        "factor_type": str(f.factor_type),
        "region": str(f.region),
        "year": int(f.year) if f.year is not None else None,
        "version": str(f.version or ""),
        "value": float(f.value),
        "unit": str(f.unit or ""),
        "source": str(f.source or ""),
    }


def resolve_factor_set_for_energy_df(
    *,
    energy_df: pd.DataFrame,
    region: str = "TR",
    electricity_method: str = "location",
    market_grid_factor_override: float | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Paket A: deterministik factor_set_ref üretir.
    Dönen:
      - factor_refs: list[dict] (FactorRef benzeri meta)
      - lookup: factor_type -> dict(meta)
    """
    df = energy_df.copy() if energy_df is not None else pd.DataFrame()
    if len(df) > 0:
        df.columns = [_norm(c) for c in df.columns]

    # fuel types best effort
    fuel_types = set()
    if "fuel_type" in df.columns:
        for x in df["fuel_type"].tolist():
            ft = _norm(x)
            if ft:
                fuel_types.add(ft)

    # legacy wide format yakalama: natural_gas_m3, diesel_l vb. (best effort)
    for c in df.columns:
        cc = _norm(c)
        if cc.endswith("_m3") and "natural_gas" in cc:
            fuel_types.add("natural_gas")
        if cc.endswith("_l") and "diesel" in cc:
            fuel_types.add("diesel")
        if "coal" in cc:
            fuel_types.add("coal")

    # electricity factor
    m = _norm(electricity_method)
    if m not in ("location", "market"):
        m = "location"

    factor_types: List[str] = []
    factor_types.append(f"grid:{m}")
    for ft in sorted(fuel_types):
        # electricity satırları fuel_type olarak gelebilir, atla
        if ft in ("electricity", "grid_electricity", "elektrik"):
            continue
        factor_types.extend([f"ncv:{ft}", f"ef:{ft}", f"of:{ft}"])

    refs: List[Dict[str, Any]] = []
    lookup: Dict[str, Dict[str, Any]] = {}

    for ft in factor_types:
        rec = _get_factor_record(ft, region=region)
        meta = _factor_meta(rec, ft, region)
        # override varsa grid meta’yı işaretle (değer yine meta.value’da; compute’de override uygulanır)
        if ft.startswith("grid:") and m == "market" and market_grid_factor_override is not None:
            meta = dict(meta)
            meta["override_value"] = float(market_grid_factor_override)
            meta["override_source"] = "Config override"
        refs.append(meta)
        lookup[ft] = meta

    # deterministik sıra
    refs.sort(key=lambda x: (str(x.get("factor_type", "")), str(x.get("region", "")), str(x.get("version", "")), str(x.get("id", ""))))
    return refs, lookup


def get_fuel_factor_pack(
    fuel_type: str,
    region: str = "TR",
    factor_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> FuelFactorPack:
    """
    Yakıt bazlı NCV/EF/OF paketini DB’den çeker.
    Paket A: factor_lookup verilirse DB’ye tekrar gitmeden deterministik kilitli değerleri kullanır.
    """
    ft = _norm(fuel_type)

    def _val(factor_type: str) -> Tuple[Optional[float], str, Dict[str, Any]]:
        if factor_lookup and factor_type in factor_lookup:
            m = factor_lookup[factor_type]
            v = m.get("value", None)
            return (float(v) if v is not None else None), str(m.get("source", "") or ""), m
        rec = _get_factor_record(factor_type, region=region)
        if not rec:
            return None, "", _factor_meta(None, factor_type, region)
        return float(rec.value), (rec.source or ""), _factor_meta(rec, factor_type, region)

    ncv, ncv_src, ncv_meta = _val(f"ncv:{ft}")
    ef, ef_src, ef_meta = _val(f"ef:{ft}")
    of, of_src, of_meta = _val(f"of:{ft}")

    if ncv is None or ef is None or of is None:
        fb = _default_fuel_pack(ft)
        if fb:
            return fb
        return FuelFactorPack(
            ncv_gj_per_unit=float(ncv or 0.0),
            ef_tco2_per_gj=float(ef or 0.0),
            of=float(of or 1.0),
            source="Eksik faktör",
            meta={"ncv": ncv_meta, "ef": ef_meta, "of": of_meta},
        )

    return FuelFactorPack(
        ncv_gj_per_unit=float(ncv),
        ef_tco2_per_gj=float(ef),
        of=float(of),
        source=" | ".join([x for x in [ncv_src, ef_src, of_src] if x])[:300],
        meta={"ncv": ncv_meta, "ef": ef_meta, "of": of_meta},
    )


def get_grid_factor(
    method: str = "location",
    region: str = "TR",
    factor_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    market_grid_factor_override: float | None = None,
) -> Tuple[float, str, Dict[str, Any]]:
    """
    kgCO2e/kWh döner.
    Paket A: factor_lookup ile kilitli faktör kullanır.
    """
    m = _norm(method)
    if m not in ("location", "market"):
        m = "location"
    factor_type = f"grid:{m}"

    meta = None
    if factor_lookup and factor_type in factor_lookup:
        meta = factor_lookup[factor_type]
        v = meta.get("value", None)
        src = str(meta.get("source", "") or "")
        val = float(v) if v is not None else (0.10 if m == "market" else 0.42)
    else:
        rec = _get_factor_record(factor_type, region=region)
        if not rec:
            val = 0.10 if m == "market" else 0.42
            src = "Demo fallback"
            meta = _factor_meta(None, factor_type, region)
            meta["value"] = val
            meta["source"] = src
        else:
            val = float(rec.value)
            src = rec.source or ""
            meta = _factor_meta(rec, factor_type, region)

    # Override (market)
    if m == "market" and market_grid_factor_override is not None:
        val = float(market_grid_factor_override)
        src = "Config override"
        meta = dict(meta or {})
        meta["override_value"] = val
        meta["override_source"] = src

    return float(val), src, (meta or {})


def _detect_energy_format(df: pd.DataFrame) -> str:
    cols = {_norm(c) for c in df.columns}
    target = {"month", "facility_id", "fuel_type", "fuel_quantity", "fuel_unit"}
    if target.issubset(cols):
        return "row_fuel"
    if {"month", "facility_id"}.issubset(cols) and (("natural_gas_m3" in cols) or ("electricity_kwh" in cols)):
        return "wide_legacy"
    return "unknown"


def _from_wide_legacy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    cols = {_norm(c): c for c in out.columns}
    rows = []
    for _, r in out.iterrows():
        month = r.get(cols.get("month"))
        facility_id = r.get(cols.get("facility_id"))
        if "natural_gas_m3" in cols:
            q = _to_float(r.get(cols["natural_gas_m3"]))
            if q:
                rows.append({"month": month, "facility_id": facility_id, "fuel_type": "natural_gas", "fuel_quantity": q, "fuel_unit": "Nm3"})
        if "electricity_kwh" in cols:
            q = _to_float(r.get(cols["electricity_kwh"]))
            if q:
                rows.append({"month": month, "facility_id": facility_id, "fuel_type": "electricity", "fuel_quantity": q, "fuel_unit": "kWh"})
    return pd.DataFrame(rows)


def energy_emissions(
    energy_df: pd.DataFrame,
    region: str = "TR",
    electricity_method: str = "location",
    market_grid_factor_override: float | None = None,
    factor_set_lock: Optional[List[Dict[str, Any]]] = None,
) -> dict:
    """
    Regülasyon yaklaşımı: yakıt bazlı direct + elektrik bazlı indirect.

    Paket A:
      - factor_set_lock verilirse DB’den “latest” çekmek yerine bu faktör setine kilitlenir.
      - fuel_rows/electricity_rows içine faktör meta referansları da eklenir.
    """
    if energy_df is None or len(energy_df) == 0:
        return {
            "direct_tco2": 0.0,
            "indirect_tco2": 0.0,
            "total_tco2": 0.0,
            "fuel_rows": [],
            "electricity_rows": [],
            "notes": ["Boş energy dataset"],
        }

    # Factor lookup
    factor_lookup = {}
    if factor_set_lock:
        for fr in factor_set_lock:
            if isinstance(fr, dict) and fr.get("factor_type"):
                factor_lookup[str(fr["factor_type"])] = fr

    fmt = _detect_energy_format(energy_df)
    if fmt == "wide_legacy":
        df = _from_wide_legacy(energy_df)
    elif fmt == "row_fuel":
        df = energy_df.copy()
    else:
        df = energy_df.copy()
        df.columns = [_norm(c) for c in df.columns]

    df.columns = [_norm(c) for c in df.columns]

    for col in ["fuel_type", "fuel_quantity"]:
        if col not in df.columns:
            df[col] = None if col == "fuel_type" else 0.0
    if "fuel_unit" not in df.columns:
        df["fuel_unit"] = ""

    def is_electric(row) -> bool:
        ft = _norm(row.get("fuel_type"))
        u = _norm(row.get("fuel_unit"))
        if ft in ("electricity", "grid_electricity", "elektrik"):
            return True
        if u in ("kwh", "mwh"):
            return True
        return False

    def to_kwh(q: float, unit: str) -> float:
        u = _norm(unit)
        if u == "mwh":
            return q * 1000.0
        if u == "kwh":
            return q
        return q

    grid_factor_kg_per_kwh, grid_src, grid_meta = get_grid_factor(
        electricity_method,
        region=region,
        factor_lookup=factor_lookup if factor_lookup else None,
        market_grid_factor_override=market_grid_factor_override,
    )

    direct_tco2 = 0.0
    indirect_tco2 = 0.0
    fuel_rows: List[dict] = []
    elec_rows: List[dict] = []
    notes: List[str] = []

    for _, r in df.iterrows():
        ft = _norm(r.get("fuel_type"))
        qty = _to_float(r.get("fuel_quantity"))
        unit = str(r.get("fuel_unit") or "")
        if qty == 0.0:
            continue

        if is_electric(r):
            kwh = to_kwh(qty, unit)
            tco2 = (kwh * grid_factor_kg_per_kwh) / 1000.0
            indirect_tco2 += tco2
            elec_rows.append(
                {
                    "fuel_type": "electricity",
                    "kwh": kwh,
                    "grid_method": _norm(electricity_method),
                    "grid_factor_kg_per_kwh": grid_factor_kg_per_kwh,
                    "tco2": tco2,
                    "source": grid_src,
                    "factor_meta": grid_meta,
                }
            )
            continue

        pack = get_fuel_factor_pack(ft, region=region, factor_lookup=factor_lookup if factor_lookup else None)
        tco2 = qty * pack.ncv_gj_per_unit * pack.ef_tco2_per_gj * pack.of
        direct_tco2 += tco2
        fuel_rows.append(
            {
                "fuel_type": ft,
                "quantity": qty,
                "unit": unit,
                "ncv_gj_per_unit": pack.ncv_gj_per_unit,
                "ef_tco2_per_gj": pack.ef_tco2_per_gj,
                "oxidation_factor": pack.of,
                "tco2": tco2,
                "source": pack.source,
                "factor_meta": pack.meta or {},
            }
        )

    total_tco2 = direct_tco2 + indirect_tco2
    if total_tco2 == 0.0:
        notes.append("Toplam emisyon 0 görünüyor. Fuel/electricity alanlarını ve faktörleri kontrol edin.")

    return {
        "direct_tco2": float(direct_tco2),
        "indirect_tco2": float(indirect_tco2),
        "total_tco2": float(total_tco2),
        "fuel_rows": fuel_rows,
        "electricity_rows": elec_rows,
        "notes": notes,
    }
