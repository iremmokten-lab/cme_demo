from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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


def _pick_latest_factor(rows: list[EmissionFactor]) -> EmissionFactor | None:
    if not rows:
        return None
    # year desc, version desc (string) basit
    rows_sorted = sorted(
        rows,
        key=lambda r: (
            int(r.year) if r.year is not None else -1,
            str(r.version or ""),
        ),
        reverse=True,
    )
    return rows_sorted[0]


def _get_factor_value(factor_type: str, region: str = "TR") -> tuple[float | None, str]:
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
    f = _pick_latest_factor(rows)
    if not f:
        return None, ""
    return float(f.value), (f.source or "")


def _default_fuel_pack(fuel_type: str) -> FuelFactorPack | None:
    ft = _norm(fuel_type)
    if ft in ("natural_gas", "ng", "dogalgaz", "doğalgaz"):
        return FuelFactorPack(ncv_gj_per_unit=0.038, ef_tco2_per_gj=0.0561, of=0.995, source="Demo fallback")
    if ft in ("diesel", "motorin"):
        return FuelFactorPack(ncv_gj_per_unit=0.036, ef_tco2_per_gj=0.0741, of=0.995, source="Demo fallback")
    if ft in ("coal", "komur", "kömür", "lignite", "linyit"):
        return FuelFactorPack(ncv_gj_per_unit=0.025, ef_tco2_per_gj=0.0946, of=0.98, source="Demo fallback")
    return None


def get_fuel_factor_pack(fuel_type: str, region: str = "TR") -> FuelFactorPack:
    """Yakıt bazlı NCV/EF/OF paketini DB’den çeker. Yoksa demo fallback."""
    ft = _norm(fuel_type)

    ncv, ncv_src = _get_factor_value(f"ncv:{ft}", region=region)
    ef, ef_src = _get_factor_value(f"ef:{ft}", region=region)
    of, of_src = _get_factor_value(f"of:{ft}", region=region)

    if ncv is None or ef is None or of is None:
        fb = _default_fuel_pack(ft)
        if fb:
            return fb
        # Son çare: sıfırla
        return FuelFactorPack(ncv_gj_per_unit=float(ncv or 0.0), ef_tco2_per_gj=float(ef or 0.0), of=float(of or 1.0), source="Eksik faktör")

    return FuelFactorPack(
        ncv_gj_per_unit=float(ncv),
        ef_tco2_per_gj=float(ef),
        of=float(of),
        source=" | ".join([x for x in [ncv_src, ef_src, of_src] if x])[:300],
    )


def get_grid_factor(method: str = "location", region: str = "TR") -> tuple[float, str]:
    """kgCO2e/kWh döner."""
    m = _norm(method)
    if m not in ("location", "market"):
        m = "location"
    v, src = _get_factor_value(f"grid:{m}", region=region)
    if v is None:
        # Demo fallback
        if m == "market":
            return 0.10, "Demo fallback"
        return 0.42, "Demo fallback"
    return float(v), src


def _detect_energy_format(df: pd.DataFrame) -> str:
    cols = {_norm(c) for c in df.columns}
    # Paket A hedef şema
    target = {"month", "facility_id", "fuel_type", "fuel_quantity", "fuel_unit"}
    if target.issubset(cols):
        return "row_fuel"
    # Eski demo şema (yaklaşık)
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
                rows.append(
                    {
                        "month": month,
                        "facility_id": facility_id,
                        "fuel_type": "natural_gas",
                        "fuel_quantity": q,
                        "fuel_unit": "Nm3",
                    }
                )
        if "electricity_kwh" in cols:
            q = _to_float(r.get(cols["electricity_kwh"]))
            if q:
                rows.append(
                    {
                        "month": month,
                        "facility_id": facility_id,
                        "fuel_type": "electricity",
                        "fuel_quantity": q,
                        "fuel_unit": "kWh",
                    }
                )
    return pd.DataFrame(rows)


def energy_emissions(
    energy_df: pd.DataFrame,
    region: str = "TR",
    electricity_method: str = "location",
    market_grid_factor_override: float | None = None,
) -> dict:
    """Regülasyon yaklaşımı: yakıt bazlı direct + elektrik bazlı indirect.

    Çıktı:
    - direct_tco2, indirect_tco2, total_tco2
    - fuel_rows: fuel bazlı satırlar (tCO2, kullanılan faktörler)
    - electricity_rows: elektrik satırları (kWh, grid_factor)
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

    fmt = _detect_energy_format(energy_df)
    if fmt == "wide_legacy":
        df = _from_wide_legacy(energy_df)
    elif fmt == "row_fuel":
        df = energy_df.copy()
    else:
        # En iyi çaba: kolon isimlerini normalize ederek beklenenlere yaklaştır
        df = energy_df.copy()
        df.columns = [_norm(c) for c in df.columns]

    # Normalize column names
    df.columns = [_norm(c) for c in df.columns]

    # Required columns
    for col in ["fuel_type", "fuel_quantity"]:
        if col not in df.columns:
            df[col] = None if col == "fuel_type" else 0.0
    if "fuel_unit" not in df.columns:
        df["fuel_unit"] = ""

    # Electricity detection: fuel_type == electricity OR unit is kwh
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

    grid_factor_kg_per_kwh, grid_src = get_grid_factor(electricity_method, region=region)
    if _norm(electricity_method) == "market" and market_grid_factor_override is not None:
        grid_factor_kg_per_kwh = float(market_grid_factor_override)
        grid_src = "Config override"

    direct_tco2 = 0.0
    indirect_tco2 = 0.0
    fuel_rows: list[dict] = []
    elec_rows: list[dict] = []
    notes: list[str] = []

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
                }
            )
            continue

        pack = get_fuel_factor_pack(ft, region=region)
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
