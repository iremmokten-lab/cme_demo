from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

from src.mrv.lineage import sha256_json
from .emissions import safe_float, validate_required_columns, validate_nonnegative


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def _pick_col(df: pd.DataFrame, *names: str) -> str:
    cols = {_norm(c): c for c in df.columns}
    for n in names:
        nn = _norm(n)
        if nn in cols:
            return cols[nn]
    return ""


def _weights_quantity(df: pd.DataFrame) -> pd.Series:
    qcol = _pick_col(df, "quantity")
    if not qcol:
        return pd.Series([0.0] * len(df))
    q = df[qcol].apply(safe_float).clip(lower=0.0)
    s = float(q.sum())
    return (q / s) if s > 0 else pd.Series([0.0] * len(df))


def _weights_energy_content(df: pd.DataFrame) -> pd.Series:
    # Beklenen kolonlar: energy_content_mj veya energy_content_gj (satır başına ürün enerji içeriği)
    mj = _pick_col(df, "energy_content_mj", "energy_content")
    gj = _pick_col(df, "energy_content_gj")
    if gj:
        w = df[gj].apply(safe_float).clip(lower=0.0)
    elif mj:
        w = df[mj].apply(safe_float).clip(lower=0.0)
    else:
        return _weights_quantity(df)
    s = float(w.sum())
    return (w / s) if s > 0 else _weights_quantity(df)


def _weights_process_step(df: pd.DataFrame) -> pd.Series:
    # Beklenen kolon: process_step_share (0-1 arası), yoksa quantity fallback
    pcol = _pick_col(df, "process_step_share", "process_share", "allocation_share")
    if not pcol:
        return _weights_quantity(df)
    w = df[pcol].apply(safe_float).clip(lower=0.0)
    s = float(w.sum())
    if s <= 0:
        return _weights_quantity(df)
    return w / s


def allocate_product_emissions(
    production_df: pd.DataFrame,
    *,
    direct_tco2_total: float,
    indirect_tco2_total: float,
    method: str = "quantity_based",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Ürün bazlı emisyon tahsisi (allocation engine, FAZ 1).

    Amaç:
      - Enerji/ısı/steam gibi upstream akışlardan gelen direct/indirect toplam emisyonu
        ürün satırlarına deterministik şekilde dağıtmak.
      - Yöntemler:
          1) quantity-based
          2) energy-content-based
          3) process step-based

    Çıktı:
      - allocation_df: sku bazında allocated_direct_tco2, allocated_indirect_tco2, weight, method
      - meta: allocation_hash ve kullanılan weights özeti

    Not:
      - Bu fonksiyon production.csv'nin minimum kolonlarını bekler: sku, quantity
      - Diğer yöntem kolonları yoksa otomatik fallback yapar.
    """
    validate_required_columns(production_df, ["sku", "quantity"], "production.csv")
    df = production_df.copy()
    df.columns = [_norm(c) for c in df.columns]

    # Normalizasyon
    df["sku"] = df["sku"].astype(str).fillna("").apply(lambda x: str(x).strip())
    df["quantity"] = df["quantity"].apply(safe_float)
    validate_nonnegative(df, ["quantity"], "production.csv")

    m = _norm(method)
    if m in ("energy_content_based", "energy-content-based", "energy_content", "energy"):
        weights = _weights_energy_content(df)
        method_used = "energy_content_based"
    elif m in ("process_step_based", "process-step-based", "process_step", "process"):
        weights = _weights_process_step(df)
        method_used = "process_step_based"
    else:
        weights = _weights_quantity(df)
        method_used = "quantity_based"

    # Deterministik: sku bazında topla (aynı sku birden fazla satırsa)
    out = df[["sku"]].copy()
    out["weight"] = weights.fillna(0.0).astype(float)

    grouped = out.groupby("sku", dropna=False)["weight"].sum().reset_index()
    # deterministik sıralama
    grouped = grouped.sort_values(["sku"], ascending=True).reset_index(drop=True)

    # Normalize tekrar (gruplama sonrası)
    wsum = float(grouped["weight"].sum()) if len(grouped) else 0.0
    if wsum <= 0.0:
        grouped["weight"] = 0.0
    else:
        grouped["weight"] = grouped["weight"] / wsum

    direct_total = float(direct_tco2_total or 0.0)
    indirect_total = float(indirect_tco2_total or 0.0)

    grouped["allocated_direct_tco2"] = grouped["weight"] * direct_total
    grouped["allocated_indirect_tco2"] = grouped["weight"] * indirect_total
    grouped["allocated_total_tco2"] = grouped["allocated_direct_tco2"] + grouped["allocated_indirect_tco2"]

    # Allocation hash: method + sku weights + totals (canonical json)
    payload = {
        "method": method_used,
        "totals": {"direct_tco2_total": direct_total, "indirect_tco2_total": indirect_total},
        "weights": [{"sku": str(r["sku"]), "weight": float(r["weight"])} for _, r in grouped.iterrows()],
    }
    allocation_hash = sha256_json(payload)

    meta = {
        "allocation_method": method_used,
        "allocation_hash": allocation_hash,
        "totals": payload["totals"],
        "weights_count": len(payload["weights"]),
        "notes": [
            "Allocation deterministik: sku sıralaması + canonical json hash ile kilitlenir.",
            "Yöntem kolonları yoksa quantity-based fallback uygulanır.",
        ],
    }

    grouped["allocation_method"] = method_used
    grouped["allocation_hash"] = allocation_hash
    return grouped, meta


def allocation_map_from_df(allocation_df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """CBAM engine için sku->alloc map üretir."""
    if allocation_df is None or len(allocation_df) == 0:
        return {}
    df = allocation_df.copy()
    df.columns = [_norm(c) for c in df.columns]
    out: Dict[str, Dict[str, float]] = {}
    for _, r in df.iterrows():
        sku = str(r.get("sku", "") or "").strip()
        if not sku:
            continue
        out[sku] = {
            "direct_alloc_tco2": float(r.get("allocated_direct_tco2", 0.0) or 0.0),
            "indirect_alloc_tco2": float(r.get("allocated_indirect_tco2", 0.0) or 0.0),
        }
    return out
