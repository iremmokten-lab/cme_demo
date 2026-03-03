from __future__ import annotations

from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        # pandas NaN
        try:
            if pd.isna(x):
                return default
        except Exception:
            pass
        return float(x)
    except Exception:
        return default


def _norm_col(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def validate_required_columns(df: pd.DataFrame, required: List[str], dataset_name: str = "dataset") -> None:
    if df is None:
        raise ValueError(f"{dataset_name}: veri bulunamadı.")
    cols = {_norm_col(c) for c in df.columns.tolist()}
    missing = [c for c in required if _norm_col(c) not in cols]
    if missing:
        raise ValueError(f"{dataset_name}: zorunlu kolon(lar) eksik: {', '.join(missing)}")


def validate_nonnegative(df: pd.DataFrame, cols: List[str], dataset_name: str = "dataset") -> None:
    if df is None or len(df) == 0:
        return
    for c in cols:
        if c not in df.columns:
            continue
        bad = df[c].apply(lambda v: safe_float(v, 0.0) < 0.0)
        if bool(bad.any()):
            raise ValueError(f"{dataset_name}: '{c}' kolonu negatif değer içeriyor.")


def allocation_map_from_df(production_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Production dataframe'den SKU/ürün map'i çıkarır.
    Orchestrator farklı şemalarla gelebileceği için best-effort çalışır.
    """
    if production_df is None or len(production_df) == 0:
        return {}

    df = production_df.copy()
    df.columns = [_norm_col(c) for c in df.columns]

    # SKU/ürün kodu
    sku_col = "sku" if "sku" in df.columns else ("product_code" if "product_code" in df.columns else None)
    qty_col = "quantity" if "quantity" in df.columns else ("qty" if "qty" in df.columns else None)

    # Ürün isim/CN
    name_col = "product_name" if "product_name" in df.columns else ("name" if "name" in df.columns else None)
    cn_col = "cn_code" if "cn_code" in df.columns else ("cn" if "cn" in df.columns else None)

    if not sku_col:
        # sku yoksa, en azından index bazlı map üret
        sku_col = "__row_id__"
        df[sku_col] = df.index.astype(str)

    if not qty_col:
        qty_col = "__qty__"
        df[qty_col] = 0.0

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        sku = str(r.get(sku_col) or "").strip()
        if not sku:
            continue
        out[sku] = {
            "sku": sku,
            "quantity": safe_float(r.get(qty_col), 0.0),
            "product_name": str(r.get(name_col) or "").strip() if name_col else "",
            "cn_code": str(r.get(cn_col) or "").strip() if cn_col else "",
        }
    return out


def allocate_energy_to_skus(production_df: pd.DataFrame, total_energy_kgco2: float) -> pd.DataFrame:
    """
    Eski kullanım için korunur.
    Total enerji emisyonunu (kgCO2) SKU'lara quantity bazlı dağıtır.
    """
    validate_required_columns(production_df, ["sku", "quantity"], "production.csv")
    df = production_df.copy()
    df.columns = [_norm_col(c) for c in df.columns]

    df["quantity"] = df["quantity"].apply(lambda v: safe_float(v, 0.0))
    validate_nonnegative(df, ["quantity"], "production.csv")

    total_qty = float(df["quantity"].sum()) if len(df) else 0.0
    if total_qty <= 0:
        df["alloc_energy_kgco2"] = 0.0
        df["alloc_energy_kgco2_per_unit"] = 0.0
        return df

    df["qty_share"] = df["quantity"] / total_qty
    df["alloc_energy_kgco2"] = df["qty_share"] * float(total_energy_kgco2)
    df["alloc_energy_kgco2_per_unit"] = np.where(
        df["quantity"] > 0,
        df["alloc_energy_kgco2"] / df["quantity"],
        0.0,
    )
    return df


def allocate_product_emissions(
    production_df: pd.DataFrame,
    *,
    scope1_tco2: float | None = None,
    scope2_tco2: float | None = None,
    total_tco2: float | None = None,
    method: str = "quantity",
    sku_col: str | None = None,
    quantity_col: str | None = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Orchestrator tarafından çağrılabilecek ürün bazlı allocation.

    - method:
        - "quantity": quantity bazlı dağıtım
        - "energy-content": MVP aşamasında quantity ile aynı davranır (placeholder)
        - "process-step": MVP aşamasında quantity ile aynı davranır (placeholder)

    Dönen:
      - allocation_df: SKU bazında alloc_scope1_tco2, alloc_scope2_tco2, alloc_total_tco2, intensity
      - meta: allocation metası (method, hash için inputlar vb.)
    """
    if production_df is None:
        production_df = pd.DataFrame()

    df = production_df.copy()
    if len(df) > 0:
        df.columns = [_norm_col(c) for c in df.columns]

    # kolon seçimi (best effort)
    sku_c = _norm_col(sku_col) if sku_col else ("sku" if "sku" in df.columns else ("product_code" if "product_code" in df.columns else None))
    qty_c = _norm_col(quantity_col) if quantity_col else ("quantity" if "quantity" in df.columns else ("qty" if "qty" in df.columns else None))

    if not sku_c:
        sku_c = "sku"
        df[sku_c] = df.index.astype(str)

    if not qty_c:
        qty_c = "quantity"
        df[qty_c] = 0.0

    df[sku_c] = df[sku_c].astype(str)
    df[qty_c] = df[qty_c].apply(lambda v: safe_float(v, 0.0))
    validate_nonnegative(df, [qty_c], "production.csv")

    # Emisyon toplamları
    s1 = safe_float(scope1_tco2, 0.0)
    s2 = safe_float(scope2_tco2, 0.0)
    tot = safe_float(total_tco2, s1 + s2) if total_tco2 is not None else (s1 + s2)

    # method normalize
    m = str(method or "quantity").strip().lower()
    if m not in ("quantity", "energy-content", "process-step"):
        m = "quantity"

    # MVP: 3 method da quantity bazlı dağıtım yapar (placeholder)
    total_qty = float(df[qty_c].sum()) if len(df) else 0.0
    if total_qty <= 0.0:
        df["alloc_scope1_tco2"] = 0.0
        df["alloc_scope2_tco2"] = 0.0
        df["alloc_total_tco2"] = 0.0
        df["intensity_tco2_per_unit"] = 0.0
    else:
        share = df[qty_c] / total_qty
        df["alloc_scope1_tco2"] = share * s1
        df["alloc_scope2_tco2"] = share * s2
        df["alloc_total_tco2"] = share * tot
        df["intensity_tco2_per_unit"] = np.where(df[qty_c] > 0, df["alloc_total_tco2"] / df[qty_c], 0.0)

    # Dönüş kolonlarını standartla
    out_cols = [sku_c, qty_c, "alloc_scope1_tco2", "alloc_scope2_tco2", "alloc_total_tco2", "intensity_tco2_per_unit"]
    allocation_df = df[out_cols].copy()
    allocation_df = allocation_df.rename(columns={sku_c: "sku", qty_c: "quantity"})

    meta = {
        "allocation_method": m,
        "allocation_basis": "quantity",
        "scope1_tco2": s1,
        "scope2_tco2": s2,
        "total_tco2": tot,
        "sku_col": sku_c,
        "quantity_col": qty_c,
    }

    return allocation_df, meta
