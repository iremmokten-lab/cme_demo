from __future__ import annotations

from typing import Any

import pandas as pd


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


# Demo CBAM CN coverage yaklaşımı:
# - production.csv'de cbam_covered varsa onu kullan (öncelikli)
# - yoksa CN code üzerinden basit prefix eşlemesi
# Not: Paket C'de bunu "CN registry" tablosuna taşıyacağız.
_CBAM_CN_PREFIXES = (
    "72",  # Iron/steel (HS Chapter 72)
    "73",  # Articles of iron/steel (73)
    "76",  # Aluminium (76)
    "31",  # Fertilizers (31) - demo
    "28",  # Inorganic chemicals (28) - demo
    "29",  # Organic chemicals (29) - demo
    "25",  # Cement etc (25) - demo
)


def is_cbam_covered_row(row: dict) -> bool:
    if "cbam_covered" in row and row["cbam_covered"] is not None and str(row["cbam_covered"]).strip() != "":
        v = str(row["cbam_covered"]).strip().lower()
        return v in ("1", "true", "yes", "evet", "covered", "y", "t")

    cn = str(row.get("cn_code") or "").strip()
    cn = cn.replace(".", "").replace(" ", "")
    if len(cn) >= 2 and cn[:2] in _CBAM_CN_PREFIXES:
        return True
    return False


def precursor_emissions_from_materials(materials_df: pd.DataFrame) -> pd.DataFrame:
    """materials.csv -> sku bazında precursor tCO2.

    Beklenen kolonlar:
    - sku
    - material_quantity (numeric)
    - emission_factor (kgCO2e / material_unit varsayımı)
    """
    if materials_df is None or len(materials_df) == 0:
        return pd.DataFrame(columns=["sku", "precursor_tco2"])

    df = materials_df.copy()
    df.columns = [_norm(c) for c in df.columns]

    if "sku" not in df.columns:
        return pd.DataFrame(columns=["sku", "precursor_tco2"])

    if "material_quantity" not in df.columns:
        df["material_quantity"] = 0.0
    if "emission_factor" not in df.columns:
        df["emission_factor"] = 0.0

    df["material_quantity"] = df["material_quantity"].apply(_to_float)
    df["emission_factor"] = df["emission_factor"].apply(_to_float)

    # emission_factor: kgCO2e / unit varsayımı
    df["precursor_kg"] = df["material_quantity"] * df["emission_factor"]
    out = df.groupby("sku", dropna=False)["precursor_kg"].sum().reset_index()
    out["precursor_tco2"] = out["precursor_kg"] / 1000.0
    return out[["sku", "precursor_tco2"]]


def cbam_compute(
    production_df: pd.DataFrame,
    energy_breakdown: dict,
    materials_df: pd.DataFrame | None,
    eua_price_eur_per_t: float,
    allocation_basis: str = "quantity",
) -> tuple[pd.DataFrame, dict]:
    """CBAM: direct+indirect+precursor + EU export exposure.

    - direct/indirect: energy_breakdown (tCO2)
    - precursor: materials.csv (sku bazında)
    - allocation: production quantity (default) veya export_to_eu_quantity

    production.csv beklenen kolonlar:
    - sku, cn_code, quantity, export_to_eu_quantity, cbam_covered
    """
    if production_df is None or len(production_df) == 0:
        empty = pd.DataFrame(columns=["sku", "cn_code", "cbam_covered", "export_to_eu_quantity", "embedded_tco2", "cbam_cost_eur"])
        return empty, {"embedded_tco2": 0.0, "cbam_cost_eur": 0.0}

    df = production_df.copy()
    df.columns = [_norm(c) for c in df.columns]

    if "sku" not in df.columns:
        df["sku"] = ""
    if "cn_code" not in df.columns:
        df["cn_code"] = ""
    if "quantity" not in df.columns:
        df["quantity"] = 0.0
    if "export_to_eu_quantity" not in df.columns:
        df["export_to_eu_quantity"] = 0.0
    if "cbam_covered" not in df.columns:
        df["cbam_covered"] = None

    df["quantity"] = df["quantity"].apply(_to_float)
    df["export_to_eu_quantity"] = df["export_to_eu_quantity"].apply(_to_float)

    # Coverage
    df["cbam_covered_calc"] = df.apply(lambda r: bool(is_cbam_covered_row(r.to_dict())), axis=1)

    # Allocation basis
    basis = _norm(allocation_basis)
    if basis == "export":
        alloc_base = df["export_to_eu_quantity"].clip(lower=0.0)
        # export boşsa quantity fallback
        if float(alloc_base.sum()) <= 0.0:
            alloc_base = df["quantity"].clip(lower=0.0)
            basis = "quantity"
    else:
        alloc_base = df["quantity"].clip(lower=0.0)
        basis = "quantity"

    alloc_sum = float(alloc_base.sum())
    if alloc_sum <= 0.0:
        alloc_weights = pd.Series([0.0] * len(df))
    else:
        alloc_weights = alloc_base / alloc_sum

    direct_t = float(energy_breakdown.get("direct_tco2", 0.0) or 0.0)
    indirect_t = float(energy_breakdown.get("indirect_tco2", 0.0) or 0.0)

    # Allocate energy emissions across SKUs
    df["direct_alloc_tco2"] = alloc_weights * direct_t
    df["indirect_alloc_tco2"] = alloc_weights * indirect_t

    # Precursor: sku bazında ekle
    prec = precursor_emissions_from_materials(materials_df) if materials_df is not None else pd.DataFrame(columns=["sku", "precursor_tco2"])
    if len(prec) > 0:
        df = df.merge(prec, on="sku", how="left")
    if "precursor_tco2" not in df.columns:
        df["precursor_tco2"] = 0.0
    df["precursor_tco2"] = df["precursor_tco2"].fillna(0.0).apply(_to_float)

    df["embedded_tco2"] = df["direct_alloc_tco2"] + df["indirect_alloc_tco2"] + df["precursor_tco2"]

    # Exposure: sadece CBAM kapsamı ve EU export>0 olan satırlar
    df["eu_export_qty"] = df["export_to_eu_quantity"].clip(lower=0.0)
    df["covered_and_export"] = (df["cbam_covered_calc"] == True) & (df["eu_export_qty"] > 0.0)

    # CBAM cost (demo): embedded_tco2 * EUA price * (EU export share in total production for that sku)
    # Not: Gerçek CBAM hesaplarında ürün bazlı embedded ve AB ihracat bağlantısı farklı katmanlarda ele alınabilir.
    # Burada satışa uygun "risk sinyali" için deterministic yaklaşım.
    df["export_share"] = 0.0
    # export_share = export_to_eu_quantity / quantity (eğer quantity>0)
    qty_pos = df["quantity"].clip(lower=0.0)
    df.loc[qty_pos > 0.0, "export_share"] = (df.loc[qty_pos > 0.0, "eu_export_qty"] / df.loc[qty_pos > 0.0, "quantity"]).clip(0.0, 1.0)

    df["cbam_cost_eur"] = 0.0
    df.loc[df["covered_and_export"], "cbam_cost_eur"] = df.loc[df["covered_and_export"], "embedded_tco2"] * float(eua_price_eur_per_t) * df.loc[df["covered_and_export"], "export_share"]

    table = df[
        [
            "sku",
            "cn_code",
            "cbam_covered_calc",
            "quantity",
            "export_to_eu_quantity",
            "direct_alloc_tco2",
            "indirect_alloc_tco2",
            "precursor_tco2",
            "embedded_tco2",
            "export_share",
            "cbam_cost_eur",
        ]
    ].copy()

    totals = {
        "embedded_tco2": float(table["embedded_tco2"].sum()),
        "cbam_cost_eur": float(table["cbam_cost_eur"].sum()),
        "direct_tco2": float(table["direct_alloc_tco2"].sum()),
        "indirect_tco2": float(table["indirect_alloc_tco2"].sum()),
        "precursor_tco2": float(table["precursor_tco2"].sum()),
        "allocation_basis": basis,
    }

    # UI alan isimleri için uyum
    table = table.rename(columns={"cbam_covered_calc": "cbam_covered"})

    return table, totals
