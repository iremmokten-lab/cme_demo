from __future__ import annotations

from typing import Any, Dict, List, Tuple

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


# ------------------------------------------------------------
# Paket D1: CN Code → CBAM Goods mapping (deterministik, açıklanabilir)
# Not: Bu MVP’de mapping “registry” olarak kod içinde deterministic tutulur.
# Prod’da bunu DB tablosu / registry yönetimi ile UI’dan yönetebilirsin.
# ------------------------------------------------------------

# Basit CBAM goods taxonomy (MVP)
_CBAM_GOODS = {
    "iron_steel": "Demir-Çelik",
    "aluminium": "Alüminyum",
    "cement": "Çimento",
    "fertilizers": "Gübre",
    "electricity": "Elektrik",
    "hydrogen": "Hidrojen",
    "chemicals": "Kimyasallar",
    "other": "Diğer",
}

# CN prefix → goods (MVP deterministic)
# CN/HS kodları regülasyon detayında daha granular olabilir; burada satılabilir “readiness” için sinyal üretir.
_CN_PREFIX_TO_GOODS: List[Tuple[str, str]] = [
    ("72", "iron_steel"),
    ("73", "iron_steel"),
    ("76", "aluminium"),
    ("25", "cement"),
    ("31", "fertilizers"),
    ("28", "chemicals"),
    ("29", "chemicals"),
    ("2716", "electricity"),  # elektrik (HS) - bazı sınıflandırmalarda
    ("2804", "hydrogen"),     # hidrojen: HS 2804 alt kırılım
]


def _clean_cn(cn: Any) -> str:
    cn_s = str(cn or "").strip()
    cn_s = cn_s.replace(".", "").replace(" ", "")
    return cn_s


def cn_to_goods(cn_code: Any) -> Dict[str, str]:
    """
    Dönüş:
      {
        "cn_code": "7208....",
        "cbam_good_key": "iron_steel",
        "cbam_good_name": "Demir-Çelik",
        "mapping_rule": "prefix:72"
      }
    """
    cn = _clean_cn(cn_code)
    if not cn:
        return {
            "cn_code": "",
            "cbam_good_key": "other",
            "cbam_good_name": _CBAM_GOODS["other"],
            "mapping_rule": "empty_cn",
        }

    # En uzun prefix kazanır (örn 2716, 2804)
    best = None
    for pref, good in _CN_PREFIX_TO_GOODS:
        if cn.startswith(pref):
            if best is None or len(pref) > len(best[0]):
                best = (pref, good)

    if best:
        pref, good = best
        return {
            "cn_code": cn,
            "cbam_good_key": good,
            "cbam_good_name": _CBAM_GOODS.get(good, _CBAM_GOODS["other"]),
            "mapping_rule": f"prefix:{pref}",
        }

    return {
        "cn_code": cn,
        "cbam_good_key": "other",
        "cbam_good_name": _CBAM_GOODS["other"],
        "mapping_rule": "prefix:none",
    }


def is_cbam_covered_row(row: dict) -> bool:
    """
    Coverage belirleme:
    1) production.csv’de cbam_covered field varsa onu kullan
    2) yoksa CN mapping ile “other” olmayan goods => covered say
    """
    if "cbam_covered" in row and row["cbam_covered"] is not None and str(row["cbam_covered"]).strip() != "":
        v = str(row["cbam_covered"]).strip().lower()
        return v in ("1", "true", "yes", "evet", "covered", "y", "t")

    cn = row.get("cn_code")
    m = cn_to_goods(cn)
    return m.get("cbam_good_key") != "other"


def precursor_emissions_from_materials(materials_df: pd.DataFrame) -> pd.DataFrame:
    """
    materials.csv -> sku bazında precursor tCO2 (embedded, upstream)

    Beklenen kolonlar (MVP):
      - sku
      - material_quantity (numeric)
      - emission_factor (kgCO2e / material_unit varsayımı)

    Not: Gerçek dünyada precursor chain / supplier EPD / LCA ile genişler.
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
    """
    Paket D1: CBAM “gerçekçi hesap” yaklaşımı (MVP)
      - Direct emissions (fuel bazlı) + Indirect emissions (electricity) energy_breakdown’dan gelir
      - Precursor emissions materials.csv’den sku bazında gelir
      - SKU → CN → CBAM goods mapping
      - Ürün bazlı embedded emissions + EU export allocation

    production.csv beklenen kolonlar:
      - sku
      - cn_code
      - quantity
      - export_to_eu_quantity
      - cbam_covered (opsiyonel override)

    allocation_basis:
      - "quantity": direct/indirect emissions üretime göre SKU’lara dağıtılır
      - "export": dağıtım bazında export_to_eu_quantity kullanılır (export boşsa quantity fallback)
    """
    if production_df is None or len(production_df) == 0:
        empty = pd.DataFrame(
            columns=[
                "sku",
                "cn_code",
                "cbam_good",
                "cbam_covered",
                "quantity",
                "export_to_eu_quantity",
                "direct_alloc_tco2",
                "indirect_alloc_tco2",
                "precursor_tco2",
                "embedded_tco2",
                "export_share",
                "cbam_cost_eur",
                "mapping_rule",
            ]
        )
        return empty, {
            "embedded_tco2": 0.0,
            "cbam_cost_eur": 0.0,
            "direct_tco2": 0.0,
            "indirect_tco2": 0.0,
            "precursor_tco2": 0.0,
            "allocation_basis": _norm(allocation_basis) or "quantity",
            "goods_summary": [],
        }

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

    # Mapping + goods
    mapping_rows = []
    for _, r in df.iterrows():
        m = cn_to_goods(r.get("cn_code"))
        mapping_rows.append(m)
    map_df = pd.DataFrame(mapping_rows)
    if len(map_df) > 0:
        df = df.reset_index(drop=True)
        df["cn_code_clean"] = map_df.get("cn_code", "")
        df["cbam_good_key"] = map_df.get("cbam_good_key", "other")
        df["cbam_good"] = map_df.get("cbam_good_name", _CBAM_GOODS["other"])
        df["mapping_rule"] = map_df.get("mapping_rule", "unknown")
    else:
        df["cbam_good_key"] = "other"
        df["cbam_good"] = _CBAM_GOODS["other"]
        df["mapping_rule"] = "unknown"

    # Coverage
    df["cbam_covered_calc"] = df.apply(lambda r: bool(is_cbam_covered_row(r.to_dict())), axis=1)

    # Allocation weights for energy emissions
    basis = _norm(allocation_basis)
    if basis == "export":
        alloc_base = df["export_to_eu_quantity"].clip(lower=0.0)
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

    df["direct_alloc_tco2"] = alloc_weights * direct_t
    df["indirect_alloc_tco2"] = alloc_weights * indirect_t

    # Precursor
    prec = precursor_emissions_from_materials(materials_df) if materials_df is not None else pd.DataFrame(columns=["sku", "precursor_tco2"])
    if len(prec) > 0:
        df = df.merge(prec, on="sku", how="left")

    if "precursor_tco2" not in df.columns:
        df["precursor_tco2"] = 0.0
    df["precursor_tco2"] = df["precursor_tco2"].fillna(0.0).apply(_to_float)

    # Embedded totals per SKU
    df["embedded_tco2"] = df["direct_alloc_tco2"] + df["indirect_alloc_tco2"] + df["precursor_tco2"]

    # Export allocation per SKU (export_to_eu_quantity / quantity)
    df["eu_export_qty"] = df["export_to_eu_quantity"].clip(lower=0.0)
    qty_pos = df["quantity"].clip(lower=0.0)

    df["export_share"] = 0.0
    df.loc[qty_pos > 0.0, "export_share"] = (
        df.loc[qty_pos > 0.0, "eu_export_qty"] / df.loc[qty_pos > 0.0, "quantity"]
    ).clip(0.0, 1.0)

    # CBAM cost signal (MVP)
    # covered_and_export: CBAM coverage ve EU export>0
    df["covered_and_export"] = (df["cbam_covered_calc"] == True) & (df["eu_export_qty"] > 0.0)
    df["cbam_cost_eur"] = 0.0
    df.loc[df["covered_and_export"], "cbam_cost_eur"] = (
        df.loc[df["covered_and_export"], "embedded_tco2"] * float(_to_float(eua_price_eur_per_t)) * df.loc[df["covered_and_export"], "export_share"]
    )

    table = df[
        [
            "sku",
            "cn_code",
            "cbam_good",
            "cbam_covered_calc",
            "quantity",
            "export_to_eu_quantity",
            "direct_alloc_tco2",
            "indirect_alloc_tco2",
            "precursor_tco2",
            "embedded_tco2",
            "export_share",
            "cbam_cost_eur",
            "mapping_rule",
        ]
    ].copy()

    table = table.rename(columns={"cbam_covered_calc": "cbam_covered"})

    # Goods summary (verification/reporting friendly)
    gs = (
        table.groupby(["cbam_good"], dropna=False)[
            ["embedded_tco2", "cbam_cost_eur", "direct_alloc_tco2", "indirect_alloc_tco2", "precursor_tco2"]
        ]
        .sum()
        .reset_index()
        .sort_values("embedded_tco2", ascending=False)
    )

    goods_summary = gs.to_dict(orient="records")

    totals = {
        "embedded_tco2": float(table["embedded_tco2"].sum()),
        "cbam_cost_eur": float(table["cbam_cost_eur"].sum()),
        "direct_tco2": float(table["direct_alloc_tco2"].sum()),
        "indirect_tco2": float(table["indirect_alloc_tco2"].sum()),
        "precursor_tco2": float(table["precursor_tco2"].sum()),
        "allocation_basis": basis,
        "goods_summary": goods_summary,
        "notes": [
            "CBAM goods mapping MVP: CN prefix tabanlı deterministik eşleme.",
            "CBAM maliyet sinyali MVP: embedded_tCO2 × EUA(€/t) × export_share (EU export / total production).",
        ],
    }

    return table, totals
