from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional

import numpy as np
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

# Fallback CN prefix → goods (DB registry yoksa / boşsa)
_CN_PREFIX_TO_GOODS: List[Tuple[str, str]] = [
    ("72", "iron_steel"),
    ("73", "iron_steel"),
    ("76", "aluminium"),
    ("25", "cement"),
    ("31", "fertilizers"),
    ("28", "chemicals"),
    ("29", "chemicals"),
    ("2716", "electricity"),
    ("2804", "hydrogen"),
]


def _clean_cn(cn: Any) -> str:
    cn_s = str(cn or "").strip()
    cn_s = cn_s.replace(".", "").replace(" ", "")
    return cn_s


# ------------------------------------------------------------
# DB tabanlı CN Registry lookup (cache’li)
# ------------------------------------------------------------
_REGISTRY_CACHE: dict = {
    "loaded": False,
    "rows": [],  # list of dicts
}


def _load_registry_rows() -> List[dict]:
    """DB’den aktif mappingleri çeker. Import başarısızsa sessizce fallback’e döner."""
    global _REGISTRY_CACHE

    if _REGISTRY_CACHE.get("loaded"):
        return _REGISTRY_CACHE.get("rows", [])

    rows: List[dict] = []
    try:
        from sqlalchemy import select

        from src.db.session import db
        from src.db.cbam_registry import CbamCnMapping

        with db() as s:
            items = (
                s.execute(
                    select(CbamCnMapping)
                    .where(CbamCnMapping.active == True)  # noqa: E712
                    .order_by(CbamCnMapping.priority.desc())
                )
                .scalars()
                .all()
            )

        for it in items:
            rows.append(
                {
                    "cn_pattern": _clean_cn(getattr(it, "cn_pattern", "")),
                    "match_type": str(getattr(it, "match_type", "prefix") or "prefix").strip().lower(),
                    "cbam_good_key": str(getattr(it, "cbam_good_key", "other") or "other").strip().lower(),
                    "cbam_good_name": str(getattr(it, "cbam_good_name", "") or "").strip(),
                    "priority": int(getattr(it, "priority", 100) or 100),
                }
            )
    except Exception:
        rows = []

    _REGISTRY_CACHE["loaded"] = True
    _REGISTRY_CACHE["rows"] = rows
    return rows


def _registry_match(cn: str) -> Optional[dict]:
    cn = _clean_cn(cn)
    if not cn:
        return None

    rows = _load_registry_rows()
    if not rows:
        return None

    exact_hits = []
    prefix_hits = []

    for r in rows:
        pat = r.get("cn_pattern") or ""
        if not pat:
            continue
        mt = (r.get("match_type") or "prefix").lower()

        if mt == "exact" and cn == pat:
            exact_hits.append(r)
        elif mt == "prefix" and cn.startswith(pat):
            prefix_hits.append(r)

    def _rank_key(rr: dict):
        # priority büyük olan öncelikli; pattern uzunluğu da tie-breaker
        return (int(rr.get("priority", 100) or 100), len(rr.get("cn_pattern", "") or ""))

    if exact_hits:
        exact_hits.sort(key=_rank_key, reverse=True)
        return exact_hits[0]

    if prefix_hits:
        prefix_hits.sort(key=_rank_key, reverse=True)
        return prefix_hits[0]

    return None


def cn_to_goods(cn_code: Any) -> Dict[str, str]:
    cn = _clean_cn(cn_code)
    if not cn:
        return {
            "cn_code": "",
            "cbam_good_key": "other",
            "cbam_good_name": _CBAM_GOODS["other"],
            "mapping_rule": "empty_cn",
        }

    # 1) DB registry öncelikli
    hit = _registry_match(cn)
    if hit:
        good_key = hit.get("cbam_good_key") or "other"
        good_name = hit.get("cbam_good_name") or _CBAM_GOODS.get(good_key, _CBAM_GOODS["other"])
        mt = (hit.get("match_type") or "prefix").lower()
        pat = hit.get("cn_pattern") or ""
        return {
            "cn_code": cn,
            "cbam_good_key": good_key,
            "cbam_good_name": good_name,
            "mapping_rule": f"registry:{mt}:{pat}",
        }

    # 2) Fallback deterministic prefix mapping
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
            "mapping_rule": f"fallback:prefix:{pref}",
        }

    return {
        "cn_code": cn,
        "cbam_good_key": "other",
        "cbam_good_name": _CBAM_GOODS["other"],
        "mapping_rule": "fallback:prefix:none",
    }


def is_cbam_covered_row(row: dict) -> bool:
    # 1) production.csv’de cbam_covered field varsa onu kullan
    if "cbam_covered" in row and row["cbam_covered"] is not None and str(row["cbam_covered"]).strip() != "":
        v = str(row["cbam_covered"]).strip().lower()
        return v in ("1", "true", "yes", "evet", "covered", "y", "t")

    # 2) yoksa CN mapping ile “other” olmayan goods => covered say
    cn = row.get("cn_code")
    m = cn_to_goods(cn)
    return m.get("cbam_good_key") != "other"


def precursor_emissions_from_materials(materials_df: pd.DataFrame) -> pd.DataFrame:
    """materials.csv -> sku bazında precursor tCO2 (embedded).

    Beklenen kolonlar (MVP):
      - sku
      - material_quantity
      - emission_factor (kgCO2e / material_unit)
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
    allocation_by_sku: dict | None = None,
    allocation_meta: dict | None = None,
    carbon_price_paid_eur_per_t: float = 0.0,
    carbon_price_paid_amount_eur: float | None = None,
    carbon_price_paid_currency: str = "EUR",
) -> tuple[pd.DataFrame, dict]:
    """CBAM hesap (FAZ 1.1 + 1.3 uyumlu).

    Direct/Indirect:
      - energy_breakdown.direct_tco2 / indirect_tco2
      - allocation_by_sku verilmişse (allocation engine) satırların direct/indirect payı buradan gelir.
      - verilmemişse legacy allocation_basis: quantity veya export

    Precursor:
      - materials.csv üzerinden sku bazlı precursor_tco2

    Çıktı:
      - cbam_table (satır bazında)
      - totals (goods_summary dahil)
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
                "allocation_method",
                "allocation_hash",
                "embedded_intensity_tco2_per_unit",
                "direct_intensity_tco2_per_unit",
                "indirect_intensity_tco2_per_unit",
                "data_type_flag",
                "carbon_price_paid_eur_per_t",
                "carbon_price_paid_amount_eur",
                "carbon_price_paid_currency",
            ]
        )
        return empty, {
            "embedded_tco2": 0.0,
            "cbam_cost_eur": 0.0,
            "direct_tco2": 0.0,
            "indirect_tco2": 0.0,
            "precursor_tco2": 0.0,
            "allocation_basis": _norm(allocation_basis) or "quantity",
            "allocation_method": (allocation_meta or {}).get("allocation_method") if isinstance(allocation_meta, dict) else None,
            "allocation_hash": (allocation_meta or {}).get("allocation_hash") if isinstance(allocation_meta, dict) else None,
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
    df["sku"] = df["sku"].astype(str).fillna("").apply(lambda x: str(x).strip())

    # Mapping + goods
    mapping_rows = []
    for _, r in df.iterrows():
        m = cn_to_goods(r.get("cn_code"))
        mapping_rows.append(m)
    map_df = pd.DataFrame(mapping_rows)
    df = df.reset_index(drop=True)
    df["cbam_good_key"] = map_df.get("cbam_good_key", "other")
    df["cbam_good"] = map_df.get("cbam_good_name", _CBAM_GOODS["other"])
    df["mapping_rule"] = map_df.get("mapping_rule", "unknown")

    # Coverage
    df["cbam_covered_calc"] = df.apply(lambda r: bool(is_cbam_covered_row(r.to_dict())), axis=1)

    direct_t = float(energy_breakdown.get("direct_tco2", 0.0) or 0.0)
    indirect_t = float(energy_breakdown.get("indirect_tco2", 0.0) or 0.0)

    alloc_method = None
    alloc_hash = None
    if isinstance(allocation_meta, dict):
        alloc_method = allocation_meta.get("allocation_method")
        alloc_hash = allocation_meta.get("allocation_hash")

    # Allocation
    if allocation_by_sku and isinstance(allocation_by_sku, dict):
        df["direct_alloc_tco2"] = 0.0
        df["indirect_alloc_tco2"] = 0.0
        for i, r in df.iterrows():
            sku = str(r.get("sku", "") or "").strip()
            m = allocation_by_sku.get(sku, {}) if sku else {}
            df.at[i, "direct_alloc_tco2"] = float((m or {}).get("direct_alloc_tco2", 0.0) or 0.0)
            df.at[i, "indirect_alloc_tco2"] = float((m or {}).get("indirect_alloc_tco2", 0.0) or 0.0)

        # Aynı sku birden fazla satırsa allocation tekrar yazılır; satır bazında quantity payı ile böl.
        sku_counts = df.groupby("sku")["sku"].transform("count")
        if (sku_counts > 1).any():
            q = df["quantity"].clip(lower=0.0)
            qsum = df.groupby("sku")["quantity"].transform("sum").replace(0.0, np.nan)
            share = (q / qsum).fillna(0.0)
            uniq_direct = df.groupby("sku")["direct_alloc_tco2"].transform("max")
            uniq_indirect = df.groupby("sku")["indirect_alloc_tco2"].transform("max")
            df["direct_alloc_tco2"] = uniq_direct * share
            df["indirect_alloc_tco2"] = uniq_indirect * share
    else:
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

        df["direct_alloc_tco2"] = alloc_weights * direct_t
        df["indirect_alloc_tco2"] = alloc_weights * indirect_t

    # Precursor
    prec = precursor_emissions_from_materials(materials_df) if materials_df is not None else pd.DataFrame(columns=["sku", "precursor_tco2"])
    if len(prec) > 0:
        df = df.merge(prec, on="sku", how="left")

    if "precursor_tco2" not in df.columns:
        df["precursor_tco2"] = 0.0
    df["precursor_tco2"] = df["precursor_tco2"].fillna(0.0).apply(_to_float)

    # Embedded
    df["embedded_tco2"] = df["direct_alloc_tco2"] + df["indirect_alloc_tco2"] + df["precursor_tco2"]

    # Export share: EU export / produced
    df["eu_export_qty"] = df["export_to_eu_quantity"].clip(lower=0.0)
    qty_pos = df["quantity"].clip(lower=0.0)

    df["export_share"] = 0.0
    df.loc[qty_pos > 0.0, "export_share"] = (
        df.loc[qty_pos > 0.0, "eu_export_qty"] / df.loc[qty_pos > 0.0, "quantity"]
    ).clip(0.0, 1.0)

    # CBAM cost signal (MVP)
    df["covered_and_export"] = (df["cbam_covered_calc"] == True) & (df["eu_export_qty"] > 0.0)
    df["cbam_cost_eur"] = 0.0
    df.loc[df["covered_and_export"], "cbam_cost_eur"] = (
        df.loc[df["covered_and_export"], "embedded_tco2"]
        * float(_to_float(eua_price_eur_per_t))
        * df.loc[df["covered_and_export"], "export_share"]
    )

    df["allocation_method"] = alloc_method or ""
    df["allocation_hash"] = alloc_hash or ""

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
            "allocation_method",
            "allocation_hash",
        ]
    ].copy()

    table = table.rename(columns={"cbam_covered_calc": "cbam_covered"})
    # --- Transitional requirements: intensity + actual/default + carbon price paid
    qty = pd.to_numeric(table.get("quantity", 0.0), errors="coerce").fillna(0.0)
    direct = pd.to_numeric(table.get("direct_alloc_tco2", 0.0), errors="coerce").fillna(0.0)
    indirect = pd.to_numeric(table.get("indirect_alloc_tco2", 0.0), errors="coerce").fillna(0.0)
    embedded = pd.to_numeric(table.get("embedded_tco2", 0.0), errors="coerce").fillna(0.0)

    table["direct_intensity_tco2_per_unit"] = (direct / qty.replace({0.0: pd.NA})).fillna(0.0)
    table["indirect_intensity_tco2_per_unit"] = (indirect / qty.replace({0.0: pd.NA})).fillna(0.0)
    table["embedded_intensity_tco2_per_unit"] = (embedded / qty.replace({0.0: pd.NA})).fillna(0.0)

    used_default = bool((energy_breakdown or {}).get("used_default_factors"))
    table["data_type_flag"] = "default" if used_default else "actual"

    # Carbon price paid in country of origin (CBAM 2023/956)
    cpp_t = float(carbon_price_paid_eur_per_t or 0.0)
    table["carbon_price_paid_eur_per_t"] = cpp_t
    table["carbon_price_paid_currency"] = str(carbon_price_paid_currency or "EUR")
    if carbon_price_paid_amount_eur is None:
        # best-effort: paid amount scaled by covered&export embedded emissions
        table["carbon_price_paid_amount_eur"] = 0.0
    else:
        table["carbon_price_paid_amount_eur"] = float(carbon_price_paid_amount_eur or 0.0)


    goods_summary = (
        table.groupby(["cbam_good"], dropna=False)[
            ["embedded_tco2", "cbam_cost_eur", "direct_alloc_tco2", "indirect_alloc_tco2", "precursor_tco2"]
        ]
        .sum()
        .reset_index()
        .sort_values("embedded_tco2", ascending=False)
        .to_dict(orient="records")
    )

    totals = {
        "embedded_tco2": float(table["embedded_tco2"].sum()),
        "cbam_cost_eur": float(table["cbam_cost_eur"].sum()),
        "direct_tco2": float(table["direct_alloc_tco2"].sum()),
        "indirect_tco2": float(table["indirect_alloc_tco2"].sum()),
        "precursor_tco2": float(table["precursor_tco2"].sum()),
        "allocation_basis": _norm(allocation_basis) or "quantity",
        "allocation_method": alloc_method,
        "allocation_hash": alloc_hash,
        "goods_summary": goods_summary,
        "notes": [
            "CN→CBAM goods eşlemesi: Önce DB registry, yoksa fallback prefix kuralları kullanılır.",
            "CBAM maliyet sinyali (MVP): embedded_tCO2 × EUA(€/t) × export_share (EU export / total production).",
        ],
    }

    return table, totals
