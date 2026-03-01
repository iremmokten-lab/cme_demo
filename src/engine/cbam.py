from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional

from decimal import Decimal, ROUND_HALF_UP
import numpy as np
import pandas as pd

from src.engine.cbam_defaults import resolve_default_intensities
from src.engine.cbam_precursor import compute_precursor_tco2_by_sku
from src.services.cbam_liability import compute_cbam_liability
from src.mrv.lineage import sha256_json


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


def _q(x: Any, digits: int = 6) -> float:
    """Deterministic quantization for floats (stable across platforms)."""
    try:
        d = Decimal(str(float(x))).quantize(Decimal("1." + ("0" * digits)), rounding=ROUND_HALF_UP)
        return float(d)
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

# Fallback CN prefix → goods
_CN_PREFIX_TO_GOODS: List[Tuple[str, str]] = [
    ("72", "iron_steel"),
    ("73", "iron_steel"),
    ("76", "aluminium"),
    ("25", "cement"),
    ("31", "fertilizers"),
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
_REGISTRY_CACHE: dict = {"loaded": False, "rows": []}


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
        return {"cn_code": "", "cbam_good_key": "other", "cbam_good_name": _CBAM_GOODS["other"], "mapping_rule": "empty_cn"}

    # 1) DB registry
    hit = _registry_match(cn)
    if hit:
        good_key = hit.get("cbam_good_key") or "other"
        good_name = hit.get("cbam_good_name") or _CBAM_GOODS.get(good_key, _CBAM_GOODS["other"])
        mt = (hit.get("match_type") or "prefix").lower()
        pat = hit.get("cn_pattern") or ""
        return {"cn_code": cn, "cbam_good_key": good_key, "cbam_good_name": good_name, "mapping_rule": f"registry:{mt}:{pat}"}

    # 2) Fallback deterministic prefix mapping (longest prefix wins)
    best = None
    for pref, good in _CN_PREFIX_TO_GOODS:
        if cn.startswith(pref):
            if best is None or len(pref) > len(best[0]):
                best = (pref, good)
    if best:
        pref, good = best
        return {"cn_code": cn, "cbam_good_key": good, "cbam_good_name": _CBAM_GOODS.get(good, _CBAM_GOODS["other"]), "mapping_rule": f"fallback:prefix:{pref}"}

    return {"cn_code": cn, "cbam_good_key": "other", "cbam_good_name": _CBAM_GOODS["other"], "mapping_rule": "fallback:prefix:none"}


def is_cbam_covered_row(row: dict) -> bool:
    # 1) explicit cbam_covered field
    if "cbam_covered" in row and row["cbam_covered"] is not None and str(row["cbam_covered"]).strip() != "":
        v = str(row["cbam_covered"]).strip().lower()
        return v in ("1", "true", "yes", "evet", "covered", "y", "t")

    # 2) inferred from CN mapping
    cn = row.get("cn_code")
    m = cn_to_goods(cn)
    return m.get("cbam_good_key") != "other"


def _pick_flag(v: Any) -> str:
    s = str(v or "").strip().lower()
    if s in ("actual", "a", "gercek", "gerçek", "fiili", "1", "true", "yes", "evet"):
        return "ACTUAL"
    if s in ("default", "d", "varsayilan", "varsayılan", "0", "false", "no", "hayir", "hayır"):
        return "DEFAULT"
    return ""


def cbam_compute(
    *,
    production_df: pd.DataFrame,
    energy_breakdown: dict,
    materials_df: pd.DataFrame | None,
    eua_price_eur_per_t: float,
    reporting_year: int,
    carbon_price_paid_eur_per_t: float = 0.0,
    allocation_basis: str = "quantity",
    allocation_by_sku: dict | None = None,
    allocation_meta: dict | None = None,
    cbam_defaults_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict]:
    """
    CBAM calculation engine (Step-3):

    Supports 6 CBAM goods groups:
      - cement, iron & steel, aluminium, fertilisers, electricity, hydrogen

    Determinism:
      - stable ordering
      - deterministic float quantization
      - deterministic default selection (pinned evidence hash)
      - deterministic precursor aggregation (cycle-aware)

    Inputs:
      - production_df (product rows): expected flexible columns
          sku, cn_code, quantity, unit/quantity_unit, export_to_eu_quantity
          OPTIONAL actual fields:
            direct_emissions_tco2e, indirect_emissions_tco2e
            direct_intensity_tco2_per_unit, indirect_intensity_tco2_per_unit
            actual_default_flag / data_type_flag
      - energy_breakdown: totals from energy engine (direct_tco2, indirect_tco2) used as fallback allocation
      - materials_df: precursor relations or material EF sheet (parsed deterministically)
      - cbam_defaults_df: default intensities with evidence fields (optional)

    Output:
      - table (per row)
      - totals (including liability estimate)
    """
    if production_df is None or len(production_df) == 0:
        empty = pd.DataFrame(
            columns=[
                "sku",
                "cn_code",
                "cbam_good",
                "cbam_good_key",
                "cbam_covered",
                "quantity",
                "quantity_unit",
                "export_to_eu_quantity",
                "data_type_flag",
                "direct_emissions_tco2e",
                "indirect_emissions_tco2e",
                "precursor_tco2e",
                "embedded_emissions_tco2e",
                "direct_intensity_tco2_per_unit",
                "indirect_intensity_tco2_per_unit",
                "embedded_intensity_tco2_per_unit",
                "carbon_price_paid_eur_per_t",
                "certificates_required",
                "estimated_payable_amount_eur",
                "mapping_rule",
                "allocation_method",
                "allocation_hash",
                "default_value_evidence_hash",
            ]
        )
        return empty, {
            "embedded_emissions_tco2e": 0.0,
            "direct_tco2e": 0.0,
            "indirect_tco2e": 0.0,
            "precursor_tco2e": 0.0,
            "cbam_cost_signal_eur": 0.0,
            "liability": compute_cbam_liability(
                year=int(reporting_year),
                embedded_emissions_tco2=0.0,
                eu_ets_price_eur_per_t=float(_to_float(eua_price_eur_per_t)),
                carbon_price_paid_eur_per_t=float(_to_float(carbon_price_paid_eur_per_t)),
            ).to_dict(),
            "goods_summary": [],
            "precursor_meta": {"precursor_method": "none", "edges": []},
        }

    df = production_df.copy()
    df.columns = [_norm(c) for c in df.columns]

    # Normalize columns
    if "sku" not in df.columns:
        if "product_code" in df.columns:
            df["sku"] = df["product_code"]
        elif "product" in df.columns:
            df["sku"] = df["product"]
        else:
            df["sku"] = ""
    if "cn_code" not in df.columns:
        df["cn_code"] = ""
    if "quantity" not in df.columns:
        df["quantity"] = 0.0
    if "export_to_eu_quantity" not in df.columns:
        if "export_qty" in df.columns:
            df["export_to_eu_quantity"] = df["export_qty"]
        else:
            df["export_to_eu_quantity"] = 0.0
    if "quantity_unit" not in df.columns:
        if "unit" in df.columns:
            df["quantity_unit"] = df["unit"]
        else:
            df["quantity_unit"] = "t"

    # data type flag
    if "actual_default_flag" in df.columns:
        df["data_type_flag"] = df["actual_default_flag"].apply(_pick_flag)
    elif "data_type_flag" in df.columns:
        df["data_type_flag"] = df["data_type_flag"].apply(_pick_flag)
    else:
        df["data_type_flag"] = ""

    # numeric
    df["quantity"] = df["quantity"].apply(_to_float)
    df["export_to_eu_quantity"] = df["export_to_eu_quantity"].apply(_to_float)
    df["sku"] = df["sku"].astype(str).fillna("").apply(lambda x: str(x).strip())
    df["cn_code"] = df["cn_code"].astype(str).fillna("").apply(lambda x: str(x).strip())
    df["quantity_unit"] = df["quantity_unit"].astype(str).fillna("t").apply(lambda x: str(x).strip())

    # Mapping CN->goods
    mapping_rows = []
    for _, r in df.iterrows():
        mapping_rows.append(cn_to_goods(r.get("cn_code")))
    map_df = pd.DataFrame(mapping_rows)
    df = df.reset_index(drop=True)
    df["cbam_good_key"] = map_df.get("cbam_good_key", "other")
    df["cbam_good"] = map_df.get("cbam_good_name", _CBAM_GOODS["other"])
    df["mapping_rule"] = map_df.get("mapping_rule", "unknown")

    # Coverage
    df["cbam_covered"] = df.apply(lambda r: bool(is_cbam_covered_row(r.to_dict())), axis=1)

    # Determine row-level emissions source
    # Priority:
    #  1) ACTUAL: direct/indirect emissions explicitly provided
    #  2) DEFAULT: resolve default intensities and compute from quantity
    #  3) Allocation fallback: use allocation_by_sku or basis weights * energy_breakdown totals

    # explicit emissions columns
    if "direct_emissions_tco2e" not in df.columns:
        if "direct_alloc_tco2" in df.columns:
            df["direct_emissions_tco2e"] = df["direct_alloc_tco2"]
        else:
            df["direct_emissions_tco2e"] = np.nan
    if "indirect_emissions_tco2e" not in df.columns:
        if "indirect_alloc_tco2" in df.columns:
            df["indirect_emissions_tco2e"] = df["indirect_alloc_tco2"]
        else:
            df["indirect_emissions_tco2e"] = np.nan

    # explicit intensities
    if "direct_intensity_tco2_per_unit" not in df.columns:
        df["direct_intensity_tco2_per_unit"] = np.nan
    if "indirect_intensity_tco2_per_unit" not in df.columns:
        df["indirect_intensity_tco2_per_unit"] = np.nan

    # Normalize numeric
    df["direct_emissions_tco2e"] = df["direct_emissions_tco2e"].apply(lambda x: np.nan if str(x) == "nan" else x)
    df["indirect_emissions_tco2e"] = df["indirect_emissions_tco2e"].apply(lambda x: np.nan if str(x) == "nan" else x)
    df["direct_emissions_tco2e"] = pd.to_numeric(df["direct_emissions_tco2e"], errors="coerce")
    df["indirect_emissions_tco2e"] = pd.to_numeric(df["indirect_emissions_tco2e"], errors="coerce")
    df["direct_intensity_tco2_per_unit"] = pd.to_numeric(df["direct_intensity_tco2_per_unit"], errors="coerce")
    df["indirect_intensity_tco2_per_unit"] = pd.to_numeric(df["indirect_intensity_tco2_per_unit"], errors="coerce")

    default_evidence_by_row: List[str] = [""] * len(df)

    # Apply defaults where flagged DEFAULT and missing emissions+intensities
    for i, r in df.iterrows():
        flag = str(r.get("data_type_flag") or "").upper()
        if flag != "DEFAULT":
            continue

        cn = str(r.get("cn_code") or "")
        good_key = str(r.get("cbam_good_key") or "")
        unit = str(r.get("quantity_unit") or "t")
        qty = float(r.get("quantity") or 0.0)

        ev, d_int, i_int = resolve_default_intensities(
            cn_code=cn,
            cbam_good_key=good_key,
            quantity_unit=unit,
            reporting_year=int(reporting_year),
            defaults_df=cbam_defaults_df,
        )
        if ev is None:
            continue

        # if intensities not set, set them
        if pd.isna(df.at[i, "direct_intensity_tco2_per_unit"]):
            df.at[i, "direct_intensity_tco2_per_unit"] = d_int
        if pd.isna(df.at[i, "indirect_intensity_tco2_per_unit"]):
            df.at[i, "indirect_intensity_tco2_per_unit"] = i_int

        # if emissions not set, compute from intensities
        if pd.isna(df.at[i, "direct_emissions_tco2e"]):
            df.at[i, "direct_emissions_tco2e"] = float(d_int) * max(0.0, qty)
        if pd.isna(df.at[i, "indirect_emissions_tco2e"]):
            df.at[i, "indirect_emissions_tco2e"] = float(i_int) * max(0.0, qty)

        default_evidence_by_row[i] = ev.row_hash

    df["default_value_evidence_hash"] = default_evidence_by_row

    # Apply allocation for remaining NaNs
    alloc_method = (allocation_meta or {}).get("allocation_method") if isinstance(allocation_meta, dict) else None
    alloc_hash = (allocation_meta or {}).get("allocation_hash") if isinstance(allocation_meta, dict) else None

    direct_total = float(energy_breakdown.get("direct_tco2", 0.0) or 0.0)
    indirect_total = float(energy_breakdown.get("indirect_tco2", 0.0) or 0.0)

    if allocation_by_sku and isinstance(allocation_by_sku, dict):
        for i, r in df.iterrows():
            if not pd.isna(df.at[i, "direct_emissions_tco2e"]) and not pd.isna(df.at[i, "indirect_emissions_tco2e"]):
                continue
            sku = str(r.get("sku") or "").strip()
            m = allocation_by_sku.get(sku, {}) if sku else {}
            d = float((m or {}).get("direct_alloc_tco2", 0.0) or 0.0)
            ind = float((m or {}).get("indirect_alloc_tco2", 0.0) or 0.0)
            if pd.isna(df.at[i, "direct_emissions_tco2e"]):
                df.at[i, "direct_emissions_tco2e"] = d
            if pd.isna(df.at[i, "indirect_emissions_tco2e"]):
                df.at[i, "indirect_emissions_tco2e"] = ind
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
            weights = pd.Series([0.0] * len(df))
        else:
            weights = alloc_base / alloc_sum

        for i in range(len(df)):
            if pd.isna(df.at[i, "direct_emissions_tco2e"]):
                df.at[i, "direct_emissions_tco2e"] = float(weights.iloc[i]) * direct_total
            if pd.isna(df.at[i, "indirect_emissions_tco2e"]):
                df.at[i, "indirect_emissions_tco2e"] = float(weights.iloc[i]) * indirect_total

    df["direct_emissions_tco2e"] = df["direct_emissions_tco2e"].fillna(0.0).apply(_to_float)
    df["indirect_emissions_tco2e"] = df["indirect_emissions_tco2e"].fillna(0.0).apply(_to_float)

    # Intensities derived if missing
    for i, r in df.iterrows():
        qty = float(r.get("quantity") or 0.0)
        if qty > 0.0:
            if pd.isna(df.at[i, "direct_intensity_tco2_per_unit"]):
                df.at[i, "direct_intensity_tco2_per_unit"] = float(r.get("direct_emissions_tco2e") or 0.0) / qty
            if pd.isna(df.at[i, "indirect_intensity_tco2_per_unit"]):
                df.at[i, "indirect_intensity_tco2_per_unit"] = float(r.get("indirect_emissions_tco2e") or 0.0) / qty
        else:
            if pd.isna(df.at[i, "direct_intensity_tco2_per_unit"]):
                df.at[i, "direct_intensity_tco2_per_unit"] = 0.0
            if pd.isna(df.at[i, "indirect_intensity_tco2_per_unit"]):
                df.at[i, "indirect_intensity_tco2_per_unit"] = 0.0

    df["direct_intensity_tco2_per_unit"] = df["direct_intensity_tco2_per_unit"].fillna(0.0).apply(_to_float)
    df["indirect_intensity_tco2_per_unit"] = df["indirect_intensity_tco2_per_unit"].fillna(0.0).apply(_to_float)

    # Embedded without precursors first
    df["embedded_emissions_tco2e_no_precursor"] = df["direct_emissions_tco2e"] + df["indirect_emissions_tco2e"]

    # Precursor: explicit + chain (chain uses embedded_no_precursor by sku)
    embedded_by_sku = df.groupby("sku", dropna=False)["embedded_emissions_tco2e_no_precursor"].sum().to_dict()
    prec_map, prec_meta = compute_precursor_tco2_by_sku(
        production_df=df,
        materials_df=materials_df,
        embedded_tco2_by_sku=embedded_by_sku,
    )
    df["precursor_tco2e"] = df["sku"].apply(lambda s: float(prec_map.get(str(s).strip(), 0.0) or 0.0))

    df["embedded_emissions_tco2e"] = df["embedded_emissions_tco2e_no_precursor"] + df["precursor_tco2e"]

    # Embedded intensity
    df["embedded_intensity_tco2_per_unit"] = 0.0
    qty_pos = df["quantity"].clip(lower=0.0)
    df.loc[qty_pos > 0.0, "embedded_intensity_tco2_per_unit"] = df.loc[qty_pos > 0.0, "embedded_emissions_tco2e"] / df.loc[qty_pos > 0.0, "quantity"]

    # Export share & cost signal (not official liability)
    df["eu_export_qty"] = df["export_to_eu_quantity"].clip(lower=0.0)
    df["export_share"] = 0.0
    df.loc[qty_pos > 0.0, "export_share"] = (
        df.loc[qty_pos > 0.0, "eu_export_qty"] / df.loc[qty_pos > 0.0, "quantity"]
    ).clip(0.0, 1.0)

    df["covered_and_export"] = (df["cbam_covered"] == True) & (df["eu_export_qty"] > 0.0)
    df["cbam_cost_signal_eur"] = 0.0
    df.loc[df["covered_and_export"], "cbam_cost_signal_eur"] = (
        df.loc[df["covered_and_export"], "embedded_emissions_tco2e"]
        * float(_to_float(eua_price_eur_per_t))
        * df.loc[df["covered_and_export"], "export_share"]
    )

    # Liability estimate (definitive regime 2026+)
    # We compute total liability; per-row liability fields are also provided.
    df["carbon_price_paid_eur_per_t"] = float(_to_float(carbon_price_paid_eur_per_t))
    df["certificates_required"] = 0.0
    df["estimated_payable_amount_eur"] = 0.0
    df["payable_share"] = 0.0
    df["payable_emissions_tco2e"] = 0.0

    for i, r in df.iterrows():
        if not bool(r.get("cbam_covered")):
            continue
        liab = compute_cbam_liability(
            year=int(reporting_year),
            embedded_emissions_tco2=float(r.get("embedded_emissions_tco2e") or 0.0) * float(r.get("export_share") or 0.0),
            eu_ets_price_eur_per_t=float(_to_float(eua_price_eur_per_t)),
            carbon_price_paid_eur_per_t=float(_to_float(carbon_price_paid_eur_per_t)),
        )
        df.at[i, "payable_share"] = float(liab.payable_share)
        df.at[i, "payable_emissions_tco2e"] = float(liab.payable_emissions_tco2)
        df.at[i, "certificates_required"] = float(liab.certificates_required)
        df.at[i, "estimated_payable_amount_eur"] = float(liab.estimated_payable_amount_eur)

    # Deterministic quantization for output numeric columns
    num_cols = [
        "quantity",
        "export_to_eu_quantity",
        "direct_emissions_tco2e",
        "indirect_emissions_tco2e",
        "precursor_tco2e",
        "embedded_emissions_tco2e",
        "direct_intensity_tco2_per_unit",
        "indirect_intensity_tco2_per_unit",
        "embedded_intensity_tco2_per_unit",
        "export_share",
        "cbam_cost_signal_eur",
        "carbon_price_paid_eur_per_t",
        "payable_share",
        "payable_emissions_tco2e",
        "certificates_required",
        "estimated_payable_amount_eur",
    ]
    for c in num_cols:
        df[c] = df[c].apply(lambda x: _q(x, 6))

    df["allocation_method"] = str(alloc_method or "")
    df["allocation_hash"] = str(alloc_hash or "")

    # If flag not provided, infer:
    # - if default evidence used => DEFAULT
    # - else ACTUAL (conservative for reporting)
    for i in range(len(df)):
        if str(df.at[i, "data_type_flag"] or "").strip():
            continue
        df.at[i, "data_type_flag"] = "DEFAULT" if str(df.at[i, "default_value_evidence_hash"] or "").strip() else "ACTUAL"

    # stable ordering
    df = df.sort_values(by=["cn_code", "sku"], ascending=[True, True], kind="mergesort").reset_index(drop=True)

    # build table keeping legacy column names for compatibility
    table = pd.DataFrame(
        {
            "sku": df["sku"],
            "cn_code": df["cn_code"],
            "cbam_good": df["cbam_good"],
            "cbam_good_key": df["cbam_good_key"],
            "cbam_covered": df["cbam_covered"],
            "quantity": df["quantity"],
            "quantity_unit": df["quantity_unit"],
            "export_to_eu_quantity": df["export_to_eu_quantity"],
            # reporting
            "data_type_flag": df["data_type_flag"],
            # legacy allocation-style names kept
            "direct_alloc_tco2": df["direct_emissions_tco2e"],
            "indirect_alloc_tco2": df["indirect_emissions_tco2e"],
            "precursor_tco2": df["precursor_tco2e"],
            "embedded_tco2": df["embedded_emissions_tco2e"],
            "export_share": df["export_share"],
            "cbam_cost_eur": df["cbam_cost_signal_eur"],
            # new explicit naming
            "direct_emissions_tco2e": df["direct_emissions_tco2e"],
            "indirect_emissions_tco2e": df["indirect_emissions_tco2e"],
            "precursor_tco2e": df["precursor_tco2e"],
            "embedded_emissions_tco2e": df["embedded_emissions_tco2e"],
            "direct_intensity_tco2_per_unit": df["direct_intensity_tco2_per_unit"],
            "indirect_intensity_tco2_per_unit": df["indirect_intensity_tco2_per_unit"],
            "embedded_intensity_tco2_per_unit": df["embedded_intensity_tco2_per_unit"],
            # definitive regime fields
            "carbon_price_paid_eur_per_t": df["carbon_price_paid_eur_per_t"],
            "payable_share": df["payable_share"],
            "payable_emissions_tco2e": df["payable_emissions_tco2e"],
            "certificates_required": df["certificates_required"],
            "estimated_payable_amount_eur": df["estimated_payable_amount_eur"],
            # provenance
            "mapping_rule": df["mapping_rule"],
            "allocation_method": df["allocation_method"],
            "allocation_hash": df["allocation_hash"],
            "default_value_evidence_hash": df["default_value_evidence_hash"],
        }
    )

    # Summary by goods (deterministic)
    goods_summary = (
        table.groupby(["cbam_good", "cbam_good_key"], dropna=False)[
            [
                "embedded_emissions_tco2e",
                "direct_emissions_tco2e",
                "indirect_emissions_tco2e",
                "precursor_tco2e",
                "cbam_cost_eur",
                "certificates_required",
                "estimated_payable_amount_eur",
            ]
        ]
        .sum()
        .reset_index()
        .sort_values(["embedded_emissions_tco2e", "cbam_good"], ascending=[False, True], kind="mergesort")
        .to_dict(orient="records")
    )

    total_emb = float(table["embedded_emissions_tco2e"].sum())
    total_cost_signal = float(table["cbam_cost_eur"].sum())
    total_direct = float(table["direct_emissions_tco2e"].sum())
    total_indirect = float(table["indirect_emissions_tco2e"].sum())
    total_prec = float(table["precursor_tco2e"].sum())

    liab_total = compute_cbam_liability(
        year=int(reporting_year),
        embedded_emissions_tco2=float(table.loc[table["cbam_covered"] == True, "embedded_emissions_tco2e"].sum())
        * 1.0,  # total; export share already used row liability, but here keep total embedded for high-level
        eu_ets_price_eur_per_t=float(_to_float(eua_price_eur_per_t)),
        carbon_price_paid_eur_per_t=float(_to_float(carbon_price_paid_eur_per_t)),
    ).to_dict()

    totals = {
        "reporting_year": int(reporting_year),
        "embedded_emissions_tco2e": _q(total_emb, 6),
        "direct_tco2e": _q(total_direct, 6),
        "indirect_tco2e": _q(total_indirect, 6),
        "precursor_tco2e": _q(total_prec, 6),
        "cbam_cost_signal_eur": _q(total_cost_signal, 6),
        "carbon_price_paid_eur_per_t": _q(float(_to_float(carbon_price_paid_eur_per_t)), 6),
        "liability": liab_total,
        "goods_summary": goods_summary,
        "precursor_meta": prec_meta,
        "allocation_basis": _norm(allocation_basis) or "quantity",
        "allocation_method": alloc_method,
        "allocation_hash": alloc_hash,
        "engine_determinism": {
            "float_quantization_digits": 6,
            "table_order": "cn_code,sku (stable mergesort)",
            "default_value_selection": "cn_exact -> good_key; unit match -> priority -> valid_from",
            "precursor": "explicit edges + optional chain, cycle-aware",
        },
        "notes_tr": [
            "CBAM 2023/1773 geçiş dönemi raporlama alanlarını destekler: CN kodu, miktar, gömülü emisyonlar, actual/default bayrağı.",
            "DEFAULT kullanımı için default value tablosu (cbam_defaults) ve kanıt hash'i (default_value_evidence_hash) üretilir.",
            "Sertifika/ödeme alanları 2026+ için tahmini hesap (definitive regime) olarak üretilir; resmi kural seti sürümlemeye uygundur.",
        ],
        "meta_hash": sha256_json(
            {
                "reporting_year": int(reporting_year),
                "carbon_price_paid_eur_per_t": float(_to_float(carbon_price_paid_eur_per_t)),
                "eua_price_eur_per_t": float(_to_float(eua_price_eur_per_t)),
                "allocation_method": str(alloc_method or ""),
                "allocation_hash": str(alloc_hash or ""),
                "precursor_meta": prec_meta,
            }
        ),
    }

    return table, totals
