from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import List, Dict, Tuple

import pandas as pd
import streamlit as st

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors

CME_ENGINE_VERSION = "0.2.0-cbam-demo"
AUDIT_LOG_FILE = "runs.jsonl"

# -------------------------
# Audit log
# -------------------------
def append_audit_log(run_id: str, inputs: dict, outputs_summary: dict) -> None:
    rec = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine_version": CME_ENGINE_VERSION,
        "event_type": "CME_RUN",
        "inputs": inputs,
        "summary": outputs_summary,
    }
    with open(AUDIT_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def read_audit_log_text() -> str:
    if not os.path.exists(AUDIT_LOG_FILE):
        return ""
    with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
        return f.read()

# -------------------------
# Config
# -------------------------
@dataclass
class CMEConfig:
    grid_factor_kg_per_kwh: float = 0.43
    gas_factor_kg_per_m3: float = 2.00
    carbon_price_eur_per_tco2: float = 85.0
    fx_eur_to_try: float = 35.0

# REQUIRED columns
REQUIRED_ENERGY_COLS = ["month", "electricity_kwh", "natural_gas_m3"]
REQUIRED_PROD_COLS = ["month", "sku", "quantity"]

# OPTIONAL CBAM columns (new)
CBAM_OPTIONAL_COLS = ["export_to_eu_quantity", "input_emission_factor_kg_per_unit"]

def _ensure_cols(df: pd.DataFrame, required: List[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} eksik kolon(lar): {missing}")

def _to_num(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def _ensure_nonneg(df: pd.DataFrame, cols: List[str], name: str) -> None:
    for c in cols:
        if c in df.columns:
            bad = df[c].isna() | (df[c] < 0)
            if bad.any():
                sample = df.loc[bad].head(1).to_dict(orient="records")
                raise ValueError(f"{name}.{c} boş/negatif. Örnek: {sample}")

def validate_inputs(energy: pd.DataFrame, prod: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    _ensure_cols(energy, REQUIRED_ENERGY_COLS, "energy.csv")
    _ensure_cols(prod, REQUIRED_PROD_COLS, "production.csv")

    e = energy.copy()
    p = prod.copy()

    e["month"] = e["month"].astype(str).str.strip()
    p["month"] = p["month"].astype(str).str.strip()
    p["sku"] = p["sku"].astype(str).str.strip()

    _to_num(e, ["electricity_kwh", "natural_gas_m3"])
    _to_num(p, ["quantity", "sale_price", "unit_cost"] + CBAM_OPTIONAL_COLS)

    _ensure_nonneg(e, ["electricity_kwh", "natural_gas_m3"], "energy.csv")
    _ensure_nonneg(p, ["quantity"], "production.csv")
    _ensure_nonneg(p, ["sale_price", "unit_cost"] + CBAM_OPTIONAL_COLS, "production.csv")

    p = p[p["quantity"] > 0].copy()
    if p.empty:
        raise ValueError("production.csv içinde quantity > 0 satır yok.")

    # If CBAM export column exists, it cannot exceed quantity
    if "export_to_eu_quantity" in p.columns:
        too_big = p["export_to_eu_quantity"] > p["quantity"]
        if too_big.any():
            sample = p.loc[too_big].head(1).to_dict(orient="records")
            raise ValueError(f"production.csv.export_to_eu_quantity quantity’den büyük olamaz. Örnek: {sample}")

    return e, p

def monthly_emissions(e: pd.DataFrame, cfg: CMEConfig) -> pd.DataFrame:
    s2 = (e["electricity_kwh"] * cfg.grid_factor_kg_per_kwh) / 1000.0
    s1 = (e["natural_gas_m3"] * cfg.gas_factor_kg_per_m3) / 1000.0
    out = pd.DataFrame({"month": e["month"], "scope2_tco2": s2, "scope1_tco2": s1})
    out["total_tco2"] = out["scope1_tco2"] + out["scope2_tco2"]
    return out

def allocate_energy_emissions(p: pd.DataFrame, m: pd.DataFrame) -> pd.DataFrame:
    mm = m[["month", "total_tco2"]].copy()
    qty = p.groupby("month", as_index=False)["quantity"].sum().rename(columns={"quantity":"month_total_qty"})
    x = p.merge(qty, on="month", how="left").merge(mm, on="month", how="left")

    if x["total_tco2"].isna().any():
        miss = sorted(x.loc[x["total_tco2"].isna(), "month"].unique().tolist())
        raise ValueError(f"energy.csv’de olmayan ay(lar) var: {miss}")

    x["qty_share"] = x["quantity"] / x["month_total_qty"]
    x["allocated_tco2_energy"] = x["total_tco2"] * x["qty_share"]
    x["allocated_kgco2_energy"] = x["allocated_tco2_energy"] * 1000.0
    x["kgco2_per_unit_energy"] = x["allocated_kgco2_energy"] / x["quantity"]
    return x

def build_sku_summary(x: pd.DataFrame, cfg: CMEConfig) -> pd.DataFrame:
    # Fill missing optional columns with 0 (so demo still works if user didn't add them yet)
    if "export_to_eu_quantity" not in x.columns:
        x["export_to_eu_quantity"] = 0.0
    if "input_emission_factor_kg_per_unit" not in x.columns:
        x["input_emission_factor_kg_per_unit"] = 0.0

    has_prices = ("sale_price" in x.columns) and ("unit_cost" in x.columns)

    rows = []
    for sku, g in x.groupby("sku"):
        qty = float(g["quantity"].sum())
        export_qty = float(g["export_to_eu_quantity"].fillna(0).sum())

        # weighted avg energy kg/unit
        kg_unit_energy = float(g["allocated_kgco2_energy"].sum() / qty)

        # weighted avg input kg/unit (given per unit already)
        # Use quantity-weighted avg if it changes month to month
        kg_unit_input = float((g["input_emission_factor_kg_per_unit"].fillna(0).mul(g["quantity"]).sum()) / qty)

        kg_unit_total = kg_unit_energy + kg_unit_input

        # totals
        total_tco2_energy = float(g["allocated_tco2_energy"].sum())
        total_tco2_input = float((kg_unit_input * qty) / 1000.0)
        total_tco2_total = total_tco2_energy + total_tco2_input

        # carbon cost per unit (TL)
        carbon_tl_unit = (kg_unit_total/1000.0) * cfg.carbon_price_eur_per_tco2 * cfg.fx_eur_to_try

        # net margin after carbon (if prices exist)
        sale = float((g["sale_price"].fillna(0).mul(g["quantity"]).sum() / qty)) if has_prices else float("nan")
        cost = float((g["unit_cost"].fillna(0).mul(g["quantity"]).sum() / qty)) if has_prices else float("nan")
        net_margin = (sale - cost - carbon_tl_unit) if has_prices else float("nan")

        # CBAM: only EU export part
        cbam_tco2 = (export_qty * kg_unit_total) / 1000.0
        cbam_cost_tl = cbam_tco2 * cfg.carbon_price_eur_per_tco2 * cfg.fx_eur_to_try

        rows.append({
            "sku": sku,
            "total_quantity": qty,
            "export_to_eu_quantity": export_qty,
            "kgco2_per_unit_energy": kg_unit_energy,
            "kgco2_per_unit_input": kg_unit_input,
            "kgco2_per_unit_total": kg_unit_total,
            "total_tco2_energy": total_tco2_energy,
            "total_tco2_total": total_tco2_total,
            "carbon_cost_tl_per_unit": carbon_tl_unit,
            "sale_price_tl": sale,
            "unit_cost_tl": cost,
            "net_margin_after_carbon_tl": net_margin,
            "cbam_tco2": cbam_tco2,
            "cbam_cost_tl": cbam_cost_tl,
        })

    out = pd.DataFrame(rows)

    if not out.empty:
        out["risk_score"] = (
            out["cbam_tco2"].rank(pct=True)*0.45 +
            out["kgco2_per_unit_total"].rank(pct=True)*0.35 +
            out["carbon_cost_tl_per_unit"].rank(pct=True)*0.20
        )
        out = out.sort_values("risk_score", ascending=False).reset_index(drop=True)
    return out

def run_cme(energy_df: pd.DataFrame, prod_df: pd.DataFrame, cfg: CMEConfig) -> Dict:
    e, p = validate_inputs(energy_df, prod_df)
    m = monthly_emissions(e, cfg)
    x = allocate_energy_emissions(p, m)
    sku = build_sku_summary(x, cfg)

    totals = {
        "months": sorted(m["month"].unique().tolist()),
        "scope1_tco2": float(m["scope1_tco2"].sum()),
        "scope2_tco2": float(m["scope2_tco2"].sum()),
        "total_tco2_energy": float(m["total_tco2"].sum()),
        "sku_count": int(sku["sku"].nunique()) if not sku.empty else 0,
        "cbam_tco2_total": float(sku["cbam_tco2"].sum()) if not sku.empty else 0.0,
        "cbam_cost_tl_total": float(sku["cbam_cost_tl"].sum()) if not sku.empty else 0.0,
    }
    return {"config": cfg.__dict__, "totals": totals, "monthly": m, "sku": sku}

# -------------------------
# PDF (kısa)
# -------------------------
def build_pdf(result: Dict, notes: List[str]) -> bytes:
    styles = getSampleStyleSheet()
    story = []
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cfg = result["config"]
    t = result["totals"]

    story.append(Paragraph("CME – CBAM + ETS-ready Demo Rapor", styles["Title"]))
    story.append(Spacer(1, 8))
    story.append(Paragraph(f"Oluşturulma: {now_utc}", styles["Normal"]))
    story.append(Paragraph("Not: Bu çıktı yönetim amaçlı tahmini allocation içerir; resmi CBAM beyanı değildir.", styles["Normal"]))
    story.append(Spacer(1, 10))

    kpi = [
        ["Gösterge", "Değer"],
        ["Energy Emissions (tCO2e)", f"{t['total_tco2_energy']:.2f}"],
        ["Scope 1 (tCO2e)", f"{t['scope1_tco2']:.2f}"],
        ["Scope 2 (tCO2e)", f"{t['scope2_tco2']:.2f}"],
        ["CBAM (EU exports) tCO2e", f"{t['cbam_tco2_total']:.2f}"],
        ["CBAM cost total (TL)", f"{t['cbam_cost_tl_total']:.0f}"],
        ["Carbon price (€/t)", f"{cfg['carbon_price_eur_per_tco2']:.2f}"],
        ["FX (TL/€)", f"{cfg['fx_eur_to_try']:.2f}"],
    ]
    tbl = Table(kpi, hAlign="LEFT", colWidths=[240, 260])
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ("PADDING", (0,0), (-1,-1), 6),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 10))

    story.append(Paragraph("Notlar", styles["Heading2"]))
    for n in notes:
        story.append(Paragraph(f"• {n}", styles["Normal"]))

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=24, rightMargin=24, topMargin=24, bottomMargin=24)
    doc.build(story)
    return buf.getvalue()

# -------------------------
# UI
# -------------------------
st.set_page_config(page_title="CME Demo", layout="wide")
st.title("CME Demo – CBAM öncelikli (ETS modülü daha sonra)")
st.caption(f"Versiyon: {CME_ENGINE_VERSION}")
st.divider()

st.subheader("1) CSV Yükle")
c1, c2 = st.columns(2)
with c1:
    energy_file = st.file_uploader("energy.csv", type=["csv"])
    st.caption("Kolonlar: month, electricity_kwh, natural_gas_m3")
with c2:
    prod_file = st.file_uploader("production.csv", type=["csv"])
    st.caption("Kolonlar: month, sku, quantity (+ sale_price, unit_cost) + CBAM kolonları")

st.subheader("2) Parametreler")
p1, p2, p3, p4 = st.columns(4)
with p1:
    grid_factor = st.number_input("Grid faktör (kgCO2e/kWh)", min_value=0.0, value=0.43, step=0.01)
with p2:
    gas_factor = st.number_input("Gaz faktör (kgCO2e/m³)", min_value=0.0, value=2.00, step=0.05)
with p3:
    carbon_price = st.slider("Karbon fiyatı (€/tCO2)", 0.0, 250.0, 85.0, 1.0)
with p4:
    fx = st.number_input("Kur (1€ kaç TL?)", min_value=0.0, value=35.0, step=0.5)

run = st.button("🚀 Hesapla", type="primary", use_container_width=True)

if run:
    if energy_file is None or prod_file is None:
        st.error("Lütfen energy.csv ve production.csv yükle.")
        st.stop()

    try:
        energy_df = pd.read_csv(energy_file)
        prod_df = pd.read_csv(prod_file)

        cfg = CMEConfig(
            grid_factor_kg_per_kwh=float(grid_factor),
            gas_factor_kg_per_m3=float(gas_factor),
            carbon_price_eur_per_tco2=float(carbon_price),
            fx_eur_to_try=float(fx),
        )

        result = run_cme(energy_df, prod_df, cfg)
        totals = result["totals"]
        sku_df = result["sku"]

        st.success("Hesaplandı ✅")

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Energy emissions (tCO2e)", f"{totals['total_tco2_energy']:.2f}")
        k2.metric("CBAM tCO2e (EU exports)", f"{totals['cbam_tco2_total']:.2f}")
        k3.metric("CBAM cost total (TL)", f"{totals['cbam_cost_tl_total']:,.0f}")
        k4.metric("SKU count", f"{totals['sku_count']}")

        st.subheader("SKU Tablosu (CBAM dahil)")
        st.dataframe(sku_df, use_container_width=True, hide_index=True)

        notes = [
            "Allocation: Aylık enerji kaynaklı emisyon, aynı ayki üretim quantity oranında SKU’lara dağıtıldı (demo).",
            "CBAM: Sadece export_to_eu_quantity ile işaretlenen AB satış miktarı için emisyon/maliyet hesaplandı.",
            "input_emission_factor_kg_per_unit: Ürün başına upstream girdi emisyonu (kgCO2e/birim) basitleştirilmiş parametredir."
        ]
        st.subheader("Notlar")
        for n in notes:
            st.write(f"- {n}")

        pdf_bytes = build_pdf(result, notes)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        st.download_button("⬇️ PDF indir", data=pdf_bytes, file_name=f"cme_cbam_rapor_{ts}.pdf", mime="application/pdf")

        run_id = str(uuid.uuid4())
        append_audit_log(run_id, inputs=cfg.__dict__, outputs_summary=totals)

        st.subheader("Audit log (runs.jsonl)")
        log_text = read_audit_log_text()
        if log_text:
            st.download_button("⬇️ runs.jsonl indir", data=log_text.encode("utf-8"), file_name="runs.jsonl")
        else:
            st.info("Log boş.")

    except Exception as e:
        st.error("Hata:")
        st.code(str(e))

st.divider()
st.subheader("Örnek production.csv (CBAM kolonları ile)")
st.code(
"""month,sku,quantity,sale_price,unit_cost,export_to_eu_quantity,input_emission_factor_kg_per_unit
2025-11,SKU-A,5000,120,78,2000,3.5
2025-11,SKU-B,3000,220,160,0,1.2
2025-12,SKU-A,6000,120,79,2400,3.5
""",
language="text",
)
