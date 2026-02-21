import streamlit as st
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timezone
from io import BytesIO

# -----------------------------
# Optional PDF (ReportLab)
# -----------------------------
PDF_AVAILABLE = True
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import cm
except Exception:
    PDF_AVAILABLE = False


# -----------------------------
# App Config
# -----------------------------
st.set_page_config(
    page_title="CME Demo — CBAM + ETS",
    layout="wide",
)

APP_VERSION = "demo-1.0-step1-ets"
AUDIT_LOG_PATH = "runs.jsonl"

DISCLAIMER_TEXT = (
    "Önemli Not: Bu rapor yönetim amaçlı tahmini bir allocation/hesaplama çıktısıdır. "
    "Resmî beyan/uyum dokümanı değildir."
)

# -----------------------------
# Helpers
# -----------------------------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def safe_float(x):
    try:
        if pd.isna(x):
            return np.nan
        return float(x)
    except Exception:
        return np.nan

def read_csv_uploaded(uploaded_file) -> pd.DataFrame:
    return pd.read_csv(uploaded_file)

def require_columns(df: pd.DataFrame, required: list, df_name: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"'{df_name}' dosyasında eksik kolon(lar): {', '.join(missing)}"
        )

def append_audit_log(record: dict):
    line = json.dumps(record, ensure_ascii=False)
    with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def format_eur(x):
    try:
        return f"€{x:,.2f}"
    except Exception:
        return str(x)

def format_tl(x):
    try:
        return f"₺{x:,.2f}"
    except Exception:
        return str(x)

def kg_to_t(x_kg):
    return x_kg / 1000.0

def clamp_nonneg(x):
    return max(0.0, float(x))

# -----------------------------
# Calculations
# -----------------------------
def compute_energy_emissions(energy_df: pd.DataFrame):
    """
    Expected columns:
      - energy_carrier: 'electricity' or 'natural_gas' (text)
      - scope: 1 or 2 (int)
      - activity_amount: numeric
      - emission_factor_kgco2_per_unit: numeric (kgCO2 per unit)
    """
    required = ["energy_carrier", "scope", "activity_amount", "emission_factor_kgco2_per_unit"]
    require_columns(energy_df, required, "energy.csv")

    df = energy_df.copy()
    df["scope"] = df["scope"].apply(safe_float).astype("Int64")
    df["activity_amount"] = df["activity_amount"].apply(safe_float)
    df["emission_factor_kgco2_per_unit"] = df["emission_factor_kgco2_per_unit"].apply(safe_float)

    # Basic clean
    df = df.dropna(subset=["scope", "activity_amount", "emission_factor_kgco2_per_unit"])
    df["emissions_kgco2"] = df["activity_amount"] * df["emission_factor_kgco2_per_unit"]

    total_kg = float(df["emissions_kgco2"].sum()) if len(df) else 0.0
    scope1_kg = float(df.loc[df["scope"] == 1, "emissions_kgco2"].sum()) if len(df) else 0.0
    scope2_kg = float(df.loc[df["scope"] == 2, "emissions_kgco2"].sum()) if len(df) else 0.0

    return df, {
        "total_kgco2": total_kg,
        "scope1_kgco2": scope1_kg,
        "scope2_kgco2": scope2_kg,
        "total_tco2": kg_to_t(total_kg),
        "scope1_tco2": kg_to_t(scope1_kg),
        "scope2_tco2": kg_to_t(scope2_kg),
    }

def allocate_energy_to_skus(production_df: pd.DataFrame, total_energy_kgco2: float):
    """
    Demo allocation: distribute total energy emissions (kgCO2) by SKU quantity share.
    Expected columns in production.csv:
      - sku
      - quantity
    """
    require_columns(production_df, ["sku", "quantity"], "production.csv")
    df = production_df.copy()
    df["quantity"] = df["quantity"].apply(safe_float)

    # Avoid division by zero
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
        0.0
    )
    return df

def compute_cbam(production_df: pd.DataFrame, eua_price_eur_per_t: float, total_energy_kgco2: float):
    """
    CBAM logic (demo):
      - Needs production.csv columns:
          sku, quantity,
          export_to_eu_quantity,
          input_emission_factor_kg_per_unit
      - Total factor (kg/unit) = allocated_energy_kg_per_unit + input_factor
      - Embedded emissions for EU exports (kg) = export_qty * total_factor
      - tCO2 = kg/1000
      - CBAM cost (EUR) = tCO2 * EUA price
    """
    required = ["sku", "quantity", "export_to_eu_quantity", "input_emission_factor_kg_per_unit"]
    require_columns(production_df, required, "production.csv")

    df = production_df.copy()
    df["quantity"] = df["quantity"].apply(safe_float)
    df["export_to_eu_quantity"] = df["export_to_eu_quantity"].apply(safe_float)
    df["input_emission_factor_kg_per_unit"] = df["input_emission_factor_kg_per_unit"].apply(safe_float)

    # Allocation
    df = allocate_energy_to_skus(df, total_energy_kgco2)

    # Basic checks
    df["export_to_eu_quantity"] = df["export_to_eu_quantity"].fillna(0.0)
    df["input_emission_factor_kg_per_unit"] = df["input_emission_factor_kg_per_unit"].fillna(0.0)
    df["quantity"] = df["quantity"].fillna(0.0)

    # Keep your existing warning: export > quantity
    export_gt_qty = df[(df["export_to_eu_quantity"] > df["quantity"]) & (df["quantity"] > 0)]
    warning = None
    if len(export_gt_qty):
        warning = (
            "Uyarı: Bazı satırlarda export_to_eu_quantity, quantity değerinden büyük görünüyor. "
            "Lütfen kontrol et."
        )

    df["total_emission_factor_kg_per_unit"] = df["alloc_energy_kgco2_per_unit"] + df["input_emission_factor_kg_per_unit"]
    df["cbam_embedded_emissions_kgco2"] = df["export_to_eu_quantity"] * df["total_emission_factor_kg_per_unit"]
    df["cbam_embedded_emissions_tco2"] = df["cbam_embedded_emissions_kgco2"].apply(kg_to_t)

    df["cbam_cost_eur"] = df["cbam_embedded_emissions_tco2"] * float(eua_price_eur_per_t)

    # Simple risk score: weighted blend of cost + intensity
    # (kept simple; Step 5 will polish messages/validations)
    cost = df["cbam_cost_eur"].fillna(0.0).to_numpy()
    intensity = df["total_emission_factor_kg_per_unit"].fillna(0.0).to_numpy()
    def minmax(x):
        if len(x) == 0:
            return x
        mn, mx = float(np.min(x)), float(np.max(x))
        if mx - mn < 1e-12:
            return np.zeros_like(x)
        return (x - mn) / (mx - mn)

    risk = 0.7 * minmax(cost) + 0.3 * minmax(intensity)
    df["risk_score_0_100"] = (risk * 100.0).round(1)

    totals = {
        "total_cbam_embedded_tco2": float(df["cbam_embedded_emissions_tco2"].sum()) if len(df) else 0.0,
        "total_cbam_cost_eur": float(df["cbam_cost_eur"].sum()) if len(df) else 0.0,
    }
    return df, totals, warning

def compute_ets(scope1_tco2: float, free_allocation_tco2: float, banked_tco2: float, eua_price: float, fx_tl_per_eur: float):
    scope1_tco2 = float(scope1_tco2)
    free_allocation_tco2 = float(free_allocation_tco2)
    banked_tco2 = float(banked_tco2)
    eua_price = float(eua_price)
    fx_tl_per_eur = float(fx_tl_per_eur)

    net_req = max(0.0, scope1_tco2 - free_allocation_tco2 - banked_tco2)
    cost_tl = net_req * eua_price * fx_tl_per_eur

    return {
        "scope1_tco2": scope1_tco2,
        "free_allocation_tco2": free_allocation_tco2,
        "banked_allowances_tco2": banked_tco2,
        "net_eua_requirement_tco2": net_req,
        "eua_price_eur_per_tco2": eua_price,
        "fx_tl_per_eur": fx_tl_per_eur,
        "ets_cost_tl": cost_tl,
    }

# -----------------------------
# PDF
# -----------------------------
def build_pdf_report(
    eua_price: float,
    fx: float,
    energy_summary: dict,
    ets_summary: dict,
    cbam_df: pd.DataFrame | None,
    cbam_totals: dict | None
) -> bytes:
    if not PDF_AVAILABLE:
        raise RuntimeError("PDF kütüphanesi (reportlab) bulunamadı.")

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    x0 = 2 * cm
    y = height - 2 * cm

    def line(text, dy=14, font="Helvetica", size=10):
        nonlocal y
        c.setFont(font, size)
        c.drawString(x0, y, str(text))
        y -= dy

    # Title
    c.setFont("Helvetica-Bold", 14)
    c.drawString(x0, y, "CME Demo Raporu — CBAM + ETS (Tahmini)")
    y -= 20

    c.setFont("Helvetica", 10)
    c.drawString(x0, y, f"Tarih (UTC): {now_iso()}")
    y -= 16
    c.drawString(x0, y, f"Uygulama sürümü: {APP_VERSION}")
    y -= 18

    # Disclaimer
    c.setFont("Helvetica-Oblique", 9)
    c.drawString(x0, y, DISCLAIMER_TEXT)
    y -= 22

    # Parameters
    c.setFont("Helvetica-Bold", 11)
    c.drawString(x0, y, "Genel Parametreler")
    y -= 16
    c.setFont("Helvetica", 10)
    c.drawString(x0, y, f"EUA price: {format_eur(eua_price)} / tCO2")
    y -= 14
    c.drawString(x0, y, f"FX: {fx:.2f} TL / €")
    y -= 18

    # Energy summary
    c.setFont("Helvetica-Bold", 11)
    c.drawString(x0, y, "Energy (Scope 1–2) Özeti")
    y -= 16
    c.setFont("Helvetica", 10)
    c.drawString(x0, y, f"Total: {energy_summary['total_tco2']:.4f} tCO2")
    y -= 14
    c.drawString(x0, y, f"Scope 1: {energy_summary['scope1_tco2']:.4f} tCO2")
    y -= 14
    c.drawString(x0, y, f"Scope 2: {energy_summary['scope2_tco2']:.4f} tCO2")
    y -= 18

    # ETS section (NEW)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(x0, y, "ETS Özeti (Tesis Bazlı)")
    y -= 16
    c.setFont("Helvetica", 10)
    c.drawString(x0, y, f"Scope 1 total: {ets_summary['scope1_tco2']:.4f} tCO2")
    y -= 14
    c.drawString(x0, y, f"Free allocation: {ets_summary['free_allocation_tco2']:.4f} tCO2")
    y -= 14
    c.drawString(x0, y, f"Banked allowances (ops.): {ets_summary['banked_allowances_tco2']:.4f} tCO2")
    y -= 14
    c.drawString(x0, y, f"Net EUA requirement: {ets_summary['net_eua_requirement_tco2']:.4f} tCO2")
    y -= 14
    c.drawString(x0, y, f"ETS cost (TL): {format_tl(ets_summary['ets_cost_tl'])}")
    y -= 18

    # CBAM section (if available)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(x0, y, "CBAM Özeti (Ürün Bazlı, Demo)")
    y -= 16
    c.setFont("Helvetica", 10)

    if cbam_df is None or cbam_totals is None or len(cbam_df) == 0:
        c.drawString(x0, y, "CBAM hesaplaması için production.csv yüklenmedi veya boş.")
        y -= 14
    else:
        c.drawString(x0, y, f"Toplam embedded: {cbam_totals['total_cbam_embedded_tco2']:.4f} tCO2")
        y -= 14
        c.drawString(x0, y, f"Toplam CBAM maliyeti: {format_eur(cbam_totals['total_cbam_cost_eur'])}")
        y -= 18

        # Top 10 risk
        top = cbam_df.sort_values("risk_score_0_100", ascending=False).head(10)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(x0, y, "En Yüksek Risk Skorlu İlk 10 SKU")
        y -= 14
        c.setFont("Helvetica", 9)
        c.drawString(x0, y, "SKU | Risk(0-100) | EU tCO2 | CBAM €")
        y -= 12
        for _, r in top.iterrows():
            text = f"{r['sku']} | {r['risk_score_0_100']} | {r['cbam_embedded_emissions_tco2']:.4f} | {r['cbam_cost_eur']:.2f}"
            c.drawString(x0, y, text[:120])
            y -= 11
            if y < 3 * cm:
                c.showPage()
                y = height - 2 * cm

    # Footer disclaimer (again)
    if y < 3 * cm:
        c.showPage()
        y = height - 2 * cm
    y -= 10
    c.setFont("Helvetica-Oblique", 9)
    c.drawString(x0, y, DISCLAIMER_TEXT)

    c.showPage()
    c.save()
    buffer.seek(0)
    return buffer.read()

# -----------------------------
# UI
# -----------------------------
st.title("CME Demo — CBAM (Ürün) + ETS (Tesis)")

with st.sidebar:
    st.subheader("Genel Parametreler")
    eua_price = st.slider("EUA price (€/tCO2)", min_value=0.0, max_value=200.0, value=80.0, step=1.0)
    fx = st.number_input("FX (TL/€)", min_value=0.0, value=35.0, step=0.5)

    st.divider()
    st.caption("Dosyalar")
    energy_file = st.file_uploader("energy.csv yükle", type=["csv"], key="energy_csv")
    production_file = st.file_uploader("production.csv yükle (CBAM için)", type=["csv"], key="production_csv")

tab_cbam, tab_ets = st.tabs(["CBAM (ürün bazlı)", "ETS (tesis bazlı)"])

# Shared: load energy
energy_df = None
energy_calc_df = None
energy_summary = {"total_tco2": 0.0, "scope1_tco2": 0.0, "scope2_tco2": 0.0, "total_kgco2": 0.0, "scope1_kgco2": 0.0, "scope2_kgco2": 0.0}

energy_error = None
if energy_file is not None:
    try:
        energy_df = read_csv_uploaded(energy_file)
        energy_calc_df, energy_summary = compute_energy_emissions(energy_df)
    except Exception as e:
        energy_error = str(e)

# CBAM Tab
with tab_cbam:
    st.subheader("CBAM — Ürün Bazlı (Demo)")
    st.write("Akış: **energy.csv (Scope 1–2)** toplam emisyonu → SKU’lara **quantity oranında dağıtım** → EU export için embedded emisyon → CBAM maliyeti (EUA fiyatı ile).")

    if energy_file is None:
        st.info("CBAM için önce **energy.csv** yükleyin.")
    elif energy_error:
        st.error(f"energy.csv okunamadı: {energy_error}")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Energy total (tCO2)", f"{energy_summary['total_tco2']:.4f}")
        c2.metric("Scope 1 (tCO2)", f"{energy_summary['scope1_tco2']:.4f}")
        c3.metric("Scope 2 (tCO2)", f"{energy_summary['scope2_tco2']:.4f}")

        st.caption("Energy detay (ilk 20 satır)")
        st.dataframe(energy_calc_df.head(20), use_container_width=True)

        st.divider()
        if production_file is None:
            st.info("CBAM hesaplamak için ayrıca **production.csv** yükleyin.")
        else:
            try:
                prod_df = read_csv_uploaded(production_file)
                cbam_df, cbam_totals, cbam_warning = compute_cbam(
                    prod_df,
                    eua_price_eur_per_t=float(eua_price),
                    total_energy_kgco2=float(energy_summary["total_kgco2"]),
                )

                if cbam_warning:
                    st.warning(cbam_warning)

                c1, c2 = st.columns(2)
                c1.metric("Toplam CBAM embedded (tCO2)", f"{cbam_totals['total_cbam_embedded_tco2']:.4f}")
                c2.metric("Toplam CBAM maliyeti (EUR)", f"{cbam_totals['total_cbam_cost_eur']:.2f}")

                st.subheader("SKU Risk Sıralaması")
                show_cols = [
                    "sku",
                    "quantity",
                    "export_to_eu_quantity",
                    "alloc_energy_kgco2_per_unit",
                    "input_emission_factor_kg_per_unit",
                    "total_emission_factor_kg_per_unit",
                    "cbam_embedded_emissions_tco2",
                    "cbam_cost_eur",
                    "risk_score_0_100",
                ]
                existing_cols = [c for c in show_cols if c in cbam_df.columns]
                cbam_sorted = cbam_df.sort_values("risk_score_0_100", ascending=False)
                st.dataframe(cbam_sorted[existing_cols], use_container_width=True)

                # Download results
                out_csv = cbam_sorted.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "CBAM sonuçlarını indir (CSV)",
                    data=out_csv,
                    file_name="cbam_results.csv",
                    mime="text/csv",
                )

                # PDF + Audit log (includes ETS too if computed below)
                st.divider()
                st.subheader("Rapor / Audit")
                st.caption("PDF rapor hem CBAM hem ETS özetini içerir. (ETS sekmesinde değer girmezsen, ETS özetinde girilenler 0 kabul edilir.)")

                # ETS inputs here too (so PDF is consistent even if user doesn't open ETS tab)
                with st.expander("PDF için ETS girdileri (opsiyonel)", expanded=False):
                    free_alloc_pdf = st.number_input("Free allocation (tCO2) — PDF", min_value=0.0, value=0.0, step=100.0, key="free_alloc_pdf")
                    banked_pdf = st.number_input("Banked allowances (tCO2) — PDF (ops.)", min_value=0.0, value=0.0, step=100.0, key="banked_pdf")
                ets_summary_for_pdf = compute_ets(
                    scope1_tco2=float(energy_summary["scope1_tco2"]),
                    free_allocation_tco2=float(free_alloc_pdf),
                    banked_tco2=float(banked_pdf),
                    eua_price=float(eua_price),
                    fx_tl_per_eur=float(fx),
                )

                colA, colB = st.columns(2)
                with colA:
                    if st.button("Audit log yaz (runs.jsonl)", type="secondary"):
                        record = {
                            "ts_utc": now_iso(),
                            "app_version": APP_VERSION,
                            "inputs": {
                                "eua_price_eur_per_tco2": float(eua_price),
                                "fx_tl_per_eur": float(fx),
                                "has_energy_csv": energy_file is not None,
                                "has_production_csv": production_file is not None,
                            },
                            "energy_summary": energy_summary,
                            "ets_summary": ets_summary_for_pdf,  # NEW
                            "cbam_totals": cbam_totals,
                        }
                        append_audit_log(record)
                        st.success("Audit log yazıldı: runs.jsonl")

                with colB:
                    if PDF_AVAILABLE:
                        if st.button("PDF raporu üret", type="primary"):
                            pdf_bytes = build_pdf_report(
                                eua_price=float(eua_price),
                                fx=float(fx),
                                energy_summary=energy_summary,
                                ets_summary=ets_summary_for_pdf,
                                cbam_df=cbam_sorted,
                                cbam_totals=cbam_totals,
                            )
                            st.download_button(
                                "PDF indir",
                                data=pdf_bytes,
                                file_name="cme_report_cbam_ets.pdf",
                                mime="application/pdf",
                            )
                    else:
                        st.warning("PDF üretimi için reportlab gerekli. requirements.txt içine 'reportlab' ekleyin.")

            except Exception as e:
                st.error(f"production.csv işlenemedi: {e}")

# ETS Tab
with tab_ets:
    st.subheader("ETS — Tesis Bazlı (Türkiye ETS Demo)")
    st.write("Bu sekme yalnızca **Scope 1** emisyonlarına bakar (energy.csv).")

    if energy_file is None:
        st.info("ETS için önce **energy.csv** yükleyin.")
    elif energy_error:
        st.error(f"energy.csv okunamadı: {energy_error}")
    else:
        left, right = st.columns([1, 1])

        with left:
            st.markdown("### Girdiler")
            free_allocation = st.number_input("Free allocation (tCO2)", min_value=0.0, value=0.0, step=100.0)
            banked_allowances = st.number_input("Banked allowances (tCO2) (opsiyonel)", min_value=0.0, value=0.0, step=100.0)

        ets_summary = compute_ets(
            scope1_tco2=float(energy_summary["scope1_tco2"]),
            free_allocation_tco2=float(free_allocation),
            banked_tco2=float(banked_allowances),
            eua_price=float(eua_price),
            fx_tl_per_eur=float(fx),
        )

        with right:
            st.markdown("### Çıktılar")
            st.metric("Scope 1 total (tCO2)", f"{ets_summary['scope1_tco2']:.4f}")
            st.metric("Net EUA requirement (tCO2)", f"{ets_summary['net_eua_requirement_tco2']:.4f}")
            st.metric("ETS cost (TL)", format_tl(ets_summary["ets_cost_tl"]))

        st.divider()
        st.caption("Audit / PDF")
        c1, c2 = st.columns(2)

        with c1:
            if st.button("ETS audit log yaz (runs.jsonl)", type="secondary"):
                record = {
                    "ts_utc": now_iso(),
                    "app_version": APP_VERSION,
                    "inputs": {
                        "eua_price_eur_per_tco2": float(eua_price),
                        "fx_tl_per_eur": float(fx),
                        "has_energy_csv": True,
                        "has_production_csv": production_file is not None,
                    },
                    "energy_summary": energy_summary,
                    "ets_summary": ets_summary,  # NEW
                    "cbam_totals": None,
                }
                append_audit_log(record)
                st.success("Audit log yazıldı: runs.jsonl")

        with c2:
            if PDF_AVAILABLE:
                if st.button("ETS PDF raporu üret", type="primary"):
                    pdf_bytes = build_pdf_report(
                        eua_price=float(eua_price),
                        fx=float(fx),
                        energy_summary=energy_summary,
                        ets_summary=ets_summary,
                        cbam_df=None,
                        cbam_totals=None,
                    )
                    st.download_button(
                        "PDF indir",
                        data=pdf_bytes,
                        file_name="cme_report_ets.pdf",
                        mime="application/pdf",
                    )
            else:
                st.warning("PDF üretimi için reportlab gerekli. requirements.txt içine 'reportlab' ekleyin.")

st.divider()
st.caption("Demo notu: CBAM ve ETS hesapları yönetim amaçlı tahmini çıktılardır; resmî beyan değildir.")
