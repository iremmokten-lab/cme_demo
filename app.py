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

    # Try better Unicode font (Turkish chars)
    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        _FONT_REGISTERED = False
        for p in [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf",
        ]:
            if os.path.exists(p):
                pdfmetrics.registerFont(TTFont("DejaVuSans", p))
                _FONT_REGISTERED = True
                break
        PDF_FONT = "DejaVuSans" if _FONT_REGISTERED else "Helvetica"
    except Exception:
        PDF_FONT = "Helvetica"

except Exception:
    PDF_AVAILABLE = False
    PDF_FONT = "Helvetica"

# -----------------------------
# App Config
# -----------------------------
st.set_page_config(page_title="CME Demo — CBAM + ETS", layout="wide")

APP_VERSION = "final-1.0-cbam-ets"
AUDIT_LOG_PATH = "runs.jsonl"

DISCLAIMER_TEXT = (
    "Önemli Not: Bu rapor yönetim amaçlı tahmini bir allocation/hesaplama çıktısıdır. "
    "Resmî beyan/uyum dokümanı değildir."
)

# -----------------------------
# CSV Templates / Demo Data
# -----------------------------
ENERGY_TEMPLATE_CSV = """energy_carrier,scope,activity_amount,emission_factor_kgco2_per_unit
electricity,2,,
natural_gas,1,,
diesel,1,,
"""

PRODUCTION_TEMPLATE_CSV = """sku,quantity,export_to_eu_quantity,input_emission_factor_kg_per_unit,cbam_covered
SKU-1,,,,1
SKU-2,,,,1
SKU-3,,,,0
"""

ENERGY_DEMO_CSV = """energy_carrier,scope,activity_amount,emission_factor_kgco2_per_unit
natural_gas,1,1000,2.00
diesel,1,200,2.68
electricity,2,5000,0.40
electricity,2,2000,0.35
"""

PRODUCTION_DEMO_CSV = """sku,quantity,export_to_eu_quantity,input_emission_factor_kg_per_unit,cbam_covered
SKU-A,1000,200,1.20,1
SKU-B,500,50,0.80,1
SKU-C,200,0,2.00,0
SKU-D,300,100,1.50,1
"""

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

def append_audit_log(record: dict):
    line = json.dumps(record, ensure_ascii=False)
    with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def kg_to_t(x_kg):
    return float(x_kg) / 1000.0

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

def missing_columns(df: pd.DataFrame, required: list) -> list:
    return [c for c in required if c not in df.columns]

def validate_required_columns(df: pd.DataFrame, required: list, df_name: str):
    miss = missing_columns(df, required)
    if miss:
        raise ValueError(f"'{df_name}' dosyasında eksik kolon(lar): " + ", ".join(miss))

def validate_nonnegative(df: pd.DataFrame, cols: list, df_name: str):
    """
    Finds first invalid row for each col:
      - missing (NaN) OR negative
    Reports with row number (1-based excluding header).
    """
    problems = []
    for c in cols:
        if c not in df.columns:
            continue
        s = df[c].apply(safe_float)
        bad = s.isna() | (s < 0)
        if bad.any():
            i = int(np.where(bad.to_numpy())[0][0])
            excel_row = i + 2  # header=1, first data row=2
            val = df.iloc[i][c]
            problems.append(f"{df_name}: kolon '{c}' satır {excel_row} geçersiz (boş/negatif): {val}")
    if problems:
        raise ValueError(" | ".join(problems))

# -----------------------------
# Calculations
# -----------------------------
def compute_energy_emissions(energy_df: pd.DataFrame):
    required = ["energy_carrier", "scope", "activity_amount", "emission_factor_kgco2_per_unit"]
    validate_required_columns(energy_df, required, "energy.csv")

    df = energy_df.copy()
    df["scope"] = df["scope"].apply(safe_float)
    df["activity_amount"] = df["activity_amount"].apply(safe_float)
    df["emission_factor_kgco2_per_unit"] = df["emission_factor_kgco2_per_unit"].apply(safe_float)

    # validations
    validate_nonnegative(df, ["activity_amount", "emission_factor_kgco2_per_unit"], "energy.csv")
    # scope must be 1 or 2
    bad_scope = ~df["scope"].isin([1, 2])
    if bad_scope.any():
        i = int(np.where(bad_scope.to_numpy())[0][0])
        excel_row = i + 2
        raise ValueError(f"energy.csv: kolon 'scope' satır {excel_row} sadece 1 veya 2 olmalı.")

    df["scope"] = df["scope"].astype(int)
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
    validate_required_columns(production_df, ["sku", "quantity"], "production.csv")
    df = production_df.copy()
    df["quantity"] = df["quantity"].apply(safe_float)

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
        0.0
    )
    return df

def compute_cbam(production_df: pd.DataFrame, eua_price_eur_per_t: float, total_energy_kgco2: float):
    required = ["sku", "quantity", "export_to_eu_quantity", "input_emission_factor_kg_per_unit"]
    validate_required_columns(production_df, required, "production.csv")

    df = production_df.copy()

    # Optional cbam_covered
    if "cbam_covered" not in df.columns:
        df["cbam_covered"] = 1

    # Normalize types
    df["quantity"] = df["quantity"].apply(safe_float)
    df["export_to_eu_quantity"] = df["export_to_eu_quantity"].apply(safe_float)
    df["input_emission_factor_kg_per_unit"] = df["input_emission_factor_kg_per_unit"].apply(safe_float)
    df["cbam_covered"] = df["cbam_covered"].apply(safe_float)

    # Validations (Step 5)
    validate_nonnegative(df, ["quantity", "export_to_eu_quantity", "input_emission_factor_kg_per_unit"], "production.csv")

    # cbam_covered must be 0/1 (if provided)
    bad_cov = ~df["cbam_covered"].fillna(1).isin([0, 1])
    if bad_cov.any():
        i = int(np.where(bad_cov.to_numpy())[0][0])
        excel_row = i + 2
        raise ValueError(f"production.csv: kolon 'cbam_covered' satır {excel_row} sadece 0 veya 1 olmalı.")
    df["cbam_covered"] = df["cbam_covered"].fillna(1).astype(int)

    # Allocation always computed (demo), but CBAM only for covered
    df = allocate_energy_to_skus(df, float(total_energy_kgco2))

    # If not covered, force export=0 for CBAM calculation
    df["export_to_eu_quantity_for_cbam"] = np.where(df["cbam_covered"] == 1, df["export_to_eu_quantity"], 0.0)

    # Keep existing warning: export > quantity (only for covered rows, more meaningful)
    export_gt_qty = df[(df["cbam_covered"] == 1) & (df["export_to_eu_quantity"] > df["quantity"]) & (df["quantity"] > 0)]
    warning = None
    if len(export_gt_qty):
        warning = (
            "Uyarı: Bazı satırlarda export_to_eu_quantity, quantity değerinden büyük. "
            "Lütfen production.csv dosyanı kontrol et."
        )

    # Embedded emissions
    df["total_emission_factor_kg_per_unit"] = df["alloc_energy_kgco2_per_unit"] + df["input_emission_factor_kg_per_unit"]
    df["cbam_embedded_emissions_kgco2"] = df["export_to_eu_quantity_for_cbam"] * df["total_emission_factor_kg_per_unit"]
    df["cbam_embedded_emissions_tco2"] = df["cbam_embedded_emissions_kgco2"].apply(kg_to_t)
    df["cbam_cost_eur"] = df["cbam_embedded_emissions_tco2"] * float(eua_price_eur_per_t)

    # Risk score (covered focus)
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
        "covered_sku_count": int((df["cbam_covered"] == 1).sum()) if len(df) else 0,
        "not_covered_sku_count": int((df["cbam_covered"] == 0).sum()) if len(df) else 0,
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
    upstream_source: str,
    upstream_note: str,
    cbam_df: pd.DataFrame | None,
    cbam_totals: dict | None
) -> bytes:
    if not PDF_AVAILABLE:
        raise RuntimeError("PDF üretimi için reportlab gerekli.")

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    x0 = 2 * cm
    y = height - 2 * cm

    def setfont(bold=False, size=10):
        if PDF_FONT == "DejaVuSans":
            c.setFont("DejaVuSans", size)
        else:
            c.setFont("Helvetica-Bold" if bold else "Helvetica", size)

    def draw(text, dy=14, bold=False, size=10):
        nonlocal y
        setfont(bold=bold, size=size)
        c.drawString(x0, y, str(text))
        y -= dy

    # Title
    draw("CME Demo Raporu — CBAM + ETS (Tahmini)", dy=20, bold=True, size=14)
    draw(f"Tarih (UTC): {now_iso()}", dy=16, size=10)
    draw(f"Uygulama sürümü: {APP_VERSION}", dy=18, size=10)

    # Disclaimer
    setfont(size=9)
    c.drawString(x0, y, DISCLAIMER_TEXT)
    y -= 22

    # Params
    draw("Genel Parametreler", dy=16, bold=True, size=11)
    draw(f"EUA price: {format_eur(eua_price)} / tCO2", dy=14)
    draw(f"FX: {fx:.2f} TL / €", dy=18)

    # Upstream note (Step 3)
    draw("Upstream Girdi (Bildirime Yardımcı Not)", dy=16, bold=True, size=11)
    draw(f"Input emission kaynağı: {upstream_source}", dy=14)
    draw(f"Kaynak notu: {upstream_note if upstream_note.strip() else '-'}", dy=18)

    # Energy
    draw("Energy (Scope 1–2) Özeti", dy=16, bold=True, size=11)
    draw(f"Total: {energy_summary['total_tco2']:.4f} tCO2", dy=14)
    draw(f"Scope 1: {energy_summary['scope1_tco2']:.4f} tCO2", dy=14)
    draw(f"Scope 2: {energy_summary['scope2_tco2']:.4f} tCO2", dy=18)

    # ETS
    draw("ETS Özeti (Tesis Bazlı)", dy=16, bold=True, size=11)
    draw(f"Scope 1 total: {ets_summary['scope1_tco2']:.4f} tCO2", dy=14)
    draw(f"Free allocation: {ets_summary['free_allocation_tco2']:.4f} tCO2", dy=14)
    draw(f"Banked allowances (ops.): {ets_summary['banked_allowances_tco2']:.4f} tCO2", dy=14)
    draw(f"Net EUA requirement: {ets_summary['net_eua_requirement_tco2']:.4f} tCO2", dy=14)
    draw(f"ETS cost (TL): {format_tl(ets_summary['ets_cost_tl'])}", dy=18)

    # CBAM
    draw("CBAM Özeti (Ürün Bazlı, Demo)", dy=16, bold=True, size=11)
    if cbam_df is None or cbam_totals is None or len(cbam_df) == 0:
        draw("CBAM hesaplaması için production.csv yüklenmedi veya boş.", dy=14)
    else:
        draw(f"Kapsamdaki SKU sayısı: {cbam_totals.get('covered_sku_count', 0)}", dy=14)
        draw(f"CBAM dışı SKU sayısı: {cbam_totals.get('not_covered_sku_count', 0)}", dy=14)
        draw(f"Toplam embedded: {cbam_totals['total_cbam_embedded_tco2']:.4f} tCO2", dy=14)
        draw(f"Toplam CBAM maliyeti: {format_eur(cbam_totals['total_cbam_cost_eur'])}", dy=18)

        # Top 10 risk
        top = cbam_df.sort_values("risk_score_0_100", ascending=False).head(10)

        if y < 6 * cm:
            c.showPage()
            y = height - 2 * cm

        draw("En Yüksek Risk Skorlu İlk 10 SKU", dy=14, bold=True, size=10)
        setfont(size=9)
        c.drawString(x0, y, "SKU | CBAM covered | Risk(0-100) | EU tCO2 | CBAM €")
        y -= 12

        for _, r in top.iterrows():
            txt = (
                f"{r['sku']} | {int(r.get('cbam_covered', 1))} | {r['risk_score_0_100']} | "
                f"{r['cbam_embedded_emissions_tco2']:.4f} | {r['cbam_cost_eur']:.2f}"
            )
            setfont(size=9)
            c.drawString(x0, y, txt[:120])
            y -= 11
            if y < 3 * cm:
                c.showPage()
                y = height - 2 * cm

    # Footer
    if y < 3 * cm:
        c.showPage()
        y = height - 2 * cm
    y -= 10
    setfont(size=9)
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
    st.subheader("Upstream girdi güven notu")
    upstream_source = st.selectbox("Input emission kaynağı", ["Supplier", "Average DB", "Estimate"])
    upstream_note = st.text_input("Kaynak notu (kısa)", value="")

    st.divider()
    st.subheader("CSV indir (Template / Demo)")
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("energy template", ENERGY_TEMPLATE_CSV.encode("utf-8"), file_name="energy_template.csv", mime="text/csv")
        st.download_button("production template", PRODUCTION_TEMPLATE_CSV.encode("utf-8"), file_name="production_template.csv", mime="text/csv")
    with c2:
        st.download_button("energy demo", ENERGY_DEMO_CSV.encode("utf-8"), file_name="energy_demo.csv", mime="text/csv")
        st.download_button("production demo", PRODUCTION_DEMO_CSV.encode("utf-8"), file_name="production_demo.csv", mime="text/csv")

    st.divider()
    st.caption("Dosyalar")
    energy_file = st.file_uploader("energy.csv yükle", type=["csv"], key="energy_csv")
    production_file = st.file_uploader("production.csv yükle (CBAM için)", type=["csv"], key="production_csv")

tab_cbam, tab_ets = st.tabs(["CBAM (ürün bazlı)", "ETS (tesis bazlı)"])

# Load energy
energy_calc_df = None
energy_summary = {
    "total_kgco2": 0.0,
    "scope1_kgco2": 0.0,
    "scope2_kgco2": 0.0,
    "total_tco2": 0.0,
    "scope1_tco2": 0.0,
    "scope2_tco2": 0.0,
}
energy_error = None

if energy_file is not None:
    try:
        energy_df = read_csv_uploaded(energy_file)
        energy_calc_df, energy_summary = compute_energy_emissions(energy_df)
    except Exception as e:
        energy_error = str(e)

# ETS inputs (store in session so PDF from CBAM tab also consistent)
if "free_allocation" not in st.session_state:
    st.session_state["free_allocation"] = 0.0
if "banked_allowances" not in st.session_state:
    st.session_state["banked_allowances"] = 0.0

# -----------------------------
# CBAM TAB
# -----------------------------
with tab_cbam:
    st.subheader("CBAM — Ürün Bazlı (Demo)")
    st.write(
        "Akış: **energy.csv (Scope 1–2)** toplam emisyonu → SKU’lara **quantity oranında dağıtım** → "
        "EU export için embedded emisyon → CBAM maliyeti (EUA fiyatı ile)."
    )

    if energy_file is None:
        st.info("CBAM için önce **energy.csv** yükleyin. (Template/Demo butonlarını sol menüden indirebilirsin.)")
    elif energy_error:
        st.error(f"energy.csv hatası: {energy_error}")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Energy total (tCO2)", f"{energy_summary['total_tco2']:.4f}")
        c2.metric("Scope 1 (tCO2)", f"{energy_summary['scope1_tco2']:.4f}")
        c3.metric("Scope 2 (tCO2)", f"{energy_summary['scope2_tco2']:.4f}")

        st.caption("Energy detay (ilk 20 satır)")
        st.dataframe(energy_calc_df.head(20), use_container_width=True)

        st.divider()
        if production_file is None:
            st.info("CBAM hesaplamak için **production.csv** yükleyin. (Template/Demo butonları sol menüde.)")
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

                # Covered summary
                st.caption(f"CBAM covered: {cbam_totals['covered_sku_count']} SKU | CBAM dışı: {cbam_totals['not_covered_sku_count']} SKU")

                c1, c2 = st.columns(2)
                c1.metric("Toplam CBAM embedded (tCO2)", f"{cbam_totals['total_cbam_embedded_tco2']:.4f}")
                c2.metric("Toplam CBAM maliyeti (EUR)", f"{cbam_totals['total_cbam_cost_eur']:.2f}")

                st.subheader("SKU Risk Sıralaması")
                show_cols = [
                    "sku",
                    "cbam_covered",
                    "quantity",
                    "export_to_eu_quantity",
                    "export_to_eu_quantity_for_cbam",
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

                out_csv = cbam_sorted.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "CBAM sonuçlarını indir (CSV)",
                    data=out_csv,
                    file_name="cbam_results.csv",
                    mime="text/csv",
                )

                st.divider()
                st.subheader("Rapor / Audit")

                # PDF uses ETS values from session
                ets_summary_for_pdf = compute_ets(
                    scope1_tco2=float(energy_summary["scope1_tco2"]),
                    free_allocation_tco2=float(st.session_state["free_allocation"]),
                    banked_tco2=float(st.session_state["banked_allowances"]),
                    eua_price=float(eua_price),
                    fx_tl_per_eur=float(fx),
                )

                colA, colB = st.columns(2)
                with colA:
                    if st.button("Audit log yaz (runs.jsonl)", type="secondary", key="audit_cbam"):
                        record = {
                            "ts_utc": now_iso(),
                            "app_version": APP_VERSION,
                            "inputs": {
                                "eua_price_eur_per_tco2": float(eua_price),
                                "fx_tl_per_eur": float(fx),
                                "upstream_source": upstream_source,
                                "upstream_note": upstream_note,
                                "ets_free_allocation_tco2": float(st.session_state["free_allocation"]),
                                "ets_banked_allowances_tco2": float(st.session_state["banked_allowances"]),
                                "has_energy_csv": energy_file is not None,
                                "has_production_csv": production_file is not None,
                            },
                            "energy_summary": energy_summary,
                            "ets_summary": ets_summary_for_pdf,
                            "cbam_totals": cbam_totals,
                        }
                        append_audit_log(record)
                        st.success("Audit log yazıldı: runs.jsonl")

                with colB:
                    if PDF_AVAILABLE:
                        if st.button("PDF raporu üret", type="primary", key="pdf_cbam"):
                            pdf_bytes = build_pdf_report(
                                eua_price=float(eua_price),
                                fx=float(fx),
                                energy_summary=energy_summary,
                                ets_summary=ets_summary_for_pdf,
                                upstream_source=upstream_source,
                                upstream_note=upstream_note,
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
                        st.warning("PDF üretimi için reportlab gerekli. requirements.txt içinde 'reportlab' olmalı.")

            except Exception as e:
                st.error(f"production.csv hatası: {e}")

# -----------------------------
# ETS TAB
# -----------------------------
with tab_ets:
    st.subheader("ETS — Tesis Bazlı (Türkiye ETS Demo)")
    st.write("Bu sekme yalnızca **Scope 1** emisyonlarına bakar (energy.csv).")

    if energy_file is None:
        st.info("ETS için önce **energy.csv** yükleyin. (Template/Demo sol menüde.)")
    elif energy_error:
        st.error(f"energy.csv hatası: {energy_error}")
    else:
        left, right = st.columns([1, 1])

        with left:
            st.markdown("### Girdiler")
            st.session_state["free_allocation"] = st.number_input(
                "Free allocation (tCO2)",
                min_value=0.0,
                value=float(st.session_state["free_allocation"]),
                step=100.0,
            )
            st.session_state["banked_allowances"] = st.number_input(
                "Banked allowances (tCO2) (opsiyonel)",
                min_value=0.0,
                value=float(st.session_state["banked_allowances"]),
                step=100.0,
            )

        ets_summary = compute_ets(
            scope1_tco2=float(energy_summary["scope1_tco2"]),
            free_allocation_tco2=float(st.session_state["free_allocation"]),
            banked_tco2=float(st.session_state["banked_allowances"]),
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
            if st.button("ETS audit log yaz (runs.jsonl)", type="secondary", key="audit_ets"):
                record = {
                    "ts_utc": now_iso(),
                    "app_version": APP_VERSION,
                    "inputs": {
                        "eua_price_eur_per_tco2": float(eua_price),
                        "fx_tl_per_eur": float(fx),
                        "upstream_source": upstream_source,
                        "upstream_note": upstream_note,
                        "ets_free_allocation_tco2": float(st.session_state["free_allocation"]),
                        "ets_banked_allowances_tco2": float(st.session_state["banked_allowances"]),
                        "has_energy_csv": True,
                        "has_production_csv": production_file is not None,
                    },
                    "energy_summary": energy_summary,
                    "ets_summary": ets_summary,
                    "cbam_totals": None,
                }
                append_audit_log(record)
                st.success("Audit log yazıldı: runs.jsonl")

        with c2:
            if PDF_AVAILABLE:
                if st.button("ETS PDF raporu üret", type="primary", key="pdf_ets"):
                    pdf_bytes = build_pdf_report(
                        eua_price=float(eua_price),
                        fx=float(fx),
                        energy_summary=energy_summary,
                        ets_summary=ets_summary,
                        upstream_source=upstream_source,
                        upstream_note=upstream_note,
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
                st.warning("PDF üretimi için reportlab gerekli. requirements.txt içinde 'reportlab' olmalı.")

st.divider()
st.caption("Demo notu: CBAM ve ETS hesapları yönetim amaçlı tahmini çıktılardır; resmî beyan değildir.")
