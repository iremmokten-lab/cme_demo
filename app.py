import streamlit as st
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timezone
from io import BytesIO, StringIO
import zipfile

# -----------------------------
# Optional PDF (ReportLab)
# -----------------------------
PDF_AVAILABLE = True
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import cm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
except Exception:
    PDF_AVAILABLE = False

# -----------------------------
# App Config
# -----------------------------
st.set_page_config(page_title="CME Demo — CBAM + ETS", layout="wide")
APP_VERSION = "final-2.0-export-demo-dashboard-font"
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

def append_audit_log(record: dict):
    line = json.dumps(record, ensure_ascii=False)
    with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def validate_required_columns(df: pd.DataFrame, required: list, df_name: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"'{df_name}' dosyasında eksik kolon(lar): " + ", ".join(missing))

def validate_nonnegative(df: pd.DataFrame, cols: list, df_name: str):
    problems = []
    for c in cols:
        if c not in df.columns:
            continue
        s = df[c].apply(safe_float)
        bad = s.isna() | (s < 0)
        if bad.any():
            i = int(np.where(bad.to_numpy())[0][0])
            excel_row = i + 2
            val = df.iloc[i][c]
            problems.append(f"{df_name}: kolon '{c}' satır {excel_row} geçersiz (boş/negatif): {val}")
    if problems:
        raise ValueError(" | ".join(problems))

def read_csv_uploaded(uploaded_file) -> pd.DataFrame:
    return pd.read_csv(uploaded_file)

def parse_csv_string(s: str) -> pd.DataFrame:
    return pd.read_csv(StringIO(s))

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

    validate_nonnegative(df, ["activity_amount", "emission_factor_kgco2_per_unit"], "energy.csv")

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

    summary = {
        "total_kgco2": total_kg,
        "scope1_kgco2": scope1_kg,
        "scope2_kgco2": scope2_kg,
        "total_tco2": kg_to_t(total_kg),
        "scope1_tco2": kg_to_t(scope1_kg),
        "scope2_tco2": kg_to_t(scope2_kg),
    }
    return df, summary

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

    if "cbam_covered" not in df.columns:
        df["cbam_covered"] = 1

    df["quantity"] = df["quantity"].apply(safe_float)
    df["export_to_eu_quantity"] = df["export_to_eu_quantity"].apply(safe_float)
    df["input_emission_factor_kg_per_unit"] = df["input_emission_factor_kg_per_unit"].apply(safe_float)
    df["cbam_covered"] = df["cbam_covered"].apply(safe_float)

    validate_nonnegative(df, ["quantity", "export_to_eu_quantity", "input_emission_factor_kg_per_unit"], "production.csv")

    bad_cov = ~df["cbam_covered"].fillna(1).isin([0, 1])
    if bad_cov.any():
        i = int(np.where(bad_cov.to_numpy())[0][0])
        excel_row = i + 2
        raise ValueError(f"production.csv: kolon 'cbam_covered' satır {excel_row} sadece 0 veya 1 olmalı.")
    df["cbam_covered"] = df["cbam_covered"].fillna(1).astype(int)

    df = allocate_energy_to_skus(df, float(total_energy_kgco2))

    df["export_to_eu_quantity_for_cbam"] = np.where(df["cbam_covered"] == 1, df["export_to_eu_quantity"], 0.0)

    export_gt_qty = df[(df["cbam_covered"] == 1) & (df["export_to_eu_quantity"] > df["quantity"]) & (df["quantity"] > 0)]
    warning = None
    if len(export_gt_qty):
        warning = (
            "Uyarı: Bazı satırlarda export_to_eu_quantity, quantity değerinden büyük. "
            "Lütfen production.csv dosyanı kontrol et."
        )

    df["total_emission_factor_kg_per_unit"] = df["alloc_energy_kgco2_per_unit"] + df["input_emission_factor_kg_per_unit"]
    df["cbam_embedded_emissions_kgco2"] = df["export_to_eu_quantity_for_cbam"] * df["total_emission_factor_kg_per_unit"]
    df["cbam_embedded_emissions_tco2"] = df["cbam_embedded_emissions_kgco2"].apply(kg_to_t)
    df["cbam_cost_eur"] = df["cbam_embedded_emissions_tco2"] * float(eua_price_eur_per_t)

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
# PDF with bundled font (DejaVuSans.ttf)
# -----------------------------
def register_pdf_font():
    """
    Uses DejaVuSans.ttf if present at repo root.
    """
    if not PDF_AVAILABLE:
        return "Helvetica"
    font_path = "DejaVuSans.ttf"
    if os.path.exists(font_path):
        try:
            pdfmetrics.registerFont(TTFont("DejaVuSans", font_path))
            return "DejaVuSans"
        except Exception:
            return "Helvetica"
    return "Helvetica"

PDF_FONT = register_pdf_font()

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

    draw("CME Demo Raporu — CBAM + ETS (Tahmini)", dy=20, bold=True, size=14)
    draw(f"Tarih (UTC): {now_iso()}", dy=16)
    draw(f"Uygulama sürümü: {APP_VERSION}", dy=18)

    setfont(size=9)
    c.drawString(x0, y, DISCLAIMER_TEXT)
    y -= 22

    draw("Genel Parametreler", dy=16, bold=True, size=11)
    draw(f"EUA price: {format_eur(eua_price)} / tCO2", dy=14)
    draw(f"FX: {fx:.2f} TL / €", dy=18)

    draw("Upstream Girdi (Bildirime Yardımcı Not)", dy=16, bold=True, size=11)
    draw(f"Input emission kaynağı: {upstream_source}", dy=14)
    draw(f"Kaynak notu: {upstream_note if upstream_note.strip() else '-'}", dy=18)

    draw("Energy (Scope 1–2) Özeti", dy=16, bold=True, size=11)
    draw(f"Total: {energy_summary['total_tco2']:.4f} tCO2", dy=14)
    draw(f"Scope 1: {energy_summary['scope1_tco2']:.4f} tCO2", dy=14)
    draw(f"Scope 2: {energy_summary['scope2_tco2']:.4f} tCO2", dy=18)

    draw("ETS Özeti (Tesis Bazlı)", dy=16, bold=True, size=11)
    draw(f"Scope 1 total: {ets_summary['scope1_tco2']:.4f} tCO2", dy=14)
    draw(f"Free allocation: {ets_summary['free_allocation_tco2']:.4f} tCO2", dy=14)
    draw(f"Banked allowances (ops.): {ets_summary['banked_allowances_tco2']:.4f} tCO2", dy=14)
    draw(f"Net EUA requirement: {ets_summary['net_eua_requirement_tco2']:.4f} tCO2", dy=14)
    draw(f"ETS cost (TL): {format_tl(ets_summary['ets_cost_tl'])}", dy=18)

    draw("CBAM Özeti (Ürün Bazlı, Demo)", dy=16, bold=True, size=11)
    if cbam_df is None or cbam_totals is None or len(cbam_df) == 0:
        draw("CBAM hesaplaması için production.csv yüklenmedi veya boş.", dy=14)
    else:
        draw(f"Kapsamdaki SKU sayısı: {cbam_totals.get('covered_sku_count', 0)}", dy=14)
        draw(f"CBAM dışı SKU sayısı: {cbam_totals.get('not_covered_sku_count', 0)}", dy=14)
        draw(f"Toplam embedded: {cbam_totals['total_cbam_embedded_tco2']:.4f} tCO2", dy=14)
        draw(f"Toplam CBAM maliyeti: {format_eur(cbam_totals['total_cbam_cost_eur'])}", dy=18)

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
# Export helpers (ADIM 6)
# -----------------------------
def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def build_excel_bytes(sheets: dict) -> bytes:
    """
    sheets: dict of {sheet_name: dataframe}
    Requires openpyxl in requirements.txt
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in sheets.items():
            safe_name = name[:31]  # Excel sheet name limit
            df.to_excel(writer, sheet_name=safe_name, index=False)
    output.seek(0)
    return output.read()

def build_zip_package(files: dict) -> bytes:
    """
    files: dict of {filename: bytes}
    """
    out = BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for fname, data in files.items():
            z.writestr(fname, data)
    out.seek(0)
    return out.read()

# -----------------------------
# UI
# -----------------------------
st.title("CME Demo — CBAM (Ürün) + ETS (Tesis)")

# Session defaults
if "demo_mode" not in st.session_state:
    st.session_state["demo_mode"] = False
if "free_allocation" not in st.session_state:
    st.session_state["free_allocation"] = 0.0
if "banked_allowances" not in st.session_state:
    st.session_state["banked_allowances"] = 0.0

with st.sidebar:
    st.subheader("Genel Parametreler")
    eua_price = st.slider("EUA price (€/tCO2)", min_value=0.0, max_value=200.0, value=80.0, step=1.0)
    fx = st.number_input("FX (TL/€)", min_value=0.0, value=35.0, step=0.5)

    st.divider()
    st.subheader("Upstream girdi güven notu")
    upstream_source = st.selectbox("Input emission kaynağı", ["Supplier", "Average DB", "Estimate"])
    upstream_note = st.text_input("Kaynak notu (kısa)", value="")

    st.divider()
    st.subheader("Tek tık demo (ADIM 7)")
    cdm1, cdm2 = st.columns(2)
    with cdm1:
        if st.button("Demo veriyi yükle", type="primary"):
            st.session_state["demo_mode"] = True
            st.success("Demo veri aktif. (CSV yüklemeden çalışır)")
    with cdm2:
        if st.button("Demo kapat"):
            st.session_state["demo_mode"] = False
            st.info("Demo kapandı. (CSV yükleyerek çalışır)")

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
    st.caption("Dosyalar (Demo kapalıysa kullan)")
    energy_file = st.file_uploader("energy.csv yükle", type=["csv"], key="energy_csv")
    production_file = st.file_uploader("production.csv yükle (CBAM için)", type=["csv"], key="production_csv")

tab_dash, tab_cbam, tab_ets = st.tabs(["Dashboard (ADIM 8)", "CBAM (ürün bazlı)", "ETS (tesis bazlı)"])

# -----------------------------
# Load data (demo vs upload)
# -----------------------------
energy_error = None
prod_error = None
energy_df = None
prod_df = None

if st.session_state["demo_mode"]:
    try:
        energy_df = parse_csv_string(ENERGY_DEMO_CSV)
        prod_df = parse_csv_string(PRODUCTION_DEMO_CSV)
    except Exception as e:
        energy_error = f"Demo yüklenemedi: {e}"
else:
    if energy_file is not None:
        try:
            energy_df = read_csv_uploaded(energy_file)
        except Exception as e:
            energy_error = f"energy.csv okunamadı: {e}"
    if production_file is not None:
        try:
            prod_df = read_csv_uploaded(production_file)
        except Exception as e:
            prod_error = f"production.csv okunamadı: {e}"

# Compute energy
energy_calc_df = None
energy_summary = {
    "total_kgco2": 0.0, "scope1_kgco2": 0.0, "scope2_kgco2": 0.0,
    "total_tco2": 0.0, "scope1_tco2": 0.0, "scope2_tco2": 0.0,
}
if energy_df is not None and energy_error is None:
    try:
        energy_calc_df, energy_summary = compute_energy_emissions(energy_df)
    except Exception as e:
        energy_error = str(e)

# ETS
ets_summary = compute_ets(
    scope1_tco2=float(energy_summary["scope1_tco2"]),
    free_allocation_tco2=float(st.session_state["free_allocation"]),
    banked_tco2=float(st.session_state["banked_allowances"]),
    eua_price=float(eua_price),
    fx_tl_per_eur=float(fx),
)

# CBAM (if production available)
cbam_df = None
cbam_totals = None
cbam_warning = None
if prod_df is not None and energy_error is None:
    try:
        cbam_df, cbam_totals, cbam_warning = compute_cbam(
            prod_df,
            eua_price_eur_per_t=float(eua_price),
            total_energy_kgco2=float(energy_summary["total_kgco2"]),
        )
    except Exception as e:
        prod_error = str(e)

# -----------------------------
# DASHBOARD (ADIM 8)
# -----------------------------
with tab_dash:
    st.subheader("Dashboard")

    if energy_error:
        st.error(f"Energy hatası: {energy_error}")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Energy total (tCO2)", f"{energy_summary['total_tco2']:.4f}")
        c2.metric("Scope 1 (tCO2)", f"{energy_summary['scope1_tco2']:.4f}")
        if cbam_totals:
            c3.metric("CBAM embedded (tCO2)", f"{cbam_totals['total_cbam_embedded_tco2']:.4f}")
            c4.metric("CBAM €", f"{cbam_totals['total_cbam_cost_eur']:.2f}")
        else:
            c3.metric("CBAM embedded (tCO2)", "-")
            c4.metric("CBAM €", "-")

        st.divider()
        c5, c6, c7 = st.columns(3)
        c5.metric("ETS Net EUA (tCO2)", f"{ets_summary['net_eua_requirement_tco2']:.4f}")
        c6.metric("ETS Cost (TL)", format_tl(ets_summary["ets_cost_tl"]))
        c7.metric("FX (TL/€)", f"{fx:.2f}")

        st.caption("Scope kırılımı (tCO2)")
        chart_df = pd.DataFrame({
            "Scope": ["Scope 1", "Scope 2"],
            "tCO2": [energy_summary["scope1_tco2"], energy_summary["scope2_tco2"]]
        })
        st.bar_chart(chart_df.set_index("Scope"))

    st.divider()
    st.subheader("Export (ADIM 6) — Tek pakette indir")
    st.write("CBAM sonuçları, ETS özeti ve energy hesap tablosunu **CSV / Excel / ZIP** olarak indirebilirsin.")

    # Prepare export dataframes
    ets_df = pd.DataFrame([ets_summary])
    meta_df = pd.DataFrame([{
        "ts_utc": now_iso(),
        "app_version": APP_VERSION,
        "eua_price_eur_per_tco2": float(eua_price),
        "fx_tl_per_eur": float(fx),
        "upstream_source": upstream_source,
        "upstream_note": upstream_note,
        "demo_mode": bool(st.session_state["demo_mode"]),
    }])

    # CSV download buttons
    colx1, colx2, colx3 = st.columns(3)
    with colx1:
        if energy_calc_df is not None:
            st.download_button("Energy hesap (CSV)", df_to_csv_bytes(energy_calc_df), "energy_calculated.csv", "text/csv")
        st.download_button("ETS summary (CSV)", df_to_csv_bytes(ets_df), "ets_summary.csv", "text/csv")
    with colx2:
        if cbam_df is not None:
            st.download_button("CBAM results (CSV)", df_to_csv_bytes(cbam_df), "cbam_results.csv", "text/csv")
        st.download_button("Run meta (CSV)", df_to_csv_bytes(meta_df), "run_meta.csv", "text/csv")
    with colx3:
        # Excel
        try:
            sheets = {
                "run_meta": meta_df,
                "ets_summary": ets_df,
            }
            if energy_calc_df is not None:
                sheets["energy_calculated"] = energy_calc_df
            if cbam_df is not None:
                sheets["cbam_results"] = cbam_df

            excel_bytes = build_excel_bytes(sheets)
            st.download_button(
                "Excel indir (xlsx)",
                data=excel_bytes,
                file_name="cme_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            # ZIP package
            files = {
                "run_meta.csv": df_to_csv_bytes(meta_df),
                "ets_summary.csv": df_to_csv_bytes(ets_df),
            }
            if energy_calc_df is not None:
                files["energy_calculated.csv"] = df_to_csv_bytes(energy_calc_df)
            if cbam_df is not None:
                files["cbam_results.csv"] = df_to_csv_bytes(cbam_df)
            files["cme_export.xlsx"] = excel_bytes
            zip_bytes = build_zip_package(files)

            st.download_button(
                "Hepsini indir (ZIP)",
                data=zip_bytes,
                file_name="cme_export_package.zip",
                mime="application/zip",
            )
        except Exception as e:
            st.warning(f"Excel/ZIP export için requirements.txt içine openpyxl ekli olmalı. Hata: {e}")

# -----------------------------
# CBAM TAB
# -----------------------------
with tab_cbam:
    st.subheader("CBAM — Ürün Bazlı (Demo)")
    if energy_error:
        st.error(f"energy.csv hatası: {energy_error}")
    elif prod_error:
        st.error(f"production.csv hatası: {prod_error}")
    else:
        if cbam_warning:
            st.warning(cbam_warning)

        if cbam_df is None:
            st.info("CBAM hesaplamak için production.csv yükleyin (veya Demo veriyi açın).")
        else:
            st.caption(f"CBAM covered: {cbam_totals['covered_sku_count']} | CBAM dışı: {cbam_totals['not_covered_sku_count']}")
            c1, c2 = st.columns(2)
            c1.metric("Toplam CBAM embedded (tCO2)", f"{cbam_totals['total_cbam_embedded_tco2']:.4f}")
            c2.metric("Toplam CBAM maliyeti (EUR)", f"{cbam_totals['total_cbam_cost_eur']:.2f}")

            show_cols = [
                "sku","cbam_covered","quantity","export_to_eu_quantity","export_to_eu_quantity_for_cbam",
                "alloc_energy_kgco2_per_unit","input_emission_factor_kg_per_unit","total_emission_factor_kg_per_unit",
                "cbam_embedded_emissions_tco2","cbam_cost_eur","risk_score_0_100",
            ]
            existing_cols = [c for c in show_cols if c in cbam_df.columns]
            cbam_sorted = cbam_df.sort_values("risk_score_0_100", ascending=False)
            st.dataframe(cbam_sorted[existing_cols], use_container_width=True, height=420)

            st.divider()
            st.subheader("Rapor / Audit")
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
                            "demo_mode": bool(st.session_state["demo_mode"]),
                        },
                        "energy_summary": energy_summary,
                        "ets_summary": ets_summary,
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
                            ets_summary=ets_summary,
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
                    st.warning("PDF üretimi için reportlab gerekli. requirements.txt içinde reportlab olmalı.")

# -----------------------------
# ETS TAB
# -----------------------------
with tab_ets:
    st.subheader("ETS — Tesis Bazlı (Türkiye ETS Demo)")
    if energy_error:
        st.error(f"energy.csv hatası: {energy_error}")
    else:
        left, right = st.columns([1,1])
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
        ets_summary_live = compute_ets(
            scope1_tco2=float(energy_summary["scope1_tco2"]),
            free_allocation_tco2=float(st.session_state["free_allocation"]),
            banked_tco2=float(st.session_state["banked_allowances"]),
            eua_price=float(eua_price),
            fx_tl_per_eur=float(fx),
        )
        with right:
            st.markdown("### Çıktılar")
            st.metric("Scope 1 total (tCO2)", f"{ets_summary_live['scope1_tco2']:.4f}")
            st.metric("Net EUA requirement (tCO2)", f"{ets_summary_live['net_eua_requirement_tco2']:.4f}")
            st.metric("ETS cost (TL)", format_tl(ets_summary_live["ets_cost_tl"]))

st.divider()
st.caption("Demo notu: CBAM ve ETS hesapları yönetim amaçlı tahmini çıktılardır; resmî beyan değildir.")
