from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

from src.mrv.lineage import sha256_bytes
from src.services.storage import REPORT_DIR

# append_audit repo'da var ama bazı ortamlarda import sorunu olmasın diye güvenli kullanalım
try:
    from src.mrv.audit import append_audit  # type: ignore
except Exception:  # pragma: no cover
    append_audit = None


def _register_fonts() -> bool:
    """
    Repo kökünde DejaVuSans.ttf ve DejaVuSans-Bold.ttf dosyaları beklenir.
    Streamlit Cloud'da font yoksa Helvetica fallback kullanılır.
    """
    try:
        pdfmetrics.registerFont(TTFont("DejaVuSans", "DejaVuSans.ttf"))
        pdfmetrics.registerFont(TTFont("DejaVuSans-Bold", "DejaVuSans-Bold.ttf"))
        return True
    except Exception:
        return False


def _fmt_num(x: Any, digits: int = 2) -> str:
    try:
        s = f"{float(x):,.{digits}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        try:
            return str(x)
        except Exception:
            return "-"


def _ensure_y(c: canvas.Canvas, y: float, font_name: str, font_size: int) -> float:
    if y < 80:
        c.showPage()
        c.setFont(font_name, font_size)
        return 780
    return y


def _wrap_text(
    c: canvas.Canvas,
    text: str,
    x: float,
    y: float,
    max_width: float,
    font_name: str,
    font_size: int,
) -> float:
    """Basit word-wrap."""
    c.setFont(font_name, font_size)
    words = (text or "").split()
    line = ""
    for w in words:
        candidate = (line + " " + w).strip()
        if c.stringWidth(candidate, font_name, font_size) <= max_width:
            line = candidate
            continue
        if line:
            y = _ensure_y(c, y, font_name, font_size)
            c.drawString(x, y, line)
            y -= (font_size + 3)
        line = w
    if line:
        y = _ensure_y(c, y, font_name, font_size)
        c.drawString(x, y, line)
        y -= (font_size + 3)
    return y


def _draw_heading(c: canvas.Canvas, x: float, y: float, title: str, bold_font: str) -> float:
    y = _ensure_y(c, y, bold_font, 14)
    c.setFont(bold_font, 14)
    c.drawString(x, y, title)
    return y - 18


def _draw_kv(c: canvas.Canvas, x: float, y: float, k: str, v: str, body_font: str) -> float:
    y = _ensure_y(c, y, body_font, 11)
    c.setFont(body_font, 11)
    c.drawString(x, y, k)
    c.drawRightString(545, y, v)
    return y - 16


def _draw_bullets(
    c: canvas.Canvas,
    items: List[str],
    x: float,
    y: float,
    max_width: float,
    body_font: str,
    font_size: int = 10,
) -> float:
    for it in items:
        it = str(it or "").strip()
        if not it:
            continue
        y = _ensure_y(c, y, body_font, font_size)
        c.setFont(body_font, font_size)
        c.drawString(x, y, "•")
        y = _wrap_text(c, it, x + 14, y, max_width=max_width - 14, font_name=body_font, font_size=font_size)
        y -= 2
    return y


def build_pdf(snapshot_id: int, report_title: str, report_data: dict) -> tuple[str, str]:
    """
    Paket D4: PDF çıktısı (CBAM + ETS + Data Quality + Methodology)

    Dönüş: (storage_uri, sha256)
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    fp = Path(REPORT_DIR) / f"snapshot_{snapshot_id}.pdf"

    c = canvas.Canvas(str(fp), pagesize=A4)
    has_font = _register_fonts()
    body_font = "DejaVuSans" if has_font else "Helvetica"
    bold_font = "DejaVuSans-Bold" if has_font else "Helvetica-Bold"

    w, h = A4
    margin_x = 50
    y = h - 60

    # Başlık
    c.setFont(bold_font, 18)
    c.drawString(margin_x, y, report_title or "Rapor")
    y -= 26

    # Meta
    c.setFont(body_font, 11)
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    c.drawString(margin_x, y, f"Tarih (UTC): {ts}")
    y -= 16
    c.drawString(margin_x, y, f"Snapshot ID: {snapshot_id}")
    y -= 18

    scenario = (report_data or {}).get("scenario") or {}
    if isinstance(scenario, dict) and scenario.get("name"):
        y = _draw_kv(c, margin_x, y, "Senaryo", str(scenario.get("name")), body_font)
        y -= 4

    # Disclaimer
    note = (
        "Önemli Not: Bu rapor yönetim amaçlı tahmini bir hesaplama çıktısıdır. "
        "CBAM/ETS uyumluluğuna yaklaşmak için tasarlanmış demo bir akıştır. "
        "Resmî beyan/raporlama için kullanılmamalıdır."
    )
    y = _wrap_text(c, note, margin_x, y, max_width=500, font_name=body_font, font_size=10)
    y -= 8

    # KPI
    y = _draw_heading(c, margin_x, y, "KPI Özeti", bold_font)
    kpis = (report_data or {}).get("kpis", {}) or {}

    # Eski anahtarlarla geriye uyumluluk
    direct = kpis.get("direct_tco2", kpis.get("energy_scope1_tco2", 0))
    indirect = kpis.get("indirect_tco2", kpis.get("energy_scope2_tco2", 0))
    total = kpis.get("total_tco2", kpis.get("energy_total_tco2", 0))

    y = _draw_kv(c, margin_x, y, "Direct Emissions (tCO2)", _fmt_num(direct, 3), body_font)
    y = _draw_kv(c, margin_x, y, "Indirect Emissions (tCO2)", _fmt_num(indirect, 3), body_font)
    y = _draw_kv(c, margin_x, y, "Toplam Emisyon (tCO2)", _fmt_num(total, 3), body_font)

    y = _draw_kv(c, margin_x, y, "ETS Net (tCO2)", _fmt_num(kpis.get("ets_net_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "ETS Maliyeti (€)", _fmt_num(kpis.get("ets_cost_eur", 0), 2), body_font)
    y = _draw_kv(c, margin_x, y, "ETS Maliyeti (TL)", _fmt_num(kpis.get("ets_cost_tl", 0), 2), body_font)

    y = _draw_kv(c, margin_x, y, "CBAM Embedded (tCO2)", _fmt_num(kpis.get("cbam_embedded_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "CBAM Maliyeti (€)", _fmt_num(kpis.get("cbam_cost_eur", 0), 2), body_font)
    y -= 8

    # Parametreler
    y = _draw_heading(c, margin_x, y, "Parametreler", bold_font)
    cfg = (report_data or {}).get("config", {}) or {}
    y = _draw_kv(c, margin_x, y, "Bölge/Ülke", str(cfg.get("region", "TR")), body_font)
    y = _draw_kv(c, margin_x, y, "Elektrik Metodu", str(cfg.get("electricity_method", "location")), body_font)
    if cfg.get("market_grid_factor_override"):
        y = _draw_kv(c, margin_x, y, "Market Grid Override (kgCO2/kWh)", _fmt_num(cfg.get("market_grid_factor_override", 0), 4), body_font)
    y = _draw_kv(c, margin_x, y, "EUA (€/t)", _fmt_num(cfg.get("eua_price_eur", 0), 2), body_font)
    y = _draw_kv(c, margin_x, y, "Kur (TL/€)", _fmt_num(cfg.get("fx_tl_per_eur", 0), 2), body_font)
    y = _draw_kv(c, margin_x, y, "Ücretsiz Tahsis (tCO2)", _fmt_num(cfg.get("free_alloc_t", 0), 2), body_font)
    y = _draw_kv(c, margin_x, y, "Banked (tCO2)", _fmt_num(cfg.get("banked_t", 0), 2), body_font)
    y -= 8

    # Metodoloji
    meth = (report_data or {}).get("methodology")
    if isinstance(meth, dict) and (meth.get("name") or meth.get("id")):
        y = _draw_heading(c, margin_x, y, "Metodoloji", bold_font)
        y = _draw_kv(c, margin_x, y, "Ad", str(meth.get("name") or "-"), body_font)
        y = _draw_kv(c, margin_x, y, "Versiyon", str(meth.get("version") or "-"), body_font)
        y = _draw_kv(c, margin_x, y, "Kapsam", str(meth.get("scope") or "-"), body_font)
        desc = str(meth.get("description") or "").strip()
        if desc:
            y = _wrap_text(c, desc, margin_x, y, max_width=500, font_name=body_font, font_size=10)
        y -= 8

    # Data Quality
    dq = (report_data or {}).get("data_quality") or {}
    if isinstance(dq, dict) and dq:
        y = _draw_heading(c, margin_x, y, "Data Quality", bold_font)
        # energy / production / materials
        for key in ("energy", "production", "materials"):
            if key in dq and isinstance(dq.get(key), dict):
                score = dq[key].get("score")
                y = _draw_kv(c, margin_x, y, f"{key}.csv DQ Skoru", f"{score}/100" if score is not None else "-", body_font)
        # findings kısa özet
        findings = []
        for key in ("energy", "production", "materials"):
            rep = (dq.get(key) or {}).get("report") or {}
            if isinstance(rep, dict):
                issues = rep.get("issues") or []
                if isinstance(issues, list) and issues:
                    findings.append(f"{key}.csv: {len(issues)} bulgu")
        if findings:
            y = _draw_bullets(c, findings, margin_x, y, max_width=500, body_font=body_font, font_size=10)
        y -= 8

    # CBAM Section
    cbam = (report_data or {}).get("cbam") or {}
    cbam_totals = {}
    if isinstance(cbam, dict):
        cbam_totals = cbam.get("totals") or {}
    y = _draw_heading(c, margin_x, y, "CBAM", bold_font)
    y = _draw_kv(c, margin_x, y, "Direct (tCO2)", _fmt_num(cbam_totals.get("direct_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "Indirect (tCO2)", _fmt_num(cbam_totals.get("indirect_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "Precursor (tCO2)", _fmt_num(cbam_totals.get("precursor_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "Embedded (tCO2)", _fmt_num(cbam_totals.get("embedded_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "CBAM Maliyet Sinyali (€)", _fmt_num(cbam_totals.get("cbam_cost_eur", 0), 2), body_font)
    y = _draw_kv(c, margin_x, y, "Allocation Basis", str(cbam_totals.get("allocation_basis", "-")), body_font)
    y -= 6

    # Goods summary (ilk 5)
    goods_summary = cbam_totals.get("goods_summary") or []
    if isinstance(goods_summary, list) and goods_summary:
        y = _draw_heading(c, margin_x, y, "CBAM Goods Özeti (Top 5)", bold_font)
        top5 = goods_summary[:5]
        for r in top5:
            good = str(r.get("cbam_good") or "-")
            emb = _fmt_num(r.get("embedded_tco2", 0), 3)
            cost = _fmt_num(r.get("cbam_cost_eur", 0), 2)
            y = _draw_kv(c, margin_x, y, good, f"{emb} tCO2 | {cost} €", body_font)
        y -= 6

    # CBAM tablo (ilk 20 satır)
    table = (report_data or {}).get("cbam_table", []) or []
    if isinstance(table, list) and table:
        y = _draw_heading(c, margin_x, y, "CBAM Tablosu (Özet)", bold_font)
        headers = ["SKU/Ürün", "Goods", "Embedded tCO2", "CBAM €"]
        col_widths = [190, 130, 110, 100]
        row_h = 16

        y = _ensure_y(c, y, bold_font, 11)
        c.setFont(bold_font, 11)
        xx = margin_x
        c.drawString(xx, y, headers[0])
        xx += col_widths[0]
        c.drawString(xx, y, headers[1])
        xx += col_widths[1]
        c.drawRightString(xx + col_widths[2] - 6, y, headers[2])
        xx += col_widths[2]
        c.drawRightString(xx + col_widths[3] - 6, y, headers[3])
        y -= 10
        c.line(margin_x, y, margin_x + sum(col_widths), y)
        y -= 14

        c.setFont(body_font, 9)
        for row in table[:20]:
            y = _ensure_y(c, y, body_font, 9)
            sku = str(row.get("sku", ""))[:28]
            good = str(row.get("cbam_good", row.get("good", "")))[:18]
            embedded = _fmt_num(row.get("embedded_tco2", 0), 3)
            cost = _fmt_num(row.get("cbam_cost_eur", 0), 2)

            xx = margin_x
            c.drawString(xx, y, sku)
            xx += col_widths[0]
            c.drawString(xx, y, good)
            xx += col_widths[1]
            c.drawRightString(xx + col_widths[2] - 6, y, embedded)
            xx += col_widths[2]
            c.drawRightString(xx + col_widths[3] - 6, y, cost)
            y -= row_h

        if len(table) > 20:
            y -= 6
            y = _ensure_y(c, y, body_font, 9)
            c.setFont(body_font, 9)
            c.drawString(margin_x, y, f"(Not: Tablo kısaltıldı. Toplam satır: {len(table)})")
            y -= 10

    y -= 8

    # ETS Section
    ets = (report_data or {}).get("ets") or {}
    y = _draw_heading(c, margin_x, y, "ETS (Verification-Ready Özet)", bold_font)

    fin = {}
    ver = {}
    if isinstance(ets, dict):
        fin = ets.get("financials") or {}
        ver = ets.get("verification") or {}

    y = _draw_kv(c, margin_x, y, "Scope-1 (tCO2)", _fmt_num(fin.get("scope1_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "Net (tCO2)", _fmt_num(fin.get("net_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "Maliyet (€)", _fmt_num(fin.get("cost_eur", 0), 2), body_font)
    y = _draw_kv(c, margin_x, y, "Maliyet (TL)", _fmt_num(fin.get("cost_tl", 0), 2), body_font)
    y -= 6

    mp = (ver.get("monitoring_plan") or {}) if isinstance(ver, dict) else {}
    if isinstance(mp, dict) and mp:
        y = _draw_heading(c, margin_x, y, "Monitoring Plan", bold_font)
        y = _draw_kv(c, margin_x, y, "Yöntem", str(mp.get("method") or "-"), body_font)
        y = _draw_kv(c, margin_x, y, "Tier", str(mp.get("tier_level") or "-"), body_font)
        y = _draw_kv(c, margin_x, y, "Veri Kaynağı", str(mp.get("data_source") or "-")[:60], body_font)
        y = _draw_kv(c, margin_x, y, "Sorumlu", str(mp.get("responsible_person") or "-")[:60], body_font)
        y -= 6

    # Uncertainty + QA/QC
    unc = ver.get("uncertainty") if isinstance(ver, dict) else None
    if isinstance(unc, dict) and unc.get("notes"):
        y = _draw_heading(c, margin_x, y, "Belirsizlik Notları", bold_font)
        y = _wrap_text(c, str(unc.get("notes")), margin_x, y, max_width=500, font_name=body_font, font_size=10)
        y -= 6

    qaqc = ver.get("qa_qc") if isinstance(ver, dict) else None
    if isinstance(qaqc, dict) and qaqc.get("notes"):
        y = _draw_heading(c, margin_x, y, "QA/QC Özeti", bold_font)
        y = _wrap_text(c, str(qaqc.get("notes")), margin_x, y, max_width=500, font_name=body_font, font_size=10)
        y -= 6

    # Activity data (ilk 12 satır)
    activity = ver.get("activity_data") if isinstance(ver, dict) else None
    if isinstance(activity, list) and activity:
        y = _draw_heading(c, margin_x, y, "Activity Data (Yakıt Satırları - Özet)", bold_font)
        headers = ["Fuel", "Qty", "Unit", "tCO2"]
        col_widths = [210, 120, 70, 100]
        row_h = 14

        y = _ensure_y(c, y, bold_font, 10)
        c.setFont(bold_font, 10)
        xx = margin_x
        c.drawString(xx, y, headers[0])
        xx += col_widths[0]
        c.drawRightString(xx + col_widths[1] - 6, y, headers[1])
        xx += col_widths[1]
        c.drawString(xx, y, headers[2])
        xx += col_widths[2]
        c.drawRightString(xx + col_widths[3] - 6, y, headers[3])
        y -= 10
        c.line(margin_x, y, margin_x + sum(col_widths), y)
        y -= 12

        c.setFont(body_font, 9)
        for r in activity[:12]:
            y = _ensure_y(c, y, body_font, 9)
            fuel = str(r.get("fuel_type") or "")[:26]
            qty = _fmt_num(r.get("quantity", 0), 3)
            unit = str(r.get("unit") or "")[:6]
            tco2 = _fmt_num(r.get("tco2", 0), 3)

            xx = margin_x
            c.drawString(xx, y, fuel)
            xx += col_widths[0]
            c.drawRightString(xx + col_widths[1] - 6, y, qty)
            xx += col_widths[1]
            c.drawString(xx, y, unit)
            xx += col_widths[2]
            c.drawRightString(xx + col_widths[3] - 6, y, tco2)
            y -= row_h

        if len(activity) > 12:
            y -= 4
            y = _ensure_y(c, y, body_font, 9)
            c.setFont(body_font, 9)
            c.drawString(margin_x, y, f"(Not: Tablo kısaltıldı. Toplam satır: {len(activity)})")
            y -= 8

    # Veri kaynakları + formüller (opsiyonel)
    sources = (report_data or {}).get("data_sources") or []
    if isinstance(sources, list) and sources:
        y = _draw_heading(c, margin_x, y, "Veri Kaynakları", bold_font)
        y = _draw_bullets(c, [str(x) for x in sources], margin_x, y, max_width=500, body_font=body_font, font_size=10)
        y -= 6

    formulas = (report_data or {}).get("formulas") or []
    if isinstance(formulas, list) and formulas:
        y = _draw_heading(c, margin_x, y, "Hesap Formülleri (Özet)", bold_font)
        y = _draw_bullets(c, [str(x) for x in formulas], margin_x, y, max_width=500, body_font=body_font, font_size=10)
        y -= 6

    c.showPage()
    c.save()

    pdf_bytes = fp.read_bytes()
    pdf_sha = sha256_bytes(pdf_bytes)

    if append_audit:
        try:
            append_audit("pdf_built", {"snapshot_id": snapshot_id, "sha256": pdf_sha, "uri": str(fp)})
        except Exception:
            pass

    return str(fp), pdf_sha
