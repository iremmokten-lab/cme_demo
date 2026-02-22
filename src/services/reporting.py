from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
    Repo kökünde DejaVuSans.ttf ve DejaVuSans-Bold.ttf olduğunu belirttiniz.
    Streamlit Cloud'da da dosyalar repo root'tan okunabilsin.
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
        return str(x)


def _draw_kv(c: canvas.Canvas, x: float, y: float, k: str, v: str, body_font: str) -> float:
    c.setFont(body_font, 11)
    c.drawString(x, y, k)
    c.drawRightString(545, y, v)
    return y - 16


def _wrap_text(c: canvas.Canvas, text: str, x: float, y: float, max_width: float, font_name: str, font_size: int):
    """
    Basit word-wrap: raporlab built-in paragraph kullanmadan, güvenli/az bağımlılık.
    """
    c.setFont(font_name, font_size)
    words = (text or "").split()
    line = ""
    lines = []
    for w in words:
        candidate = (line + " " + w).strip()
        if c.stringWidth(candidate, font_name, font_size) <= max_width:
            line = candidate
        else:
            if line:
                lines.append(line)
            line = w
    if line:
        lines.append(line)

    for ln in lines:
        c.drawString(x, y, ln)
        y -= (font_size + 3)
        if y < 80:
            c.showPage()
            c.setFont(font_name, font_size)
            y = 780
    return y


def build_pdf(snapshot_id: int, report_title: str, report_data: dict) -> tuple[str, str]:
    """
    PDF üretir, dosyayı REPORT_DIR altına kaydeder.
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
    c.drawString(margin_x, y, report_title or "CME Demo Raporu — CBAM + ETS (Tahmini)")
    y -= 26

    # Meta
    c.setFont(body_font, 11)
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    c.drawString(margin_x, y, f"Tarih (UTC): {ts}")
    y -= 16
    c.drawString(margin_x, y, f"Snapshot ID: {snapshot_id}")
    y -= 22

    # Not
    note = (
        "Önemli Not: Bu rapor yönetim amaçlı tahmini bir hesaplama çıktısıdır. "
        "Resmî beyan/raporlama için kullanılmamalıdır."
    )
    y = _wrap_text(c, note, margin_x, y, max_width=500, font_name=body_font, font_size=10)
    y -= 10

    # KPI'lar
    c.setFont(bold_font, 14)
    c.drawString(margin_x, y, "KPI Özeti")
    y -= 18

    kpis = (report_data or {}).get("kpis", {}) or {}
    y = _draw_kv(c, margin_x, y, "Toplam Emisyon (tCO2)", _fmt_num(kpis.get("energy_total_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "Scope-1 (tCO2)", _fmt_num(kpis.get("energy_scope1_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "Scope-2 (tCO2)", _fmt_num(kpis.get("energy_scope2_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "ETS Net (tCO2)", _fmt_num(kpis.get("ets_net_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "ETS Maliyeti (TL)", _fmt_num(kpis.get("ets_cost_tl", 0), 2), body_font)
    y = _draw_kv(c, margin_x, y, "CBAM Embedded (tCO2)", _fmt_num(kpis.get("cbam_embedded_tco2", 0), 3), body_font)
    y = _draw_kv(c, margin_x, y, "CBAM Maliyeti (€)", _fmt_num(kpis.get("cbam_cost_eur", 0), 2), body_font)
    y -= 8

    # Konfig
    cfg = (report_data or {}).get("config", {}) or {}
    c.setFont(bold_font, 14)
    c.drawString(margin_x, y, "Parametreler")
    y -= 18
    y = _draw_kv(c, margin_x, y, "EUA (€/t)", _fmt_num(cfg.get("eua_price_eur", 0), 2), body_font)
    y = _draw_kv(c, margin_x, y, "Kur (TL/€)", _fmt_num(cfg.get("fx_tl_per_eur", 0), 2), body_font)
    y = _draw_kv(c, margin_x, y, "Ücretsiz Tahsis (tCO2)", _fmt_num(cfg.get("free_alloc_t", 0), 2), body_font)
    y = _draw_kv(c, margin_x, y, "Banked (tCO2)", _fmt_num(cfg.get("banked_t", 0), 2), body_font)
    y -= 10

    # CBAM tablosu (ilk 20 satır)
    table = (report_data or {}).get("cbam_table", []) or []
    if table:
        c.setFont(bold_font, 14)
        c.drawString(margin_x, y, "CBAM Tablosu (Özet)")
        y -= 18

        headers = ["SKU/Ürün", "Embedded tCO2", "CBAM €"]
        col_widths = [260, 140, 120]
        row_h = 16

        c.setFont(bold_font, 11)
        xx = margin_x
        c.drawString(xx, y, headers[0])
        xx += col_widths[0]
        c.drawRightString(xx - 6, y, headers[1])
        xx += col_widths[1]
        c.drawRightString(xx - 6, y, headers[2])
        y -= 10
        c.line(margin_x, y, margin_x + sum(col_widths), y)
        y -= 14

        c.setFont(body_font, 10)
        for row in table[:20]:
            if y < 80:
                c.showPage()
                y = 780
                c.setFont(body_font, 10)

            sku = str(row.get("sku", row.get("product", row.get("name", ""))))
            embedded = _fmt_num(row.get("embedded_tco2", row.get("embedded_t", 0)), 3)
            cost = _fmt_num(row.get("cbam_cost_eur", row.get("cbam_eur", 0)), 2)

            xx = margin_x
            c.drawString(xx, y, sku[:40])
            xx += col_widths[0]
            c.drawRightString(xx - 6, y, embedded)
            xx += col_widths[1]
            c.drawRightString(xx - 6, y, cost)
            y -= row_h

        if len(table) > 20:
            y -= 6
            c.setFont(body_font, 9)
            c.drawString(margin_x, y, f"(Not: Tablo kısaltıldı. Toplam satır: {len(table)})")

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
