from pathlib import Path
from datetime import datetime, timezone

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

from src.services.storage import REPORT_DIR
from src.mrv.lineage import sha256_bytes
from src.mrv.audit import append_audit


def _register_fonts() -> bool:
    try:
        pdfmetrics.registerFont(TTFont("DejaVuSans", "DejaVuSans.ttf"))
        pdfmetrics.registerFont(TTFont("DejaVuSans-Bold", "DejaVuSans-Bold.ttf"))
        return True
    except Exception:
        return False


def _fmt_num(x, digits=2):
    try:
        s = f"{float(x):,.{digits}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(x)


def _draw_kv(c: canvas.Canvas, x: float, y: float, k: str, v: str, body_font: str):
    c.setFont(body_font, 11)
    c.drawString(x, y, k)
    c.drawRightString(540, y, v)


def _draw_table(c: canvas.Canvas, x: float, y: float, headers: list[str], rows: list[list[str]], body_font: str, bold_font: str):
    # SKU | Risk | EU tCO2 | CBAM €
    col_widths = [140, 110, 130, 110]
    row_h = 18

    c.setFont(bold_font, 11)
    xx = x
    for i, h in enumerate(headers):
        if i == 0:
            c.drawString(xx, y, h)
        else:
            c.drawRightString(xx + col_widths[i] - 6, y, h)
        xx += col_widths[i]

    y -= 10
    c.line(x, y, x + sum(col_widths), y)
    y -= 14

    c.setFont(body_font, 10)
    for r in rows:
        xx = x
        for i, cell in enumerate(r):
            txt = str(cell)
            if i == 0:
                c.drawString(xx, y, txt)
            else:
                c.drawRightString(xx + col_widths[i] - 6, y, txt)
            xx += col_widths[i]
        y -= row_h
        if y < 80:
            c.showPage()
            y = 760
            c.setFont(body_font, 10)

    return y


def build_pdf(snapshot_id: int, report_title: str, report_data: dict) -> tuple[str, str]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    fp = Path(REPORT_DIR) / f"snapshot_{snapshot_id}.pdf"
    c = canvas.Canvas(str(fp), pagesize=A4)

    has_font = _register_fonts()
    body_font = "DejaVuSans" if has_font else "Helvetica"
    bold_font = "DejaVuSans-Bold" if has_font else "Helvetica-Bold"

    w, h = A4
    margin_x = 50

    c.setFont(bold_font, 18)
    c.drawString(margin_x, h - 60, report_title or "CME Demo Raporu — CBAM + ETS (Tahmini)")

    c.setFont(body_font, 11)
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    c.drawString(margin_x, h - 85, f"Tarih (UTC): {ts}")
    c.drawString(margin_x, h - 102, f"Snapshot ID: {snapshot_id}")

    c.setFont(body_font, 10)
    c.drawString(
        margin_x,
        h - 122,
        "Önemli Not: Bu rapor yönetim amaçlı tahmini bir allocation/hesaplama çıktısıdır. Resmî beyan/uyum dokümanı değildir.",
    )

    y = h - 160

    kpis = (report_data or {}).get("kpis", {}) or {}
    cfg = (report_data or {}).get("config", {}) or {}
    top_skus = (report_data or {}).get("top_skus", []) or []

    c.setFont(bold_font, 13)
    c.drawString(margin_x, y, "Genel Parametreler")
    y -= 22

    _draw_kv(c, margin_x, y, "EUA fiyatı (€/tCO2)", _fmt_num(cfg.get("eua_price_eur", 0), 2), body_font); y -= 16
    _draw_kv(c, margin_x, y, "FX (TL/€)", _fmt_num(cfg.get("fx_tl_per_eur", 0), 2), body_font); y -= 16
    _draw_kv(c, margin_x, y, "Free allocation (tCO2)", _fmt_num(cfg.get("free_alloc_t", 0), 4), body_font); y -= 16
    _draw_kv(c, margin_x, y, "Banked allowances (tCO2)", _fmt_num(cfg.get("banked_t", 0), 4), body_font); y -= 26

    c.setFont(bold_font, 13)
    c.drawString(margin_x, y, "Energy (Scope 1–2) Özeti")
    y -= 22

    _draw_kv(c, margin_x, y, "Total (tCO2)", _fmt_num(kpis.get("energy_total_tco2", 0), 4), body_font); y -= 16
    _draw_kv(c, margin_x, y, "Scope 1 (tCO2)", _fmt_num(kpis.get("energy_scope1_tco2", 0), 4), body_font); y -= 16
    _draw_kv(c, margin_x, y, "Scope 2 (tCO2)", _fmt_num(kpis.get("energy_scope2_tco2", 0), 4), body_font); y -= 26

    c.setFont(bold_font, 13)
    c.drawString(margin_x, y, "ETS Özeti (Tesis Bazlı)")
    y -= 22
    _draw_kv(c, margin_x, y, "Net EUA gereksinimi (tCO2)", _fmt_num(kpis.get("ets_net_tco2", 0), 4), body_font); y -= 16
    _draw_kv(c, margin_x, y, "ETS maliyeti (TL)", _fmt_num(kpis.get("ets_cost_tl", 0), 2), body_font); y -= 26

    c.setFont(bold_font, 13)
    c.drawString(margin_x, y, "CBAM Özeti (Ürün Bazlı, Demo)")
    y -= 22
    _draw_kv(c, margin_x, y, "Toplam embedded (tCO2)", _fmt_num(kpis.get("cbam_embedded_tco2", 0), 4), body_font); y -= 16
    _draw_kv(c, margin_x, y, "Toplam CBAM maliyeti (€)", _fmt_num(kpis.get("cbam_cost_eur", 0), 2), body_font); y -= 26

    if top_skus:
        c.setFont(bold_font, 13)
        c.drawString(margin_x, y, "En Yüksek Risk Skorlu İlk 10 SKU")
        y -= 22

        headers = ["SKU", "Risk (0–100)", "EU tCO2", "CBAM €"]
        rows = []
        for r in top_skus[:10]:
            rows.append([
                str(r.get("sku", "")),
                _fmt_num(r.get("risk", 0), 1),
                _fmt_num(r.get("eu_tco2", 0), 4),
                _fmt_num(r.get("cbam_eur", 0), 2),
            ])

        y = _draw_table(c, margin_x, y, headers, rows, body_font, bold_font)

    c.setFont(body_font, 9)
    c.drawString(margin_x, 40, "Önemli Not: Bu rapor yönetim amaçlı tahmini bir çıktıdır. Resmî beyan/uyum dokümanı değildir.")
    c.showPage()
    c.save()

    b = fp.read_bytes()
    sha = sha256_bytes(b)
    append_audit("pdf_generated", {"snapshot_id": snapshot_id, "uri": str(fp), "sha256": sha})
    return str(fp), sha
