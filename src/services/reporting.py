from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

from src.services.storage import REPORT_DIR
from src.mrv.lineage import sha256_bytes
from src.mrv.audit import append_audit


# Font dosyaları repo kökünde: DejaVuSans.ttf ve DejaVuSans-Bold.ttf
# Bu font Türkçe karakterleri düzgün basar.
def _register_fonts():
    try:
        pdfmetrics.registerFont(TTFont("DejaVuSans", "DejaVuSans.ttf"))
        pdfmetrics.registerFont(TTFont("DejaVuSans-Bold", "DejaVuSans-Bold.ttf"))
        return True
    except Exception:
        # font bulunamazsa Helvetica ile devam eder
        return False


def build_pdf(snapshot_id: int, title: str, kpis: dict) -> tuple[str, str]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    fp = Path(REPORT_DIR) / f"snapshot_{snapshot_id}.pdf"
    c = canvas.Canvas(str(fp), pagesize=A4)
    w, h = A4

    has_font = _register_fonts()

    # Başlık
    if has_font:
        c.setFont("DejaVuSans-Bold", 18)
    else:
        c.setFont("Helvetica-Bold", 18)
    c.drawString(50, h - 60, "CME Raporu")

    # Alt başlık / açıklama (Türkçe)
    if has_font:
        c.setFont("DejaVuSans", 11)
    else:
        c.setFont("Helvetica", 11)

    c.drawString(50, h - 85, "Önemli Not: Bu rapor yönetim amaçlı tahmini bir çıktıdır. Resmî beyan değildir.")
    c.drawString(50, h - 102, f"Snapshot ID: {snapshot_id}")

    # KPI listesi
    if has_font:
        c.setFont("DejaVuSans", 12)
    else:
        c.setFont("Helvetica", 12)

    y = h - 140
    for k, v in (kpis or {}).items():
        c.drawString(50, y, f"{k}: {v}")
        y -= 18
        if y < 60:
            c.showPage()
            if has_font:
                c.setFont("DejaVuSans", 12)
            else:
                c.setFont("Helvetica", 12)
            y = h - 60

    c.showPage()
    c.save()

    b = fp.read_bytes()
    sha = sha256_bytes(b)

    append_audit("pdf_generated", {"snapshot_id": snapshot_id, "uri": str(fp), "sha256": sha})
    return str(fp), sha
