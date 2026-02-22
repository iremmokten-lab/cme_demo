from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from src.services.storage import REPORT_DIR, write_bytes
from src.mrv.lineage import sha256_bytes
from src.mrv.audit import append_audit

def build_pdf(snapshot_id: int, title: str, kpis: dict) -> tuple[str, str]:
    fp = Path(REPORT_DIR) / f"snapshot_{snapshot_id}.pdf"
    c = canvas.Canvas(str(fp), pagesize=A4)
    w, h = A4

    c.setFont("Helvetica-Bold", 18)
    c.drawString(50, h - 60, title)

    c.setFont("Helvetica", 12)
    y = h - 110
    for k, v in kpis.items():
        c.drawString(50, y, f"{k}: {v}")
        y -= 18

    c.showPage()
    c.save()

    b = fp.read_bytes()
    sha = sha256_bytes(b)
    append_audit("pdf_generated", {"snapshot_id": snapshot_id, "uri": str(fp), "sha256": sha})
    return str(fp), sha
