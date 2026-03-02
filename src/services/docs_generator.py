from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas


def build_methodology_summary_md(title: str, sections: List[Dict[str, str]]) -> str:
    lines = [f"# {title}", ""]
    for s in sections:
        lines.append(f"## {s.get('heading','')}".strip())
        lines.append("")
        lines.append(s.get("body","").strip())
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def build_pdf_from_text(title: str, paragraphs: List[str]) -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    y = height - 50
    c.setTitle(title)

    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, title[:90])
    y -= 30

    c.setFont("Helvetica", 10)
    for p in paragraphs:
        # simple wrapping
        for line in _wrap(p, 95):
            if y < 60:
                c.showPage()
                y = height - 50
                c.setFont("Helvetica", 10)
            c.drawString(50, y, line)
            y -= 14
        y -= 8

    c.showPage()
    c.save()
    return buf.getvalue()


def _wrap(text: str, max_len: int) -> List[str]:
    words = (text or "").split()
    lines = []
    cur = []
    n = 0
    for w in words:
        if n + len(w) + (1 if cur else 0) > max_len:
            lines.append(" ".join(cur))
            cur = [w]
            n = len(w)
        else:
            cur.append(w)
            n += len(w) + (1 if cur[:-1] else 0)
    if cur:
        lines.append(" ".join(cur))
    return lines
