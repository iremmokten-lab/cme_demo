from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Tuple

from sqlalchemy import select

from src.db.models import CalculationSnapshot, Report
from src.db.session import db
from src.mrv.lineage import sha256_bytes
from src.services.reporting import build_pdf
from src.services.storage import REPORT_DIR, write_bytes


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj or {}, ensure_ascii=False, sort_keys=True, default=str, indent=2).encode("utf-8")


def build_ai_optimization_report(snapshot_id: int) -> Tuple[Path, Path]:
    """AI Optimization için JSON + PDF üretir (deterministik).

    Dosyalar:
      storage/reports/snapshot_<id>/ai_optimization.json
      storage/reports/snapshot_<id>/ai_optimization.pdf
    """
    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            raise ValueError("Snapshot bulunamadı.")
        try:
            results = json.loads(snap.results_json or "{}")
        except Exception:
            results = {}
        try:
            cfg = json.loads(snap.config_json or "{}")
        except Exception:
            cfg = {}

    ai = (results or {}).get("ai") if isinstance(results, dict) else None
    payload = {
        "schema": "ai_optimization_report.v1",
        "snapshot_id": int(snapshot_id),
        "engine_version": str(getattr(snap, "engine_version", "") or ""),
        "input_hash": str(getattr(snap, "input_hash", "") or ""),
        "result_hash": str(getattr(snap, "result_hash", "") or ""),
        "config": cfg,
        "ai": ai or {},
    }

    out_dir = REPORT_DIR / f"snapshot_{int(snapshot_id)}"
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "ai_optimization.json"
    pdf_path = out_dir / "ai_optimization.pdf"

    write_bytes(json_path, _json_bytes(payload))

    # PDF (reporting.build_pdf içinde yeni bölüm ile)
    p, _h = build_pdf(int(snapshot_id), "AI & Optimizasyon Raporu", {"kpis": (results or {}).get("kpis", {}), "config": cfg, "ai": ai or {}})
    # build_pdf zaten REPORT_DIR'a yazıyor; bizim hedef path'e kopyalayalım
    try:
        Path(p).replace(pdf_path)
    except Exception:
        try:
            pdf_path.write_bytes(Path(p).read_bytes())
        except Exception:
            pdf_path.write_bytes(b"")

    return json_path, pdf_path


def persist_ai_reports_as_db_reports(project_id: int, snapshot_id: int, created_by_user_id: int | None = None) -> None:
    """Report tablosuna kayıt düşer. Evidence pack export'ta kolay dahil edilir."""
    json_path, pdf_path = build_ai_optimization_report(int(snapshot_id))

    rows = [
        ("ai_optimization_json", json_path),
        ("ai_optimization_pdf", pdf_path),
    ]

    with db() as s:
        for rtype, p in rows:
            bts = p.read_bytes() if p.exists() else b""
            rh = sha256_bytes(bts) if bts else ""
            # upsert: aynı snapshot+type varsa güncelle
            existing = (
                s.execute(
                    select(Report)
                    .where(Report.project_id == int(project_id), Report.snapshot_id == int(snapshot_id), Report.report_type == str(rtype))
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if existing:
                existing.file_path = str(p)
                existing.file_hash = str(rh)
                continue
            s.add(
                Report(
                    project_id=int(project_id),
                    snapshot_id=int(snapshot_id),
                    report_type=str(rtype),
                    file_path=str(p),
                    file_hash=str(rh),
                    meta_json=json.dumps({"schema": "ai_report_meta.v1"}, ensure_ascii=False),
                    created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
                )
            )
        s.commit()
