from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from sqlalchemy import select

from src.db.models import CalculationSnapshot, Report
from src.db.session import db
from src.mrv.lineage import sha256_bytes
from src.services.storage import REPORT_DIR, write_bytes
from src.services.carbon_cost_engine import compute_carbon_cost_report


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj or {}, ensure_ascii=False, sort_keys=True, indent=2, default=str).encode("utf-8")


def _fmt_num_tr(x: Any, digits: int = 2) -> str:
    try:
        s = f"{float(x):,.{digits}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(x)


def _build_carbon_cost_pdf(snapshot_id: int, payload: Dict[str, Any]) -> bytes:
    out_path = Path("/tmp") / f"carbon_cost_{int(snapshot_id)}.pdf"
    c = canvas.Canvas(str(out_path), pagesize=A4)

    w, h = A4
    x = 50
    y = h - 60

    c.setFont("Helvetica-Bold", 16)
    c.drawString(x, y, "Karbon Maliyeti Raporu (Faz 2)")
    y -= 22

    c.setFont("Helvetica", 10)
    c.drawString(x, y, f"Snapshot ID: {payload.get('snapshot_id')}  |  Project ID: {payload.get('project_id')}")
    y -= 14

    year = payload.get("year")
    if year:
        c.drawString(x, y, f"Dönem (Yıl): {year}")
        y -= 14

    c.drawString(x, y, f"Oluşturma Zamanı (UTC): {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    y -= 18

    ets = (payload.get("ets") or {}) if isinstance(payload.get("ets"), dict) else {}
    cbam = (payload.get("cbam") or {}) if isinstance(payload.get("cbam"), dict) else {}
    totals = (payload.get("totals") or {}) if isinstance(payload.get("totals"), dict) else {}
    assumptions = (payload.get("assumptions") or {}) if isinstance(payload.get("assumptions"), dict) else {}

    c.setFont("Helvetica-Bold", 12)
    c.drawString(x, y, "1) ETS (EU ETS / TR ETS modu ile uyumlu görünüm)")
    y -= 16

    c.setFont("Helvetica", 10)
    lines = [
        ("Scope1 (tCO2)", _fmt_num_tr(ets.get("scope1_tco2", 0.0))),
        ("Ücretsiz tahsis (tCO2)", _fmt_num_tr(ets.get("free_alloc_tco2", 0.0))),
        ("Banked / devreden (tCO2)", _fmt_num_tr(ets.get("banked_tco2", 0.0))),
        ("Net yükümlülük (tCO2)", _fmt_num_tr(ets.get("net_tco2", 0.0))),
        ("Fiyat (€/t)", _fmt_num_tr(ets.get("price_eur_per_t", assumptions.get("eua_price_eur_per_t", 0.0)))),
        ("ETS maliyeti (€)", _fmt_num_tr(ets.get("cost_eur", 0.0))),
        ("ETS maliyeti (TL)", _fmt_num_tr(ets.get("cost_tl", 0.0))),
    ]
    for k, v in lines:
        c.drawString(x, y, k)
        c.drawRightString(545, y, v)
        y -= 14
        if y < 90:
            c.showPage()
            y = h - 60
            c.setFont("Helvetica", 10)

    y -= 6
    c.setFont("Helvetica-Bold", 12)
    c.drawString(x, y, "2) CBAM (Sertifika ve Tahmini Ödeme)")
    y -= 16

    liab = (cbam.get("liability") or {}) if isinstance(cbam.get("liability"), dict) else {}
    lines2 = [
        ("Embedded emissions (tCO2)", _fmt_num_tr(liab.get("embedded_emissions_tco2", 0.0))),
        ("Payable share", _fmt_num_tr(liab.get("payable_share", 0.0), 4)),
        ("Payable emissions (tCO2)", _fmt_num_tr(liab.get("payable_emissions_tco2", 0.0))),
        ("Ödenmiş karbon fiyatı (€/t)", _fmt_num_tr(liab.get("carbon_price_paid_eur_per_t", assumptions.get("carbon_price_paid_eur_per_t", 0.0)))),
        ("Sertifika gereksinimi", _fmt_num_tr(liab.get("certificates_required", cbam.get("certificates_required", 0.0)))),
        ("Tahmini CBAM ödeme (€)", _fmt_num_tr(cbam.get("estimated_payable_amount_eur", 0.0))),
        ("Tahmini CBAM ödeme (TL)", _fmt_num_tr(cbam.get("estimated_payable_amount_tl", 0.0))),
    ]
    c.setFont("Helvetica", 10)
    for k, v in lines2:
        c.drawString(x, y, k)
        c.drawRightString(545, y, v)
        y -= 14
        if y < 90:
            c.showPage()
            y = h - 60
            c.setFont("Helvetica", 10)

    y -= 6
    c.setFont("Helvetica-Bold", 12)
    c.drawString(x, y, "3) Toplam")
    y -= 16
    c.setFont("Helvetica", 10)
    c.drawString(x, y, "Toplam maliyet (€)")
    c.drawRightString(545, y, _fmt_num_tr(totals.get("total_cost_eur", 0.0)))
    y -= 14
    c.drawString(x, y, "Toplam maliyet (TL)")
    c.drawRightString(545, y, _fmt_num_tr(totals.get("total_cost_tl", 0.0)))
    y -= 18

    notes = assumptions.get("notes_tr") if isinstance(assumptions.get("notes_tr"), list) else []
    if notes:
        c.setFont("Helvetica-Bold", 12)
        c.drawString(x, y, "Notlar")
        y -= 14
        c.setFont("Helvetica", 10)
        for n in notes:
            c.drawString(x, y, f"• {str(n)}")
            y -= 12
            if y < 90:
                c.showPage()
                y = h - 60
                c.setFont("Helvetica", 10)

    c.save()
    return out_path.read_bytes()


def save_carbon_cost_reports(
    *,
    project_id: int,
    snapshot_id: int,
    created_by_user_id: Optional[int] = None,
) -> Dict[str, Report]:
    """Carbon cost raporlarını üretir ve DB'ye kaydeder.

    Çıktılar:
      - storage/reports/<snapshot_id>/carbon_cost.json   (report_type=carbon_cost)
      - storage/reports/<snapshot_id>/carbon_cost.pdf    (report_type=carbon_cost_pdf)

    Dönüş: {"json": Report, "pdf": Report}
    """
    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            raise ValueError("Snapshot bulunamadı.")
        try:
            results = json.loads(snap.results_json) if snap.results_json else {}
        except Exception:
            results = {}
        try:
            cfg = json.loads(snap.config_json) if snap.config_json else {}
        except Exception:
            cfg = {}

    payload = compute_carbon_cost_report(
        snapshot_id=int(snapshot_id),
        project_id=int(project_id),
        results_json=results if isinstance(results, dict) else {},
        config=cfg if isinstance(cfg, dict) else {},
    ).to_dict()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    base_dir = REPORT_DIR / str(int(snapshot_id))
    base_dir.mkdir(parents=True, exist_ok=True)

    # JSON
    json_bytes = _json_bytes(payload)
    json_sha = sha256_bytes(json_bytes)
    json_path = base_dir / "carbon_cost.json"
    write_bytes(json_path, json_bytes)

    # PDF
    pdf_bytes = _build_carbon_cost_pdf(int(snapshot_id), payload)
    pdf_sha = sha256_bytes(pdf_bytes)
    pdf_path = base_dir / "carbon_cost.pdf"
    write_bytes(pdf_path, pdf_bytes)

    out: Dict[str, Report] = {}

    def upsert(report_type: str, path: Path, sha: str) -> Report:
        with db() as s2:
            existing = (
                s2.execute(
                    select(Report)
                    .where(Report.snapshot_id == int(snapshot_id))
                    .where(Report.report_type == str(report_type))
                    .order_by(Report.created_at.desc())
                )
                .scalars()
                .first()
            )
            if existing:
                existing.file_path = str(path)
                existing.file_hash = str(sha)
                existing.meta_json = json.dumps(
                    {"schema": payload.get("schema"), "year": payload.get("year"), "snapshot_id": int(snapshot_id)},
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                )
                if created_by_user_id is not None:
                    existing.created_by_user_id = int(created_by_user_id)
                s2.add(existing)
                s2.commit()
                s2.refresh(existing)
                return existing

            r = Report(
                project_id=int(project_id),
                snapshot_id=int(snapshot_id),
                report_type=str(report_type),
                file_path=str(path),
                file_hash=str(sha),
                meta_json=json.dumps(
                    {"schema": payload.get("schema"), "year": payload.get("year"), "snapshot_id": int(snapshot_id)},
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                ),
                created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
            )
            s2.add(r)
            s2.commit()
            s2.refresh(r)
            return r

    out["json"] = upsert("carbon_cost", json_path, json_sha)
    out["pdf"] = upsert("carbon_cost_pdf", pdf_path, pdf_sha)
    return out
