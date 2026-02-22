from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import select

from src.db.session import db
from src.db.models import CalculationSnapshot, DatasetUpload, EmissionFactor, Methodology, Report
from src.mrv.lineage import sha256_bytes, sha256_json
from src.services.reporting import build_pdf
from src.services.storage import EVIDENCE_DIR, EXPORT_DIR


def build_xlsx_from_results(results_json: str) -> bytes:
    results = json.loads(results_json) if results_json else {}
    kpis = results.get("kpis", {}) or {}
    table = results.get("cbam_table", []) or []

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        pd.DataFrame([kpis]).to_excel(writer, index=False, sheet_name="KPIs")
        pd.DataFrame(table).to_excel(writer, index=False, sheet_name="CBAM_Table")
    return out.getvalue()


def build_zip(snapshot_id: int, results_json: str) -> bytes:
    """Mevcut export: JSON + XLSX (geriye dönük)."""
    xlsx_bytes = build_xlsx_from_results(results_json)

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"snapshot_{snapshot_id}_results.json", results_json or "{}")
        z.writestr(f"snapshot_{snapshot_id}_export.xlsx", xlsx_bytes)
    return out.getvalue()


def _safe_read_bytes(uri: str) -> bytes:
    try:
        p = Path(str(uri))
        if p.exists():
            return p.read_bytes()
    except Exception:
        pass
    return b""


def _snapshot_input_uris(snapshot: CalculationSnapshot) -> dict:
    """Hem eski hem yeni input_hashes_json formatlarını tolere et."""
    try:
        ih = json.loads(snapshot.input_hashes_json or "{}")
    except Exception:
        ih = {}

    # Yeni format: {"energy": {...}, "production": {...}}
    if isinstance(ih, dict) and ("energy" in ih or "production" in ih):
        out = {}
        for k in ("energy", "production"):
            v = ih.get(k) or {}
            if isinstance(v, dict):
                out[k] = {"uri": v.get("uri") or "", "sha256": v.get("sha256") or ""}
        return out

    # Eski format: {"energy_uri": "...", "production_uri": "..."}
    if isinstance(ih, dict):
        return {
            "energy": {"uri": ih.get("energy_uri") or "", "sha256": ""},
            "production": {"uri": ih.get("production_uri") or "", "sha256": ""},
        }

    return {"energy": {"uri": "", "sha256": ""}, "production": {"uri": "", "sha256": ""}}


def _ensure_pdf_for_snapshot(snapshot: CalculationSnapshot) -> tuple[bytes, str]:
    """Snapshot için PDF varsa onu kullan; yoksa üret."""
    results = {}
    try:
        results = json.loads(snapshot.results_json or "{}")
    except Exception:
        results = {}

    kpis = (results.get("kpis") or {}) if isinstance(results, dict) else {}
    cbam_table = (results.get("cbam_table") or []) if isinstance(results, dict) else []
    scenario = (results.get("scenario") or {}) if isinstance(results, dict) else {}

    try:
        cfg = json.loads(snapshot.config_json or "{}")
    except Exception:
        cfg = {}

    # Metodoloji detayını çek
    meth_payload = None
    if getattr(snapshot, "methodology_id", None):
        with db() as s:
            m = s.get(Methodology, int(snapshot.methodology_id))
            if m:
                meth_payload = {
                    "id": m.id,
                    "name": m.name,
                    "description": m.description,
                    "scope": m.scope,
                    "version": m.version,
                    "created_at": (m.created_at.isoformat() if getattr(m, "created_at", None) else None),
                }

    title = "CME Demo Raporu — CBAM + ETS (Tahmini)"
    if isinstance(scenario, dict) and scenario.get("name"):
        title = f"Senaryo Raporu — {scenario.get('name')} (Tahmini)"

    # DB'de kayıtlı rapor var mı?
    with db() as s:
        r = (
            s.execute(
                select(Report)
                .where(Report.snapshot_id == snapshot.id, Report.report_type == "pdf")
                .order_by(Report.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )

    if r and getattr(r, "storage_uri", None):
        pdf_bytes = _safe_read_bytes(str(r.storage_uri))
        if pdf_bytes:
            return pdf_bytes, getattr(r, "sha256", sha256_bytes(pdf_bytes))

    # Yoksa üret
    payload = {
        "kpis": kpis,
        "config": cfg,
        "cbam_table": cbam_table,
        "scenario": scenario,
        "methodology": meth_payload,
        "data_sources": [
            "energy.csv (yüklenen dosya)",
            "production.csv (yüklenen dosya)",
            "EmissionFactor Library (DB)",
        ],
        "formulas": [
            "Direct emissions (örnek): fuel_quantity × NCV × emission_factor",
            "Indirect emissions (örnek): electricity_kwh × grid_factor",
            "CBAM embedded (demo): ürün faktörü × AB ihracat miktarı",
        ],
    }
    pdf_uri, pdf_sha = build_pdf(snapshot.id, title, payload)
    pdf_bytes = _safe_read_bytes(pdf_uri)

    # DB’ye rapor kaydı (duplicate kontrolü)
    if pdf_bytes:
        try:
            with db() as s:
                ex = (
                    s.execute(
                        select(Report)
                        .where(Report.snapshot_id == snapshot.id, Report.report_type == "pdf", Report.sha256 == pdf_sha)
                        .limit(1)
                    )
                    .scalars()
                    .first()
                )
                if not ex:
                    s.add(Report(snapshot_id=snapshot.id, report_type="pdf", storage_uri=str(pdf_uri), sha256=pdf_sha))
                    s.commit()
        except Exception:
            pass

    return pdf_bytes, pdf_sha


def build_evidence_pack(snapshot_id: int) -> bytes:
    """Evidence pack export (ZIP).

    İçerik:
    - input/energy.csv, input/production.csv
    - factor_library/emission_factors.json
    - methodology/methodology.json
    - snapshot/snapshot.json
    - report/report.pdf
    - manifest.json
    """
    with db() as s:
        snapshot = s.get(CalculationSnapshot, int(snapshot_id))
    if not snapshot:
        raise ValueError("Snapshot bulunamadı.")

    # Inputs
    inputs = _snapshot_input_uris(snapshot)
    energy_bytes = _safe_read_bytes(inputs.get("energy", {}).get("uri", ""))
    prod_bytes = _safe_read_bytes(inputs.get("production", {}).get("uri", ""))

    # Factor library
    with db() as s:
        factors = s.execute(select(EmissionFactor).order_by(EmissionFactor.factor_type, EmissionFactor.year.desc())).scalars().all()
    factor_rows = []
    factor_versions = {}
    for f in factors:
        factor_rows.append(
            {
                "factor_type": f.factor_type,
                "value": f.value,
                "unit": f.unit,
                "source": f.source,
                "year": f.year,
                "version": f.version,
                "region": f.region,
            }
        )
        # basit versiyon toplama: tip bazında en güncel version/year
        key = f"{f.factor_type}:{f.region}"
        if key not in factor_versions:
            factor_versions[key] = {"version": f.version, "year": f.year}

    factors_json = json.dumps({"emission_factors": factor_rows}, ensure_ascii=False, indent=2)

    # Methodology
    methodology_version = None
    meth_json = json.dumps({}, ensure_ascii=False, indent=2)
    if getattr(snapshot, "methodology_id", None):
        with db() as s:
            m = s.get(Methodology, int(snapshot.methodology_id))
        if m:
            methodology_version = m.version
            meth_json = json.dumps(
                {
                    "id": m.id,
                    "name": m.name,
                    "description": m.description,
                    "scope": m.scope,
                    "version": m.version,
                    "created_at": (m.created_at.isoformat() if getattr(m, "created_at", None) else None),
                },
                ensure_ascii=False,
                indent=2,
            )

    # Snapshot json (tek dosyada, manifest ile birlikte doğrulanabilir)
    try:
        cfg = json.loads(snapshot.config_json or "{}")
    except Exception:
        cfg = {}
    try:
        ih = json.loads(snapshot.input_hashes_json or "{}")
    except Exception:
        ih = {}
    try:
        res = json.loads(snapshot.results_json or "{}")
    except Exception:
        res = {}

    snapshot_payload = {
        "snapshot_id": snapshot.id,
        "created_at": (snapshot.created_at.isoformat() if getattr(snapshot, "created_at", None) else None),
        "engine_version": snapshot.engine_version,
        "methodology_id": getattr(snapshot, "methodology_id", None),
        "result_hash": snapshot.result_hash,
        "config": cfg,
        "input_hashes": ih,
        "results": res,
    }
    snapshot_json = json.dumps(snapshot_payload, ensure_ascii=False, indent=2)

    # PDF
    pdf_bytes, report_hash = _ensure_pdf_for_snapshot(snapshot)

    # Manifest
    manifest = {
        "snapshot_id": snapshot.id,
        "engine_version": snapshot.engine_version,
        "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "input_hashes": inputs,
        "factor_versions": factor_versions,
        "methodology_version": methodology_version,
        "report_hash": report_hash,
        "snapshot_hash": sha256_bytes(snapshot_json.encode("utf-8")),
        "factors_hash": sha256_bytes(factors_json.encode("utf-8")),
        "methodology_hash": sha256_bytes(meth_json.encode("utf-8")),
    }
    manifest_json = json.dumps(manifest, ensure_ascii=False, indent=2)

    # ZIP build
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", manifest_json)

        # inputs
        z.writestr("input/energy.csv", energy_bytes or b"")
        z.writestr("input/production.csv", prod_bytes or b"")

        # reference data
        z.writestr("factor_library/emission_factors.json", factors_json.encode("utf-8"))
        z.writestr("methodology/methodology.json", meth_json.encode("utf-8"))

        # snapshot + report
        z.writestr("snapshot/snapshot.json", snapshot_json.encode("utf-8"))
        z.writestr("report/report.pdf", pdf_bytes or b"")

    # (Opsiyonel) DB disk alanı için evidence pack'i saklama: sadece bytes döndürüyoruz.
    # İleride EVIDENCE_DIR altına yazmak isterseniz: write_bytes(EVIDENCE_DIR/...).
    return out.getvalue()
