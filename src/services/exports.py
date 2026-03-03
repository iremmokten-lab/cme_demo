from __future__ import annotations

import base64
import hmac
import io
import json
import os
import zipfile
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from src.mrv.lineage import sha256_bytes
from src.services.storage import EVIDENCE_DOCS_CATEGORIES


def build_xlsx_from_results(results_json: str) -> bytes:
    """Sonuç JSON'undan XLSX üretir.

    Not: Bu fonksiyon DB'ye ihtiyaç duymaz. Streamlit Cloud ortamında import
    sırası / model uyumsuzluğu gibi problemler yüzünden uygulama açılışının
    bozulmaması için bağımsız tutulur.
    """
    results = {}
    try:
        results = json.loads(results_json) if results_json else {}
    except Exception:
        results = {}

    kpis = results.get("kpis", {}) or {}
    table = results.get("cbam_table", []) or []

    cbam_goods: List[Dict[str, Any]] = []
    try:
        cbam_goods = (results.get("cbam") or {}).get("totals", {}).get("goods_summary", []) or []
    except Exception:
        cbam_goods = []

    ets_activity: List[Dict[str, Any]] = []
    try:
        ets_activity = ((results.get("ets") or {}).get("verification") or {}).get("activity_data", []) or []
    except Exception:
        ets_activity = []

    out = io.BytesIO()
    # openpyxl bağımlılığı requirements.txt içinde olmalı (pandas ExcelWriter engine="openpyxl")
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        pd.DataFrame([kpis]).to_excel(writer, index=False, sheet_name="KPIs")
        pd.DataFrame(table).to_excel(writer, index=False, sheet_name="CBAM_Table")

        if isinstance(cbam_goods, list) and cbam_goods:
            pd.DataFrame(cbam_goods).to_excel(writer, index=False, sheet_name="CBAM_Goods")

        if isinstance(ets_activity, list) and ets_activity:
            pd.DataFrame(ets_activity).to_excel(writer, index=False, sheet_name="ETS_Activity")

    return out.getvalue()


def build_zip(snapshot_id: int, results_json: str) -> bytes:
    """Snapshot sonuçları için ZIP (JSON + XLSX) üretir."""
    xlsx_bytes = build_xlsx_from_results(results_json)
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"snapshot_{snapshot_id}_results.json", results_json or "{}")
        z.writestr(f"snapshot_{snapshot_id}_export.xlsx", xlsx_bytes)
    return out.getvalue()


def _json_bytes(payload: Any) -> bytes:
    try:
        return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    except Exception:
        return b"{}"


def _safe_read_bytes(uri: str) -> bytes:
    try:
        p = Path(str(uri))
        if p.exists():
            return p.read_bytes()
    except Exception:
        pass
    return b""


def _hmac_signature(payload: bytes) -> str | None:
    """Manifest imzası: ENV üzerinden HMAC anahtarı varsa üretir."""
    key = os.getenv("EVIDENCE_PACK_HMAC_KEY", "").strip()
    if not key:
        return None
    try:
        digest = hmac.new(key.encode("utf-8"), payload, sha256).digest()
        return base64.b64encode(digest).decode("utf-8")
    except Exception:
        return None


def _snapshot_input_uris(snapshot: Any) -> dict:
    """Snapshot içindeki input uri/sha referanslarını toleranslı okur."""
    try:
        ih = json.loads(getattr(snapshot, "input_hashes_json", "") or "{}")
    except Exception:
        ih = {}

    if isinstance(ih, dict) and ("energy" in ih or "production" in ih):
        out = {}
        for k in ("energy", "production", "materials"):
            v = ih.get(k) or {}
            if isinstance(v, dict):
                out[k] = {"uri": v.get("uri") or "", "sha256": v.get("sha256") or ""}
        return out

    if isinstance(ih, dict):
        return {
            "energy": {"uri": ih.get("energy_uri") or "", "sha256": ""},
            "production": {"uri": ih.get("production_uri") or "", "sha256": ""},
            "materials": {"uri": "", "sha256": ""},
        }

    return {
        "energy": {"uri": "", "sha256": ""},
        "production": {"uri": "", "sha256": ""},
        "materials": {"uri": "", "sha256": ""},
    }


def _ensure_pdf_for_snapshot(snapshot: Any) -> Tuple[bytes, str]:
    """Snapshot için PDF raporu üretir veya mevcut kaydı okur (best-effort)."""
    # Lazy imports: sayfa import'unda kırılmasın
    from src.db.session import db
    from sqlalchemy import select

    # Modelleri temkinli al
    try:
        from src.db.models import Report, Methodology, DatasetUpload
    except Exception:
        Report = None  # type: ignore
        Methodology = None  # type: ignore
        DatasetUpload = None  # type: ignore

    from src.services.reporting import build_pdf

    results = {}
    try:
        results = json.loads(getattr(snapshot, "results_json", "") or "{}")
    except Exception:
        results = {}

    kpis = (results.get("kpis") or {}) if isinstance(results, dict) else {}
    cbam_table = (results.get("cbam_table") or []) if isinstance(results, dict) else []
    scenario = (results.get("scenario") or {}) if isinstance(results, dict) else {}
    cbam_section = (results.get("cbam") or {}) if isinstance(results, dict) else {}
    ets_section = (results.get("ets") or {}) if isinstance(results, dict) else {}

    try:
        cfg = json.loads(getattr(snapshot, "config_json", "") or "{}")
    except Exception:
        cfg = {}

    # Methodology payload
    meth_payload = None
    try:
        meth_id = getattr(snapshot, "methodology_id", None)
        if meth_id and Methodology is not None:
            with db() as s:
                m = s.get(Methodology, int(meth_id))
                if m:
                    meth_payload = {
                        "id": getattr(m, "id", None),
                        "name": getattr(m, "name", None),
                        "description": getattr(m, "description", None),
                        "scope": getattr(m, "scope", None),
                        "version": getattr(m, "version", None),
                        "created_at": (
                            getattr(m, "created_at", None).isoformat() if getattr(m, "created_at", None) else None
                        ),
                    }
    except Exception:
        meth_payload = None

    title = "Rapor — CBAM + ETS (Readiness)"
    if isinstance(scenario, dict) and scenario.get("name"):
        title = f"Senaryo Raporu — {scenario.get('name')} (Readiness)"

    # DB’de hazır pdf var mı?
    try:
        if Report is not None:
            with db() as s:
                r = (
                    s.execute(
                        select(Report)
                        .where(Report.snapshot_id == getattr(snapshot, "id", None), Report.report_type == "pdf")
                        .order_by(Report.created_at.desc())
                        .limit(1)
                    )
                    .scalars()
                    .first()
                )
                if r and getattr(r, "storage_uri", None):
                    pdf_bytes = _safe_read_bytes(str(getattr(r, "storage_uri", "")))
                    if pdf_bytes:
                        return pdf_bytes, getattr(r, "sha256", sha256_bytes(pdf_bytes))
    except Exception:
        pass

    # Data quality (upload’lardan derive)
    dq: Dict[str, Any] = {}
    inputs = _snapshot_input_uris(snapshot)
    try:
        if DatasetUpload is not None:
            with db() as s:
                for key in ("energy", "production", "materials"):
                    sha_val = (inputs.get(key) or {}).get("sha256") or ""
                    if not sha_val:
                        continue
                    up = (
                        s.execute(
                            select(DatasetUpload)
                            .where(
                                DatasetUpload.project_id == getattr(snapshot, "project_id", None),
                                DatasetUpload.dataset_type == key,
                                DatasetUpload.sha256 == sha_val,
                            )
                            .order_by(DatasetUpload.uploaded_at.desc())
                            .limit(1)
                        )
                        .scalars()
                        .first()
                    )
                    if up:
                        try:
                            dq_report = json.loads(getattr(up, "data_quality_report_json", "") or "{}")
                        except Exception:
                            dq_report = {}
                        dq[key] = {"score": getattr(up, "data_quality_score", None), "report": dq_report}
    except Exception:
        dq = {}

    pdf_bytes = build_pdf(
        title=title,
        kpis=kpis,
        cbam_table=cbam_table,
        cfg=cfg,
        cbam_section=cbam_section,
        ets_section=ets_section,
        dq=dq,
        methodology=meth_payload,
    )
    pdf_hash = sha256_bytes(pdf_bytes)

    # best-effort: Report kaydı
    try:
        if Report is not None:
            uri = str(Path("./storage/reports") / f"snapshot_{getattr(snapshot, 'id', 'na')}_report.pdf")
            Path(uri).parent.mkdir(parents=True, exist_ok=True)
            Path(uri).write_bytes(pdf_bytes)
            with db() as s:
                r = Report(
                    snapshot_id=getattr(snapshot, "id", None),
                    report_type="pdf",
                    storage_uri=uri,
                    sha256=pdf_hash,
                    created_at=datetime.now(timezone.utc),
                )
                s.add(r)
                s.commit()
    except Exception:
        pass

    return pdf_bytes, pdf_hash


def build_evidence_pack(snapshot_id: int) -> bytes:
    """Evidence Pack ZIP üretir (manifest + inputs + snapshot + report + evidence docs).

    Amaç: Audit-ready, doğrulanabilir bir paket üretmek.
    Import sırası / model farklılıkları gibi sebeplerle sayfa açılışını bozmamak için
    tüm DB ve model importları lazy yapılır.
    """
    from src.db.session import db
    from sqlalchemy import select

    # Modelleri toleranslı al
    try:
        from src.db.models import CalculationSnapshot, EmissionFactor, EvidenceDocument, Methodology
    except Exception:
        CalculationSnapshot = None  # type: ignore
        EmissionFactor = None  # type: ignore
        EvidenceDocument = None  # type: ignore
        Methodology = None  # type: ignore

    if CalculationSnapshot is None:
        # Model yüklenemiyorsa, boş zip döndür (sayfa yine de açılır)
        out = io.BytesIO()
        with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr("manifest.json", _json_bytes({"error": "CalculationSnapshot modeli yüklenemedi"}))
        return out.getvalue()

    with db() as s:
        snapshot = s.get(CalculationSnapshot, int(snapshot_id))
        if not snapshot:
            out = io.BytesIO()
            with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
                z.writestr("manifest.json", _json_bytes({"error": "Snapshot bulunamadı", "snapshot_id": snapshot_id}))
            return out.getvalue()

    inputs = _snapshot_input_uris(snapshot)

    energy_bytes = _safe_read_bytes((inputs.get("energy") or {}).get("uri") or "")
    prod_bytes = _safe_read_bytes((inputs.get("production") or {}).get("uri") or "")
    mat_bytes = _safe_read_bytes((inputs.get("materials") or {}).get("uri") or "")

    # factors
    factor_versions: List[Dict[str, Any]] = []
    factors_json: List[Dict[str, Any]] = []
    try:
        if EmissionFactor is not None:
            with db() as s:
                rows = s.execute(select(EmissionFactor).order_by(EmissionFactor.id.asc()).limit(2000)).scalars().all()
                for f in rows:
                    d = {
                        "id": getattr(f, "id", None),
                        "factor_type": getattr(f, "factor_type", None),
                        "region": getattr(f, "region", None),
                        "year": getattr(f, "year", None),
                        "version": getattr(f, "version", None),
                        "value": getattr(f, "value", None),
                        "unit": getattr(f, "unit", None),
                        "source": getattr(f, "source", None),
                    }
                    factors_json.append(d)
                for d in factors_json:
                    factor_versions.append(
                        {
                            "factor_type": d.get("factor_type"),
                            "version": d.get("version"),
                            "region": d.get("region"),
                            "year": d.get("year"),
                        }
                    )
    except Exception:
        factor_versions = []
        factors_json = []

    # methodology
    methodology_version = None
    meth_obj: Dict[str, Any] = {}
    try:
        meth_id = getattr(snapshot, "methodology_id", None)
        if meth_id and Methodology is not None:
            with db() as s:
                m = s.get(Methodology, int(meth_id))
                if m:
                    methodology_version = getattr(m, "version", None)
                    meth_obj = {
                        "id": getattr(m, "id", None),
                        "name": getattr(m, "name", None),
                        "description": getattr(m, "description", None),
                        "scope": getattr(m, "scope", None),
                        "version": getattr(m, "version", None),
                    }
    except Exception:
        methodology_version = None
        meth_obj = {}

    # report pdf
    pdf_bytes, report_hash = _ensure_pdf_for_snapshot(snapshot)

    # evidence docs
    evidence_manifest: List[Dict[str, Any]] = []
    evidence_files_to_zip: List[Tuple[str, bytes]] = []
    try:
        if EvidenceDocument is not None:
            with db() as s:
                rows = (
                    s.execute(
                        select(EvidenceDocument)
                        .where(EvidenceDocument.snapshot_id == getattr(snapshot, "id", None))
                        .order_by(EvidenceDocument.created_at.asc())
                    )
                    .scalars()
                    .all()
                )
                for e in rows:
                    uri = str(getattr(e, "storage_uri", "") or "")
                    bts = _safe_read_bytes(uri) if uri else b""
                    cat = getattr(e, "category", "") or "documents"
                    if cat not in EVIDENCE_DOCS_CATEGORIES:
                        cat = "documents"
                    fname = Path(uri).name if uri else f"evidence_{getattr(e,'id', 'na')}.bin"
                    zip_path = f"evidence/{cat}/{fname}"
                    evidence_manifest.append(
                        {
                            "id": getattr(e, "id", None),
                            "category": cat,
                            "filename": fname,
                            "storage_uri": uri,
                            "sha256": sha256_bytes(bts) if bts else None,
                            "created_at": (
                                getattr(e, "created_at", None).isoformat() if getattr(e, "created_at", None) else None
                            ),
                        }
                    )
                    evidence_files_to_zip.append((zip_path, bts))
    except Exception:
        evidence_manifest = []
        evidence_files_to_zip = []

    snapshot_payload = {
        "id": getattr(snapshot, "id", None),
        "project_id": getattr(snapshot, "project_id", None),
        "period": getattr(snapshot, "period", None),
        "engine_version": getattr(snapshot, "engine_version", None),
        "input_hashes_json": getattr(snapshot, "input_hashes_json", None),
        "config_json": getattr(snapshot, "config_json", None),
        "results_json": getattr(snapshot, "results_json", None),
        "result_hash": getattr(snapshot, "result_hash", None),
        "created_at": (getattr(snapshot, "created_at", None).isoformat() if getattr(snapshot, "created_at", None) else None),
        "locked": getattr(snapshot, "locked", None),
        "shared_with_client": getattr(snapshot, "shared_with_client", None),
        "previous_snapshot_hash": getattr(snapshot, "previous_snapshot_hash", None),
    }

    # Hashes
    snapshot_json_bytes = _json_bytes(snapshot_payload)
    factors_json_bytes = _json_bytes(factors_json)
    meth_json_bytes = _json_bytes(meth_obj)
    evidence_index_bytes = _json_bytes({"evidence_documents": evidence_manifest})

    manifest_base = {
        "snapshot_id": getattr(snapshot, "id", None),
        "engine_version": getattr(snapshot, "engine_version", None),
        "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "input_hashes": inputs,
        "factor_versions": factor_versions,
        "methodology_version": methodology_version,
        "previous_snapshot_hash": getattr(snapshot, "previous_snapshot_hash", None),
        "report_hash": report_hash,
        "snapshot_hash": sha256_bytes(snapshot_json_bytes),
        "factors_hash": sha256_bytes(factors_json_bytes),
        "methodology_hash": sha256_bytes(meth_json_bytes),
        "evidence_index_hash": sha256_bytes(evidence_index_bytes),
    }

    sig = _hmac_signature(_json_bytes(manifest_base))
    manifest = dict(manifest_base)
    manifest["signature"] = sig

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", _json_bytes(manifest))

        # inputs
        z.writestr("input/energy.csv", energy_bytes or b"")
        z.writestr("input/production.csv", prod_bytes or b"")
        z.writestr("input/materials.csv", mat_bytes or b"")

        # reference data
        z.writestr("factor_library/emission_factors.json", factors_json_bytes)
        z.writestr("methodology/methodology.json", meth_json_bytes)

        # snapshot + report
        z.writestr("snapshot/snapshot.json", snapshot_json_bytes)
        z.writestr("report/report.pdf", pdf_bytes or b"")

        # evidence
        z.writestr("evidence/evidence_index.json", evidence_index_bytes)
        for path_in_zip, bts in evidence_files_to_zip:
            z.writestr(path_in_zip, bts or b"")

    return out.getvalue()
