from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import bcrypt
import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import CalculationSnapshot, DatasetUpload, EvidenceDocument, Methodology, MonitoringPlan, Report, User
from src.db.session import db
from src.mrv.audit import append_audit, infer_company_id_for_snapshot, infer_company_id_for_user
from src.mrv.lineage import sha256_bytes
from src.mrv.orchestrator import run_full_snapshot
from src.services import projects as prj
from src.services.alerts import upsert_alerts, resolve_alert, list_open_alerts_for_user
from src.services.exports import build_evidence_pack, build_xlsx_from_results
from src.services.ingestion import validate_csv, data_quality_score_from_df
from src.services.reporting import build_pdf
from src.services.storage import (
    EVIDENCE_DOCS_DIR,
    EVIDENCE_DIR,
    EXPORT_DIR,
    REPORT_DIR,
    UPLOAD_DIR,
    ensure_storage_dirs,
    load_bytes,
    save_bytes,
)
from src.services.templates_xlsx import build_mrv_template_xlsx
from src.ui.components import section_header


def _safe_name(name: str) -> str:
    s = (name or "").strip().replace("\\", "/").split("/")[-1]
    return "".join([c for c in s if c.isalnum() or c in (".", "-", "_")])[:180]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(x: dict) -> str:
    try:
        return json.dumps(x, ensure_ascii=False, indent=2)
    except Exception:
        return "{}"


def _save_upload_dedup(
    project_id: int,
    dataset_type: str,
    file_name: str,
    file_bytes: bytes,
    user_id: int | None,
    data_quality_score: int | None,
    data_quality_report: dict | None,
    evidence_document_id: int | None = None,
    document_ref: str = "",
) -> str:
    sha = sha256_bytes(file_bytes)
    safe = _safe_name(file_name) or f"{dataset_type}.csv"
    fp = UPLOAD_DIR / f"project_{project_id}" / f"{dataset_type}_{sha[:10]}_{safe}"
    fp.parent.mkdir(parents=True, exist_ok=True)
    save_bytes(fp, file_bytes)
    storage_uri = str(fp.as_posix())

    with db() as s:
        du = DatasetUpload(
            project_id=int(project_id),
            dataset_type=str(dataset_type),
            original_filename=str(file_name),
            storage_uri=storage_uri,
            sha256=str(sha),
            schema_version="v1",
            data_quality_score=float(data_quality_score) if data_quality_score is not None else None,
            data_quality_report_json=_json_dumps(data_quality_report or {}),
        )
        s.add(du)
        s.commit()
        s.refresh(du)

    append_audit(
        "dataset_uploaded",
        {"dataset_type": dataset_type, "sha256": sha, "evidence_document_id": evidence_document_id, "document_ref": document_ref},
        user_id=user_id,
        company_id=None,
        entity_type="dataset_upload",
        entity_id=int(du.id),
    )

    return storage_uri


def _list_project_uploads(project_id: int) -> list[DatasetUpload]:
    with db() as s:
        return (
            s.execute(select(DatasetUpload).where(DatasetUpload.project_id == int(project_id)).order_by(DatasetUpload.uploaded_at.desc()))
            .scalars()
            .all()
        )


def _list_project_snapshots(project_id: int) -> list[CalculationSnapshot]:
    with db() as s:
        return (
            s.execute(select(CalculationSnapshot).where(CalculationSnapshot.project_id == int(project_id)).order_by(CalculationSnapshot.created_at.desc()))
            .scalars()
            .all()
        )


def _read_snapshot_results(snapshot: CalculationSnapshot) -> dict:
    try:
        return json.loads(snapshot.results_json or "{}")
    except Exception:
        return {}


def _create_demo_users_if_needed():
    with db() as s:
        u = s.execute(select(User).where(User.email == "admin@demo.com")).scalars().first()
        if not u:
            pw = bcrypt.hashpw("ChangeMe123!".encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
            company_id = prj.ensure_demo_company().id
            u = User(email="admin@demo.com", password_hash=pw, role="consultant_admin", company_id=int(company_id), is_active=True)
            s.add(u)
        v = s.execute(select(User).where(User.email == "verifier@demo.com")).scalars().first()
        if not v:
            pw = bcrypt.hashpw("ChangeMe123!".encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
            company_id = prj.ensure_demo_company().id
            v = User(email="verifier@demo.com", password_hash=pw, role="verifier", company_id=int(company_id), is_active=True)
            s.add(v)
        c = s.execute(select(User).where(User.email == "client@demo.com")).scalars().first()
        if not c:
            pw = bcrypt.hashpw("ChangeMe123!".encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
            company_id = prj.ensure_demo_company().id
            c = User(email="client@demo.com", password_hash=pw, role="client", company_id=int(company_id), is_active=True)
            s.add(c)
        s.commit()


def consultant_app(user):
    ensure_storage_dirs()
    _create_demo_users_if_needed()

    st.title("Consultant Panel")
    st.caption("Faz 2: Dashboard + Alerts + Excel ingestion + Verifier portal desteği")

    company_id = prj.require_company_id(user)

    section_header("Tesisler")
    facs = prj.list_facilities(company_id)
    with st.expander("Tesis oluştur", expanded=False):
        with st.form("create_facility"):
            name = st.text_input("Tesis adı")
            country = st.text_input("Ülke kodu", value="TR")
            sector = st.text_input("Sektör", value="")
            ok = st.form_submit_button("Oluştur", type="primary")
        if ok:
            try:
                prj.create_facility(company_id, name=name, country_code=country, sector=sector)
                st.success("Tesis oluşturuldu.")
                st.rerun()
            except Exception as e:
                st.error(f"Tesis oluşturulamadı: {e}")

    if facs:
        st.dataframe(pd.DataFrame([{"id": f.id, "name": f.name, "country_code": f.country_code, "sector": f.sector} for f in facs]), use_container_width=True)
    else:
        st.info("Henüz tesis yok.")

    st.divider()
    section_header("Projeler")
    projs = prj.list_company_projects_for_user(user)

    with st.expander("Proje oluştur", expanded=False):
        with st.form("create_project"):
            pname = st.text_input("Proje adı")
            fopts = ["(boş)"] + [f"{f.id} • {f.name}" for f in facs]
            fsel = st.selectbox("Facility", fopts, index=0)
            pdesc = st.text_area("Açıklama", value="")
            ok2 = st.form_submit_button("Oluştur", type="primary")
        if ok2:
            try:
                fid = None
                if fsel != "(boş)":
                    fid = int(str(fsel).split("•")[0].strip())
                prj.create_project(company_id, facility_id=fid, name=pname, description=pdesc)
                st.success("Proje oluşturuldu.")
                st.rerun()
            except Exception as e:
                st.error(f"Proje oluşturulamadı: {e}")

    if not projs:
        st.info("Henüz proje yok.")
        return

    pmap = {f"{p.id} • {p.name}": int(p.id) for p in projs}
    plabel = st.selectbox("Aktif proje", list(pmap.keys()))
    project_id = pmap[plabel]

    st.divider()
    section_header("Uploads (CSV + XLSX)")

    # Evidence document selector (opsiyonel)
    with db() as s:
        docs = s.execute(select(EvidenceDocument).where(EvidenceDocument.project_id == int(project_id)).order_by(EvidenceDocument.uploaded_at.desc())).scalars().all()
    doc_label = "(yok)"
    evidence_document_id = None
    if docs:
        dmap = {"(yok)": None}
        for d in docs[:200]:
            dmap[f"{d.id} • {d.original_filename or ''} • {str(d.uploaded_at)[:19]}"] = int(d.id)
        doc_label = st.selectbox("Evidence dokümanı ile ilişkilendir (opsiyonel)", list(dmap.keys()), index=0)
        evidence_document_id = dmap.get(doc_label)

    document_ref_text = f"EvidenceDocument:{int(evidence_document_id)}" if evidence_document_id else ""

    col1, col2, col3 = st.columns(3)
    with col1:
        up_energy = st.file_uploader("energy.csv yükleyin", type=["csv"], key=f"energy_{project_id}")
    with col2:
        up_prod = st.file_uploader("production.csv yükleyin", type=["csv"], key=f"prod_{project_id}")
    with col3:
        up_mat = st.file_uploader("materials.csv (precursor) yükleyin", type=["csv"], key=f"mat_{project_id}")

    def _handle_upload(uploaded, dtype: str):
        if uploaded is None:
            return
        try:
            b = uploaded.getvalue()
            df = pd.read_csv(pd.io.common.BytesIO(b))
            errs = validate_csv(dtype, df)
            if errs:
                st.error(f"{dtype} validation hataları: {errs}")
                return
            dq_score, dq_report = data_quality_score_from_df(dtype, df)
            _save_upload_dedup(
                project_id=int(project_id),
                dataset_type=dtype,
                file_name=str(uploaded.name),
                file_bytes=b,
                user_id=getattr(user, "id", None),
                data_quality_score=int(dq_score) if dq_score is not None else None,
                data_quality_report=dq_report,
                evidence_document_id=evidence_document_id,
                document_ref=document_ref_text,
            )
            st.success(f"{dtype} yüklendi.")
        except Exception as e:
            st.error(f"Yükleme başarısız: {e}")

    if up_energy:
        _handle_upload(up_energy, "energy")
    if up_prod:
        _handle_upload(up_prod, "production")
    if up_mat:
        _handle_upload(up_mat, "materials")

    # ---- Faz 2: XLSX ingestion + template download ----
    st.markdown("### Excel (XLSX) Ingestion (Faz 2)")
    colx1, colx2 = st.columns([2, 1])
    with colx1:
        up_xlsx = st.file_uploader("MRV Template XLSX yükleyin (energy/production/materials sheet)", type=["xlsx"], key=f"xlsx_{project_id}")
    with colx2:
        tpl = build_mrv_template_xlsx()
        st.download_button(
            "📥 XLSX Template indir",
            data=tpl,
            file_name="mrv_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    if up_xlsx is not None:
        try:
            import io

            xbytes = up_xlsx.getvalue()
            xf = pd.ExcelFile(io.BytesIO(xbytes))
            needed = ["energy", "production", "materials"]
            missing = [s for s in needed if s not in [n.strip() for n in xf.sheet_names]]
            if missing:
                st.error(f"XLSX içinde eksik sheet var: {missing}. Gerekli: energy, production, materials")
            else:
                st.success("XLSX okundu. Sheet'ler CSV olarak kaydedilecek.")
                for dtype in needed:
                    df = pd.read_excel(xf, sheet_name=dtype)
                    errs = validate_csv(dtype, df)
                    if errs:
                        st.error(f"{dtype} sheet validation hataları: {errs}")
                        continue
                    csv_bytes = df.to_csv(index=False).encode("utf-8")

                    class _U:
                        def __init__(self, name, bb):
                            self.name = name
                            self._bb = bb

                        def getvalue(self):
                            return self._bb

                    _handle_upload(_U(f"{dtype}.csv", csv_bytes), dtype)
                st.success("XLSX ingestion tamamlandı.")
        except Exception as e:
            st.error(f"XLSX ingestion başarısız: {e}")

    st.divider()
    section_header("Mevcut Uploadlar")
    ups = _list_project_uploads(project_id)
    if ups:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "id": u.id,
                        "dataset_type": u.dataset_type,
                        "filename": u.original_filename,
                        "sha256": u.sha256,
                        "dq_score": u.data_quality_score,
                        "uploaded_at": str(u.uploaded_at)[:19],
                    }
                    for u in ups[:200]
                ]
            ),
            use_container_width=True,
        )
    else:
        st.caption("Henüz upload yok.")

    st.divider()
    section_header("Snapshot Workflow")
    snaps = _list_project_snapshots(project_id)
    if snaps:
        st.caption(f"Mevcut snapshot: {len(snaps)}")
    else:
        st.caption("Henüz snapshot yok.")

    with st.expander("Yeni snapshot üret", expanded=False):
        with st.form("run_snapshot"):
            share = st.checkbox("Client ile paylaş (shared_with_client=True)", value=True)
            lock = st.checkbox("Snapshot kilitle (locked=True)", value=True)
            ok = st.form_submit_button("Snapshot üret", type="primary")
        if ok:
            try:
                snap_id = run_full_snapshot(project_id=int(project_id), created_by_user_id=getattr(user, "id", None))
                with db() as s:
                    sn = s.get(CalculationSnapshot, int(snap_id))
                    if sn:
                        sn.shared_with_client = bool(share)
                        sn.locked = bool(lock)
                        s.add(sn)
                        s.commit()
                # Faz 2: alerts
                try:
                    with db() as s:
                        sn = s.get(CalculationSnapshot, int(snap_id))
                    if sn:
                        upsert_alerts(company_id=int(company_id), snapshot=sn)
                except Exception:
                    pass

                st.success(f"Snapshot üretildi: #{snap_id}")
                st.rerun()
            except Exception as e:
                st.error(f"Snapshot üretilemedi: {e}")

    if snaps:
        st.subheader("Snapshotlar")
        labels = [f"#{s.id} • {str(s.created_at)[:19]} • shared={bool(s.shared_with_client)} • locked={bool(s.locked)}" for s in snaps[:200]]
        smap = {labels[i]: int(snaps[i].id) for i in range(len(labels))}
        ssel = st.selectbox("Snapshot seç", list(smap.keys()))
        sid = smap[ssel]
        with db() as s:
            sn = s.get(CalculationSnapshot, int(sid))

        if sn:
            res = _read_snapshot_results(sn)
            with st.expander("Snapshot JSON (kısaltılmış)", expanded=False):
                st.json(
                    {
                        "kpis": res.get("kpis", {}),
                        "compliance_checks": (res.get("compliance_checks") or [])[:50],
                        "qa_flags": (res.get("qa_flags") or [])[:50],
                    }
                )

            st.divider()
            cA, cB, cC = st.columns(3)
            with cA:
                if st.button("Evidence Pack oluştur", type="primary"):
                    data = build_evidence_pack(int(sn.id))
                    st.download_button(
                        "Evidence Pack ZIP indir",
                        data=data,
                        file_name=f"evidence_pack_snapshot_{sn.id}.zip",
                        mime="application/zip",
                        use_container_width=True,
                    )
            with cB:
                if st.button("XLSX export"):
                    x = build_xlsx_from_results(int(sn.id))
                    st.download_button(
                        "XLSX indir",
                        data=x,
                        file_name=f"results_snapshot_{sn.id}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )
            with cC:
                if st.button("PDF export"):
                    pdf = build_pdf(int(sn.id))
                    st.download_button(
                        "PDF indir",
                        data=pdf,
                        file_name=f"report_snapshot_{sn.id}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )

    st.divider()
    section_header("Alerts (Faz 2)")
    alerts = list_open_alerts_for_user(user, limit=100)
    if alerts:
        adf = pd.DataFrame(
            [
                {
                    "id": a.id,
                    "severity": a.severity,
                    "title": a.title,
                    "message": a.message,
                    "snapshot_id": a.snapshot_id,
                    "created_at": str(a.created_at)[:19],
                }
                for a in alerts
            ]
        )
        st.dataframe(adf, use_container_width=True)

        if _is_consultant(user):
            aid = st.number_input("Resolve Alert ID", min_value=0, step=1, value=0)
            if st.button("Alert'ı kapat"):
                try:
                    resolve_alert(user, int(aid))
                    st.success("Alert kapatıldı.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Alert kapatılamadı: {e}")
    else:
        st.caption("Açık alert yok.")
