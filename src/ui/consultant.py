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
from src.services import projects as prj
from src.services.exports import build_evidence_pack, build_zip, build_xlsx_from_results
from src.services.ingestion import data_quality_assess, validate_csv
from src.services.reporting import build_pdf
from src.services.storage import EVIDENCE_DOCS_CATEGORIES, EVIDENCE_DOCS_DIR, UPLOAD_DIR, write_bytes
from src.services.workflow import run_full
from src.services.templates_xlsx import build_mrv_template_xlsx
from src.services.ingestion import read_xlsx_sheets


def _hash_pw(pw: str) -> str:
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _safe_name(name: str) -> str:
    return (name or "").replace("/", "_").replace("\\", "_").strip()


def _fmt_tr(x, digits=2) -> str:
    try:
        s = f"{float(x):,.{digits}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "0"


def _read_results(snapshot: CalculationSnapshot) -> dict:
    try:
        return json.loads(snapshot.results_json) if snapshot.results_json else {}
    except Exception:
        return {}


def _get_scenario_from_results(results: dict) -> dict:
    if not isinstance(results, dict):
        return {}

    scen = results.get("scenario")
    if isinstance(scen, dict) and scen:
        return scen

    name = results.get("scenario_name")
    params = results.get("scenario_params")
    if isinstance(name, str) and name.strip():
        out = {"name": name.strip()}
        if isinstance(params, dict):
            out.update(params)
        return out

    return {}


def _ensure_scenario_metadata_in_snapshot(snapshot_id: int):
    try:
        with db() as s:
            sn = s.get(CalculationSnapshot, int(snapshot_id))
            if not sn:
                return
            res = _read_results(sn)
            scen = _get_scenario_from_results(res)
            if scen and isinstance(res, dict):
                res["scenario"] = scen
                sn.results_json = json.dumps(res, ensure_ascii=False)
                s.add(sn)
                s.commit()
    except Exception:
        pass


def _save_upload_dedup(project_id: int, dataset_type: str, file_name: str, file_bytes: bytes) -> tuple[str, str]:
    sha = sha256_bytes(file_bytes)
    safe = _safe_name(file_name) or f"{dataset_type}.csv"
    fp = UPLOAD_DIR / f"project_{project_id}" / f"{dataset_type}_{sha[:10]}_{safe}"
    fp.parent.mkdir(parents=True, exist_ok=True)
    write_bytes(fp, file_bytes)
    return str(fp.as_posix()), sha


def _save_evidence_document(project_id: int, category: str, file_name: str, file_bytes: bytes, user_id: int | None, notes: str = "") -> EvidenceDocument:
    sha = sha256_bytes(file_bytes)
    safe = _safe_name(file_name)
    folder = EVIDENCE_DOCS_DIR / f"project_{project_id}" / str(category)
    folder.mkdir(parents=True, exist_ok=True)
    fp = folder / f"{sha[:10]}_{safe}"
    write_bytes(fp, file_bytes)
    storage_uri = str(fp.as_posix())

    with db() as s:
        doc = EvidenceDocument(
            project_id=int(project_id),
            category=str(category),
            original_filename=str(file_name),
            storage_uri=storage_uri,
            sha256=str(sha),
            notes=str(notes or ""),
        )
        s.add(doc)
        s.commit()
        s.refresh(doc)

    append_audit(
        "evidence_doc_saved",
        {"project_id": int(project_id), "category": str(category), "filename": str(file_name), "sha256": str(sha)},
        user_id=user_id,
        company_id=None,
        entity_type="evidence_document",
        entity_id=getattr(doc, "id", None),
    )

    return doc


def _bootstrap_demo_users():
    with db() as s:
        c = s.execute(select(prj.Company).order_by(prj.Company.id).limit(1)).scalars().first()  # type: ignore
        if not c:
            c = prj.Company(name="Demo Company")  # type: ignore
            s.add(c)
            s.flush()

        # admin
        u = s.execute(select(User).where(User.email == "admin@demo.com").limit(1)).scalars().first()
        if not u:
            u = User(email="admin@demo.com", password_hash=_hash_pw("ChangeMe123!"), role="consultantadmin", company_id=c.id)
            s.add(u)

        # verifier
        v = s.execute(select(User).where(User.email == "verifier@demo.com").limit(1)).scalars().first()
        if not v:
            v = User(email="verifier@demo.com", password_hash=_hash_pw("ChangeMe123!"), role="verifier", company_id=c.id)
            s.add(v)

        # client
        cl = s.execute(select(User).where(User.email == "client@demo.com").limit(1)).scalars().first()
        if not cl:
            cl = User(email="client@demo.com", password_hash=_hash_pw("ChangeMe123!"), role="client", company_id=c.id)
            s.add(cl)

        s.commit()


def consultant_app(user):
    _bootstrap_demo_users()

    st.title("Consultant Panel")
    st.caption("Faz 2: Dashboard + Alerts + Excel ingestion + Verifier portal desteği")

    # Project selection
    projs = prj.list_company_projects_for_user(user)
    if not projs:
        st.info("Henüz proje yok. Önce tesis/proje oluşturun.")
        return

    pmap = {f"{p.name} (#{p.id})": int(p.id) for p in projs}
    plabel = st.selectbox("Proje seç", list(pmap.keys()))
    project_id = pmap[plabel]

    tabs = st.tabs(
        [
            "Veri Yükleme",
            "Evidence",
            "Hesaplama",
            "Senaryolar",
            "Raporlar ve İndirme",
            "Geçmiş",
            "Kurulum",
        ]
    )

    # Uploads
    with tabs[0]:
        st.subheader("CSV Uploads (energy / production / materials)")
        st.caption("Yeni şema: energy(month, facility_id, fuel_type, fuel_quantity, fuel_unit)")

        up_energy = st.file_uploader("energy.csv", type=["csv"], key="up_energy")
        up_prod = st.file_uploader("production.csv", type=["csv"], key="up_prod")
        up_mat = st.file_uploader("materials.csv (precursor)", type=["csv"], key="up_mat")

        def _handle_upload(uploaded, dtype: str):
            if uploaded is None:
                return
            bts = uploaded.getvalue()
            df = pd.read_csv(uploaded)
            errs = validate_csv(dtype, df)
            if errs:
                st.error(" | ".join(errs))
                return

            score, report = data_quality_assess(dtype, df)
            storage_uri, sha = _save_upload_dedup(
                project_id=project_id,
                dataset_type=dtype,
                file_name=getattr(uploaded, "name", f"{dtype}.csv"),
                file_bytes=bts,
            )

            with db() as s:
                du = DatasetUpload(
                    project_id=int(project_id),
                    dataset_type=str(dtype),
                    original_filename=str(getattr(uploaded, "name", f"{dtype}.csv")),
                    storage_uri=str(storage_uri),
                    sha256=str(sha),
                    schema_version="v1",
                    data_quality_score=float(score),
                    data_quality_report_json=json.dumps(report, ensure_ascii=False),
                )
                s.add(du)
                s.commit()

            append_audit(
                "dataset_uploaded",
                {"project_id": int(project_id), "dataset_type": str(dtype), "sha256": str(sha), "dq_score": int(score)},
                user_id=getattr(user, "id", None),
                company_id=infer_company_id_for_user(user),
                entity_type="dataset_upload",
                entity_id=None,
            )

            st.success(f"{dtype}.csv yüklendi ✅ (sha={sha[:10]}…) | Data Quality: {score}/100")
            with st.expander("Data Quality raporu", expanded=False):
                st.json(report)

        try:
            _handle_upload(up_energy, "energy")
            _handle_upload(up_prod, "production")
            _handle_upload(up_mat, "materials")
        except Exception as e:
            st.error("Upload hatası")
            st.exception(e)

        st.markdown("### Excel (XLSX) Ingestion (Faz 2)")
        st.caption("CSV yanında XLSX template ingestion. Sheet isimleri: energy, production, materials")

        colx1, colx2 = st.columns([2, 1])
        with colx2:
            tpl = build_mrv_template_xlsx()
            st.download_button(
                "📥 XLSX Template indir",
                data=tpl,
                file_name="mrv_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

        with colx1:
            xfile = st.file_uploader("MRV Template XLSX yükle", type=["xlsx"], key=f"xlsx_{project_id}")

        if xfile is not None:
            import io

            try:
                sheets = read_xlsx_sheets(xfile.getvalue())
                missing = [k for k in ("energy", "production", "materials") if k not in sheets]
                if missing:
                    st.error(f"XLSX içinde eksik sheet var: {missing}. Gerekli: energy, production, materials")
                else:
                    st.success("XLSX okundu. Sheet'ler CSV gibi ingest edilecek.")
                    class _Uploaded(io.BytesIO):
                        def __init__(self, name: str, b: bytes):
                            super().__init__(b)
                            self.name = name
                        def getvalue(self):
                            return bytes(super().getvalue())

                    for dtype, df in sheets.items():
                        csv_bytes = df.to_csv(index=False).encode("utf-8")
                        _handle_upload(_Uploaded(f"{dtype}.csv", csv_bytes), dtype)
                    st.success("XLSX ingestion tamamlandı ✅")
            except Exception as e:
                st.error("XLSX ingestion başarısız")
                st.exception(e)

    # Evidence
    with tabs[1]:
        st.subheader("Evidence Dokümanları (Kurumsal)")
        st.caption("Dokümanları kategorilere göre saklar ve evidence pack’e otomatik dahil eder.")

        left, right = st.columns([2, 3])

        with left:
            cat = st.selectbox("Kategori", EVIDENCE_DOCS_CATEGORIES, index=0, key="ev_cat")
            ev_file = st.file_uploader("Doküman yükle (PDF/PNG/XLSX vb.)", type=None, key="ev_file_uploader")
            ev_notes = st.text_area("Not (opsiyonel)", value="", key="ev_notes")

            if st.button("Dokümanı kaydet", type="primary", key="btn_save_evidence_doc"):
                if not ev_file:
                    st.warning("Önce bir dosya seçin.")
                else:
                    bts = ev_file.getvalue()
                    doc = _save_evidence_document(
                        project_id=project_id,
                        category=str(cat),
                        file_name=ev_file.name,
                        file_bytes=bts,
                        user_id=getattr(user, "id", None),
                        notes=str(ev_notes or ""),
                    )
                    append_audit(
                        "evidence_document_uploaded",
                        {"project_id": int(project_id), "doc_id": int(doc.id), "category": str(cat)},
                        user_id=getattr(user, "id", None),
                        company_id=infer_company_id_for_user(user),
                        entity_type="evidence_document",
                        entity_id=int(doc.id),
                    )
                    st.success(f"Doküman kaydedildi (#{doc.id})")
                    st.rerun()

        with right:
            with db() as s:
                docs = (
                    s.execute(select(EvidenceDocument).where(EvidenceDocument.project_id == int(project_id)).order_by(EvidenceDocument.uploaded_at.desc()))
                    .scalars()
                    .all()
                )
            if docs:
                st.dataframe(
                    pd.DataFrame(
                        [
                            {
                                "id": d.id,
                                "category": d.category,
                                "filename": d.original_filename,
                                "uploaded_at": str(d.uploaded_at)[:19],
                                "sha": (d.sha256 or "")[:10],
                                "notes": (d.notes or "")[:80],
                            }
                            for d in docs[:300]
                        ]
                    ),
                    use_container_width=True,
                )
            else:
                st.info("Henüz evidence dokümanı yok.")

    # Compute
    with tabs[2]:
        st.subheader("Hesaplama (Snapshot)")
        st.caption("Deterministik snapshot üretir. Paylaşım için shared_with_client işaretleyin.")

        if st.button("Snapshot üret (run_full)", type="primary", use_container_width=True):
            try:
                snap_id = run_full(project_id=project_id, created_by_user_id=getattr(user, "id", None))
                _ensure_scenario_metadata_in_snapshot(snap_id)
                st.success(f"Snapshot üretildi: #{snap_id}")
                st.rerun()
            except Exception as e:
                st.error("Snapshot üretilemedi")
                st.exception(e)

    # Scenarios
    with tabs[3]:
        st.subheader("Senaryolar (MVP)")
        st.caption("Bu demo sürümünde senaryo parametreleri engine sonuç JSON'u üzerinden görüntülenir.")
        st.info("Faz 3'te optimizer + scenario runner genişletilecek.")

    # Reports & downloads
    with tabs[4]:
        st.subheader("Raporlar ve İndirme")
        with db() as s:
            snaps = (
                s.execute(select(CalculationSnapshot).where(CalculationSnapshot.project_id == int(project_id)).order_by(CalculationSnapshot.created_at.desc()))
                .scalars()
                .all()
            )

        if not snaps:
            st.info("Henüz snapshot yok.")
        else:
            labels = [f"#{sn.id} • {str(sn.created_at)[:19]} • shared={bool(sn.shared_with_client)} • locked={bool(sn.locked)}" for sn in snaps[:200]]
            sel = st.selectbox("Snapshot seç", labels)
            sid = int(sel.split("•")[0].replace("#", "").strip())

            with db() as s:
                sn = s.get(CalculationSnapshot, sid)

            if sn:
                c1, c2, c3 = st.columns(3)
                with c1:
                    if st.button("Evidence Pack", type="primary", use_container_width=True):
                        data = build_evidence_pack(int(sn.id))
                        st.download_button(
                            "Evidence Pack indir",
                            data=data,
                            file_name=f"evidence_pack_snapshot_{sn.id}.zip",
                            mime="application/zip",
                            use_container_width=True,
                        )
                with c2:
                    if st.button("XLSX Export", use_container_width=True):
                        x = build_xlsx_from_results(int(sn.id))
                        st.download_button(
                            "XLSX indir",
                            data=x,
                            file_name=f"results_snapshot_{sn.id}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                        )
                with c3:
                    if st.button("PDF Export", use_container_width=True):
                        pdf = build_pdf(int(sn.id))
                        st.download_button(
                            "PDF indir",
                            data=pdf,
                            file_name=f"report_snapshot_{sn.id}.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                        )

                st.divider()
                with st.expander("Snapshot JSON (kısaltılmış)", expanded=False):
                    res = _read_results(sn)
                    st.json(
                        {
                            "kpis": res.get("kpis", {}),
                            "compliance_checks": (res.get("compliance_checks") or [])[:50],
                            "qa_flags": (res.get("qa_flags") or [])[:50],
                        }
                    )

    # History
    with tabs[5]:
        st.subheader("Geçmiş")
        st.caption("Uploads / snapshots kayıtlarını DB’den listeler.")
        with db() as s:
            ups = (
                s.execute(select(DatasetUpload).where(DatasetUpload.project_id == int(project_id)).order_by(DatasetUpload.uploaded_at.desc()))
                .scalars()
                .all()
            )
        if ups:
            st.dataframe(
                pd.DataFrame(
                    [
                        {"id": u.id, "type": u.dataset_type, "file": u.original_filename, "dq": u.data_quality_score, "at": str(u.uploaded_at)[:19]}
                        for u in ups[:300]
                    ]
                ),
                use_container_width=True,
            )
        else:
            st.info("Upload yok.")

    # Setup
    with tabs[6]:
        st.subheader("Kurulum / Demo Bilgisi")
        st.markdown(
            """
- Admin: **admin@demo.com / ChangeMe123!**  
- Client: **client@demo.com / ChangeMe123!**  
- Verifier: **verifier@demo.com / ChangeMe123!**

Verifier Portal sayfası: sol menüden açılabilir (verifier rolü gerekir).
"""
        )
