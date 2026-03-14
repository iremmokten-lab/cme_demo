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
from src.mrv.replay import replay
from src.services.snapshots import lock_snapshot, set_snapshot_shared_with_client
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





def _render_project_setup(user):
    company_id = prj.require_company_id(user)
    companies = prj.list_companies_for_user(user)
    company = companies[0] if companies else None

    st.markdown("### Tesis / Proje Oluştur")
    if company is not None:
        st.caption(f"Aktif şirket: {getattr(company, 'name', 'Demo Company')} (#{getattr(company, 'id', company_id)})")

    facilities = prj.list_facilities(company_id)
    facility_options = {"Tesis seçilmesin": 0}
    for fac in facilities:
        facility_options[f"{fac.name} (#{fac.id})"] = int(fac.id)

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("#### Yeni Tesis")
        with st.form("create_facility_form", clear_on_submit=True):
            facility_name = st.text_input("Tesis adı", placeholder="Örn. Dilovası Tesisi")
            facility_country = st.text_input("Ülke kodu", value="TR", max_chars=2)
            facility_sector = st.text_input("Sektör", placeholder="Örn. Çimento")
            create_facility_btn = st.form_submit_button("Tesis oluştur", type="primary", use_container_width=True)

        if create_facility_btn:
            try:
                fac = prj.create_facility(
                    company_id=company_id,
                    name=facility_name,
                    country_code=facility_country or "TR",
                    sector=facility_sector or "",
                )
                append_audit(
                    "facility_created",
                    {"company_id": int(company_id), "facility_id": int(fac.id), "name": fac.name},
                    user_id=getattr(user, "id", None),
                    company_id=int(company_id),
                    entity_type="facility",
                    entity_id=int(fac.id),
                )
                st.success(f"Tesis oluşturuldu: {fac.name} (#{fac.id})")
                st.rerun()
            except Exception as e:
                st.error(f"Tesis oluşturulamadı: {e}")

    with c2:
        st.markdown("#### Yeni Proje")
        with st.form("create_project_form", clear_on_submit=True):
            project_name = st.text_input("Proje adı", placeholder="Örn. 2026 Q1 MRV")
            project_description = st.text_area("Proje açıklaması", value="", placeholder="Opsiyonel açıklama")
            selected_facility_label = st.selectbox("Bağlı tesis", list(facility_options.keys()))
            create_project_btn = st.form_submit_button("Proje oluştur", type="primary", use_container_width=True)

        if create_project_btn:
            try:
                selected_facility_id = int(facility_options.get(selected_facility_label, 0))
                project = prj.create_project(
                    company_id=company_id,
                    facility_id=selected_facility_id or None,
                    name=project_name,
                    description=project_description or "",
                )
                append_audit(
                    "project_created",
                    {
                        "company_id": int(company_id),
                        "project_id": int(project.id),
                        "facility_id": int(selected_facility_id) if selected_facility_id else None,
                        "name": project.name,
                    },
                    user_id=getattr(user, "id", None),
                    company_id=int(company_id),
                    entity_type="project",
                    entity_id=int(project.id),
                )
                st.success(f"Proje oluşturuldu: {project.name} (#{project.id})")
                st.rerun()
            except Exception as e:
                st.error(f"Proje oluşturulamadı: {e}")

    st.markdown("### Mevcut Kayıtlar")
    facilities = prj.list_facilities(company_id)
    projects = prj.list_projects(company_id)

    left, right = st.columns(2)
    with left:
        if facilities:
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "id": f.id,
                            "tesis": f.name,
                            "ülke": getattr(f, "country_code", "") or getattr(f, "country", ""),
                            "sektör": getattr(f, "sector", "") or "",
                        }
                        for f in facilities
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Henüz tesis yok.")

    with right:
        if projects:
            fac_name = {int(f.id): f.name for f in facilities}
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "id": p.id,
                            "proje": p.name,
                            "tesis": fac_name.get(int(p.facility_id), "-") if getattr(p, "facility_id", None) else "-",
                            "açıklama": (getattr(p, "description", "") or "")[:120],
                        }
                        for p in projects
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Henüz proje yok.")

def consultant_app(user):
    _bootstrap_demo_users()

    st.title("Consultant Panel")
    st.caption("Faz 2: Dashboard + Alerts + Excel ingestion + Verifier portal desteği")

    # Project selection
    projs = prj.list_company_projects_for_user(user)
    if not projs:
        st.info("Henüz proje yok. Aşağıdan tesis ve proje oluşturabilirsiniz.")
        _render_project_setup(user)
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
        st.subheader("CSV Uploads (energy / production / materials / cbam_defaults)")
        st.caption("Yeni şema: energy(month, facility_id, fuel_type, fuel_quantity, fuel_unit)")

        up_energy = st.file_uploader("energy.csv", type=["csv"], key="up_energy")
        up_prod = st.file_uploader("production.csv", type=["csv"], key="up_prod")
        up_mat = st.file_uploader("materials.csv (precursor)", type=["csv"], key="up_mat")
        up_cbam_def = st.file_uploader("cbam_defaults.csv (DEFAULT intensiteler)", type=["csv"], key="up_cbam_def")

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
            _handle_upload(up_cbam_def, "cbam_defaults")
        except Exception as e:
            st.error("Upload hatası")
            st.exception(e)

        st.markdown("### Excel (XLSX) Ingestion (Faz 2)")
        st.caption("CSV yanında XLSX template ingestion. Sheet isimleri: energy, production, materials, cbam_defaults")

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
                required = ("energy", "production")
                missing = [k for k in required if k not in sheets]
                if missing:
                    st.error(f"XLSX içinde eksik sheet var: {missing}. Gerekli: {list(required)} | Opsiyonel: ['materials', 'cbam_defaults']")
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

                st.subheader("🧾 Snapshot Kontrolleri (Audit-ready)")
                colA, colB, colC = st.columns(3)
                with colA:
                    if st.button("Replay Doğrula", use_container_width=True):
                        try:
                            rep = replay(int(sn.id))
                            ok_in = rep["checks"]["input_hash_match"]
                            ok_out = rep["checks"]["result_hash_match"]
                            if ok_in and ok_out:
                                st.success("Replay doğrulandı: input_hash ve result_hash eşleşiyor ✅")
                            else:
                                st.error("Replay doğrulanamadı ❌")
                            st.json(rep)
                        except Exception as e:
                            st.error(f"Replay hatası: {e}")
                
                with colB:
                    if st.button("Snapshot'ı Kilitle (Immutable)", disabled=bool(sn.locked), use_container_width=True):
                        try:
                            sn2 = lock_snapshot(int(sn.id), user=user)
                            st.success("Snapshot kilitlendi. Artık değiştirilemez ve silinemez ✅")
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))
                
                with colC:
                    shared = st.toggle("Client ile Paylaş", value=bool(sn.shared_with_client))
                    if shared != bool(sn.shared_with_client):
                        try:
                            set_snapshot_shared_with_client(int(sn.id), bool(shared), user=user)
                            st.success("Paylaşım ayarı güncellendi ✅")
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))
                
                st.caption(f"input_hash: {getattr(sn,'input_hash','')}")
                st.caption(f"result_hash: {getattr(sn,'result_hash','')}")
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
        _render_project_setup(user)
        st.divider()
        st.markdown(
            """
- Admin: **admin@demo.com / ChangeMe123!**  
- Client: **client@demo.com / ChangeMe123!**  
- Verifier: **verifier@demo.com / ChangeMe123!**

Verifier Portal sayfası: sol menüden açılabilir (verifier rolü gerekir).
"""
        )
