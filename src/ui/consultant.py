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
from src.mrv.lineage import sha256_bytes
from src.services import projects as prj
from src.services.exports import build_evidence_pack, build_zip, build_xlsx_from_results
from src.services.ingestion import data_quality_assess, validate_csv
from src.services.reporting import build_pdf
from src.services.storage import EVIDENCE_DOCS_CATEGORIES, EVIDENCE_DOCS_DIR, UPLOAD_DIR, write_bytes
from src.services.workflow import run_full


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


def _first_existing_upload(session, project_id: int, dataset_type: str, sha: str):
    q = (
        select(DatasetUpload)
        .where(
            DatasetUpload.project_id == project_id,
            DatasetUpload.dataset_type == dataset_type,
            DatasetUpload.sha256 == sha,
        )
        .limit(1)
    )
    return session.execute(q).scalars().first()


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
    fp = UPLOAD_DIR / f"project_{project_id}" / dataset_type / f"{sha}_{safe}"

    dq_report_json = "{}"
    try:
        dq_report_json = json.dumps(data_quality_report or {}, ensure_ascii=False)
    except Exception:
        dq_report_json = "{}"

    with db() as s:
        existing = _first_existing_upload(s, project_id, dataset_type, sha)
        if existing:
            try:
                uri = getattr(existing, "storage_uri", None)
                if uri:
                    p = Path(str(uri))
                    if not p.exists():
                        write_bytes(fp, file_bytes)
                        try:
                            existing.storage_uri = str(fp)
                            s.add(existing)
                            s.commit()
                        except Exception:
                            s.rollback()
                else:
                    if not fp.exists():
                        write_bytes(fp, file_bytes)
            except Exception:
                pass

            # Paket B: data quality / evidence link update (best-effort)
            try:
                changed = False
                if existing.data_quality_score is None and data_quality_score is not None:
                    existing.data_quality_score = int(data_quality_score)
                    changed = True
                if (existing.data_quality_report_json in (None, "", "{}")) and dq_report_json not in (None, "", "{}"):
                    existing.data_quality_report_json = dq_report_json
                    changed = True
                if evidence_document_id and not getattr(existing, "evidence_document_id", None):
                    existing.evidence_document_id = int(evidence_document_id)
                    changed = True
                if document_ref and not (existing.document_ref or ""):
                    existing.document_ref = str(document_ref)
                    changed = True
                if changed:
                    s.add(existing)
                    s.commit()
            except Exception:
                s.rollback()

            return existing.sha256 or sha

    write_bytes(fp, file_bytes)
    u = DatasetUpload(
        project_id=project_id,
        dataset_type=dataset_type,
        original_filename=safe,
        sha256=sha,
        storage_uri=str(fp),
        uploaded_by_user_id=user_id,
        evidence_document_id=evidence_document_id,
        document_ref=document_ref or "",
        data_quality_score=int(data_quality_score) if data_quality_score is not None else None,
        data_quality_report_json=dq_report_json,
    )
    with db() as s:
        s.add(u)
        s.commit()
    return sha


def _list_methodologies() -> list[Methodology]:
    with db() as s:
        return s.execute(select(Methodology).order_by(Methodology.created_at.desc())).scalars().all()


def _get_methodology_dict(m: Methodology | None) -> dict | None:
    if not m:
        return None
    return {
        "id": m.id,
        "name": m.name,
        "description": m.description,
        "scope": m.scope,
        "version": m.version,
        "created_at": (m.created_at.isoformat() if getattr(m, "created_at", None) else None),
    }


def _latest_monitoring_plan(facility_id: int) -> MonitoringPlan | None:
    with db() as s:
        return (
            s.execute(
                select(MonitoringPlan)
                .where(MonitoringPlan.facility_id == facility_id)
                .order_by(MonitoringPlan.updated_at.desc(), MonitoringPlan.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )


def _upsert_monitoring_plan(
    facility_id: int,
    method: str,
    tier_level: str,
    data_source: str,
    qa_procedure: str,
    responsible_person: str,
) -> None:
    now = datetime.now(timezone.utc)
    with db() as s:
        mp = (
            s.execute(
                select(MonitoringPlan)
                .where(MonitoringPlan.facility_id == facility_id)
                .order_by(MonitoringPlan.updated_at.desc(), MonitoringPlan.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        if not mp:
            mp = MonitoringPlan(
                facility_id=facility_id,
                method=method,
                tier_level=tier_level,
                data_source=data_source,
                qa_procedure=qa_procedure,
                responsible_person=responsible_person,
                created_at=now,
                updated_at=now,
            )
            s.add(mp)
        else:
            mp.method = method
            mp.tier_level = tier_level
            mp.data_source = data_source
            mp.qa_procedure = qa_procedure
            mp.responsible_person = responsible_person
            mp.updated_at = now
            s.add(mp)
        s.commit()


def _save_evidence_document(
    project_id: int,
    category: str,
    file_name: str,
    file_bytes: bytes,
    user_id: int | None,
    notes: str = "",
) -> EvidenceDocument:
    sha = sha256_bytes(file_bytes)
    safe = _safe_name(file_name) or "evidence.bin"
    cat = (category or "documents").strip()
    if cat not in EVIDENCE_DOCS_CATEGORIES:
        cat = "documents"

    # path: storage/evidence/<cat>/<project_id>/<sha>_<filename>
    fp = EVIDENCE_DOCS_DIR / cat / f"project_{project_id}" / f"{sha}_{safe}"
    write_bytes(fp, file_bytes)

    with db() as s:
        # dedup: aynı sha + project + category
        existing = (
            s.execute(
                select(EvidenceDocument)
                .where(
                    EvidenceDocument.project_id == project_id,
                    EvidenceDocument.category == cat,
                    EvidenceDocument.sha256 == sha,
                )
                .limit(1)
            )
            .scalars()
            .first()
        )
        if existing:
            # dosya yoksa yeniden yazılmış olabilir; storage_uri check
            try:
                p = Path(str(existing.storage_uri))
                if not p.exists():
                    existing.storage_uri = str(fp)
                    s.add(existing)
                    s.commit()
            except Exception:
                pass
            return existing

        doc = EvidenceDocument(
            project_id=project_id,
            category=cat,
            original_filename=safe,
            sha256=sha,
            storage_uri=str(fp),
            uploaded_by_user_id=user_id,
            notes=notes or "",
        )
        s.add(doc)
        s.commit()
        s.refresh(doc)
        return doc


def consultant_app(user):
    st.title("Danışman Kontrol Paneli")

    companies = prj.list_companies_for_user(user)
    if not companies:
        st.warning("Bu kullanıcı için şirket bulunamadı.")
        return

    with st.sidebar:
        st.markdown("### Şirket")
        company_map = {c.name: c.id for c in companies}
        company_name = st.selectbox("Şirket seçin", list(company_map.keys()), index=0)
        company_id = company_map[company_name]

        st.markdown("### Tesis")
        facilities = prj.list_facilities(company_id)
        fac_opts = {"(yok)": None}
        for f in facilities:
            label = f"{f.name}"
            if getattr(f, "country", None):
                label += f" ({f.country})"
            fac_opts[label] = f.id
        fac_label = st.selectbox("Tesis seçin", list(fac_opts.keys()), index=0)
        facility_id = fac_opts[fac_label]

        with st.expander("Yeni tesis oluştur"):
            fn = st.text_input("Tesis adı", key="new_facility_name")
            cc = st.text_input("Ülke", value="TR", key="new_facility_country")
            ss = st.text_input("Sektör", value="", key="new_facility_sector")
            if st.button("Tesis ekle", key="btn_add_facility"):
                if not fn.strip():
                    st.warning("Tesis adı boş olamaz.")
                else:
                    prj.create_facility(company_id, fn, cc, ss)
                    st.success("Tesis oluşturuldu.")
                    st.rerun()

        st.divider()
        st.markdown("### ETS Monitoring Plan")
        if facility_id:
            mp = _latest_monitoring_plan(int(facility_id))
            with st.expander("Monitoring Plan (oluştur / güncelle)", expanded=False):
                method = st.selectbox(
                    "Yöntem",
                    ["standard", "mass_balance", "calculation", "measurement"],
                    index=0 if not mp else (["standard", "mass_balance", "calculation", "measurement"].index(mp.method) if mp.method in ["standard", "mass_balance", "calculation", "measurement"] else 0),
                    key="mp_method",
                )
                tier = st.selectbox(
                    "Tier seviyesi",
                    ["Tier 1", "Tier 2", "Tier 3"],
                    index=1 if not mp else (["Tier 1", "Tier 2", "Tier 3"].index(mp.tier_level) if mp.tier_level in ["Tier 1", "Tier 2", "Tier 3"] else 1),
                    key="mp_tier",
                )
                data_source = st.text_input("Veri kaynağı", value="" if not mp else (mp.data_source or ""), key="mp_source")
                responsible = st.text_input("Sorumlu kişi", value="" if not mp else (mp.responsible_person or ""), key="mp_resp")
                qa_proc = st.text_area("QA prosedürü (özet)", value="" if not mp else (mp.qa_procedure or ""), key="mp_qa")

                if st.button("Monitoring Plan kaydet", type="primary", key="btn_save_mp"):
                    _upsert_monitoring_plan(
                        facility_id=int(facility_id),
                        method=str(method),
                        tier_level=str(tier),
                        data_source=str(data_source),
                        qa_procedure=str(qa_proc),
                        responsible_person=str(responsible),
                    )
                    st.success("Monitoring Plan kaydedildi ✅")
                    st.rerun()
        else:
            st.caption("Monitoring Plan için önce tesis seçin.")

        st.divider()
        st.markdown("### Proje")
        projects = prj.list_projects(company_id)

        NEW_LABEL = "(yeni proje oluştur)"
        proj_items = [(f"{p.name} / {p.year} (id:{p.id})", p.id) for p in projects]
        labels = [lbl for lbl, _ in proj_items] + [NEW_LABEL]
        id_by_label = {lbl: pid for lbl, pid in proj_items}

        if "selected_project_id" not in st.session_state:
            st.session_state["selected_project_id"] = proj_items[0][1] if proj_items else None

        default_index = 0
        if proj_items and st.session_state["selected_project_id"]:
            for i, (_, pid) in enumerate(proj_items):
                if pid == st.session_state["selected_project_id"]:
                    default_index = i
                    break
        else:
            default_index = len(labels) - 1

        psel = st.selectbox("Proje seçin", labels, index=default_index, key="project_selectbox_ui")

        if psel == NEW_LABEL:
            pn = st.text_input("Proje adı", key="new_project_name")
            py = st.number_input("Yıl", 2000, 2100, 2025, key="new_project_year")
            if st.button("Proje oluştur", type="primary", key="btn_create_project"):
                if not pn.strip():
                    st.warning("Proje adı boş olamaz.")
                else:
                    newp = prj.create_project(company_id, facility_id, pn, int(py))
                    st.session_state["selected_project_id"] = newp.id
                    st.success(f"Proje oluşturuldu: id={newp.id}")
                    st.rerun()
            st.info("Devam etmek için proje oluşturun veya mevcut bir proje seçin.")
            st.stop()

        project_id = id_by_label.get(psel)
        if not project_id:
            st.error("Seçili proje bulunamadı.")
            st.stop()

        st.session_state["selected_project_id"] = project_id

        st.divider()
        st.markdown("### Parametreler")

        region = st.text_input("Bölge/Ülke (factor region)", value="TR", key="param_region")
        eua = st.slider("EUA fiyatı (€/t)", 0.0, 300.0, 80.0, key="param_eua")
        fx = st.number_input("Kur (TL/€)", value=35.0, key="param_fx")
        free_alloc = st.number_input("Ücretsiz tahsis (tCO2)", value=0.0, key="param_free")
        banked = st.number_input("Banked / devreden (tCO2)", value=0.0, key="param_banked")

        st.markdown("#### Elektrik Emisyon Metodu")
        elec_method = st.selectbox("Metod", ["location", "market"], index=0, key="param_elec_method")
        market_override = st.number_input(
            "Market-based grid factor override (kgCO2e/kWh) — opsiyonel",
            value=0.0,
            help="0 ise override uygulanmaz.",
            key="param_market_override",
        )
        cbam_alloc = st.selectbox("CBAM allocation basis", ["quantity", "export"], index=0, key="param_cbam_alloc")

        uncertainty_notes = st.text_area(
            "ETS belirsizlik notu (verification için)",
            value="",
            key="param_uncertainty",
        )

        st.divider()
        st.markdown("### Metodoloji")
        meths = _list_methodologies()
        meth_labels = ["(seçilmedi)"] + [f"{m.name} • {m.version} (id:{m.id})" for m in meths]
        meth_sel = st.selectbox("Metodoloji seçin", meth_labels, index=0, key="meth_select")
        methodology_id = None
        if meth_sel != "(seçilmedi)":
            methodology_id = meths[meth_labels.index(meth_sel) - 1].id

    config = {
        "region": str(region).strip() or "TR",
        "eua_price_eur": float(eua),
        "fx_tl_per_eur": float(fx),
        "free_alloc_t": float(free_alloc),
        "banked_t": float(banked),
        "electricity_method": str(elec_method),
        "cbam_allocation_basis": str(cbam_alloc),
        "uncertainty_notes": str(uncertainty_notes or ""),
    }
    if market_override and float(market_override) > 0.0:
        config["market_grid_factor_override"] = float(market_override)

    st.subheader("Proje Özeti")
    with db() as s:
        last_uploads = (
            s.execute(
                select(DatasetUpload)
                .where(DatasetUpload.project_id == project_id)
                .order_by(DatasetUpload.uploaded_at.desc())
                .limit(10)
            )
            .scalars()
            .all()
        )
        last_snaps = (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == project_id)
                .order_by(CalculationSnapshot.created_at.desc())
                .limit(10)
            )
            .scalars()
            .all()
        )

    u_energy = next((u for u in last_uploads if u.dataset_type == "energy"), None)
    u_prod = next((u for u in last_uploads if u.dataset_type == "production"), None)
    u_mat = next((u for u in last_uploads if u.dataset_type == "materials"), None)

    a, b, c, d = st.columns(4)
    a.metric("energy.csv", "Var ✅" if u_energy else "Yok ❌", f"DQ: {u_energy.data_quality_score}/100" if u_energy and u_energy.data_quality_score is not None else "")
    b.metric("production.csv", "Var ✅" if u_prod else "Yok ❌", f"DQ: {u_prod.data_quality_score}/100" if u_prod and u_prod.data_quality_score is not None else "")
    c.metric("materials.csv", "Var ✅" if u_mat else "Yok (precursor yok)", f"DQ: {u_mat.data_quality_score}/100" if u_mat and u_mat.data_quality_score is not None else "")
    d.metric("Son snapshot", f"ID:{last_snaps[0].id}" if last_snaps else "-")

    st.divider()

    tabs = st.tabs(
        [
            "Veri Yükleme",
            "Evidence (Dokümanlar)",
            "Hesaplama",
            "Senaryolar",
            "Raporlar ve İndirme",
            "Geçmiş",
            "Kullanıcılar",
        ]
    )

    # Veri Yükleme
    with tabs[0]:
        st.subheader("CSV Yükleme (Data Quality ile)")

        st.caption(
            "Paket B: Yükleme sırasında otomatik veri kalite kontrolleri yapılır ve 0–100 skor üretilir. "
            "İstersen ilgili CSV’yi bir evidence dokümanına bağlayabilirsin (örn. fatura PDF’si)."
        )

        # Evidence seçimi (CSV upload'a bağlamak için)
        with db() as s:
            ev_docs = (
                s.execute(
                    select(EvidenceDocument)
                    .where(EvidenceDocument.project_id == project_id)
                    .order_by(EvidenceDocument.uploaded_at.desc())
                )
                .scalars()
                .all()
            )
        ev_options = {"(bağlama yok)": None}
        for ddoc in ev_docs[:200]:
            ev_options[f"{ddoc.category} • {ddoc.original_filename} • {ddoc.sha256[:10]}… (id:{ddoc.id})"] = ddoc.id
        ev_sel = st.selectbox("CSV için doküman referansı (opsiyonel)", list(ev_options.keys()), index=0, key="csv_doc_ref_sel")
        evidence_document_id = ev_options[ev_sel]
        document_ref_text = ""
        if evidence_document_id:
            document_ref_text = f"EvidenceDocument:{int(evidence_document_id)}"

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
            bts = uploaded.getvalue()
            df = pd.read_csv(uploaded)
            errs = validate_csv(dtype, df)
            if errs:
                st.error(" | ".join(errs))
                return

            score, report = data_quality_assess(dtype, df)
            sha = _save_upload_dedup(
                project_id=project_id,
                dataset_type=dtype,
                file_name=uploaded.name,
                file_bytes=bts,
                user_id=getattr(user, "id", None),
                data_quality_score=score,
                data_quality_report=report,
                evidence_document_id=int(evidence_document_id) if evidence_document_id else None,
                document_ref=document_ref_text,
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

    # Evidence
    with tabs[1]:
        st.subheader("Evidence Dokümanları (Kurumsal)")

        st.caption(
            "Paket B: Dokümanları kategorilere göre saklayıp (documents/meter_readings/invoices/contracts) "
            "CSV upload’larına referans olarak bağlayabilirsin."
        )

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
                    st.success(f"Evidence kaydedildi ✅ id={doc.id}")
                    st.rerun()

        with right:
            with db() as s:
                docs = (
                    s.execute(
                        select(EvidenceDocument)
                        .where(EvidenceDocument.project_id == project_id)
                        .order_by(EvidenceDocument.uploaded_at.desc())
                    )
                    .scalars()
                    .all()
                )

            if not docs:
                st.info("Henüz evidence dokümanı yok.")
            else:
                rows = []
                for ddoc in docs[:300]:
                    rows.append(
                        {
                            "id": ddoc.id,
                            "category": ddoc.category,
                            "filename": ddoc.original_filename,
                            "uploaded_at": ddoc.uploaded_at,
                            "sha256": (ddoc.sha256[:12] + "…") if ddoc.sha256 else "",
                            "notes": (ddoc.notes[:60] + "…") if (ddoc.notes and len(ddoc.notes) > 60) else (ddoc.notes or ""),
                        }
                    )
                st.dataframe(rows, use_container_width=True)

                # İndirme butonları
                st.markdown("#### İndir")
                pick = st.selectbox(
                    "Doküman seçin",
                    [f"{d.category} • {d.original_filename} (id:{d.id})" for d in docs[:200]],
                    index=0,
                    key="ev_dl_pick",
                )
                sel_doc = docs[[f"{d.category} • {d.original_filename} (id:{d.id})" for d in docs[:200]].index(pick)]
                p = Path(str(sel_doc.storage_uri))
                if p.exists():
                    st.download_button(
                        "Seçili dokümanı indir",
                        data=p.read_bytes(),
                        file_name=sel_doc.original_filename,
                        mime="application/octet-stream",
                        use_container_width=True,
                        key=f"ev_dl_{sel_doc.id}",
                    )
                else:
                    st.warning("Dosya bulunamadı.")

    # Hesaplama
    with tabs[2]:
        st.subheader("Baseline Hesaplama (Regülasyon Yakın Motor)")
        if st.button("Baseline çalıştır", type="primary", key="btn_run_baseline"):
            try:
                snap = run_full(
                    project_id,
                    config=config,
                    scenario=None,
                    methodology_id=methodology_id,
                    created_by_user_id=getattr(user, "id", None),
                )
                st.session_state["last_snapshot_id"] = snap.id
                st.success(f"Hesaplama tamamlandı ✅ Snapshot ID: {snap.id}")
            except Exception as e:
                st.error("Hesaplama başarısız")
                st.exception(e)

    # Senaryolar
    with tabs[3]:
        st.subheader("Senaryolar")

        left, right = st.columns(2)
        with left:
            scen_name = st.text_input("Senaryo adı", value="Senaryo 1", key="scen_name")
            renewable_share_pct = st.slider("Yenilenebilir enerji payı (%)", 0, 100, 0, key="scen_ren")
            energy_reduction_pct = st.slider("Enerji tüketimi azaltımı (%)", 0, 100, 0, key="scen_red")
        with right:
            supplier_factor_multiplier = st.slider("Tedarikçi emisyon faktörü çarpanı", 0.50, 2.00, 1.00, 0.05, key="scen_sup")
            export_mix_multiplier = st.slider("AB ihracat miktarı çarpanı", 0.00, 2.00, 1.00, 0.05, key="scen_exp")

        scenario = {
            "name": scen_name.strip() or "Senaryo",
            "renewable_share": float(renewable_share_pct) / 100.0,
            "energy_reduction_pct": float(energy_reduction_pct) / 100.0,
            "supplier_factor_multiplier": float(supplier_factor_multiplier),
            "export_mix_multiplier": float(export_mix_multiplier),
        }

        if st.button("Senaryoyu çalıştır", type="primary", key="btn_run_scenario"):
            try:
                snap = run_full(
                    project_id,
                    config=config,
                    scenario=scenario,
                    methodology_id=methodology_id,
                    created_by_user_id=getattr(user, "id", None),
                )
                st.session_state["last_snapshot_id"] = snap.id
                st.success(f"Senaryo tamamlandı ✅ Snapshot ID: {snap.id}")
                st.rerun()
            except Exception as e:
                st.error("Senaryo başarısız")
                st.exception(e)

    # Raporlar ve İndirme
    with tabs[4]:
        st.subheader("Raporlar ve İndirme")

        with db() as s:
            snaps = (
                s.execute(
                    select(CalculationSnapshot)
                    .where(CalculationSnapshot.project_id == project_id)
                    .order_by(CalculationSnapshot.created_at.desc())
                )
                .scalars()
                .all()
            )

        if not snaps:
            st.info("Önce snapshot üretin.")
            st.stop()

        preferred_id = st.session_state.get("last_snapshot_id")
        labels = []
        for sn in snaps[:50]:
            r = _read_results(sn)
            scen = (r.get("scenario") or {}) if isinstance(r, dict) else {}
            kind = "Senaryo" if scen else "Baseline"
            name = scen.get("name") if scen else ""
            lock_tag = "🔒" if getattr(sn, "locked", False) else ""
            share_tag = "👁️" if getattr(sn, "shared_with_client", False) else ""
            chain_tag = "⛓️" if getattr(sn, "previous_snapshot_hash", None) else ""
            labels.append(f"{lock_tag}{share_tag}{chain_tag} ID:{sn.id} • {kind}{(' — ' + name) if name else ''} • {sn.created_at}")

        default_index = 0
        if preferred_id:
            try:
                preferred_id = int(preferred_id)
                for i, sn in enumerate(snaps[:50]):
                    if sn.id == preferred_id:
                        default_index = i
                        break
            except Exception:
                pass

        sel = st.selectbox("Snapshot seçin", labels, index=default_index, key="report_snap_select")
        sn = snaps[labels.index(sel)]
        results = _read_results(sn)

        colA, colB, colC = st.columns(3)
        with colA:
            zip_bytes = build_zip(sn.id, sn.results_json or "{}")
            st.download_button("ZIP indir (JSON + XLSX)", data=zip_bytes, file_name=f"snapshot_{sn.id}.zip", mime="application/zip", use_container_width=True)

        with colB:
            xlsx_bytes = build_xlsx_from_results(sn.results_json or "{}")
            st.download_button(
                "XLSX indir",
                data=xlsx_bytes,
                file_name=f"snapshot_{sn.id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

        with colC:
            ep = build_evidence_pack(sn.id)
            st.download_button(
                "Evidence Pack (ZIP) — Signed",
                data=ep,
                file_name=f"evidence_pack_snapshot_{sn.id}.zip",
                mime="application/zip",
                use_container_width=True,
                type="primary" if getattr(sn, "locked", False) else "secondary",
            )
            st.caption("Not: İmza için env var `EVIDENCE_SIGNING_KEY` set edilmeli. Yoksa signature=null olur.")

    # Geçmiş
    with tabs[5]:
        st.subheader("Geçmiş")

        with db() as s:
            uploads = (
                s.execute(select(DatasetUpload).where(DatasetUpload.project_id == project_id).order_by(DatasetUpload.uploaded_at.desc()))
                .scalars()
                .all()
            )
        if uploads:
            rows = []
            for u in uploads[:300]:
                dq = u.data_quality_score
                rows.append(
                    {
                        "ID": u.id,
                        "Tür": u.dataset_type,
                        "Dosya": u.original_filename,
                        "Tarih": u.uploaded_at,
                        "DQ": f"{dq}/100" if dq is not None else "",
                        "DocRef": u.document_ref or "",
                        "EvidenceID": u.evidence_document_id or "",
                        "SHA": (u.sha256[:10] + "…") if u.sha256 else "",
                    }
                )
            st.dataframe(rows, use_container_width=True)
        else:
            st.info("Henüz upload yok.")

    # Kullanıcılar
    with tabs[6]:
        st.subheader("Kullanıcı Yönetimi")
        st.caption("Client Dashboard'u test etmek için müşteri kullanıcı oluşturabilirsiniz.")

        with db() as s:
            users = s.execute(select(User).where(User.company_id == company_id).order_by(User.id.desc())).scalars().all()

        if users:
            st.dataframe(
                [{"id": u.id, "email": u.email, "role": u.role, "company_id": u.company_id} for u in users],
                use_container_width=True,
            )
        else:
            st.info("Bu şirkette kullanıcı yok.")

        st.divider()
        st.markdown("#### Yeni müşteri kullanıcı oluştur")
        new_email = st.text_input("E-posta", key="new_user_email")
        new_pw = st.text_input("Şifre", type="password", key="new_user_pw")
        role = st.selectbox("Rol", ["clientviewer", "clientadmin"], index=0, key="new_user_role")

        if st.button("Kullanıcı oluştur", type="primary", key="btn_create_user"):
            if not new_email.strip() or not new_pw.strip():
                st.warning("E-posta ve şifre zorunlu.")
            else:
                with db() as s:
                    existing = s.execute(select(User).where(User.email == new_email).limit(1)).scalars().first()
                    if existing:
                        st.error("Bu e-posta zaten kayıtlı.")
                    else:
                        u = User(email=new_email.strip(), password_hash=_hash_pw(new_pw), role=role, company_id=company_id)
                        s.add(u)
                        s.commit()
                st.success("Kullanıcı oluşturuldu ✅")
                st.rerun()
