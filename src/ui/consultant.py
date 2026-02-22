import json
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.ui.components import h2
from src.services import projects as prj
from src.db.session import db
from src.db.models import DatasetUpload, CalculationSnapshot, Report
from src.services.ingestion import validate_csv
from src.services.storage import UPLOAD_DIR, write_bytes
from src.mrv.lineage import sha256_bytes
from src.mrv.audit import append_audit
from src.services.workflow import run_full
from src.services.reporting import build_pdf
from src.services.exports import build_zip, build_xlsx_from_results


def _safe_name(name: str) -> str:
    return (name or "").replace("/", "_").replace("\\", "_").strip()


def _save_upload(project_id: int, dataset_type: str, file_name: str, file_bytes: bytes, user_id: int | None):
    sha = sha256_bytes(file_bytes)
    safe = _safe_name(file_name) or f"{dataset_type}.csv"
    fp = UPLOAD_DIR / f"project_{project_id}" / dataset_type / f"{sha}_{safe}"
    write_bytes(fp, file_bytes)

    u = DatasetUpload(
        project_id=project_id,
        dataset_type=dataset_type,
        original_filename=safe,
        sha256=sha,
        schema_version="v1",
        storage_uri=str(fp),
        uploaded_by_user_id=user_id,
    )
    with db() as s:
        s.add(u)
        s.commit()

    append_audit("upload_saved", {"project_id": project_id, "type": dataset_type, "sha": sha, "uri": str(fp)})
    return sha


def consultant_app(user):
    st.title("Danışman Paneli")

    companies = prj.list_companies_for_user(user)
    if not companies:
        st.warning("Company yok.")
        return

    if "selected_project_id" not in st.session_state:
        st.session_state["selected_project_id"] = None

    with st.sidebar:
        st.markdown("### Şirket")
        company_map = {c.name: c.id for c in companies}
        company_name = st.selectbox("Seç", list(company_map.keys()), index=0)
        company_id = company_map[company_name]

        st.markdown("### Tesis")
        facilities = prj.list_facilities(company_id)
        fac_opts = {"(yok)": None}
        for f in facilities:
            fac_opts[f"{f.name} ({f.country})"] = f.id
        fac_label = st.selectbox("Tesis", list(fac_opts.keys()), index=0)
        facility_id = fac_opts[fac_label]

        with st.expander("Tesis oluştur"):
            fn = st.text_input("Tesis adı")
            cc = st.text_input("Ülke", value="TR")
            ss = st.text_input("Sektör", value="")
            if st.button("Tesis ekle"):
                try:
                    if not fn.strip():
                        st.warning("Tesis adı boş olamaz.")
                    else:
                        prj.create_facility(company_id, fn, cc, ss)
                        st.success("Tesis oluşturuldu.")
                        st.rerun()
                except Exception as e:
                    st.exception(e)

        st.markdown("### Proje")
        projects = prj.list_projects(company_id)
        proj_opts = {"(yeni)": None}
        for p in projects:
            proj_opts[f"{p.name} / {p.year} (id:{p.id})"] = p.id

        # seçili proje yoksa ilk projeyi seç
        if st.session_state["selected_project_id"] not in proj_opts.values():
            st.session_state["selected_project_id"] = None

        proj_labels = list(proj_opts.keys())
        current_id = st.session_state["selected_project_id"]
        if current_id is None:
            proj_index = 0
        else:
            label = next(k for k, v in proj_opts.items() if v == current_id)
            proj_index = proj_labels.index(label)

        psel = st.selectbox("Proje", proj_labels, index=proj_index)

        if psel == "(yeni)":
            pn = st.text_input("Proje adı")
            py = st.number_input("Yıl", 2000, 2100, 2025)
            if st.button("Proje oluştur", type="primary"):
                try:
                    if not pn.strip():
                        st.warning("Proje adı boş olamaz.")
                    else:
                        newp = prj.create_project(company_id, facility_id, pn, int(py))
                        st.session_state["selected_project_id"] = newp.id
                        st.success(f"Proje oluşturuldu: id={newp.id}")
                        st.rerun()
                except Exception as e:
                    st.exception(e)
            st.info("Devam etmek için proje oluşturun veya mevcut bir proje seçin.")
            return

        project_id = proj_opts[psel]
        st.session_state["selected_project_id"] = project_id

        st.divider()
        st.markdown("### Parametreler")
        eua = st.slider("EUA (€/t)", 0.0, 200.0, 80.0)
        fx = st.number_input("FX (TL/€)", value=35.0)
        free_alloc = st.number_input("Free allocation (tCO2)", value=0.0)
        banked = st.number_input("Banked (tCO2)", value=0.0)

   tabs = st.tabs(["Yükleme", "Hesapla", "Geçmiş", "Raporlar / İndir", "Senaryolar"])

    # ---------------- Upload ----------------
    with tabs[0]:
        h2("CSV Yükleme", "energy.csv + production.csv")
        col1, col2 = st.columns(2)
        with col1:
            up_energy = st.file_uploader("energy.csv", type=["csv"], key=f"u_energy_{project_id}")
        with col2:
            up_prod = st.file_uploader("production.csv", type=["csv"], key=f"u_prod_{project_id}")

        if up_energy is not None:
            try:
                b = up_energy.getvalue()
                df = pd.read_csv(up_energy)
                errs = validate_csv("energy", df)
                if errs:
                    st.error(" | ".join(errs))
                else:
                    sha = _save_upload(project_id, "energy", up_energy.name, b, user.id)
                    st.success(f"energy.csv yüklendi ✅ sha={sha[:10]}…")
            except Exception as e:
                st.exception(e)

        if up_prod is not None:
            try:
                b = up_prod.getvalue()
                df = pd.read_csv(up_prod)
                errs = validate_csv("production", df)
                if errs:
                    st.error(" | ".join(errs))
                else:
                    sha = _save_upload(project_id, "production", up_prod.name, b, user.id)
                    st.success(f"production.csv yüklendi ✅ sha={sha[:10]}…")
            except Exception as e:
                st.exception(e)

    # ---------------- Run ----------------
    with tabs[1]:
        h2("Hesaplama", "Baseline snapshot üretir")
        config = {
            "eua_price_eur": float(eua),
            "fx_tl_per_eur": float(fx),
            "free_alloc_t": float(free_alloc),
            "banked_t": float(banked),
        }
        if st.button("Baseline Çalıştır", type="primary"):
            try:
                snap = run_full(project_id, config=config, scenario=None)
                st.success(f"Snapshot oluşturuldu: id={snap.id}  hash={snap.result_hash[:10]}…")
            except Exception as e:
                st.exception(e)

    # ---------------- History ----------------
    with tabs[2]:
        h2("Geçmiş", "Uploads + Snapshots")
        with db() as s:
            uploads = s.execute(
                select(DatasetUpload)
                .where(DatasetUpload.project_id == project_id)
                .order_by(DatasetUpload.uploaded_at.desc())
            ).scalars().all()

            snaps = s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == project_id)
                .order_by(CalculationSnapshot.created_at.desc())
            ).scalars().all()

        st.markdown("#### Uploads")
        if uploads:
            st.dataframe([{
                "id": u.id,
                "type": u.dataset_type,
                "file": u.original_filename,
                "sha256": u.sha256,
                "at": u.uploaded_at,
            } for u in uploads], use_container_width=True)
        else:
            st.info("Henüz upload yok.")

        st.markdown("#### Snapshots")
        if snaps:
            st.dataframe([{
                "id": sn.id,
                "engine": sn.engine_version,
                "hash": sn.result_hash,
                "at": sn.created_at,
                "scenario": json.loads(sn.results_json).get("scenario", {}),
            } for sn in snaps], use_container_width=True)
        else:
            st.info("Henüz snapshot yok. 'Hesapla' sekmesinden baseline çalıştırın.")

    # ---------------- Reports / Export ----------------
    with tabs[3]:
        h2("Raporlar / Export", "PDF + ZIP/XLSX/JSON indir")
        with db() as s:
            snaps = s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == project_id)
                .order_by(CalculationSnapshot.created_at.desc())
            ).scalars().all()

        if not snaps:
            st.info("Önce snapshot üretin.")
            return

        options = [f"id:{sn.id}  {sn.created_at}" for sn in snaps]
        sel = st.selectbox("Snapshot seç", options)
        sn = snaps[options.index(sel)]

        results = json.loads(sn.results_json)
        kpis = results.get("kpis", {})
        cbam_table = results.get("cbam_table", [])

        # Top SKU: en yüksek cbam_cost_eur
        top = []
        if isinstance(cbam_table, list) and cbam_table:
            try:
                df = pd.DataFrame(cbam_table)
                # risk yoksa basit risk skoru türetelim: cbam_cost_eur normalize (0-100)
                if "cbam_cost_eur" in df.columns:
                    mx = float(df["cbam_cost_eur"].max()) if float(df["cbam_cost_eur"].max()) > 0 else 1.0
                    df["risk"] = (df["cbam_cost_eur"] / mx) * 100.0
                else:
                    df["risk"] = 0.0
                # EU tCO2: embedded_t varsa onu kullan
                eu_t = df["embedded_t"] if "embedded_t" in df.columns else 0.0
                df["eu_tco2"] = eu_t
                df = df.sort_values("risk", ascending=False).head(10)
                for _, r in df.iterrows():
                    top.append({
                        "sku": r.get("sku", ""),
                        "risk": float(r.get("risk", 0.0)),
                        "eu_tco2": float(r.get("eu_tco2", 0.0)),
                        "cbam_eur": float(r.get("cbam_cost_eur", 0.0)) if "cbam_cost_eur" in df.columns else 0.0,
                    })
            except Exception:
                top = []

        config = json.loads(sn.config_json) if sn.config_json else {}

        colA, colB, colC, colD = st.columns(4)

        with colA:
            if st.button("PDF Üret", type="primary"):
                try:
                    pdf_uri, pdf_sha = build_pdf(
                        sn.id,
                        "CME Demo Raporu — CBAM + ETS (Tahmini)",
                        {"kpis": kpis, "config": config, "top_skus": top},
                    )
                    with db() as s:
                        r = Report(snapshot_id=sn.id, report_type="pdf", storage_uri=pdf_uri, sha256=pdf_sha)
                        s.add(r)
                        s.commit()
                    st.success("PDF üretildi ✅")
                except Exception as e:
                    st.exception(e)

        with colB:
            # ZIP (json + xlsx)
            try:
                zip_bytes = build_zip(sn.id, sn.results_json)
                st.download_button("ZIP indir (JSON+XLSX)", data=zip_bytes, file_name=f"snapshot_{sn.id}.zip")
            except Exception as e:
                st.exception(e)

        with colC:
            # XLSX
            try:
                xlsx_bytes = build_xlsx_from_results(sn.results_json)
                st.download_button("XLSX indir", data=xlsx_bytes, file_name=f"snapshot_{sn.id}.xlsx")
            except Exception as e:
                st.exception(e)

        with colD:
            st.download_button("JSON indir", data=sn.results_json.encode("utf-8"), file_name=f"snapshot_{sn.id}.json")

        st.divider()

        # Report list + PDF download
        with db() as s:
            reports = s.execute(
                select(Report).where(Report.snapshot_id == sn.id).order_by(Report.created_at.desc())
            ).scalars().all()

        st.markdown("#### Üretilen PDF’ler")
        if not reports:
            st.info("Henüz PDF yok. 'PDF Üret' ile oluşturun.")
        else:
            for r in reports:
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(f"📄 Report id={r.id} | {r.created_at} | sha={r.sha256[:10]}…")
                    st.caption(r.storage_uri)
                with col2:
                    try:
                        p = Path(r.storage_uri)
                        if p.exists():
                            st.download_button(
                                "PDF indir",
                                data=p.read_bytes(),
                                file_name=p.name,
                                mime="application/pdf",
                                key=f"dl_pdf_{r.id}",
                            )
                        else:
                            st.warning("PDF dosyası deploy ortamında bulunamadı (reboot sonrası silinmiş olabilir).")
                    except Exception as e:
                        st.exception(e)

    # ---------------- Scenarios (Step 5) ----------------
    with tabs[4]:
        h2("Senaryolar (Adım 5)", "Before/After CBAM € ve ETS TL kıyaslama")

        config = {
            "eua_price_eur": float(eua),
            "fx_tl_per_eur": float(fx),
            "free_alloc_t": float(free_alloc),
            "banked_t": float(banked),
        }

        st.markdown("### Senaryo Parametreleri")
        renewable_share = st.slider("Yenilenebilir payı (Scope2 azaltır)", 0.0, 1.0, 0.0, 0.05)
        energy_reduction = st.slider("Enerji azaltım (%)", 0.0, 0.8, 0.0, 0.05)
        supplier_mult = st.slider("Tedarikçi faktör çarpanı", 0.2, 1.5, 1.0, 0.05)
        export_mix = st.slider("AB ihracat çarpanı", 0.0, 1.5, 1.0, 0.05)

        scenario = {
            "renewable_share": float(renewable_share),
            "energy_reduction_pct": float(energy_reduction),
            "supplier_factor_multiplier": float(supplier_mult),
            "export_mix_multiplier": float(export_mix),
        }

        col1, col2 = st.columns(2)
        with col1:
            if st.button("BEFORE (baseline) çalıştır"):
                try:
                    snap_b = run_full(project_id, config=config, scenario=None)
                    st.session_state["before_id"] = snap_b.id
                    st.success(f"Before snapshot={snap_b.id}")
                except Exception as e:
                    st.exception(e)
        with col2:
            if st.button("AFTER (senaryo) çalıştır", type="primary"):
                try:
                    snap_a = run_full(project_id, config=config, scenario=scenario)
                    st.session_state["after_id"] = snap_a.id
                    st.success(f"After snapshot={snap_a.id}")
                except Exception as e:
                    st.exception(e)

        before_id = st.session_state.get("before_id")
        after_id = st.session_state.get("after_id")

        if before_id and after_id:
            with db() as s:
                b = s.get(CalculationSnapshot, before_id)
                a = s.get(CalculationSnapshot, after_id)

            rb = json.loads(b.results_json)
            ra = json.loads(a.results_json)
            kb = rb.get("kpis", {})
            ka = ra.get("kpis", {})

            st.markdown("### Before / After Özet")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("CBAM € (Before)", f'{kb.get("cbam_cost_eur", 0):.2f}')
            c2.metric("CBAM € (After)", f'{ka.get("cbam_cost_eur", 0):.2f}', delta=f'{(ka.get("cbam_cost_eur", 0)-kb.get("cbam_cost_eur", 0)):.2f}')
            c3.metric("ETS TL (Before)", f'{kb.get("ets_cost_tl", 0):.2f}')
            c4.metric("ETS TL (After)", f'{ka.get("ets_cost_tl", 0):.2f}', delta=f'{(ka.get("ets_cost_tl", 0)-kb.get("ets_cost_tl", 0)):.2f}')
        else:
            st.info("Önce BEFORE ve AFTER çalıştırın.")
