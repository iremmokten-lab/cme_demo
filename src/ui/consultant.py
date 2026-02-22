import json
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.session import db
from src.db.models import DatasetUpload, CalculationSnapshot, Report
from src.mrv.lineage import sha256_bytes
from src.services import projects as prj
from src.services.exports import build_zip, build_xlsx_from_results
from src.services.ingestion import validate_csv
from src.services.reporting import build_pdf
from src.services.storage import UPLOAD_DIR, write_bytes
from src.services.workflow import run_full


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
) -> str:
    sha = sha256_bytes(file_bytes)
    safe = _safe_name(file_name) or f"{dataset_type}.csv"

    fp = UPLOAD_DIR / f"project_{project_id}" / dataset_type / f"{sha}_{safe}"

    with db() as s:
        existing = _first_existing_upload(s, project_id, dataset_type, sha)
        if existing:
            # Diskte yoksa yeniden yaz
            try:
                if getattr(existing, "storage_uri", None):
                    p = Path(str(existing.storage_uri))
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
            return existing.sha256 or sha

    write_bytes(fp, file_bytes)

    u = DatasetUpload(
        project_id=project_id,
        dataset_type=dataset_type,
        original_filename=safe,
        sha256=sha,
        storage_uri=str(fp),
        uploaded_by_user_id=user_id,
    )

    with db() as s:
        s.add(u)
        s.commit()

    return sha


def consultant_app(user):
    st.title("Danışman Kontrol Paneli")

    # =======================
    # SOL MENÜ
    # =======================
    try:
        companies = prj.list_companies_for_user(user)
    except Exception as e:
        st.error("Şirket listesi alınamadı.")
        st.exception(e)
        return

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
                try:
                    if not fn.strip():
                        st.warning("Tesis adı boş olamaz.")
                    else:
                        prj.create_facility(company_id, fn, cc, ss)
                        st.success("Tesis oluşturuldu.")
                        st.rerun()
                except Exception as e:
                    st.error("Tesis oluşturulamadı.")
                    st.exception(e)

        st.markdown("### Proje")

        # ---- Kritik düzeltme:
        # Projeleri önce, "(yeni proje oluştur)" seçeneğini en sona koyuyoruz.
        projects = prj.list_projects(company_id)

        proj_items = []
        for p in projects:
            year = getattr(p, "year", "")
            label = f"{p.name} / {year} (id:{p.id})"
            proj_items.append((label, p.id))

        NEW_LABEL = "(yeni proje oluştur)"
        proj_labels = [lbl for (lbl, _) in proj_items] + [NEW_LABEL]

        # Varsayılan seçimi boş bırakmayalım: projeler varsa ilk projeyi seç
        if "project_select_label" not in st.session_state:
            st.session_state["project_select_label"] = proj_labels[0] if proj_items else NEW_LABEL

        psel = st.selectbox(
            "Proje seçin",
            proj_labels,
            key="project_select_label",
        )

        if psel == NEW_LABEL:
            pn = st.text_input("Proje adı", key="new_project_name")
            py = st.number_input("Yıl", 2000, 2100, 2025, key="new_project_year")

            if st.button("Proje oluştur", type="primary", key="btn_create_project"):
                try:
                    if not pn.strip():
                        st.warning("Proje adı boş olamaz.")
                    else:
                        newp = prj.create_project(company_id, facility_id, pn, int(py))
                        # Yeni projeyi otomatik seç
                        new_label = f"{newp.name} / {newp.year} (id:{newp.id})"
                        st.session_state["project_select_label"] = new_label
                        st.success(f"Proje oluşturuldu: id={newp.id}")
                        st.rerun()
                except Exception as e:
                    st.error("Proje oluşturulamadı.")
                    st.exception(e)

            st.info("Devam etmek için proje oluşturun veya mevcut bir proje seçin.")
            st.stop()

        # psel bir proje label’ı => id’sini bulalım
        project_id = None
        for lbl, pid in proj_items:
            if lbl == psel:
                project_id = pid
                break

        if project_id is None:
            st.error("Seçili proje bulunamadı. Lütfen sayfayı yenileyin.")
            st.stop()

        st.divider()
        st.markdown("### Parametreler")
        eua = st.slider("EUA fiyatı (€/t)", 0.0, 300.0, 80.0, key="param_eua")
        fx = st.number_input("Kur (TL/€)", value=35.0, key="param_fx")
        free_alloc = st.number_input("Ücretsiz tahsis (tCO2)", value=0.0, key="param_free")
        banked = st.number_input("Banked / devreden (tCO2)", value=0.0, key="param_banked")

    config = {
        "eua_price_eur": float(eua),
        "fx_tl_per_eur": float(fx),
        "free_alloc_t": float(free_alloc),
        "banked_t": float(banked),
    }

    # =======================
    # ÜST DASHBOARD
    # =======================
    st.subheader("Proje Özeti")

    with db() as s:
        last_uploads = (
            s.execute(
                select(DatasetUpload)
                .where(DatasetUpload.project_id == project_id)
                .order_by(DatasetUpload.uploaded_at.desc())
                .limit(5)
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

    u_energy = next((u for u in last_uploads if getattr(u, "dataset_type", "") == "energy"), None)
    u_prod = next((u for u in last_uploads if getattr(u, "dataset_type", "") == "production"), None)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Energy.csv", "Var ✅" if u_energy else "Yok ❌")
    c2.metric("Production.csv", "Var ✅" if u_prod else "Yok ❌")
    c3.metric("Snapshot sayısı", str(len(last_snaps)))
    c4.metric("Son snapshot", f"ID:{last_snaps[0].id}" if last_snaps else "-")

    if not u_energy or not u_prod:
        st.warning("Bu projede gerekli CSV’ler eksik görünüyor. Önce Veri Yükleme sekmesinden yükleyin.")
    elif not last_snaps:
        st.info("CSV’ler var. Şimdi Hesaplama veya Senaryolar sekmesinden snapshot üretin.")
    else:
        r = _read_results(last_snaps[0])
        k = (r.get("kpis") or {}) if isinstance(r, dict) else {}
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Toplam Emisyon (tCO2)", _fmt_tr(k.get("energy_total_tco2", 0), 3))
        k2.metric("Scope-1 (tCO2)", _fmt_tr(k.get("energy_scope1_tco2", 0), 3))
        k3.metric("CBAM (€)", _fmt_tr(k.get("cbam_cost_eur", 0), 2))
        k4.metric("ETS (TL)", _fmt_tr(k.get("ets_cost_tl", 0), 2))

    st.divider()

    # =======================
    # SEKME YAPISI
    # =======================
    tabs = st.tabs(
        [
            "Veri Yükleme",
            "Hesaplama",
            "Senaryolar",
            "Raporlar ve İndirme",
            "Geçmiş",
        ]
    )

    # 1) VERİ YÜKLEME
    with tabs[0]:
        st.subheader("CSV Yükleme")
        st.caption("Aynı dosya tekrar yüklenirse yeni kayıt açılmaz (dedup).")

        col1, col2 = st.columns(2)
        with col1:
            up_energy = st.file_uploader("energy.csv yükleyin", type=["csv"], key=f"energy_{project_id}")
        with col2:
            up_prod = st.file_uploader("production.csv yükleyin", type=["csv"], key=f"prod_{project_id}")

        def _handle_upload(uploaded, dtype: str):
            if uploaded is None:
                return
            try:
                b = uploaded.getvalue()
                df = pd.read_csv(uploaded)
                errs = validate_csv(dtype, df)
                if errs:
                    st.error(" | ".join(errs))
                    return
                sha = _save_upload_dedup(project_id, dtype, uploaded.name, b, getattr(user, "id", None))
                st.success(f"{dtype}.csv yüklendi ✅ (sha={sha[:10]}…)")
            except Exception as e:
                st.error("Yükleme sırasında hata oluştu.")
                st.exception(e)

        _handle_upload(up_energy, "energy")
        _handle_upload(up_prod, "production")

    # 2) HESAPLAMA
    with tabs[1]:
        st.subheader("Hesaplama (Baseline)")
        if st.button("Baseline çalıştır", type="primary", key="btn_run_baseline"):
            try:
                snap = run_full(project_id, config=config, scenario=None)
                st.success(f"Hesaplama tamamlandı ✅ Snapshot ID: {snap.id} (hash={snap.result_hash[:10]}…)")
            except Exception as e:
                st.error("Hesaplama başarısız.")
                st.exception(e)

    # 3) SENARYOLAR
    with tabs[2]:
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
                snap = run_full(project_id, config=config, scenario=scenario)
                st.success(f"Senaryo tamamlandı ✅ Snapshot ID: {snap.id} (hash={snap.result_hash[:10]}…)")
            except Exception as e:
                st.error("Senaryo çalıştırma başarısız.")
                st.exception(e)

    # 4) RAPORLAR / EXPORT
    with tabs[3]:
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
            st.info("Önce snapshot üretin (Hesaplama / Senaryolar).")
            st.stop()

        labels = []
        for sn in snaps[:50]:
            r = _read_results(sn)
            scen = (r.get("scenario") or {}) if isinstance(r, dict) else {}
            kind = "Senaryo" if scen else "Baseline"
            name = scen.get("name") if scen else ""
            labels.append(f"ID:{sn.id} • {kind}{(' — ' + name) if name else ''} • {sn.created_at}")

        sel = st.selectbox("Snapshot seçin", labels, index=0, key="report_snap_select")
        sn = snaps[labels.index(sel)]

        results = _read_results(sn)
        kpis = (results.get("kpis") or {}) if isinstance(results, dict) else {}
        try:
            snap_config = json.loads(sn.config_json) if sn.config_json else {}
        except Exception:
            snap_config = {}

        colA, colB, colC, colD = st.columns(4)

        pdf_bytes = None
        with colA:
            if st.button("PDF üret", type="primary", key="btn_make_pdf"):
                try:
                    payload = {
                        "kpis": kpis,
                        "config": snap_config,
                        "cbam_table": results.get("cbam_table", []),
                        "scenario": results.get("scenario", {}),
                    }
                    pdf_uri, pdf_sha = build_pdf(sn.id, "CME Demo Raporu — CBAM + ETS (Tahmini)", payload)

                    try:
                        with db() as s:
                            ex = (
                                s.execute(
                                    select(Report)
                                    .where(
                                        Report.snapshot_id == sn.id,
                                        Report.report_type == "pdf",
                                        Report.sha256 == pdf_sha,
                                    )
                                    .limit(1)
                                )
                                .scalars()
                                .first()
                            )
                            if not ex:
                                s.add(Report(snapshot_id=sn.id, report_type="pdf", storage_uri=pdf_uri, sha256=pdf_sha))
                                s.commit()
                    except Exception:
                        pass

                    p = Path(str(pdf_uri))
                    if p.exists():
                        pdf_bytes = p.read_bytes()

                    st.success("PDF üretildi ✅")
                except Exception as e:
                    st.error("PDF üretimi başarısız.")
                    st.exception(e)

        with colB:
            zip_bytes = build_zip(sn.id, sn.results_json or "{}")
            st.download_button(
                "ZIP indir",
                data=zip_bytes,
                file_name=f"snapshot_{sn.id}.zip",
                mime="application/zip",
                use_container_width=True,
            )

        with colC:
            xlsx_bytes = build_xlsx_from_results(sn.results_json or "{}")
            st.download_button(
                "XLSX indir",
                data=xlsx_bytes,
                file_name=f"snapshot_{sn.id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

        with colD:
            st.download_button(
                "JSON indir",
                data=(sn.results_json or "{}").encode("utf-8"),
                file_name=f"snapshot_{sn.id}.json",
                mime="application/json",
                use_container_width=True,
            )

        if pdf_bytes:
            st.download_button(
                "PDF indir (az önce üretilen)",
                data=pdf_bytes,
                file_name=f"snapshot_{sn.id}.pdf",
                mime="application/pdf",
                type="primary",
                use_container_width=True,
            )

    # 5) GEÇMİŞ
    with tabs[4]:
        st.subheader("Geçmiş")
        with db() as s:
            uploads = (
                s.execute(
                    select(DatasetUpload)
                    .where(DatasetUpload.project_id == project_id)
                    .order_by(DatasetUpload.uploaded_at.desc())
                )
                .scalars()
                .all()
            )
            snaps = (
                s.execute(
                    select(CalculationSnapshot)
                    .where(CalculationSnapshot.project_id == project_id)
                    .order_by(CalculationSnapshot.created_at.desc())
                )
                .scalars()
                .all()
            )

        st.markdown("#### Yüklemeler")
        if uploads:
            st.dataframe(
                [
                    {"ID": u.id, "Tür": u.dataset_type, "Dosya": u.original_filename, "Tarih": u.uploaded_at}
                    for u in uploads
                ],
                use_container_width=True,
            )
        else:
            st.info("Henüz upload yok.")

        st.markdown("#### Snapshot’lar")
        if snaps:
            rows = []
            for sn in snaps:
                r = _read_results(sn)
                scen = (r.get("scenario") or {}) if isinstance(r, dict) else {}
                kind = "Senaryo" if scen else "Baseline"
                name = scen.get("name") if scen else ""
                rows.append(
                    {
                        "ID": sn.id,
                        "Tür": f"{kind}{(' — ' + name) if name else ''}",
                        "Hash": (sn.result_hash[:12] + "…") if sn.result_hash else "",
                        "Tarih": sn.created_at,
                    }
                )
            st.dataframe(rows, use_container_width=True)
        else:
            st.info("Henüz snapshot yok.")
