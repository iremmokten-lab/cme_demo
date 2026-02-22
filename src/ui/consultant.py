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


def _first_existing_upload(session, project_id: int, dataset_type: str, sha: str):
    """
    DB'de aynı (project_id, dataset_type, sha256) için birden çok satır olsa bile patlamasın:
    HER ZAMAN ilk kaydı döndür.
    """
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
    """
    1) MultipleResultsFound hatasını kesin kaldırır (scalar_one_or_none yok).
    2) Aynı dosya tekrar yüklenirse yeni satır açmaz (mümkünse).
    3) DB'de eskiden kalma duplicate satırlar olsa bile patlamaz; ilkini kullanır.
    """
    sha = sha256_bytes(file_bytes)
    safe = _safe_name(file_name) or f"{dataset_type}.csv"

    fp = UPLOAD_DIR / f"project_{project_id}" / dataset_type / f"{sha}_{safe}"

    with db() as s:
        existing = _first_existing_upload(s, project_id, dataset_type, sha)
        if existing:
            # Aynı dosya zaten var: history şişmesin, yeni satır açmayalım.
            # Diskte dosya yoksa (reboot sonrası) yeniden yazalım ki workflow uri bulabilsin.
            try:
                if hasattr(existing, "storage_uri") and existing.storage_uri:
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
                    # storage_uri kolonu yoksa yine de dosyayı yazalım.
                    if not fp.exists():
                        write_bytes(fp, file_bytes)
            except Exception:
                # Bu noktada upload ekranını patlatmak istemiyoruz.
                pass

            return existing.sha256 or sha

    # Yeni upload: önce diske yaz, sonra DB'ye kaydet
    write_bytes(fp, file_bytes)

    u = DatasetUpload(
        project_id=project_id,
        dataset_type=dataset_type,
        original_filename=safe,
        sha256=sha,
    )

    # Repo'daki model alanları değişebileceği için "varsa set et" yaklaşımı:
    if hasattr(u, "schema_version"):
        setattr(u, "schema_version", "v1")
    if hasattr(u, "storage_uri"):
        setattr(u, "storage_uri", str(fp))
    if hasattr(u, "content_bytes"):
        setattr(u, "content_bytes", file_bytes)
    if hasattr(u, "uploaded_by_user_id"):
        setattr(u, "uploaded_by_user_id", user_id)

    with db() as s:
        s.add(u)
        s.commit()

    return sha


def _fmt_float(x, digits=2) -> str:
    try:
        return f"{float(x):,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(x)


def consultant_app(user):
    st.title("Danışman Paneli")

    # =======================
    # SOL MENÜ (SEÇİMLER)
    # =======================
    try:
        companies = prj.list_companies_for_user(user)
    except Exception as e:
        st.error("Şirket listesi alınamadı.")
        st.exception(e)
        return

    if not companies:
        st.warning("Bu kullanıcı için tanımlı şirket yok.")
        return

    with st.sidebar:
        st.markdown("### Şirket")
        company_map = {c.name: c.id for c in companies}
        company_name = st.selectbox("Şirket seçin", list(company_map.keys()), index=0)
        company_id = company_map[company_name]

        st.markdown("### Tesis")
        try:
            facilities = prj.list_facilities(company_id)
        except Exception as e:
            st.error("Tesis listesi alınamadı.")
            st.exception(e)
            return

        fac_opts = {"(yok)": None}
        for f in facilities:
            label = f"{f.name}"
            if getattr(f, "country", None):
                label += f" ({f.country})"
            fac_opts[label] = f.id

        fac_label = st.selectbox("Tesis seçin", list(fac_opts.keys()), index=0)
        facility_id = fac_opts[fac_label]

        with st.expander("Yeni tesis oluştur"):
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
        try:
            projects = prj.list_projects(company_id)
        except Exception as e:
            st.error("Proje listesi alınamadı.")
            st.exception(e)
            return

        proj_opts = {"(yeni proje oluştur)": None}
        for p in projects:
            year = getattr(p, "year", "")
            proj_opts[f"{p.name} / {year} (id:{p.id})"] = p.id

        psel = st.selectbox("Proje seçin", list(proj_opts.keys()), index=0)

        if psel == "(yeni proje oluştur)":
            pn = st.text_input("Proje adı")
            py = st.number_input("Yıl", 2000, 2100, 2026)
            if st.button("Proje oluştur", type="primary"):
                try:
                    if not pn.strip():
                        st.warning("Proje adı boş olamaz.")
                    else:
                        newp = prj.create_project(company_id, facility_id, pn, int(py))
                        st.success(f"Proje oluşturuldu: id={newp.id}")
                        st.rerun()
                except Exception as e:
                    st.exception(e)

            st.info("Devam etmek için proje oluşturun veya mevcut bir proje seçin.")
            st.stop()

        project_id = proj_opts[psel]

        st.divider()
        st.markdown("### Parametreler")
        eua = st.slider("EUA fiyatı (€/t)", 0.0, 300.0, 80.0)
        fx = st.number_input("Kur (TL/€)", value=35.0)
        free_alloc = st.number_input("Ücretsiz tahsis (tCO2)", value=0.0)
        banked = st.number_input("Banked / devreden (tCO2)", value=0.0)

    # =======================
    # SEKME YAPISI (TÜRKÇE)
    # =======================
    tabs = st.tabs(
        [
            "Veri Yükleme",
            "Hesaplama",
            "Geçmiş",
            "Raporlar ve İndirme",
            "Senaryolar",
        ]
    )

    # =======================
    # 1) VERİ YÜKLEME
    # =======================
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

    # =======================
    # 2) HESAPLAMA
    # =======================
    with tabs[1]:
        st.subheader("Hesaplama")

        config = {
            "eua_price_eur": float(eua),
            "fx_tl_per_eur": float(fx),
            "free_alloc_t": float(free_alloc),
            "banked_t": float(banked),
        }

        if st.button("Baseline çalıştır", type="primary"):
            try:
                snap = run_full(project_id, config=config, scenario=None)
                st.success(f"Hesaplama tamamlandı ✅ Snapshot ID: {snap.id} (hash={snap.result_hash[:10]}…)")
            except Exception as e:
                st.error("Hesaplama başarısız.")
                st.exception(e)

    # =======================
    # 3) GEÇMİŞ
    # =======================
    with tabs[2]:
        st.subheader("Geçmiş")

        try:
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
        except Exception as e:
            st.error("Geçmiş verileri okunamadı.")
            st.exception(e)
            return

        st.markdown("#### Yüklemeler")
        st.dataframe(
            [
                {
                    "ID": u.id,
                    "Tür": getattr(u, "dataset_type", ""),
                    "Dosya": getattr(u, "original_filename", ""),
                    "SHA256": (u.sha256[:12] + "…") if getattr(u, "sha256", None) else "",
                    "Tarih": getattr(u, "uploaded_at", None),
                }
                for u in uploads
            ],
            use_container_width=True,
        )

        st.markdown("#### Snapshot'lar")
        st.dataframe(
            [
                {
                    "ID": sn.id,
                    "Hash": (sn.result_hash[:12] + "…") if getattr(sn, "result_hash", None) else "",
                    "Tarih": getattr(sn, "created_at", None),
                    "Engine": getattr(sn, "engine_version", ""),
                }
                for sn in snaps
            ],
            use_container_width=True,
        )

    # =======================
    # 4) RAPORLAR / EXPORT
    # =======================
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
            st.info("Önce bir snapshot üretin (Hesaplama sekmesi).")
            st.stop()

        options = [f"ID:{sn.id} — {sn.created_at}" for sn in snaps]
        sel = st.selectbox("Snapshot seçin", options, index=0)
        sn = snaps[options.index(sel)]

        # Sonuçları oku
        try:
            results = json.loads(sn.results_json) if sn.results_json else {}
        except Exception:
            results = {}

        kpis = results.get("kpis", {})
        try:
            config = json.loads(sn.config_json) if sn.config_json else {}
        except Exception:
            config = {}

        # KPI özet
        st.markdown("#### Özet KPI'lar")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Toplam Emisyon (tCO2)", _fmt_float(kpis.get("energy_total_tco2", 0), 3))
        c2.metric("Scope-1 (tCO2)", _fmt_float(kpis.get("energy_scope1_tco2", 0), 3))
        c3.metric("CBAM Maliyeti (€)", _fmt_float(kpis.get("cbam_cost_eur", 0), 2))
        c4.metric("ETS Maliyeti (TL)", _fmt_float(kpis.get("ets_cost_tl", 0), 2))

        st.divider()

        # Export butonları
        colA, colB, colC, colD = st.columns(4)

        pdf_uri = None
        pdf_sha = None
        pdf_bytes_for_download = None

        with colA:
            if st.button("PDF üret", type="primary"):
                try:
                    report_payload = {
                        "kpis": kpis,
                        "config": config,
                        "cbam_table": results.get("cbam_table", []),
                        "scenario": results.get("scenario", {}),
                    }
                    pdf_uri, pdf_sha = build_pdf(
                        sn.id,
                        "CME Demo Raporu — CBAM + ETS (Tahmini)",
                        report_payload,
                    )

                    # Aynı PDF (sha) zaten kayıtlıysa yeni satır açma
                    try:
                        with db() as s:
                            existing_r = (
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
                            if not existing_r:
                                r = Report(
                                    snapshot_id=sn.id,
                                    report_type="pdf",
                                    storage_uri=pdf_uri,
                                    sha256=pdf_sha,
                                )
                                s.add(r)
                                s.commit()
                    except Exception:
                        # Report tablosu/kolonları değişmiş olabilir; üretimi bozmayalım.
                        pass

                    # İndirme için bytes hazırlayalım
                    p = Path(pdf_uri)
                    if p.exists():
                        pdf_bytes_for_download = p.read_bytes()

                    st.success("PDF üretildi ✅")
                except Exception as e:
                    st.error("PDF üretimi başarısız.")
                    st.exception(e)

        with colB:
            try:
                zip_bytes = build_zip(sn.id, sn.results_json)
                st.download_button(
                    "ZIP indir (JSON + XLSX)",
                    data=zip_bytes,
                    file_name=f"snapshot_{sn.id}.zip",
                    mime="application/zip",
                    use_container_width=True,
                )
            except Exception as e:
                st.error("ZIP export başarısız.")
                st.exception(e)

        with colC:
            try:
                xlsx_bytes = build_xlsx_from_results(sn.results_json)
                st.download_button(
                    "XLSX indir",
                    data=xlsx_bytes,
                    file_name=f"snapshot_{sn.id}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            except Exception as e:
                st.error("XLSX export başarısız.")
                st.exception(e)

        with colD:
            try:
                st.download_button(
                    "JSON indir",
                    data=(sn.results_json or "{}").encode("utf-8"),
                    file_name=f"snapshot_{sn.id}.json",
                    mime="application/json",
                    use_container_width=True,
                )
            except Exception as e:
                st.error("JSON export başarısız.")
                st.exception(e)

        # Üretilen PDF aynı sayfada indirilsin
        if pdf_bytes_for_download:
            st.download_button(
                "PDF indir (az önce üretilen)",
                data=pdf_bytes_for_download,
                file_name=f"snapshot_{sn.id}.pdf",
                mime="application/pdf",
                type="primary",
                use_container_width=True,
            )

        st.divider()
        st.markdown("#### Daha önce üretilmiş PDF'ler")

        try:
            with db() as s:
                reports = (
                    s.execute(
                        select(Report)
                        .where(Report.snapshot_id == sn.id, Report.report_type == "pdf")
                        .order_by(Report.created_at.desc())
                    )
                    .scalars()
                    .all()
                )
        except Exception:
            reports = []

        if not reports:
            st.info("Bu snapshot için kayıtlı PDF bulunamadı.")
        else:
            for r in reports:
                uri = getattr(r, "storage_uri", None)
                sha = getattr(r, "sha256", None)
                created = getattr(r, "created_at", None)

                label = f"PDF — {created} — sha:{(sha[:10] + '…') if sha else '-'}"
                if uri:
                    p = Path(str(uri))
                    if p.exists():
                        st.download_button(
                            label,
                            data=p.read_bytes(),
                            file_name=p.name,
                            mime="application/pdf",
                            key=f"dl_pdf_{getattr(r, 'id', sha) or sha}",
                        )
                    else:
                        st.warning(f"{label} — Dosya disk üzerinde bulunamadı (reboot sonrası silinmiş olabilir).")

    # =======================
    # 5) SENARYOLAR
    # =======================
    with tabs[4]:
        st.subheader("Senaryolar")
        st.info("Senaryo özellikleri bu demo sürümünde devre dışıdır / yakında eklenecektir.")
