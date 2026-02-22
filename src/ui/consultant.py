from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import bcrypt
import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import CalculationSnapshot, DatasetUpload, Methodology, MonitoringPlan, Report, User
from src.db.session import db
from src.mrv.lineage import sha256_bytes
from src.services import projects as prj
from src.services.exports import build_evidence_pack, build_zip, build_xlsx_from_results
from src.services.ingestion import validate_csv
from src.services.reporting import build_pdf
from src.services.storage import UPLOAD_DIR, write_bytes
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
) -> str:
    sha = sha256_bytes(file_bytes)
    safe = _safe_name(file_name) or f"{dataset_type}.csv"
    fp = UPLOAD_DIR / f"project_{project_id}" / dataset_type / f"{sha}_{safe}"

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

        # Monitoring Plan (ETS MRV)
        st.divider()
        st.markdown("### ETS Monitoring Plan")
        if facility_id:
            mp = _latest_monitoring_plan(int(facility_id))
            with st.expander("Monitoring Plan (oluştur / güncelle)", expanded=False):
                method = st.selectbox(
                    "Yöntem",
                    ["standard", "mass_balance", "calculation", "measurement"],
                    index=0 if not mp else max(["standard", "mass_balance", "calculation", "measurement"].index(mp.method) if mp.method in ["standard", "mass_balance", "calculation", "measurement"] else 0, 0),
                    key="mp_method",
                )
                tier = st.selectbox(
                    "Tier seviyesi",
                    ["Tier 1", "Tier 2", "Tier 3"],
                    index=1 if not mp else max(["Tier 1", "Tier 2", "Tier 3"].index(mp.tier_level) if mp.tier_level in ["Tier 1", "Tier 2", "Tier 3"] else 1, 0),
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
            help="0 ise override uygulanmaz. Örn: REC/PPA ile market-based değer düşebilir.",
            key="param_market_override",
        )
        cbam_alloc = st.selectbox("CBAM allocation basis", ["quantity", "export"], index=0, key="param_cbam_alloc")

        uncertainty_notes = st.text_area(
            "ETS belirsizlik notu (verification için)",
            value="",
            help="Tier metoduna göre belirsizlik hesapları ileride detaylandırılır. Şimdilik not olarak rapor payload’ına girer.",
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

        with st.expander("Yeni metodoloji oluştur"):
            mn = st.text_input("Metodoloji adı", key="meth_new_name")
            mv = st.text_input("Versiyon", value="v1", key="meth_new_version")
            ms = st.text_input("Kapsam", value="CBAM+ETS", key="meth_new_scope")
            md = st.text_area("Açıklama", key="meth_new_desc")
            if st.button("Metodolojiyi kaydet", type="primary", key="btn_create_meth"):
                if not mn.strip():
                    st.warning("Metodoloji adı boş olamaz.")
                else:
                    with db() as s:
                        m = Methodology(name=mn.strip(), description=md or "", scope=ms.strip(), version=mv.strip() or "v1")
                        s.add(m)
                        s.commit()
                    st.success("Metodoloji oluşturuldu ✅")
                    st.rerun()

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
    a.metric("energy.csv", "Var ✅" if u_energy else "Yok ❌")
    b.metric("production.csv", "Var ✅" if u_prod else "Yok ❌")
    c.metric("materials.csv", "Var ✅" if u_mat else "Yok (precursor yok)")
    d.metric("Son snapshot", f"ID:{last_snaps[0].id}" if last_snaps else "-")

    st.divider()

    tabs = st.tabs(
        [
            "Veri Yükleme",
            "Hesaplama",
            "Senaryolar",
            "Raporlar ve İndirme",
            "Geçmiş",
            "Kullanıcılar",
        ]
    )

    # Veri Yükleme
    with tabs[0]:
        st.subheader("CSV Yükleme")

        col1, col2, col3 = st.columns(3)
        with col1:
            up_energy = st.file_uploader("energy.csv yükleyin", type=["csv"], key=f"energy_{project_id}")
        with col2:
            up_prod = st.file_uploader("production.csv yükleyin", type=["csv"], key=f"prod_{project_id}")
        with col3:
            up_mat = st.file_uploader("materials.csv (precursor) yükleyin", type=["csv"], key=f"mat_{project_id}")

        st.caption(
            "Paket A: Precursor emisyonlar için **materials.csv** yükleyin. "
            "Energy için yeni şema: month, facility_id, fuel_type, fuel_quantity, fuel_unit."
        )

        def _handle_upload(uploaded, dtype: str):
            if uploaded is None:
                return
            bts = uploaded.getvalue()
            df = pd.read_csv(uploaded)
            errs = validate_csv(dtype, df)
            if errs:
                st.error(" | ".join(errs))
                return
            sha = _save_upload_dedup(project_id, dtype, uploaded.name, bts, getattr(user, "id", None))
            st.success(f"{dtype}.csv yüklendi ✅ (sha={sha[:10]}…)")

        try:
            _handle_upload(up_energy, "energy")
            _handle_upload(up_prod, "production")
            _handle_upload(up_mat, "materials")
        except Exception as e:
            st.error("Upload hatası")
            st.exception(e)

    # Hesaplama
    with tabs[1]:
        st.subheader("Baseline Hesaplama (Regülasyon Yakın Motor)")
        st.caption(
            "Paket A: Direct/Indirect/Precursor ayrımı yapılır. "
            "Metodoloji snapshot içine kaydedilir. Snapshot reuse aktiftir (aynı input+config → reuse)."
        )

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

                r = _read_results(snap)
                k = (r.get("kpis") or {}) if isinstance(r, dict) else {}
                st.info(
                    f"Direct: {_fmt_tr(k.get('direct_tco2', 0), 3)} tCO2 | "
                    f"Indirect: {_fmt_tr(k.get('indirect_tco2', 0), 3)} tCO2 | "
                    f"Precursor: {_fmt_tr(((r.get('cbam') or {}).get('totals') or {}).get('precursor_tco2', 0), 3)} tCO2"
                )
            except Exception as e:
                st.error("Hesaplama başarısız")
                st.exception(e)

    # Senaryolar
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

        st.caption(
            "Not (Paket A): renewable_share ve supplier_factor_multiplier bu MVP’de doğrudan enerji/malzeme faktörlerini otomatik değiştirmez. "
            "Enerji azaltımı ve export mix uygulanır. Market-based grid factor override ile dolaylı etkiler modellenebilir."
        )

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
                st.success(f"Senaryo tamamlandı ✅ Snapshot ID: {snap.id} (hash={snap.result_hash[:10]}…)")
                st.rerun()
            except Exception as e:
                st.error("Senaryo başarısız")
                st.exception(e)

        # Son snapshot KPI
        last_id = st.session_state.get("last_snapshot_id")
        if last_id:
            with db() as s:
                last_snap = s.get(CalculationSnapshot, int(last_id))
            if last_snap and last_snap.project_id == project_id:
                st.divider()
                st.subheader("Son Üretilen Sonuç (KPI)")
                r = _read_results(last_snap)
                k = (r.get("kpis") or {}) if isinstance(r, dict) else {}
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Direct (tCO2)", _fmt_tr(k.get("direct_tco2", 0), 3))
                c2.metric("Indirect (tCO2)", _fmt_tr(k.get("indirect_tco2", 0), 3))
                c3.metric("CBAM (€)", _fmt_tr(k.get("cbam_cost_eur", 0), 2))
                c4.metric("ETS (TL)", _fmt_tr(k.get("ets_cost_tl", 0), 2))

                if st.button("Bu sonucu Raporlar sekmesinde aç", key="btn_go_reports"):
                    st.session_state["open_reports_for_snapshot_id"] = last_snap.id
                    st.rerun()

    # Raporlar ve İndirme
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
            st.info("Önce snapshot üretin.")
            st.stop()

        preferred_id = st.session_state.get("open_reports_for_snapshot_id") or st.session_state.get("last_snapshot_id")

        labels = []
        id_list = []
        for sn in snaps[:50]:
            r = _read_results(sn)
            scen = (r.get("scenario") or {}) if isinstance(r, dict) else {}
            kind = "Senaryo" if scen else "Baseline"
            name = scen.get("name") if scen else ""
            lock_tag = "🔒" if getattr(sn, "locked", False) else ""
            share_tag = "👁️" if getattr(sn, "shared_with_client", False) else ""
            prev = getattr(sn, "previous_snapshot_hash", None)
            chain_tag = "⛓️" if prev else ""
            labels.append(f"{lock_tag}{share_tag}{chain_tag} ID:{sn.id} • {kind}{(' — ' + name) if name else ''} • {sn.created_at}")
            id_list.append(sn.id)

        default_index = 0
        if preferred_id:
            try:
                preferred_id = int(preferred_id)
                if preferred_id in id_list:
                    default_index = id_list.index(preferred_id)
            except Exception:
                pass

        sel = st.selectbox("Snapshot seçin", labels, index=default_index, key="report_snap_select")
        sn = snaps[labels.index(sel)]
        st.session_state.pop("open_reports_for_snapshot_id", None)

        results = _read_results(sn)
        kpis = (results.get("kpis") or {}) if isinstance(results, dict) else {}
        try:
            snap_config = json.loads(sn.config_json) if sn.config_json else {}
        except Exception:
            snap_config = {}

        st.markdown("#### Snapshot Yönetimi")
        st.caption("⛓️ = hash chain aktif (previous_snapshot_hash mevcut).")
        mcol1, mcol2, mcol3 = st.columns([1, 1, 2])

        with mcol1:
            if getattr(sn, "locked", False):
                st.success("Durum: Kilitli 🔒")
                if st.button("Kilidi aç", key="btn_unlock"):
                    with db() as s:
                        obj = s.get(CalculationSnapshot, sn.id)
                        if obj:
                            obj.locked = False
                            obj.locked_at = None
                            obj.locked_by_user_id = None
                            s.add(obj)
                            s.commit()
                    st.rerun()
            else:
                st.info("Durum: Kilitsiz")
                if st.button("Snapshot'ı kilitle", type="primary", key="btn_lock"):
                    with db() as s:
                        obj = s.get(CalculationSnapshot, sn.id)
                        if obj:
                            obj.locked = True
                            obj.locked_at = datetime.now(timezone.utc)
                            obj.locked_by_user_id = getattr(user, "id", None)
                            s.add(obj)
                            s.commit()
                    st.rerun()

        with mcol2:
            shared = bool(getattr(sn, "shared_with_client", False))
            new_shared = st.toggle("Müşteri ile paylaş", value=shared, key=f"toggle_share_{sn.id}")
            if new_shared != shared:
                with db() as s:
                    obj = s.get(CalculationSnapshot, sn.id)
                    if obj:
                        obj.shared_with_client = bool(new_shared)
                        s.add(obj)
                        s.commit()
                st.rerun()

        with mcol3:
            prev_hash = getattr(sn, "previous_snapshot_hash", None)
            st.caption(f"Engine: {getattr(sn, 'engine_version', '-')}")
            st.caption(f"Result hash: {(sn.result_hash[:16] + '…') if getattr(sn, 'result_hash', None) else '-'}")
            st.caption(f"Previous hash: {(prev_hash[:16] + '…') if prev_hash else '(yok)'}")

        st.divider()
        st.markdown("#### KPI Özeti (Paket A)")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Direct (tCO2)", _fmt_tr(kpis.get("direct_tco2", 0), 3))
        c2.metric("Indirect (tCO2)", _fmt_tr(kpis.get("indirect_tco2", 0), 3))
        cbam_prec = (((results.get("cbam") or {}).get("totals") or {}).get("precursor_tco2", 0))
        c3.metric("Precursor (tCO2)", _fmt_tr(cbam_prec, 3))
        c4.metric("CBAM (€)", _fmt_tr(kpis.get("cbam_cost_eur", 0), 2))
        c5.metric("ETS (TL)", _fmt_tr(kpis.get("ets_cost_tl", 0), 2))

        st.divider()
        colA, colB, colC, colD, colE = st.columns(5)

        pdf_bytes = None
        with colA:
            if st.button("PDF üret", type="primary", key="btn_make_pdf"):
                try:
                    meth_payload = None
                    if getattr(sn, "methodology_id", None):
                        with db() as s:
                            m = s.get(Methodology, int(sn.methodology_id))
                        meth_payload = _get_methodology_dict(m)

                    payload = {
                        "kpis": kpis,
                        "config": snap_config,
                        "cbam_table": results.get("cbam_table", []),
                        "scenario": results.get("scenario", {}),
                        "methodology": meth_payload,
                        "data_sources": [
                            "energy.csv (yüklenen dosya)",
                            "production.csv (yüklenen dosya)",
                            "materials.csv (opsiyonel, precursor)",
                            "EmissionFactor Library (DB)",
                            "Monitoring Plan (DB, facility bazlı)",
                        ],
                        "formulas": [
                            "Direct emissions: fuel_quantity × NCV × emission_factor × oxidation_factor",
                            "Indirect emissions: electricity_kwh × grid_factor (location/market)",
                            "Precursor emissions: materials.material_quantity × materials.emission_factor",
                            "CBAM exposure (demo): embedded_tCO2 × EUA × export_share",
                        ],
                    }

                    title = "Rapor — CBAM + ETS (Regülasyon Yakın, Tahmini)"
                    scen = payload.get("scenario") or {}
                    if isinstance(scen, dict) and scen.get("name"):
                        title = f"Senaryo Raporu — {scen.get('name')} (Tahmini)"

                    pdf_uri, pdf_sha = build_pdf(sn.id, title, payload)

                    # duplicate report olmasın
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
                    st.error("PDF üretilemedi")
                    st.exception(e)

        with colB:
            zip_bytes = build_zip(sn.id, sn.results_json or "{}")
            st.download_button(
                "ZIP indir (JSON + XLSX)",
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

        with colE:
            try:
                ep = build_evidence_pack(sn.id)
                st.download_button(
                    "Evidence Pack (ZIP)",
                    data=ep,
                    file_name=f"evidence_pack_snapshot_{sn.id}.zip",
                    mime="application/zip",
                    use_container_width=True,
                    type="primary" if getattr(sn, "locked", False) else "secondary",
                )
            except Exception as e:
                st.error("Evidence pack üretilemedi")
                st.exception(e)

        if pdf_bytes:
            st.download_button(
                "PDF indir (az önce üretilen)",
                data=pdf_bytes,
                file_name=f"snapshot_{sn.id}.pdf",
                mime="application/pdf",
                type="primary",
                use_container_width=True,
            )

    # Geçmiş
    with tabs[4]:
        st.subheader("Geçmiş")
        with db() as s:
            uploads = (
                s.execute(select(DatasetUpload).where(DatasetUpload.project_id == project_id).order_by(DatasetUpload.uploaded_at.desc()))
                .scalars()
                .all()
            )
            snaps = (
                s.execute(select(CalculationSnapshot).where(CalculationSnapshot.project_id == project_id).order_by(CalculationSnapshot.created_at.desc()))
                .scalars()
                .all()
            )

        st.markdown("#### Yüklemeler")
        if uploads:
            st.dataframe(
                [
                    {
                        "ID": u.id,
                        "Tür": u.dataset_type,
                        "Dosya": u.original_filename,
                        "Tarih": u.uploaded_at,
                        "SHA": (u.sha256[:10] + "…") if u.sha256 else "",
                    }
                    for u in uploads
                ],
                use_container_width=True,
            )
        else:
            st.info("Henüz upload yok.")

        st.markdown("#### Snapshot'lar")
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
                        "Tarih": sn.created_at,
                        "Kilitli": bool(getattr(sn, "locked", False)),
                        "Paylaşıldı": bool(getattr(sn, "shared_with_client", False)),
                        "Metodoloji": getattr(sn, "methodology_id", None),
                        "Prev Hash": "Var" if getattr(sn, "previous_snapshot_hash", None) else "Yok",
                    }
                )
            st.dataframe(rows, use_container_width=True)
        else:
            st.info("Henüz snapshot yok.")

    # Kullanıcılar
    with tabs[5]:
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
