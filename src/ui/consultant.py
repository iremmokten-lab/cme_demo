import json
import streamlit as st
import pandas as pd
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
from src.services.exports import build_zip


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
    st.title("Consultant Panel")

    # ---- sidebar selectors ----
    companies = prj.list_companies_for_user(user)
    if not companies:
        st.warning("Company yok.")
        return

    # Session state keys
    if "selected_company_id" not in st.session_state:
        st.session_state["selected_company_id"] = companies[0].id
    if "selected_project_id" not in st.session_state:
        st.session_state["selected_project_id"] = None

    with st.sidebar:
        st.markdown("### Company")
        company_map = {c.name: c.id for c in companies}
        company_name = st.selectbox("Select", list(company_map.keys()), index=0)
        company_id = company_map[company_name]
        st.session_state["selected_company_id"] = company_id

        st.markdown("### Facility")
        facilities = prj.list_facilities(company_id)
        fac_opts = {"(none)": None}
        for f in facilities:
            fac_opts[f"{f.name} ({f.country})"] = f.id
        fac_label = st.selectbox("Facility", list(fac_opts.keys()), index=0)
        facility_id = fac_opts[fac_label]

        with st.expander("Create facility"):
            fn = st.text_input("Facility name")
            cc = st.text_input("Country", value="TR")
            ss = st.text_input("Sector", value="")
            if st.button("Add facility"):
                try:
                    if not fn.strip():
                        st.warning("Facility name boş olamaz.")
                    else:
                        prj.create_facility(company_id, fn, cc, ss)
                        st.success("Facility oluşturuldu.")
                        st.rerun()
                except Exception as e:
                    st.exception(e)

        st.markdown("### Project")
        projects = prj.list_projects(company_id)

        # Build project options
        proj_opts = {"(new)": None}
        for p in projects:
            proj_opts[f"{p.name} / {p.year} (id:{p.id})"] = p.id

        # If selected project no longer exists, reset
        if st.session_state["selected_project_id"] not in proj_opts.values():
            st.session_state["selected_project_id"] = None

        # Selectbox index
        proj_labels = list(proj_opts.keys())
        current_id = st.session_state["selected_project_id"]
        if current_id is None:
            proj_index = 0
        else:
            # find label for current_id
            label = next(k for k, v in proj_opts.items() if v == current_id)
            proj_index = proj_labels.index(label)

        psel = st.selectbox("Project", proj_labels, index=proj_index)

        if psel == "(new)":
            pn = st.text_input("Project name", key="new_project_name")
            py = st.number_input("Year", 2000, 2100, 2025, key="new_project_year")
            if st.button("Create project", type="primary"):
                try:
                    if not pn.strip():
                        st.warning("Project name boş olamaz.")
                    else:
                        newp = prj.create_project(company_id, facility_id, pn, int(py))
                        st.session_state["selected_project_id"] = newp.id
                        st.success(f"Project oluşturuldu: id={newp.id}")
                        st.rerun()
                except Exception as e:
                    st.exception(e)
            # Project seçilmeden aşağıya geçme
            st.info("Devam etmek için proje oluşturun veya mevcut bir proje seçin.")
            return

        project_id = proj_opts[psel]
        st.session_state["selected_project_id"] = project_id

        st.divider()
        st.markdown("### Prices / ETS")
        eua = st.slider("EUA (€/t)", 0.0, 200.0, 80.0)
        fx = st.number_input("FX (TL/EUR)", value=35.0)
        free_alloc = st.number_input("Free allocation (tCO2)", value=0.0)
        banked = st.number_input("Banked allowances (tCO2)", value=0.0)

    # ---- main tabs (only when project selected) ----
    tabs = st.tabs(["Upload", "Run", "History", "Reports/Export", "Scenarios (Step 5)"])

    # Upload
    with tabs[0]:
        h2("CSV Upload", "energy.csv + production.csv")
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
                    st.success(f"energy upload OK sha={sha[:10]}…")
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
                    st.success(f"production upload OK sha={sha[:10]}…")
            except Exception as e:
                st.exception(e)

    # Run (baseline)
    with tabs[1]:
        h2("Run Calculation", "baseline snapshot oluşturur")
        config = {
            "eua_price_eur": float(eua),
            "fx_tl_per_eur": float(fx),
            "free_alloc_t": float(free_alloc),
            "banked_t": float(banked),
        }
        if st.button("Run baseline", type="primary"):
            try:
                snap = run_full(project_id, config=config, scenario=None)
                st.success(f"Snapshot: {snap.id}  hash={snap.result_hash[:10]}…")
            except Exception as e:
                st.exception(e)

    # History
    with tabs[2]:
        h2("History", "Uploads + Snapshots")
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
            st.info("Henüz snapshot yok. Run tabından baseline çalıştır.")

    # Reports/Export
    with tabs[3]:
        h2("PDF + Export", "Seçilen snapshot için PDF/XLSX/ZIP")
        with db() as s:
            snaps = s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == project_id)
                .order_by(CalculationSnapshot.created_at.desc())
            ).scalars().all()

        if not snaps:
            st.info("Önce snapshot üret.")
        else:
            options = [f"id:{sn.id}  {sn.created_at}" for sn in snaps]
            sel = st.selectbox("Snapshot", options)
            sn = snaps[options.index(sel)]

            results = json.loads(sn.results_json)
            kpis = results.get("kpis", {})

            colA, colB, colC = st.columns(3)

            with colA:
                if st.button("Generate PDF"):
                    try:
                        pdf_uri, pdf_sha = build_pdf(sn.id, "CME Report", kpis)
                        with db() as s:
                            r = Report(snapshot_id=sn.id, report_type="pdf", storage_uri=pdf_uri, sha256=pdf_sha)
                            s.add(r)
                            s.commit()
                        st.success("PDF üretildi.")
                    except Exception as e:
                        st.exception(e)

            with colB:
                try:
                    zip_bytes = build_zip(sn.id, sn.results_json)
                    st.download_button("Download ZIP (json+xlsx)", data=zip_bytes, file_name=f"snapshot_{sn.id}.zip")
                except Exception as e:
                    st.exception(e)

            with colC:
                st.download_button("Download results.json", data=sn.results_json.encode("utf-8"),
                                   file_name=f"snapshot_{sn.id}_results.json")

            with db() as s:
                reports = s.execute(
                    select(Report).where(Report.snapshot_id == sn.id).order_by(Report.created_at.desc())
                ).scalars().all()
            if reports:
                st.markdown("#### Reports")
                st.dataframe([{
                    "id": r.id,
                    "type": r.report_type,
                    "uri": r.storage_uri,
                    "sha256": r.sha256,
                    "at": r.created_at
                } for r in reports], use_container_width=True)

    # Scenarios Step 5
    with tabs[4]:
        h2("Reduction Engine (Step 5)", "Before/After CBAM€ + ETS TL + SKU savings")
        config = {
            "eua_price_eur": float(eua),
            "fx_tl_per_eur": float(fx),
            "free_alloc_t": float(free_alloc),
            "banked_t": float(banked),
        }

        st.markdown("### Scenario knobs")
        renewable_share = st.slider("Renewable share (scope2 azaltır)", 0.0, 1.0, 0.0, 0.05)
        energy_reduction = st.slider("Energy reduction %", 0.0, 0.8, 0.0, 0.05)
        supplier_mult = st.slider("Supplier factor multiplier", 0.2, 1.5, 1.0, 0.05)
        export_mix = st.slider("EU export multiplier", 0.0, 1.5, 1.0, 0.05)

        scenario = {
            "renewable_share": float(renewable_share),
            "energy_reduction_pct": float(energy_reduction),
            "supplier_factor_multiplier": float(supplier_mult),
            "export_mix_multiplier": float(export_mix),
        }

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Run BEFORE (baseline)"):
                try:
                    snap_b = run_full(project_id, config=config, scenario=None)
                    st.session_state["before_id"] = snap_b.id
                    st.success(f"Before snapshot={snap_b.id}")
                except Exception as e:
                    st.exception(e)
        with col2:
            if st.button("Run AFTER (scenario)", type="primary"):
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
            kb = rb["kpis"]
            ka = ra["kpis"]

            st.markdown("### Before/After summary")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("CBAM € Before", f'{kb["cbam_cost_eur"]:.2f}')
            c2.metric("CBAM € After", f'{ka["cbam_cost_eur"]:.2f}', delta=f'{(ka["cbam_cost_eur"]-kb["cbam_cost_eur"]):.2f}')
            c3.metric("ETS TL Before", f'{kb["ets_cost_tl"]:.2f}')
            c4.metric("ETS TL After", f'{ka["ets_cost_tl"]:.2f}', delta=f'{(ka["ets_cost_tl"]-kb["ets_cost_tl"]):.2f}')

            dfb = pd.DataFrame(rb.get("cbam_table", []))
            dfa = pd.DataFrame(ra.get("cbam_table", []))
            if not dfb.empty and not dfa.empty:
                keep = ["sku", "cbam_cost_eur"]
                dfb = dfb[keep].rename(columns={"cbam_cost_eur": "before_cbam_eur"})
                dfa = dfa[keep].rename(columns={"cbam_cost_eur": "after_cbam_eur"})
                merged = dfb.merge(dfa, on="sku", how="outer").fillna(0.0)
                merged["savings_eur"] = merged["before_cbam_eur"] - merged["after_cbam_eur"]
                merged = merged.sort_values("savings_eur", ascending=False)
                st.markdown("### Top risk / savings SKU")
                st.dataframe(merged, use_container_width=True)
        else:
            st.info("Önce BEFORE ve AFTER run butonlarına bas.")
