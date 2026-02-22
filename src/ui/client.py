from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import CalculationSnapshot, Project, Report
from src.db.session import db
from src.services.exports import build_evidence_pack, build_zip, build_xlsx_from_results
from src.services.reporting import build_pdf


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


def client_app(user):
    st.title("Müşteri Kontrol Paneli")

    if not getattr(user, "company_id", None):
        st.error("Bu kullanıcıya şirket atanmadı.")
        return

    with db() as s:
        projects = (
            s.execute(select(Project).where(Project.company_id == user.company_id).order_by(Project.created_at.desc()))
            .scalars()
            .all()
        )

    if not projects:
        st.warning("Henüz proje yok.")
        st.markdown(
            """
            **Ne yapılmalı?**
            - Danışman proje oluşturmalı
            - CSV’ler yüklenmeli
            - Baseline veya Senaryo çalıştırılmalı
            - Snapshot paylaşılmalı (👁️)
            """
        )
        return

    project_labels = [f"{p.name} / {p.year} (id:{p.id})" for p in projects]
    psel = st.selectbox("Proje seçin", project_labels, index=0)
    project = projects[project_labels.index(psel)]

    with db() as s:
        snaps_all = (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == project.id)
                .order_by(CalculationSnapshot.created_at.desc())
            )
            .scalars()
            .all()
        )

    # Müşteriye sadece paylaşılan snapshot'ları göster
    snaps = [sn for sn in snaps_all if bool(getattr(sn, "shared_with_client", False))]

    if not snaps:
        st.info("Bu proje için henüz paylaşılan snapshot yok. (Danışman panelinden 'Müşteri ile paylaş' açılmalı.)")
        return

    snap_labels = []
    for sn in snaps[:50]:
        r = _read_results(sn)
        scen = (r.get("scenario") or {}) if isinstance(r, dict) else {}
        kind = "Senaryo" if scen else "Baseline"
        name = scen.get("name") if scen else ""
        lock_tag = "🔒" if getattr(sn, "locked", False) else ""
        snap_labels.append(f"{lock_tag} ID:{sn.id} • {kind}{(' — ' + name) if name else ''} • {sn.created_at}")

    sel = st.selectbox("Snapshot seçin", snap_labels, index=0)
    snapshot = snaps[snap_labels.index(sel)]

    results = _read_results(snapshot)
    kpis = (results.get("kpis") or {}) if isinstance(results, dict) else {}

    # KPI
    st.subheader("KPI Özeti")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Toplam Emisyon (tCO2)", _fmt_tr(kpis.get("energy_total_tco2", 0), 3))
    c2.metric("Scope-1 (tCO2)", _fmt_tr(kpis.get("energy_scope1_tco2", 0), 3))
    c3.metric("CBAM (€)", _fmt_tr(kpis.get("cbam_cost_eur", 0), 2))
    c4.metric("ETS (TL)", _fmt_tr(kpis.get("ets_cost_tl", 0), 2))

    # Trend
    st.divider()
    st.subheader("Trend (son 20 paylaşılan snapshot)")
    trend_rows = []
    for sn in reversed(snaps[:20]):
        r = _read_results(sn)
        k = (r.get("kpis") or {}) if isinstance(r, dict) else {}
        trend_rows.append(
            {
                "Tarih": sn.created_at,
                "Toplam Emisyon (tCO2)": float(k.get("energy_total_tco2", 0) or 0),
                "CBAM (€)": float(k.get("cbam_cost_eur", 0) or 0),
                "ETS (TL)": float(k.get("ets_cost_tl", 0) or 0),
            }
        )
    if trend_rows:
        df = pd.DataFrame(trend_rows).set_index("Tarih")
        st.line_chart(df)

    # PDF Raporlar
    st.divider()
    st.subheader("PDF Raporlar")

    with db() as s:
        reports = (
            s.execute(
                select(Report)
                .where(Report.snapshot_id == snapshot.id, Report.report_type == "pdf")
                .order_by(Report.created_at.desc())
            )
            .scalars()
            .all()
        )

    if not reports:
        st.info("Bu snapshot için henüz PDF rapor yok.")
        if st.button("PDF üret (bu snapshot)", type="primary"):
            try:
                try:
                    cfg = json.loads(snapshot.config_json or "{}")
                except Exception:
                    cfg = {}
                payload = {
                    "kpis": kpis,
                    "config": cfg,
                    "cbam_table": results.get("cbam_table", []),
                    "scenario": results.get("scenario", {}),
                }
                title = "CME Demo Raporu — CBAM + ETS (Tahmini)"
                scen = payload.get("scenario") or {}
                if isinstance(scen, dict) and scen.get("name"):
                    title = f"Senaryo Raporu — {scen.get('name')} (Tahmini)"

                pdf_uri, pdf_sha = build_pdf(snapshot.id, title, payload)

                # DB’ye kaydet (duplicate olsa da patlamasın)
                try:
                    with db() as s:
                        ex = (
                            s.execute(
                                select(Report)
                                .where(
                                    Report.snapshot_id == snapshot.id,
                                    Report.report_type == "pdf",
                                    Report.sha256 == pdf_sha,
                                )
                                .limit(1)
                            )
                            .scalars()
                            .first()
                        )
                        if not ex:
                            s.add(Report(snapshot_id=snapshot.id, report_type="pdf", storage_uri=pdf_uri, sha256=pdf_sha))
                            s.commit()
                except Exception:
                    pass

                st.success("PDF üretildi ✅")
                st.rerun()
            except Exception as e:
                st.error("PDF üretimi başarısız.")
                st.exception(e)
    else:
        for r in reports:
            uri = getattr(r, "storage_uri", None)
            created = getattr(r, "created_at", None)
            sha = getattr(r, "sha256", None)
            cols = st.columns([4, 2])
            cols[0].write(f"{created} • sha:{(sha[:10] + '…') if sha else '-'}")
            if uri:
                p = Path(str(uri))
                if p.exists():
                    cols[1].download_button(
                        "PDF indir",
                        data=p.read_bytes(),
                        file_name=p.name,
                        mime="application/pdf",
                        key=f"client_pdf_{r.id}",
                        use_container_width=True,
                    )
                else:
                    cols[1].warning("Dosya bulunamadı")

    # Export
    st.divider()
    st.subheader("Export / İndirme")
    colA, colB, colC, colD = st.columns(4)

    try:
        zip_bytes = build_zip(snapshot.id, snapshot.results_json or "{}")
        colA.download_button(
            "ZIP indir (JSON + XLSX)",
            data=zip_bytes,
            file_name=f"snapshot_{snapshot.id}.zip",
            mime="application/zip",
            use_container_width=True,
        )
    except Exception as e:
        colA.error("ZIP üretilemedi")
        colA.exception(e)

    try:
        xlsx_bytes = build_xlsx_from_results(snapshot.results_json or "{}")
        colB.download_button(
            "XLSX indir",
            data=xlsx_bytes,
            file_name=f"snapshot_{snapshot.id}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    except Exception as e:
        colB.error("XLSX üretilemedi")
        colB.exception(e)

    colC.download_button(
        "JSON indir",
        data=(snapshot.results_json or "{}").encode("utf-8"),
        file_name=f"snapshot_{snapshot.id}.json",
        mime="application/json",
        use_container_width=True,
    )

    # Evidence pack
    if getattr(snapshot, "locked", False):
        try:
            ep = build_evidence_pack(snapshot.id)
            colD.download_button(
                "Evidence Pack (ZIP)",
                data=ep,
                file_name=f"evidence_pack_snapshot_{snapshot.id}.zip",
                mime="application/zip",
                use_container_width=True,
                type="primary",
            )
        except Exception as e:
            colD.error("Evidence pack üretilemedi")
            colD.exception(e)
    else:
        colD.info("Evidence pack için snapshot kilitli olmalı (🔒).")
