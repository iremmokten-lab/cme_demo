import json
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.session import db
from src.db.models import Project, CalculationSnapshot, Report
from src.services.exports import build_zip, build_xlsx_from_results

# mail opsiyonel: secrets yoksa panel kırılmasın
try:
    from src.services.mailer import send_pdf_mail
except Exception:
    send_pdf_mail = None


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


def _read_kpis(snapshot: CalculationSnapshot) -> dict:
    r = _read_results(snapshot)
    return (r.get("kpis") or {}) if isinstance(r, dict) else {}


def client_app(user):
    st.title("Müşteri Kontrol Paneli")

    if not getattr(user, "company_id", None):
        st.error("Bu kullanıcıya şirket atanmadı. (company_id boş)")
        st.info("Lütfen danışman/admin kullanıcı şirket ataması yapsın.")
        return

    # -------------------------
    # Projeler
    # -------------------------
    with db() as s:
        projects = (
            s.execute(
                select(Project)
                .where(Project.company_id == user.company_id)
                .order_by(Project.created_at.desc())
            )
            .scalars()
            .all()
        )

    if not projects:
        st.warning("Henüz proje yok.")
        st.markdown(
            """
**Ne yapmalısınız?**
- Danışman panelinden bir proje oluşturulmalı
- energy.csv ve production.csv yüklenmeli
- Baseline veya Senaryo çalıştırılmalı
            """
        )
        return

    # Üst seçim alanı
    project_labels = [f"{p.name} / {p.year} (id:{p.id})" for p in projects]
    psel = st.selectbox("Proje seçin", project_labels, index=0)
    project = projects[project_labels.index(psel)]

    # Snapshotlar
    with db() as s:
        snaps = (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == project.id)
                .order_by(CalculationSnapshot.created_at.desc())
            )
            .scalars()
            .all()
        )

    if not snaps:
        st.warning("Bu proje için henüz snapshot yok.")
        st.markdown(
            """
**Ne yapmalısınız?**
- Danışman panelinde **Hesaplama** sekmesinden Baseline çalıştırın
- veya **Senaryolar** sekmesinden senaryo çalıştırın
            """
        )
        return

    # Snapshot seçimi: varsayılan en yeni
    snap_labels = []
    for sn in snaps[:50]:
        r = _read_results(sn)
        scen = (r.get("scenario") or {}) if isinstance(r, dict) else {}
        kind = "Senaryo" if scen else "Baseline"
        name = scen.get("name") if scen else ""
        label = f"ID:{sn.id} • {kind}{(' — ' + name) if name else ''} • {sn.created_at}"
        snap_labels.append(label)

    sel = st.selectbox("Snapshot seçin", snap_labels, index=0)
    snapshot = snaps[snap_labels.index(sel)]

    results = _read_results(snapshot)
    kpis = (results.get("kpis") or {}) if isinstance(results, dict) else {}

    # -------------------------
    # Özet KPI Kartları
    # -------------------------
    st.subheader("KPI Özeti")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Toplam Emisyon (tCO2)", _fmt_tr(kpis.get("energy_total_tco2", 0), 3))
    c2.metric("Scope-1 (tCO2)", _fmt_tr(kpis.get("energy_scope1_tco2", 0), 3))
    c3.metric("CBAM Maliyeti (€)", _fmt_tr(kpis.get("cbam_cost_eur", 0), 2))
    c4.metric("ETS Maliyeti (TL)", _fmt_tr(kpis.get("ets_cost_tl", 0), 2))

    # -------------------------
    # Trend Grafikleri (son 20 snapshot)
    # -------------------------
    st.divider()
    st.subheader("Trend (son 20 snapshot)")

    trend_rows = []
    for sn in reversed(snaps[:20]):  # eski -> yeni
        k = _read_kpis(sn)
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
        # Streamlit varsayılan renkler
        st.line_chart(df)

    # -------------------------
    # Raporlar (PDF)
    # -------------------------
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
        st.info("Bu snapshot için henüz PDF rapor yok (danışman üretmiş olmalı).")
    else:
        for r in reports:
            uri = getattr(r, "storage_uri", None)
            created = getattr(r, "created_at", None)
            sha = getattr(r, "sha256", None)

            cols = st.columns([4, 2, 2])
            cols[0].write(f"📄 PDF • {created} • sha:{(sha[:10] + '…') if sha else '-'}")

            if uri:
                p = Path(str(uri))
                if p.exists():
                    data = p.read_bytes()
                    cols[1].download_button(
                        "PDF indir",
                        data=data,
                        file_name=p.name,
                        mime="application/pdf",
                        key=f"client_pdf_{r.id}",
                        use_container_width=True,
                    )
                else:
                    cols[1].warning("Dosya bulunamadı")
            else:
                cols[1].warning("URI yok")

            # Mail opsiyonel
            if send_pdf_mail is None:
                cols[2].caption("Mail özelliği kapalı")
            else:
                with cols[2]:
                    with st.popover("📧 Mail ile gönder"):
                        to_email = st.text_input("Alıcı e-posta", key=f"mail_to_{r.id}")
                        if st.button("Gönder", key=f"send_{r.id}", type="primary"):
                            try:
                                if not to_email:
                                    st.warning("E-posta girin.")
                                else:
                                    p = Path(str(uri))
                                    if not p.exists():
                                        st.error("PDF dosyası bulunamadı.")
                                    else:
                                        send_pdf_mail(to_email, p.read_bytes(), p.name)
                                        st.success("Gönderildi ✅")
                            except Exception as e:
                                st.error("Mail gönderimi başarısız.")
                                st.exception(e)

    # -------------------------
    # Export (ZIP/XLSX/JSON)
    # -------------------------
    st.divider()
    st.subheader("Export / İndirme")

    colA, colB, colC = st.columns(3)
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

    try:
        colC.download_button(
            "JSON indir",
            data=(snapshot.results_json or "{}").encode("utf-8"),
            file_name=f"snapshot_{snapshot.id}.json",
            mime="application/json",
            use_container_width=True,
        )
    except Exception as e:
        colC.error("JSON hazırlanamadı")
        colC.exception(e)
