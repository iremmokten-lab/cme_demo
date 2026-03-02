from __future__ import annotations

import json
import time
import streamlit as st

from src.services.portal_readiness import validate_portal_zip_structure, compute_readiness_score
from src.services.performance_benchmark import BenchmarkCase, run_benchmarks
from src.services.security_audit_suite import default_security_checks, build_security_audit_report
from src.services.regulation_watcher import WatchedSpec, check_specs
from src.services.docs_generator import build_methodology_summary_md, build_pdf_from_text

st.set_page_config(page_title="Final Kapanış Kontrolleri", layout="wide")

st.title("✅ Final Kapanış Kontrolleri (Son %5)")
st.caption("Bu sayfa: portal paketi kontrolü, performans ölçümü, güvenlik denetim raporu, regülasyon değişiklik takibi ve dokümantasyon çıktısı üretir.")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "1) Portal Readiness",
    "2) Performans",
    "3) Güvenlik Denetimi",
    "4) Regülasyon Takibi",
    "5) Dokümantasyon"
])

with tab1:
    st.subheader("Portal Readiness (ZIP Paket Kontrolü)")
    st.write("CBAM portalına göndereceğin ZIP paketini yükle. Sistem yapıyı kontrol eder.")
    up = st.file_uploader("CBAM Portal ZIP", type=["zip"])
    if up is not None:
        b = up.read()
        ok, errs, warns, meta = validate_portal_zip_structure(b)
        score = compute_readiness_score(xsd_ok=True, structure_ok=ok, error_count=len(errs), warning_count=len(warns))
        st.metric("Readiness Score", score)
        if ok:
            st.success("ZIP yapısı temel olarak uygun görünüyor.")
        else:
            st.error("ZIP yapısında kritik sorunlar var.")
        if errs:
            st.subheader("Hatalar")
            st.json(errs)
        if warns:
            st.subheader("Uyarılar")
            st.json(warns)
        st.subheader("Meta")
        st.json(meta)

with tab2:
    st.subheader("Performans Testleri (Simülasyon)")
    st.caption("Gerçek dataset olmadan, temel sistem yükünü simüle eder. Ölçüm sonuçları raporlanır.")
    n = st.slider("Simüle edilecek satır sayısı", min_value=10_000, max_value=300_000, value=50_000, step=10_000)

    def _case_hash():
        # hashing cost simulation
        import hashlib, os
        h = hashlib.sha256()
        for i in range(2000):
            h.update(str(i).encode("utf-8"))
        return h.hexdigest()

    def _case_json():
        payload = [{"i": i, "v": float(i) / 3.0, "s": "x"*10} for i in range(int(n))]
        _ = json.dumps(payload, ensure_ascii=False)
        return len(_)

    def _case_sort():
        arr = list(range(int(n)))
        arr.reverse()
        arr.sort()
        return arr[0], arr[-1]

    if st.button("Benchmark çalıştır", type="primary"):
        report = run_benchmarks([
            BenchmarkCase("hash_simulation", _case_hash),
            BenchmarkCase("json_dump_n_rows", _case_json),
            BenchmarkCase("sort_n_rows", _case_sort),
        ])
        st.json(report)
        st.download_button("performance_report.json indir", data=json.dumps(report, ensure_ascii=False, indent=2), file_name="performance_report.json", mime="application/json")

with tab3:
    st.subheader("Güvenlik Denetimi (İskelet Rapor)")
    st.caption("Bu rapor format standardı sağlar. Repo entegre edilince gerçek RLS/tenant testleri eklenebilir.")
    checks = default_security_checks()
    report = build_security_audit_report(checks=checks, meta={"note": "Gerçek izolasyon testleri için DB bağlantılı testler eklenmelidir."})
    st.json(report)
    st.download_button("security_audit_report.json indir", data=json.dumps(report, ensure_ascii=False, indent=2), file_name="security_audit_report.json", mime="application/json")

with tab4:
    st.subheader("Regülasyon Takibi (Watcher)")
    st.caption("URL ver ve içerik hash'ini izle. Değişiklik olursa 'CHANGED' döner.")
    default_specs = [
        {"name":"CBAM Registry & Reporting", "url":""},
        {"name":"ETS MRR (EUR-Lex)", "url":""},
    ]
    specs_json = st.text_area("İzlenecek spec listesi (JSON)", value=json.dumps(default_specs, ensure_ascii=False, indent=2), height=160)
    if st.button("Kontrol et"):
        try:
            items = json.loads(specs_json)
            specs = [WatchedSpec(name=i.get("name",""), url=i.get("url","")) for i in items if i.get("url")]
            if not specs:
                st.warning("En az bir URL girmen gerekiyor.")
            else:
                rep = check_specs(specs)
                st.json(rep)
                st.download_button("regulation_watch_report.json indir", data=json.dumps(rep, ensure_ascii=False, indent=2), file_name="regulation_watch_report.json", mime="application/json")
        except Exception as e:
            st.error(f"JSON okunamadı: {e}")

with tab5:
    st.subheader("Dokümantasyon Üretimi")
    st.caption("Metodoloji özet dokümanı (MD) ve PDF üretir.")
    title = st.text_input("Doküman başlığı", value="Methodology & Compliance Summary")
    s1 = st.text_area("Bölüm 1", value="CBAM: boundary, allocation, scrap, electricity methodology ve kanıt yönetimi.", height=90)
    s2 = st.text_area("Bölüm 2", value="ETS: monitoring plan, tier justification, uncertainty ve QA/QC evidence yaklaşımı.", height=90)
    s3 = st.text_area("Bölüm 3", value="Audit: snapshot/replay/determinism, evidence pack ve regülasyon sürüm kilidi.", height=90)

    if st.button("MD + PDF üret", type="primary"):
        md = build_methodology_summary_md(title, [
            {"heading":"CBAM", "body": s1},
            {"heading":"ETS", "body": s2},
            {"heading":"Audit", "body": s3},
        ])
        pdf = build_pdf_from_text(title, [s1, s2, s3])
        st.download_button("methodology_summary.md indir", data=md.encode("utf-8"), file_name="methodology_summary.md", mime="text/markdown")
        st.download_button("methodology_summary.pdf indir", data=pdf, file_name="methodology_summary.pdf", mime="application/pdf")
