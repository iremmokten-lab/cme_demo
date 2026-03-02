from __future__ import annotations

import json
import streamlit as st

from src.services.authz import current_user, login_view, logout_button
from src.services.projects import list_company_projects_for_user
from src.services.storage import read_bytes, write_bytes, storage_path_for_project
from src.services.cbam_reporting import build_cbam_xml_for_project_quarter
from src.services.cbam_xsd_validator import CBAMXSDValidator
from src.services.cbam_schema_registry import fetch_official_cbam_xsd_zip
from src.services.cbam_portal_simulator import simulate_portal_acceptance
from src.services.supplier_portal import list_producers, create_producer, submit_attestation, list_attestations

st.set_page_config(page_title="CBAM Portal Simulator", layout="wide")

user = current_user()
if not user:
    login_view()
    st.stop()

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

st.title("🧪 CBAM Portal Simulator + Supplier/Producer")
st.caption("Amaç: CBAM XML'in resmi XSD'den geçmesini ve portal benzeri kontrolleri test etmek. Ayrıca producer attestation yönetimi.")

projects = list_company_projects_for_user(user)
if not projects:
    st.warning("Bu şirkete bağlı proje bulunamadı.")
    st.stop()

proj_label_to_id = {f"{p.name} (#{p.id})": int(p.id) for p in projects}
proj_label = st.selectbox("Proje seç", options=list(proj_label_to_id.keys()))
project_id = proj_label_to_id[proj_label]

c1, c2 = st.columns(2)
with c1:
    year = st.number_input("Yıl", min_value=2023, max_value=2100, value=2025, step=1)
with c2:
    quarter = st.selectbox("Çeyrek", [1,2,3,4], index=0)

st.divider()
st.subheader("1) Official XSD (indir + doğrula)")
st.caption("Resmi XSD ZIP URL'sini gir. (CBAM Registry & Reporting sayfasındaki quarterly report XSD ZIP)")

xsd_url = st.text_input("Resmi XSD ZIP URL", value="")
xsd_version = st.text_input("XSD sürüm etiketi (ör. 2025-04-01)", value="2025-04-01")

validator = None
if st.button("XSD indir ve hazırla"):
    if not xsd_url.strip():
        st.error("XSD URL boş olamaz.")
    else:
        try:
            schema = fetch_official_cbam_xsd_zip(xsd_url.strip(), xsd_version.strip())
            st.success(f"XSD indirildi. sha256={schema.sha256[:16]}...")
        except Exception as e:
            st.error(f"XSD indirilemedi: {e}")

st.divider()
st.subheader("2) XML üret ve portal simülasyonu")
st.caption("Seçilen proje ve dönem için CBAM XML üretir, XSD ile doğrular ve portal kabul simülasyonu çalıştırır.")

if st.button("XML üret + doğrula + simüle et", type="primary"):
    try:
        # Build XML (existing service)
        xml_bytes = build_cbam_xml_for_project_quarter(project_id=int(project_id), period_year=int(year), period_quarter=int(quarter))
        # Validator uses cached schema if available; falls back to packaged XSD if configured in app secrets.
        validator = CBAMXSDValidator.default_official()
        res = simulate_portal_acceptance(xml_bytes, xsd_validator=validator)
        if res.ok:
            st.success("Portal simülasyonu: PASS ✅")
        else:
            st.error("Portal simülasyonu: FAIL ❌")
            st.json(res.errors)
        # Save XML to storage
        uri = write_bytes(storage_path_for_project(project_id, f"cbam/xml/{year}Q{quarter}.xml"), xml_bytes)
        st.info(f"XML kaydedildi: {uri}")
    except Exception as e:
        st.error(f"İşlem başarısız: {e}")

st.divider()
st.subheader("3) Producer yönetimi + Attestation")
st.caption("CBAM için üretici beyanı/attestation toplamak için basit portal ekranı.")

producers = list_producers(getattr(user, "company_id", None) or 0)
with st.expander("➕ Yeni producer ekle"):
    name = st.text_input("Producer adı")
    country = st.text_input("Ülke", value="TR")
    vat = st.text_input("Vergi/VAT no", value="")
    email = st.text_input("İletişim e-posta", value="")
    if st.button("Producer ekle"):
        try:
            p = create_producer(int(getattr(user, "company_id", 0) or 0), name=name, country=country, vat_or_tax_id=vat, contact_email=email)
            st.success(f"Producer eklendi: #{p.id}")
            st.rerun()
        except Exception as e:
            st.error(f"Producer eklenemedi: {e}")

producers = list_producers(getattr(user, "company_id", None) or 0)
if not producers:
    st.info("Henüz producer yok.")
else:
    prod_map = {f"#{p.id} • {p.name} • {p.country}": int(p.id) for p in producers}
    prod_label = st.selectbox("Producer seç", list(prod_map.keys()))
    producer_id = prod_map[prod_label]

    st.markdown("### Attestation gönder")
    signed_by = st.text_input("İmzalayan (isim/e-posta)", value=user.email)
    declaration = st.text_area("Declaration JSON", value=json.dumps({"note":"producer declaration"}, ensure_ascii=False, indent=2), height=180)
    if st.button("Attestation gönder"):
        try:
            dec = json.loads(declaration)
            a = submit_attestation(producer_id=producer_id, project_id=int(project_id), period_year=int(year), period_quarter=int(quarter), declaration=dec, signed_by=signed_by)
            st.success(f"Attestation gönderildi: #{a.id}")
        except Exception as e:
            st.error(f"Gönderilemedi: {e}")

    st.markdown("### Bu dönem attestations")
    atts = list_attestations(project_id=int(project_id), period_year=int(year), period_quarter=int(quarter))
    if not atts:
        st.caption("Attestation yok.")
    else:
        st.dataframe([{"id":a.id, "producer_id":a.producer_id, "status":a.status, "signed_by":a.signed_by, "signed_at":str(a.signed_at)} for a in atts], use_container_width=True)
