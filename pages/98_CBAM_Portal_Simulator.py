from __future__ import annotations
from src.db.session import init_db
import json
import streamlit as st

from src.services.authz import current_user, login_view, logout_button
from src.services.projects import list_company_projects_for_user
from src.services.storage import write_bytes, storage_path_for_project
from src.services.cbam_reporting import build_cbam_xml_for_project_quarter
from src.services.cbam_xsd_validator import CBAMXSDValidator
from src.services.cbam_schema_registry import fetch_official_cbam_xsd_zip
from src.services.cbam_portal_simulator import simulate_portal_acceptance
from src.services.supplier_portal import list_producers, create_producer, submit_attestation, list_attestations

st.set_page_config(page_title="CBAM Portal Simulator", layout="wide")
init_db()
user = current_user()
if not user:
    login_view(); st.stop()

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

st.title("🧪 CBAM Portal Simulator + Producer/Supplier")
projects = list_company_projects_for_user(user)
if not projects:
    st.warning("Proje bulunamadı."); st.stop()
proj_map = {f"{p.name} (#{p.id})": int(p.id) for p in projects}
proj_label = st.selectbox("Proje", list(proj_map.keys()))
project_id = proj_map[proj_label]

c1,c2 = st.columns(2)
with c1: year = st.number_input("Yıl", min_value=2023, max_value=2100, value=2025, step=1)
with c2: quarter = st.selectbox("Çeyrek", [1,2,3,4], index=0)

st.divider()
st.subheader("1) Resmi XSD indir (opsiyonel)")
xsd_url = st.text_input("Resmi XSD ZIP URL", value="")
xsd_version = st.text_input("XSD etiket", value="2025-04-01")
if st.button("XSD indir"):
    if not xsd_url.strip():
        st.error("XSD URL boş olamaz.")
    else:
        try:
            schema = fetch_official_cbam_xsd_zip(xsd_url.strip(), xsd_version.strip())
            st.success(f"İndirildi. sha256={schema.sha256[:16]}...")
        except Exception as e:
            st.error(f"Hata: {e}")

st.divider()
st.subheader("2) XML üret + doğrula")
if st.button("XML üret", type="primary"):
    try:
        xml_bytes = build_cbam_xml_for_project_quarter(project_id=int(project_id), period_year=int(year), period_quarter=int(quarter))
        validator = CBAMXSDValidator.default_official()
        res = simulate_portal_acceptance(xml_bytes, xsd_validator=validator)
        if res.ok:
            st.success("PASS ✅ (XSD)")
        else:
            st.error("FAIL ❌")
            st.json(res.errors)
        uri = write_bytes(storage_path_for_project(project_id, f"cbam/xml/{year}Q{quarter}.xml"), xml_bytes)
        st.info(f"Kaydedildi: {uri}")
    except Exception as e:
        st.error(f"Başarısız: {e}")

st.divider()
st.subheader("3) Producer + Attestation")
company_id = int(getattr(user, "company_id", 0) or 0)
producers = list_producers(company_id)
with st.expander("➕ Yeni producer"):
    name = st.text_input("Producer adı", value="")
    country = st.text_input("Ülke", value="TR")
    vat = st.text_input("Vergi/VAT", value="")
    email = st.text_input("E-posta", value="")
    if st.button("Producer ekle"):
        try:
            p = create_producer(company_id, name=name, country=country, vat_or_tax_id=vat, contact_email=email)
            st.success(f"Eklendi: #{p.id}")
            st.rerun()
        except Exception as e:
            st.error(str(e))

producers = list_producers(company_id)
if producers:
    prod_map = {f"#{p.id} • {p.name} • {p.country}": int(p.id) for p in producers}
    prod_label = st.selectbox("Producer seç", list(prod_map.keys()))
    producer_id = prod_map[prod_label]
    signed_by = st.text_input("İmzalayan", value=user.email)
    declaration = st.text_area("Declaration JSON", value=json.dumps({"note":"producer declaration"}, ensure_ascii=False, indent=2), height=160)
    if st.button("Attestation gönder"):
        try:
            dec = json.loads(declaration)
            a = submit_attestation(producer_id=producer_id, project_id=int(project_id), period_year=int(year), period_quarter=int(quarter), declaration=dec, signed_by=signed_by)
            st.success(f"Gönderildi: #{a.id}")
        except Exception as e:
            st.error(str(e))

    atts = list_attestations(project_id=int(project_id), period_year=int(year), period_quarter=int(quarter))
    st.dataframe([{"id":a.id,"producer_id":a.producer_id,"status":a.status,"signed_by":a.signed_by} for a in atts], use_container_width=True)
else:
    st.info("Henüz producer yok.")
