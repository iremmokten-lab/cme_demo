from __future__ import annotations
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.services.projects import list_company_projects_for_user
from src.erp.masterdata.service import (
    upsert_product, list_products,
    upsert_material, list_materials,
    upsert_process, list_processes,
    set_bom_line, list_bom
)

st.set_page_config(page_title="Carbon ERP • Master Data", layout="wide")
user=current_user()
if not user:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

st.title("🏭 Carbon ERP • Master Data")
st.caption("ERP seviyesi için ürün/ham madde/proses/BOM master data yönetimi.")

projects=list_company_projects_for_user(user)
if not projects:
    st.warning("Proje bulunamadı."); st.stop()
proj_map={f"{p.name} (#{p.id})": int(p.id) for p in projects}
proj_label=st.selectbox("Proje seç", list(proj_map.keys()))
project_id=proj_map[proj_label]
actor_id=int(getattr(user,"id",0) or 0) or None

tab1,tab2,tab3,tab4=st.tabs(["Ürünler","Malzemeler","Prosesler","BOM"])

with tab1:
    st.subheader("Ürün")
    c1,c2,c3=st.columns(3)
    with c1: sku=st.text_input("SKU", value="")
    with c2: name=st.text_input("Ürün adı", value="")
    with c3: cn=st.text_input("CN Code", value="")
    uom=st.text_input("Birim (UOM)", value="t")
    if st.button("Kaydet / Güncelle", type="primary"):
        if not sku or not name:
            st.error("SKU ve Ürün adı zorunlu.")
        else:
            upsert_product(project_id, sku, name, cn_code=cn, uom=uom, actor_user_id=actor_id)
            st.success("Kaydedildi.")
            st.rerun()
    st.dataframe([{"id":p.id,"sku":p.sku,"name":p.name,"cn_code":p.cn_code,"uom":p.uom,"active":p.is_active} for p in list_products(project_id)], use_container_width=True)

with tab2:
    st.subheader("Malzeme")
    c1,c2=st.columns(2)
    with c1: code=st.text_input("Kod", value="", key="m_code")
    with c2: mname=st.text_input("Malzeme adı", value="", key="m_name")
    muom=st.text_input("Birim (UOM)", value="t", key="m_uom")
    if st.button("Kaydet / Güncelle", type="primary", key="m_save"):
        if not code or not mname:
            st.error("Kod ve Malzeme adı zorunlu.")
        else:
            upsert_material(project_id, code, mname, uom=muom, actor_user_id=actor_id)
            st.success("Kaydedildi.")
            st.rerun()
    st.dataframe([{"id":m.id,"code":m.code,"name":m.name,"uom":m.uom,"active":m.is_active} for m in list_materials(project_id)], use_container_width=True)

with tab3:
    st.subheader("Proses")
    c1,c2,c3=st.columns(3)
    with c1: pcode=st.text_input("Proses kodu", value="", key="p_code")
    with c2: pname=st.text_input("Proses adı", value="", key="p_name")
    with c3: sector=st.selectbox("Sektör", ["generic","cement","steel","aluminium","fertiliser","electricity","hydrogen"], index=0)
    if st.button("Kaydet / Güncelle", type="primary", key="p_save"):
        if not pcode or not pname:
            st.error("Kod ve Proses adı zorunlu.")
        else:
            upsert_process(project_id, pcode, pname, sector=sector, actor_user_id=actor_id)
            st.success("Kaydedildi.")
            st.rerun()
    st.dataframe([{"id":p.id,"code":p.code,"name":p.name,"sector":p.sector} for p in list_processes(project_id)], use_container_width=True)

with tab4:
    st.subheader("BOM (Ürün içeriği)")
    products=list_products(project_id)
    materials=list_materials(project_id)
    if not products or not materials:
        st.info("BOM tanımlamak için önce ürün ve malzeme oluştur.")
    else:
        pmap={f"#{p.id} {p.sku} • {p.name}": p.id for p in products}
        mmap={f"#{m.id} {m.code} • {m.name}": m.id for m in materials}
        c1,c2,c3=st.columns(3)
        with c1: psel=st.selectbox("Ürün", list(pmap.keys()))
        with c2: msel=st.selectbox("Malzeme", list(mmap.keys()))
        with c3: qty=st.text_input("Birim başına miktar", value="0")
        if st.button("BOM satırı kaydet", type="primary"):
            set_bom_line(project_id, int(pmap[psel]), int(mmap[msel]), qty_per_unit=qty, actor_user_id=actor_id)
            st.success("Kaydedildi.")
            st.rerun()
    st.dataframe([{"id":b.id,"product_id":b.product_id,"material_id":b.material_id,"qty_per_unit":b.qty_per_unit} for b in list_bom(project_id)], use_container_width=True)
