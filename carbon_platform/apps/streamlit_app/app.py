import os
import streamlit as st
import httpx

st.set_page_config(page_title="Carbon Compliance Platform", layout="wide")

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

def api_client():
    headers = {}
    token = st.session_state.get("access_token")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.Client(base_url=API_BASE_URL, timeout=60.0, headers=headers)

def ensure_login():
    if "access_token" not in st.session_state:
        st.warning("Devam etmek için giriş yapın.")
        st.stop()

st.sidebar.title("Carbon Compliance Platform")
st.sidebar.caption("CBAM + EU ETS + MRV | Türkçe UI")

page = st.sidebar.radio(
    "Menü",
    [
        "Giriş",
        "Tesisler",
        "Dokümanlar",
        "MRV / İzleme Planı",
        "Faktör Kütüphanesi",
        "Ürün & Malzeme Kataloğu",
        "CBAM Veri Girişi",
        "CBAM Hesap / Rapor",
        "Doğrulama (Verification)",
        "Senaryo & Optimizasyon",
        "Veri Toplama",
        "Hesaplamalar",
        "Evidence Pack",
    ],
)

st.sidebar.divider()
st.sidebar.write("API:", API_BASE_URL)

# -------------------------
# GİRİŞ
# -------------------------
if page == "Giriş":
    st.header("Giriş / Tenant Bootstrap")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Tenant + Admin oluştur (Bootstrap)")
        tname = st.text_input("Tenant adı", value="")
        aemail = st.text_input("Admin e-posta", value="")
        apass = st.text_input("Admin şifre", value="", type="password")
        if st.button("Bootstrap Oluştur"):
            with api_client() as c:
                r = c.post("/tenants/bootstrap", json={"tenant_name": tname, "admin_email": aemail, "admin_password": apass})
            if r.status_code == 200:
                st.success(f"Tenant oluşturuldu. tenant_id: {r.json()['id']}")
            else:
                st.error(r.text)

        st.subheader("Tenant listesini getir")
        if st.button("Tenantları Listele"):
            with api_client() as c:
                r = c.get("/tenants/list")
            if r.status_code == 200:
                st.json(r.json())
            else:
                st.error(r.text)

    with col2:
        st.subheader("Giriş")
        tenant_id = st.text_input("Tenant ID", value=st.session_state.get("tenant_id", ""))
        email = st.text_input("E-posta", value=st.session_state.get("email", ""))
        password = st.text_input("Şifre", type="password", value="")
        if st.button("Giriş Yap"):
            with httpx.Client(base_url=API_BASE_URL, timeout=60.0) as c:
                r = c.post("/auth/login", json={"tenant_id": tenant_id, "email": email, "password": password})
            if r.status_code == 200:
                st.session_state["access_token"] = r.json()["access_token"]
                st.session_state["tenant_id"] = tenant_id
                st.session_state["email"] = email
                st.success("Giriş başarılı.")
            else:
                st.error(r.text)

    if "access_token" in st.session_state:
        st.info("Giriş yapıldı. Sol menüden devam edin.")
        with api_client() as c:
            me = c.get("/auth/me")
        if me.status_code == 200:
            st.caption("Kullanıcı scope bilgisi (Facility-scope RLS v2):")
            st.json(me.json())

# -------------------------
# TESİSLER
# -------------------------
elif page == "Tesisler":
    ensure_login()
    st.header("Tesis Yönetimi")

    with st.expander("Yeni tesis oluştur", expanded=True):
        name = st.text_input("Tesis adı")
        country = st.text_input("Ülke")
        ets = st.checkbox("EU ETS kapsamı", value=False)
        cbam = st.checkbox("CBAM kapsamı", value=False)
        if st.button("Tesis Oluştur"):
            with api_client() as c:
                r = c.post("/facilities/", json={"name": name, "country": country or None, "ets_in_scope": ets, "cbam_in_scope": cbam})
            if r.status_code == 200:
                st.success("Tesis oluşturuldu.")
            else:
                st.error(r.text)

    st.subheader("Tesisler")
    with api_client() as c:
        r = c.get("/facilities/")
    if r.status_code == 200:
        st.dataframe(r.json(), use_container_width=True)
    else:
        st.error(r.text)

# -------------------------
# DOKÜMANLAR
# -------------------------
elif page == "Dokümanlar":
    ensure_login()
    st.header("Doküman Yönetimi (S3 / Local)")

    st.subheader("Doküman Yükle")
    doc_type = st.selectbox("Doküman tipi", ["invoice", "calibration", "lab_report", "customs", "other"])
    up = st.file_uploader("Dosya seç", type=None)
    if st.button("Yükle") and up is not None:
        files = {"file": (up.name, up.getvalue())}
        data = {"doc_type": doc_type}
        with api_client() as c:
            rr = c.post("/documents/upload", data=data, files=files)
        if rr.status_code == 200:
            st.success("Yüklendi.")
            st.json(rr.json())
        else:
            st.error(rr.text)

    st.subheader("Doküman Listesi")
    with api_client() as c:
        r = c.get("/documents/")
    if r.status_code == 200:
        docs = r.json()
        st.dataframe(docs, use_container_width=True)
        if docs:
            doc_id = st.text_input("Presigned URL için document_id")
            if st.button("Presigned URL üret"):
                with api_client() as c:
                    pr = c.get(f"/documents/{doc_id}/presign")
                if pr.status_code == 200:
                    st.success("URL üretildi (süreli).")
                    st.write(pr.json()["url"])
                else:
                    st.error(pr.text)
    else:
        st.error(r.text)

# -------------------------
# MRV / İZLEME PLANI
# -------------------------
elif page == "MRV / İzleme Planı":
    ensure_login()
    st.header("MRV / İzleme Planı (Monitoring Plan)")
    with api_client() as c:
        fac = c.get("/facilities/")
    facilities = fac.json() if fac.status_code == 200 else []
    if not facilities:
        st.warning("Önce tesis oluşturun.")
        st.stop()
    fac_map = {f["name"]: f["id"] for f in facilities}
    fac_name = st.selectbox("Tesis seç", list(fac_map.keys()))
    facility_id = fac_map[fac_name]

    colA, colB = st.columns(2)
    with colA:
        st.subheader("Methodology Registry (Tier/Method)")
        code = st.text_input("Kod (örn: ETS_FUEL_COMBUSTION_TIER2)")
        name = st.text_input("İsim (Türkçe)")
        scope = st.selectbox("Kapsam", ["ETS", "CBAM", "MRV"])
        tier_level = st.text_input("Tier (opsiyonel: Tier 1/2/3)")
        reg_ref = st.text_input("Regülasyon referansı (örn: EU ETS MRR (EU) 2018/2066)")
        desc = st.text_area("Açıklama (opsiyonel)")
        if st.button("Methodology Oluştur"):
            with api_client() as c:
                r = c.post("/mrv/methodologies", json={
                    "code": code, "name": name, "scope": scope,
                    "tier_level": tier_level or None,
                    "reg_reference": reg_ref,
                    "description_tr": desc or None
                })
            if r.status_code == 200:
                st.success("Oluşturuldu.")
            else:
                st.error(r.text)

        with api_client() as c:
            r = c.get("/mrv/methodologies", params={"scope": scope, "status": "active"})
        if r.status_code == 200:
            st.dataframe(r.json(), use_container_width=True)
        else:
            st.error(r.text)

    with colB:
        st.subheader("Monitoring Plan Oluştur")
        version = st.number_input("Versiyon", value=1, step=1
