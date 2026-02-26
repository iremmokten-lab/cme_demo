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
                r = c.post(
                    "/tenants/bootstrap",
                    json={"tenant_name": tname, "admin_email": aemail, "admin_password": apass},
                )
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
                r = c.post(
                    "/facilities/",
                    json={"name": name, "country": country or None, "ets_in_scope": ets, "cbam_in_scope": cbam},
                )
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
                r = c.post(
                    "/mrv/methodologies",
                    json={
                        "code": code,
                        "name": name,
                        "scope": scope,
                        "tier_level": tier_level or None,
                        "reg_reference": reg_ref,
                        "description_tr": desc or None,
                    },
                )
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
        version = st.number_input("Versiyon", value=1, step=1, min_value=1)
        eff_from = st.text_input("Geçerlilik başlangıcı (YYYY-MM-DD, opsiyonel)")
        eff_to = st.text_input("Geçerlilik bitişi (YYYY-MM-DD, opsiyonel)")
        notes = st.text_area("Genel notlar (TR) (opsiyonel)")

        if st.button("Monitoring Plan Oluştur"):
            with api_client() as c:
                r = c.post(
                    "/mrv/monitoring-plans",
                    json={
                        "facility_id": facility_id,
                        "version": int(version),
                        "effective_from": eff_from or None,
                        "effective_to": eff_to or None,
                        "overall_notes_tr": notes or None,
                    },
                )
            if r.status_code == 200:
                st.success("Monitoring Plan oluşturuldu.")
            else:
                st.error(r.text)

        st.subheader("Monitoring Plan Listesi")
        with api_client() as c:
            r = c.get("/mrv/monitoring-plans", params={"facility_id": facility_id})
        plans = r.json() if r.status_code == 200 else []
        if r.status_code == 200:
            st.dataframe(plans, use_container_width=True)
        else:
            st.error(r.text)

        st.divider()
        st.subheader("Monitoring Method (Plan'a bağlı)")

        plan_ids = [p["id"] for p in plans] if isinstance(plans, list) else []
        if plan_ids:
            mp_id = st.selectbox("Monitoring Plan seç", plan_ids)
        else:
            mp_id = None
            st.info("Önce Monitoring Plan oluşturun.")

        emission_source = st.selectbox("Emisyon kaynağı", ["fuel", "electricity", "process", "material"])
        method_type = st.selectbox("Metot tipi", ["Calculation", "Measurement"])
        tier_level2 = st.text_input("Tier (opsiyonel)")
        uncert = st.text_input("Uncertainty class (opsiyonel)")
        methodology_id = st.text_input("Methodology ID (opsiyonel)")
        ref_std = st.text_input("Reference standard (opsiyonel: ISO/IPCC vb.)")

        if st.button("Monitoring Method Ekle") and mp_id:
            with api_client() as c:
                r = c.post(
                    "/mrv/monitoring-methods",
                    json={
                        "monitoring_plan_id": mp_id,
                        "emission_source": emission_source,
                        "method_type": method_type,
                        "tier_level": tier_level2 or None,
                        "uncertainty_class": uncert or None,
                        "methodology_id": methodology_id or None,
                        "reference_standard": ref_std or None,
                    },
                )
            if r.status_code == 200:
                st.success("Monitoring Method eklendi.")
            else:
                st.error(r.text)

        if mp_id:
            with api_client() as c:
                r = c.get("/mrv/monitoring-methods", params={"monitoring_plan_id": mp_id})
            if r.status_code == 200:
                st.dataframe(r.json(), use_container_width=True)
            else:
                st.error(r.text)

        st.divider()
        st.subheader("Metering Assets (Sayaç / Ölçüm ekipmanı)")
        asset_type = st.text_input("Asset type (örn: Gas meter / Electricity meter)")
        serial = st.text_input("Serial no (opsiyonel)")
        cal_sched = st.text_input("Calibration schedule (opsiyonel)")
        last_doc = st.text_input("Last calibration document_id (opsiyonel)")

        if st.button("Metering Asset Ekle"):
            with api_client() as c:
                r = c.post(
                    "/mrv/metering-assets",
                    json={
                        "facility_id": facility_id,
                        "asset_type": asset_type,
                        "serial_no": serial or None,
                        "calibration_schedule": cal_sched or None,
                        "last_calibration_doc_id": last_doc or None,
                    },
                )
            if r.status_code == 200:
                st.success("Metering Asset eklendi.")
            else:
                st.error(r.text)

        with api_client() as c:
            r = c.get("/mrv/metering-assets", params={"facility_id": facility_id})
        if r.status_code == 200:
            st.dataframe(r.json(), use_container_width=True)
        else:
            st.error(r.text)

        st.divider()
        st.subheader("QA/QC Controls")
        if mp_id:
            ctrl_type = st.text_input("Control type (örn: cross-check / reconciliation)")
            freq = st.text_input("Frequency (opsiyonel: monthly/quarterly)")
            acc = st.text_area("Acceptance criteria (TR) (opsiyonel)")
            if st.button("QA/QC Control Ekle"):
                with api_client() as c:
                    r = c.post(
                        "/mrv/qaqc-controls",
                        json={
                            "monitoring_plan_id": mp_id,
                            "control_type": ctrl_type,
                            "frequency": freq or None,
                            "acceptance_criteria_tr": acc or None,
                        },
                    )
                if r.status_code == 200:
                    st.success("QA/QC control eklendi.")
                else:
                    st.error(r.text)

            with api_client() as c:
                r = c.get("/mrv/qaqc-controls", params={"monitoring_plan_id": mp_id})
            if r.status_code == 200:
                st.dataframe(r.json(), use_container_width=True)
            else:
                st.error(r.text)
        else:
            st.info("QA/QC eklemek için önce Monitoring Plan seçin/oluşturun.")

# -------------------------
# FAKTÖR KÜTÜPHANESİ
# -------------------------
elif page == "Faktör Kütüphanesi":
    ensure_login()
    st.header("Faktör Kütüphanesi (Emission Factors)")
    st.caption("ETS/CBAM/MRV hesaplamaları için faktör yönetimi (Governance + versiyonlama).")

    with api_client() as c:
        r = c.get("/factors/")
    if r.status_code == 200:
        st.dataframe(r.json(), use_container_width=True)
    else:
        st.error(r.text)

    st.subheader("Yeni faktör ekle")
    factor_type = st.selectbox("Factor type", ["fuel", "grid", "process", "material_embedded", "custom"])
    key = st.text_input("Key (örn: NAT_GAS / TR_GRID_2024 / CLINKER)")
    value = st.number_input("Value", value=0.0)
    unit = st.text_input("Unit (örn: tCO2/TJ, tCO2/MWh)")
    source = st.text_input("Source (örn: IPCC 2006 / EU MRR / National)")
    valid_from = st.text_input("Valid from (YYYY-MM-DD, opsiyonel)")
    valid_to = st.text_input("Valid to (YYYY-MM-DD, opsiyonel)")
    notes = st.text_area("Notes (TR, opsiyonel)")

    if st.button("Faktör Oluştur"):
        with api_client() as c:
            rr = c.post(
                "/factors/",
                json={
                    "factor_type": factor_type,
                    "key": key,
                    "value": value,
                    "unit": unit or None,
                    "source": source or None,
                    "valid_from": valid_from or None,
                    "valid_to": valid_to or None,
                    "notes_tr": notes or None,
                },
            )
        if rr.status_code == 200:
            st.success("Faktör oluşturuldu.")
        else:
            st.error(rr.text)

# -------------------------
# ÜRÜN & MALZEME KATALOĞU
# -------------------------
elif page == "Ürün & Malzeme Kataloğu":
    ensure_login()
    st.header("Ürün & Malzeme Kataloğu (CBAM ürünleri + precursor)")
    with api_client() as c:
        fac = c.get("/facilities/")
    facilities = fac.json() if fac.status_code == 200 else []
    if not facilities:
        st.warning("Önce tesis oluşturun.")
        st.stop()
    fac_map = {f["name"]: f["id"] for f in facilities}
    fac_name = st.selectbox("Tesis seç", list(fac_map.keys()))
    facility_id = fac_map[fac_name]

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Ürün oluştur")
        product_code = st.text_input("Product code")
        pname = st.text_input("Ürün adı")
        unit = st.text_input("Birim (örn: ton)")
        cn = st.text_input("CN code (opsiyonel)")
        if st.button("Ürün Oluştur"):
            with api_client() as c:
                r = c.post(
                    "/catalog/products",
                    json={
                        "facility_id": facility_id,
                        "product_code": product_code,
                        "name": pname,
                        "unit": unit or "ton",
                        "cn_code": cn or None,
                    },
                )
            if r.status_code == 200:
                st.success("Ürün oluşturuldu.")
            else:
                st.error(r.text)

        with api_client() as c:
            r = c.get("/catalog/products", params={"facility_id": facility_id})
        if r.status_code == 200:
            st.dataframe(r.json(), use_container_width=True)
        else:
            st.error(r.text)

    with col2:
        st.subheader("Malzeme oluştur (precursor)")
        material_code = st.text_input("Material code")
        mname = st.text_input("Malzeme adı")
        munit = st.text_input("Birim (örn: ton)")
        embedded_factor_id = st.text_input("Embedded factor id (opsiyonel)")
        if st.button("Malzeme Oluştur"):
            with api_client() as c:
                r = c.post(
                    "/catalog/materials",
                    json={
                        "material_code": material_code,
                        "name": mname,
                        "unit": munit or "ton",
                        "embedded_factor_id": embedded_factor_id or None,
                    },
                )
            if r.status_code == 200:
                st.success("Malzeme oluşturuldu.")
            else:
                st.error(r.text)

        with api_client() as c:
            r = c.get("/catalog/materials")
        if r.status_code == 200:
            st.dataframe(r.json(), use_container_width=True)
        else:
            st.error(r.text)

# -------------------------
# CBAM VERİ GİRİŞİ
# -------------------------
elif page == "CBAM Veri Girişi":
    ensure_login()
    st.header("CBAM Veri Girişi (Production + Materials + Exports)")
    st.caption("Embedded emissions hesaplaması için üretim ve precursor girdileri.")

    with api_client() as c:
        fac = c.get("/facilities/")
    facilities = fac.json() if fac.status_code == 200 else []
    if not facilities:
        st.warning("Önce tesis oluşturun.")
        st.stop()
    fac_map = {f["name"]: f["id"] for f in facilities}
    fac_name = st.selectbox("Tesis seç", list(fac_map.keys()))
    facility_id = fac_map[fac_name]

    activity_record_id = st.text_input("Activity record id (MRV Activity Data ID)")

    st.subheader("Production Record ekle")
    product_id = st.text_input("Product id")
    qty = st.number_input("Quantity", value=0.0)
    unit = st.text_input("Unit", value="ton")
    doc_id = st.text_input("Document id (opsiyonel)")
    if st.button("Production Kaydet"):
        with api_client() as c:
            r = c.post(
                "/cbam/production",
                json={
                    "activity_record_id": activity_record_id,
                    "product_id": product_id,
                    "quantity": qty,
                    "unit": unit,
                    "doc_id": doc_id or None,
                },
            )
        if r.status_code == 200:
            st.success("Kaydedildi.")
        else:
            st.error(r.text)

    st.subheader("Material Input ekle (precursor)")
    m_product_id = st.text_input("Product id (malzeme bağlı)")
    material_id = st.text_input("Material id")
    m_qty = st.number_input("Material quantity", value=0.0)
    m_unit = st.text_input("Material unit", value="ton")
    if st.button("Material Input Kaydet"):
        with api_client() as c:
            r = c.post(
                "/cbam/material-inputs",
                json={
                    "activity_record_id": activity_record_id,
                    "product_id": m_product_id,
                    "material_id": material_id,
                    "quantity": m_qty,
                    "unit": m_unit,
                },
            )
        if r.status_code == 200:
            st.success("Kaydedildi.")
        else:
            st.error(r.text)

    st.subheader("Exports ekle")
    e_product_id = st.text_input("Export product id")
    e_qty = st.number_input("Export quantity", value=0.0)
    e_unit = st.text_input("Export unit", value="ton")
    dest = st.text_input("Destination (örn: EU)")
    period_start = st.text_input("Period start (YYYY-MM-DD)")
    period_end = st.text_input("Period end (YYYY-MM-DD)")
    if st.button("Export Kaydet"):
        with api_client() as c:
            r = c.post(
                "/cbam/exports",
                json={
                    "facility_id": facility_id,
                    "product_id": e_product_id,
                    "quantity": e_qty,
                    "unit": e_unit,
                    "destination": dest,
                    "period_start": period_start,
                    "period_end": period_end,
                },
            )
        if r.status_code == 200:
            st.success("Kaydedildi.")
        else:
            st.error(r.text)

# -------------------------
# CBAM HESAP / RAPOR
# -------------------------
elif page == "CBAM Hesap / Rapor":
    ensure_login()
    st.header("CBAM Hesaplama & Rapor")
    with api_client() as c:
        fac = c.get("/facilities/")
    facilities = fac.json() if fac.status_code == 200 else []
    if not facilities:
        st.warning("Önce tesis oluşturun.")
        st.stop()
    fac_map = {f["name"]: f["id"] for f in facilities}
    fac_name = st.selectbox("Tesis seç", list(fac_map.keys()))
    facility_id = fac_map[fac_name]

    activity_record_id = st.text_input("Activity record id")
    period_start = st.text_input("Period start (YYYY-MM-DD)")
    period_end = st.text_input("Period end (YYYY-MM-DD)")
    ets_price = st.number_input("ETS fiyatı (EUR/tCO2)", value=0.0)
    method = st.selectbox("Method", ["default", "company_specific"])
    notes = st.text_area("Notlar (opsiyonel)")

    if st.button("CBAM Run"):
        with api_client() as c:
            r = c.post(
                "/cbam/run",
                json={
                    "facility_id": facility_id,
                    "activity_record_id": activity_record_id,
                    "period_start": period_start,
                    "period_end": period_end,
                    "ets_price": ets_price,
                    "method": method,
                    "notes": notes or None,
                },
            )
        if r.status_code == 200:
            st.success("Hesaplandı.")
            st.json(r.json())
        else:
            st.error(r.text)

# -------------------------
# DOĞRULAMA
# -------------------------
elif page == "Doğrulama (Verification)":
    ensure_login()
    st.header("Verification Workflow (ETS/CBAM doğrulama)")
    st.caption("Bulgu (finding) + CAPA aksiyonları + status takibi.")

    with api_client() as c:
        fac = c.get("/facilities/")
    facilities = fac.json() if fac.status_code == 200 else []
    if not facilities:
        st.warning("Önce tesis oluşturun.")
        st.stop()
    fac_map = {f["name"]: f["id"] for f in facilities}
    fac_name = st.selectbox("Tesis seç", list(fac_map.keys()))
    facility_id = fac_map[fac_name]

    st.subheader("Yeni verification case")
    scope = st.selectbox("Scope", ["ETS", "CBAM"])
    period_start = st.text_input("Period start")
    period_end = st.text_input("Period end")
    verifier_org = st.text_input("Verifier org (opsiyonel)")
    notes = st.text_area("Notes (opsiyonel)")
    if st.button("Case Oluştur"):
        with api_client() as c:
            r = c.post(
                "/verification/cases",
                json={
                    "facility_id": facility_id,
                    "scope": scope,
                    "period_start": period_start,
                    "period_end": period_end,
                    "verifier_org": verifier_org or None,
                    "notes": notes or None,
                },
            )
        if r.status_code == 200:
            st.success("Case oluşturuldu.")
        else:
            st.error(r.text)

    with api_client() as c:
        r = c.get("/verification/cases", params={"facility_id": facility_id, "scope": scope})
    cases = r.json() if r.status_code == 200 else []
    if r.status_code == 200:
        st.dataframe(cases, use_container_width=True)
    else:
        st.error(r.text)

# -------------------------
# SENARYO & OPTİMİZASYON
# -------------------------
elif page == "Senaryo & Optimizasyon":
    ensure_login()
    st.header("Senaryo & Optimizasyon")
    st.caption("Carbon cost minimizasyonu: CBAM_cost * ETS_cost için senaryo run.")

    with api_client() as c:
        fac = c.get("/facilities/")
    facilities = fac.json() if fac.status_code == 200 else []
    if not facilities:
        st.warning("Önce tesis oluşturun.")
        st.stop()
    fac_map = {f["name"]: f["id"] for f in facilities}
    fac_name = st.selectbox("Tesis seç", list(fac_map.keys()))
    facility_id = fac_map[fac_name]

    st.subheader("Scenario oluştur")
    name = st.text_input("Scenario adı")
    desc = st.text_area("Açıklama (opsiyonel)")
    if st.button("Scenario Oluştur"):
        with api_client() as c:
            r = c.post(
                "/scenario/scenarios",
                json={"facility_id": facility_id, "name": name, "description": desc or None, "status": "draft"},
            )
        if r.status_code == 200:
            st.success("Scenario oluşturuldu.")
        else:
            st.error(r.text)

    with api_client() as c:
        r = c.get("/scenario/scenarios", params={"facility_id": facility_id})
    scenarios = r.json() if r.status_code == 200 else []
    if r.status_code == 200:
        st.dataframe(scenarios, use_container_width=True)
    else:
        st.error(r.text)

# -------------------------
# DİĞER SAYFALAR (placeholder)
# -------------------------
elif page in ["Veri Toplama", "Hesaplamalar", "Evidence Pack"]:
    ensure_login()
    st.header(page)
    st.info("Bu sayfa API servisleri tamamlandıkça genişletilecektir. Şu an repo stabilizasyon aşamasındadır.")
else:
    ensure_login()
    st.header(page)
    st.info("Bu menü için henüz UI tanımı tamamlanmadı.")
