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
        version = st.number_input("Versiyon", value=1, step=1)
        eff_from = st.text_input("Effective from (YYYY-MM-DD)")
        eff_to = st.text_input("Effective to (YYYY-MM-DD)")
        notes = st.text_area("Genel notlar")
        if st.button("Monitoring Plan Oluştur"):
            with api_client() as c:
                r = c.post("/mrv/monitoring-plans", json={
                    "facility_id": facility_id,
                    "version": int(version),
                    "effective_from": eff_from or None,
                    "effective_to": eff_to or None,
                    "overall_notes_tr": notes or None
                })
            if r.status_code == 200:
                st.success("Monitoring plan oluşturuldu.")
            else:
                st.error(r.text)

        st.subheader("Monitoring Plan Listesi")
        with api_client() as c:
            r = c.get("/mrv/monitoring-plans", params={"facility_id": facility_id})
        if r.status_code == 200:
            plans = r.json()
            st.dataframe(plans, use_container_width=True)
        else:
            st.error(r.text)
            st.stop()

        plan_ids = [p["id"] for p in plans] if plans else []
        if not plan_ids:
            st.info("Bu tesis için monitoring plan yok.")
            st.stop()

        selected_plan = st.selectbox("Plan seç", plan_ids)

        st.subheader("Monitoring Method Ekle")
        emission_source = st.selectbox("Emisyon kaynağı", ["fuel", "electricity", "process", "material"])
        method_type = st.selectbox("Yöntem tipi", ["Calculation", "Measurement"])
        mm_tier = st.text_input("Tier (opsiyonel)")
        mm_unc = st.text_input("Uncertainty sınıfı (opsiyonel)")
        mm_methodology_id = st.text_input("Methodology ID (opsiyonel)")
        mm_refstd = st.text_input("Reference standard (opsiyonel)")
        if st.button("Monitoring Method Oluştur"):
            with api_client() as c:
                r = c.post("/mrv/monitoring-methods", json={
                    "monitoring_plan_id": selected_plan,
                    "emission_source": emission_source,
                    "method_type": method_type,
                    "tier_level": mm_tier or None,
                    "uncertainty_class": mm_unc or None,
                    "methodology_id": mm_methodology_id or None,
                    "reference_standard": mm_refstd or None
                })
            if r.status_code == 200:
                st.success("Eklendi.")
            else:
                st.error(r.text)

        st.subheader("Monitoring Methods")
        with api_client() as c:
            r = c.get("/mrv/monitoring-methods", params={"monitoring_plan_id": selected_plan})
        if r.status_code == 200:
            st.dataframe(r.json(), use_container_width=True)
        else:
            st.error(r.text)

        st.subheader("QA/QC Kontrol Ekle")
        qc_type = st.text_input("Kontrol tipi (örn: completeness_check)")
        qc_freq = st.text_input("Sıklık (örn: monthly)")
        qc_criteria = st.text_area("Kabul kriteri (TR)")
        if st.button("QA/QC Oluştur"):
            with api_client() as c:
                r = c.post("/mrv/qaqc-controls", json={
                    "monitoring_plan_id": selected_plan,
                    "control_type": qc_type,
                    "frequency": qc_freq or None,
                    "acceptance_criteria_tr": qc_criteria or None
                })
            if r.status_code == 200:
                st.success("Eklendi.")
            else:
                st.error(r.text)

        with api_client() as c:
            r = c.get("/mrv/qaqc-controls", params={"monitoring_plan_id": selected_plan})
        if r.status_code == 200:
            st.dataframe(r.json(), use_container_width=True)
        else:
            st.error(r.text)

        st.subheader("Metering Asset Ekle")
        asset_type = st.selectbox("Asset tipi", ["gas_meter", "electricity_meter", "flow_meter"])
        serial_no = st.text_input("Seri no (opsiyonel)")
        cal_sched = st.text_input("Kalibrasyon planı (örn: yearly)")
        last_doc = st.text_input("Son kalibrasyon doc_id (opsiyonel)")
        if st.button("Metering Asset Oluştur"):
            with api_client() as c:
                r = c.post("/mrv/metering-assets", json={
                    "facility_id": facility_id,
                    "asset_type": asset_type,
                    "serial_no": serial_no or None,
                    "calibration_schedule": cal_sched or None,
                    "last_calibration_doc_id": last_doc or None
                })
            if r.status_code == 200:
                st.success("Eklendi.")
            else:
                st.error(r.text)

        with api_client() as c:
            r = c.get("/mrv/metering-assets", params={"facility_id": facility_id})
        if r.status_code == 200:
            st.dataframe(r.json(), use_container_width=True)
        else:
            st.error(r.text)

# -------------------------
# FAKTÖR KÜTÜPHANESİ
# -------------------------
elif page == "Faktör Kütüphanesi":
    ensure_login()
    st.header("Faktör Kütüphanesi & Governance")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Faktör Kaynağı Ekle")
        publisher = st.text_input("Yayıncı / Kurum")
        doc_url = st.text_input("Doküman URL (opsiyonel)")
        pub_date = st.text_input("Yayın tarihi (opsiyonel)")
        juris = st.text_input("Yargı alanı (opsiyonel)")
        if st.button("Kaynak Oluştur"):
            with api_client() as c:
                r = c.post("/factors/sources", json={"publisher": publisher, "document_url": doc_url or None, "publication_date": pub_date or None, "jurisdiction": juris or None})
            if r.status_code == 200:
                st.success(f"Kaynak oluşturuldu: {r.json()['id']}")
            else:
                st.error(r.text)

    with col2:
        st.subheader("Faktör Ekle (Önce 'proposed', sonra onay)")
        factor_type = st.selectbox("Faktör tipi", ["NCV", "EF", "grid", "process", "oxidation", "precursor"])
        value = st.number_input("Değer", value=0.0, format="%.8f")
        unit = st.text_input("Birim (örn: tCO2e/kWh, tCO2e/ton, tCO2e/GJ)")
        gas = st.text_input("Gaz (opsiyonel: CO2/CH4/N2O/CO2e)")
        source_id = st.text_input("Source ID (opsiyonel)")
        if st.button("Faktör Oluştur"):
            with api_client() as c:
                r = c.post("/factors/", json={"factor_type": factor_type, "value": value, "unit": unit, "gas": gas or None, "source_id": source_id or None, "version": 1})
            if r.status_code == 200:
                st.success(f"Faktör oluşturuldu: {r.json()['id']} (status: {r.json()['status']})")
            else:
                st.error(r.text)

    st.subheader("Faktörler")
    status = st.selectbox("Filtre: status", ["", "proposed", "approved", "retired"])
    ftype = st.selectbox("Filtre: factor_type", ["", "NCV", "EF", "grid", "process", "oxidation", "precursor"])
    params = {}
    if status:
        params["status"] = status
    if ftype:
        params["factor_type"] = ftype
    with api_client() as c:
        r = c.get("/factors/", params=params)
    if r.status_code == 200:
        st.dataframe(r.json(), use_container_width=True)
        st.caption("Not: Faktör onayı için admin rolü gerekir.")
        approve_id = st.text_input("Onaylanacak factor_id")
        if st.button("Faktörü Onayla"):
            with api_client() as c:
                rr = c.post(f"/factors/{approve_id}/approve")
            if rr.status_code == 200:
                st.success("Onaylandı.")
            else:
                st.error(rr.text)
    else:
        st.error(r.text)

# -------------------------
# ÜRÜN & MALZEME KATALOĞU
# -------------------------
elif page == "Ürün & Malzeme Kataloğu":
    ensure_login()
    st.header("Ürün & Malzeme (Precursor) Kataloğu")

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
        st.subheader("Ürün Oluştur")
        pcode = st.text_input("Ürün kodu")
        pname = st.text_input("Ürün adı")
        punit = st.text_input("Birim", value="ton")
        cn = st.text_input("CN/KN Code (opsiyonel)")
        if st.button("Ürün Kaydet"):
            with api_client() as c:
                r = c.post("/catalog/products", json={
                    "facility_id": facility_id,
                    "product_code": pcode,
                    "name": pname,
                    "unit": punit,
                    "cn_code": cn or None
                })
            if r.status_code == 200:
                st.success("Ürün kaydedildi.")
            else:
                st.error(r.text)

        st.subheader("Ürün Listesi")
        with api_client() as c:
            r = c.get("/catalog/products", params={"facility_id": facility_id})
        if r.status_code == 200:
            st.dataframe(r.json(), use_container_width=True)
        else:
            st.error(r.text)

    with col2:
        st.subheader("Malzeme (Precursor) Oluştur")
        mcode = st.text_input("Malzeme kodu")
        mname = st.text_input("Malzeme adı")
        munit = st.text_input("Birim", value="ton")
        embedded_factor_id = st.text_input("Embedded factor_id (approved) (opsiyonel)")
        if st.button("Malzeme Kaydet"):
            with api_client() as c:
                r = c.post("/catalog/materials", json={
                    "material_code": mcode,
                    "name": mname,
                    "unit": munit,
                    "embedded_factor_id": embedded_factor_id or None
                })
            if r.status_code == 200:
                st.success("Malzeme kaydedildi.")
            else:
                st.error(r.text)

        st.subheader("Malzeme Listesi")
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
    st.header("CBAM Veri Girişi (Üretim / Precursor / İhracat)")

    with api_client() as c:
        fac = c.get("/facilities/")
    facilities = fac.json() if fac.status_code == 200 else []
    if not facilities:
        st.warning("Önce tesis oluşturun.")
        st.stop()
    fac_map = {f["name"]: f["id"] for f in facilities}
    fac_name = st.selectbox("Tesis seç", list(fac_map.keys())))
    facility_id = fac_map[fac_name]

    # activity record seç
    with api_client() as c:
        ar = c.get("/activity/records", params={"facility_id": facility_id})
    records = ar.json() if ar.status_code == 200 else []
    if not records:
        st.warning("Bu tesis için activity record yok. Önce Veri Toplama'dan oluşturun.")
        st.stop()
    record_id = st.selectbox("Activity Record", [x["id"] for x in records])

    # products for facility
    with api_client() as c:
        pr = c.get("/catalog/products", params={"facility_id": facility_id})
    products = pr.json() if pr.status_code == 200 else []
    if not products:
        st.warning("Bu tesis için ürün yok. Önce Ürün & Malzeme Kataloğu'ndan ürün ekleyin.")
        st.stop()
    prod_map = {f'{p["product_code"]} - {p["name"]}': p["id"] for p in products}

    # materials
    with api_client() as c:
        mr = c.get("/catalog/materials")
    materials = mr.json() if mr.status_code == 200 else []
    mat_map = {f'{m["material_code"]} - {m["name"]}': m["id"] for m in materials} if materials else {}

    st.subheader("Ürün Bazlı Üretim (ProductionRecord)")
    prod_sel = st.selectbox("Ürün", list(prod_map.keys()))
    prod_qty = st.number_input("Üretim miktarı", value=0.0)
    prod_unit = st.text_input("Birim", value="ton")
    if st.button("Üretim Kaydı Ekle"):
        with api_client() as c:
            r = c.post("/cbam/production", json={
                "activity_record_id": record_id,
                "product_id": prod_map[prod_sel],
                "quantity": prod_qty,
                "unit": prod_unit
            })
        if r.status_code == 200:
            st.success("Üretim kaydı eklendi.")
        else:
            st.error(r.text)

    with api_client() as c:
        r = c.get("/cbam/production", params={"activity_record_id": record_id})
    if r.status_code == 200:
        st.dataframe(r.json(), use_container_width=True)

    st.divider()
    st.subheader("Ürün Bazlı Precursor / Material Input")
    if not materials:
        st.info("Malzeme kataloğu boş. Precursor girişi için malzeme ekleyin.")
    else:
        prod_sel2 = st.selectbox("Ürün (precursor için)", list(prod_map.keys()), key="prod2")
        mat_sel = st.selectbox("Malzeme", list(mat_map.keys()))
        mi_qty = st.number_input("Tüketim miktarı", value=0.0)
        mi_unit = st.text_input("Birim", value="ton", key="mi_unit")
        override_factor = st.text_input("Override embedded factor_id (approved) (opsiyonel)")
        if st.button("Material Input Ekle"):
            with api_client() as c:
                r = c.post("/cbam/material-inputs", json={
                    "activity_record_id": record_id,
                    "product_id": prod_map[prod_sel2],
                    "material_id": mat_map[mat_sel],
                    "quantity": mi_qty,
                    "unit": mi_unit,
                    "embedded_factor_id": override_factor or None
                })
            if r.status_code == 200:
                st.success("Material input eklendi.")
            else:
                st.error(r.text)

        with api_client() as c:
            r = c.get("/cbam/material-inputs", params={"activity_record_id": record_id})
        if r.status_code == 200:
            st.dataframe(r.json(), use_container_width=True)

    st.divider()
    st.subheader("İhracat (ExportRecord)")
    period_start = st.text_input("Dönem başlangıç (YYYY-MM-DD)")
    period_end = st.text_input("Dönem bitiş (YYYY-MM-DD)")
    prod_sel3 = st.selectbox("Ürün (ihracat)", list(prod_map.keys()), key="prod3")
    ex_qty = st.number_input("İhracat miktarı", value=0.0)
    ex_unit = st.text_input("Birim", value="ton", key="ex_unit")
    dest = st.text_input("Varış ülkesi (opsiyonel)")
    if st.button("İhracat Kaydı Ekle"):
        with api_client() as c:
            r = c.post("/cbam/exports", json={
                "facility_id": facility_id,
                "product_id": prod_map[prod_sel3],
                "period_start": period_start,
                "period_end": period_end,
                "export_qty": ex_qty,
                "unit": ex_unit,
                "destination": dest or None
            })
        if r.status_code == 200:
            st.success("İhracat kaydı eklendi.")
        else:
            st.error(r.text)

    with api_client() as c:
        r = c.get("/cbam/exports", params={"facility_id": facility_id, "period_start": period_start, "period_end": period_end})
    if r.status_code == 200:
        st.dataframe(r.json(), use_container_width=True)

# -------------------------
# CBAM HESAP / RAPOR
# -------------------------
elif page == "CBAM Hesap / Rapor":
    ensure_login()
    st.header("CBAM Hesap / Rapor (Embedded Emissions)")

    with api_client() as c:
        fac = c.get("/facilities/")
    facilities = fac.json() if fac.status_code == 200 else []
    if not facilities:
        st.warning("Önce tesis oluşturun.")
        st.stop()

    fac_map = {f["name"]: f["id"] for f in facilities}
    fac_name = st.selectbox("Tesis seç", list(fac_map.keys()))
    facility_id = fac_map[fac_name]

    with api_client() as c:
        ar = c.get("/activity/records", params={"facility_id": facility_id})
    records = ar.json() if ar.status_code == 200 else []
    if not records:
        st.warning("Bu tesis için activity record yok.")
        st.stop()
    activity_record_id = st.selectbox("Activity Record", [x["id"] for x in records])

    period_start = st.text_input("Dönem başlangıç (YYYY-MM-DD)", key="cbam_ps")
    period_end = st.text_input("Dönem bitiş (YYYY-MM-DD)", key="cbam_pe")
    ets_price = st.number_input("ETS fiyatı (EUR/tCO2)", value=75.0)

    st.caption("Ön koşul: Bu activity_record için /calc/run ile facility-level hesap yapılmış olmalı.")

    if st.button("CBAM Rapor Üret"):
        with api_client() as c:
            r = c.post("/cbam/run", json={
                "facility_id": facility_id,
                "activity_record_id": activity_record_id,
                "period_start": period_start,
                "period_end": period_end,
                "ets_price_eur_per_tco2": ets_price
            })
        if r.status_code == 200:
            out = r.json()
            st.success(f"CBAM rapor üretildi. report_hash: {out['report_hash']}")
            st.json(out["report"])
        else:
            st.error(r.text)

# -------------------------
# VERİ TOPLAMA / HESAPLAMALAR / EVIDENCE (önceki ekranlar aynı)
# -------------------------
elif page == "Veri Toplama":
    ensure_login()
    st.header("Veri Toplama (Activity Data)")

    with api_client() as c:
        fac = c.get("/facilities/")
    facilities = fac.json() if fac.status_code == 200 else []
    fac_map = {f["name"]: f["id"] for f in facilities} if facilities else {}

    st.subheader("Activity Record Oluştur")
    if facilities:
        fac_name = st.selectbox("Tesis seç", list(fac_map.keys()))
        facility_id = fac_map[fac_name]
    else:
        st.warning("Önce tesis oluşturun.")
        st.stop()

    period_start = st.text_input("Dönem başlangıç (YYYY-MM-DD)")
    period_end = st.text_input("Dönem bitiş (YYYY-MM-DD)")
    source_system = st.text_input("Kaynak sistem (opsiyonel)")
    if st.button("Activity Record Oluştur"):
        with api_client() as c:
            r = c.post("/activity/records", json={"facility_id": facility_id, "period_start": period_start, "period_end": period_end, "source_system": source_system or None})
        if r.status_code == 200:
            st.success(f"Oluşturuldu: {r.json()['id']}")
        else:
            st.error(r.text)

    st.subheader("Activity Record Listesi")
    with api_client() as c:
        r = c.get("/activity/records", params={"facility_id": facility_id})
    if r.status_code == 200:
        records = r.json()
        st.dataframe(records, use_container_width=True)
    else:
        st.error(r.text)
        st.stop()

    record_ids = [x["id"] for x in records] if records else []
    if not record_ids:
        st.info("Bu tesis için activity record yok.")
        st.stop()

    selected_record = st.selectbox("Kayıt seç", record_ids)

    st.divider()
    st.subheader("Yakıt Girişi (Direct emissions)")
    fuel_type = st.text_input("Yakıt tipi")
    qty = st.number_input("Miktar", value=0.0)
    unit = st.text_input("Birim (örn: ton, Nm3, m3)")
    ncv_id = st.text_input("NCV factor_id (approved)")
    ef_id = st.text_input("EF factor_id (approved)")
    of_id = st.text_input("Oxidation factor_id (approved veya boş -> 1.0)")
    if st.button("Yakıt Ekle"):
        with api_client() as c:
            rr = c.post("/activity/fuel", json={
                "activity_record_id": selected_record,
                "fuel_type": fuel_type,
                "quantity": qty,
                "unit": unit,
                "ncv_factor_id": ncv_id or None,
                "ef_factor_id": ef_id or None,
                "oxidation_factor_id": of_id or None
            })
        if rr.status_code == 200:
            st.success("Yakıt eklendi.")
        else:
            st.error(rr.text)

    st.subheader("Elektrik Girişi (Electricity emissions)")
    kwh = st.number_input("kWh", value=0.0)
    grid_id = st.text_input("Grid factor_id (approved)")
    market_based = st.checkbox("Market-based", value=False)
    if st.button("Elektrik Ekle"):
        with api_client() as c:
            rr = c.post("/activity/electricity", json={
                "activity_record_id": selected_record,
                "kwh": kwh,
                "grid_factor_id": grid_id or None,
                "market_based_flag": market_based
            })
        if rr.status_code == 200:
            st.success("Elektrik eklendi.")
        else:
            st.error(rr.text)

    st.subheader("Proses Girişi (Process emissions + Production)")
    proc_type = st.text_input("Proses tipi")
    prod_qty = st.number_input("Üretim miktarı", value=0.0)
    prod_unit = st.text_input("Birim (örn: ton)")
    proc_factor_id = st.text_input("Process factor_id (approved)")
    if st.button("Proses Ekle"):
        with api_client() as c:
            rr = c.post("/activity/process", json={
                "activity_record_id": selected_record,
                "process_type": proc_type,
                "production_qty": prod_qty,
                "unit": prod_unit,
                "process_factor_id": proc_factor_id or None
            })
        if rr.status_code == 200:
            st.success("Proses eklendi.")
        else:
            st.error(rr.text)

    st.divider()
    if st.button("Kayıt Doğrula (validated)"):
        with api_client() as c:
            rr = c.post(f"/activity/records/{selected_record}/validate")
        if rr.status_code == 200:
            st.success("Record validated.")
        else:
            st.error(rr.text)

elif page == "Hesaplamalar":
    ensure_login()
    st.header("ETS + Facility Hesaplama")

    with api_client() as c:
        fac = c.get("/facilities/")
    facilities = fac.json() if fac.status_code == 200 else []
    if not facilities:
        st.warning("Önce tesis oluşturun.")
        st.stop()

    fac_map = {f["name"]: f["id"] for f in facilities}
    fac_name = st.selectbox("Tesis seç", list(fac_map.keys()))
    facility_id = fac_map[fac_name]

    with api_client() as c:
        r = c.get("/activity/records", params={"facility_id": facility_id})
    records = r.json() if r.status_code == 200 else []
    if not records:
        st.warning("Bu tesis için activity record yok.")
        st.stop()

    record_ids = [x["id"] for x in records]
    activity_record_id = st.selectbox("Activity Record seç", record_ids)
    ets_price = st.number_input("ETS fiyatı (EUR/tCO2)", value=75.0)
    allowances = st.number_input("Allowances (tCO2)", value=0.0)

    if st.button("Hesapla"):
        with api_client() as c:
            rr = c.post("/calc/run", json={
                "facility_id": facility_id,
                "activity_record_id": activity_record_id,
                "ets_price_eur_per_tco2": ets_price,
                "allowances_tco2": allowances
            })
        if rr.status_code == 200:
            out = rr.json()
            st.success(f"Hesap tamamlandı. result_hash: {out['result_hash']}")
            st.json(out["result"])
        else:
            st.error(rr.text)

elif page == "Evidence Pack":
    ensure_login()
    st.header("Evidence Pack (Manifest + Hash)")

    period_start = st.text_input("Dönem başlangıç (YYYY-MM-DD)")
    period_end = st.text_input("Dönem bitiş (YYYY-MM-DD)")
    scope = st.selectbox("Kapsam", ["CBAM", "ETS", "MRV"])
    facility_id = st.text_input("Facility ID (opsiyonel: boş -> tüm tesisler)")

    if st.button("Evidence Pack Oluştur"):
        with api_client() as c:
            rr = c.post("/evidence/build", json={
                "period_start": period_start,
                "period_end": period_end,
                "scope": scope,
                "facility_id": facility_id or None
            })
        if rr.status_code == 200:
            st.success("Evidence pack üretildi.")
            st.json(rr.json())
        else:
            st.error(rr.text)
