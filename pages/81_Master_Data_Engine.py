from __future__ import annotations

import streamlit as st
import pandas as pd
from datetime import datetime, timezone

from src.db.session import db, init_db
from src.services.authz import current_user, login_view, logout_button
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.master_data.schemas import FacilityUpsert, ProductUpsert, CNCodeUpsert, BOMEdgeUpsert
from src.master_data.service import MasterDataService

st.set_page_config(page_title="Master Data Engine", layout="wide")

# DB init
init_db()


def _rerun():
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            return


user = current_user()
if not user:
    login_view()
    st.stop()

role = str(getattr(user, "role", "") or "").lower()
if not role.startswith("consultant"):
    st.error("Bu sayfa sadece danışman (consultant) rolüne açıktır.")
    st.stop()

company_id = infer_company_id_for_user(user)
if not company_id:
    st.error("Şirket bilgisi bulunamadı.")
    st.stop()

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

append_audit(
    "page_viewed",
    {"page": "master_data_engine"},
    user_id=getattr(user, "id", None),
    company_id=company_id,
    entity_type="page",
    entity_id=None,
)

st.title("Faz 1 — Master Data Engine")
st.caption(
    "Sade anlatım: Bu sayfa tesis/ürün/CN kodu/BOM (ürün ağacı) kayıtlarını **kayıt geçmişiyle** yönetir. "
    "Eski kayıtlar silinmez; güncelleme olursa yeni versiyon oluşur."
)

tabs = st.tabs(["🏭 Tesisler", "📦 Ürünler", "🧬 BOM (Ürün Ağacı)", "🏷️ CN Kodları", "🧾 Değişiklik Kayıtları"])


def _utcnow_naive():
    # Streamlit date/time input çoğunlukla naive döner; UTC varsayıyoruz.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _err(e: Exception):
    st.error(str(e))


with db() as s:
    svc = MasterDataService(s, company_id=company_id, user_id=getattr(user, "id", None))

    # ----------------------
    # Facilities
    # ----------------------
    with tabs[0]:
        st.subheader("Tesis Kaydı (Facility Registry)")
        st.write("Tesis bilgisi değişirse, sistem **yeni versiyon** oluşturur. Böylece geçmiş denetimler bozulmaz.")

        facs = svc.list_facilities()
        st.dataframe(pd.DataFrame(facs), use_container_width=True)

        st.divider()
        st.markdown("### Yeni tesis ekle / mevcut tesisi güncelle")
        col1, col2 = st.columns(2)

        with col1:
            existing_ids = [None] + [int(x["facility_id"]) for x in facs if x.get("facility_id")]
            sel = st.selectbox("Güncellenecek tesis (boş bırak = yeni tesis)", options=existing_ids, index=0)
            name = st.text_input("Tesis adı", value="")
            country = st.text_input("Ülke", value="TR")
            sector = st.text_input("Sektör", value="")

        with col2:
            valid_from = st.datetime_input("Geçerlilik başlangıcı (valid_from)", value=_utcnow_naive())
            st.caption("Bu tarih, hesap tekrarında hangi kaydın kullanılacağını belirler.")

        if st.button("Kaydet (Tesis)", type="primary"):
            try:
                payload = FacilityUpsert(
                    facility_id=sel,
                    name=name,
                    country=country,
                    sector=sector,
                    valid_from=valid_from,
                )
                svc.upsert_facility(payload)
                s.commit()
                st.success("Tesis kaydedildi ✅")
                _rerun()
            except Exception as e:
                s.rollback()
                _err(e)

    # ----------------------
    # Products
    # ----------------------
    with tabs[1]:
        st.subheader("Ürün Kaydı (Product Master)")
        st.write("Ürünler CBAM için önemlidir: ürün → CN kodu → sektör. Güncelleme olursa yeni versiyon oluşur.")

        prods = svc.list_products()
        st.dataframe(pd.DataFrame(prods), use_container_width=True)

        st.divider()
        st.markdown("### Yeni ürün ekle / mevcut ürünü güncelle")
        prod_options = [("Yeni ürün", "")] + [
            (f'{p["name"]} | CN {p["cn_code"]} | v{p["version"]}', p["logical_id"]) for p in prods
        ]
        sel_label = st.selectbox("Güncellenecek ürün (Yeni ürün seçebilirsiniz)", options=[x[0] for x in prod_options], index=0)
        logical_id = ""
        for lab, lid in prod_options:
            if lab == sel_label:
                logical_id = lid
                break

        name = st.text_input("Ürün adı", value="")
        cn_code = st.text_input("CN kodu (ör. 72081000)", value="")
        sector = st.text_input("Sektör (opsiyonel)", value="")
        valid_from = st.datetime_input("Geçerlilik başlangıcı (valid_from)", value=_utcnow_naive(), key="prod_valid_from")

        if st.button("Kaydet (Ürün)", type="primary"):
            try:
                payload = ProductUpsert(
                    logical_id=logical_id or None,
                    name=name,
                    cn_code=cn_code,
                    sector=sector,
                    valid_from=valid_from,
                )
                svc.upsert_product(payload)
                s.commit()
                st.success("Ürün kaydedildi ✅")
                _rerun()
            except Exception as e:
                s.rollback()
                _err(e)

    # ----------------------
    # BOM
    # ----------------------
    with tabs[2]:
        st.subheader("BOM — Ürün Ağacı (Bill of Materials / Precursor Graph)")
        st.write("BOM: Bir ürünün hangi alt girdilerden oluştuğunu belirtir. Sistem **döngü (cycle)** oluşmasına izin vermez.")

        prods = svc.list_products()
        prod_map = {int(p["id"]): f'{p["name"]} (id={p["id"]})' for p in prods}
        edges = svc.list_bom_edges()

        dfe = pd.DataFrame(edges)
        if not dfe.empty:
            dfe["parent_name"] = dfe["parent_product_id"].map(prod_map)
            dfe["child_name"] = dfe["child_product_id"].map(prod_map)
        st.dataframe(dfe, use_container_width=True)

        st.divider()
        st.markdown("### BOM ilişkisi ekle / güncelle (aynı parent→child varsa yeni versiyon olur)")
        if len(prods) < 2:
            st.info("BOM eklemek için önce en az 2 ürün oluşturun.")
        else:
            col1, col2, col3 = st.columns(3)
            with col1:
                parent_id = st.selectbox(
                    "Parent ürün",
                    options=[int(p["id"]) for p in prods],
                    format_func=lambda x: prod_map.get(int(x), str(x)),
                )
            with col2:
                child_id = st.selectbox(
                    "Child (girdi) ürün",
                    options=[int(p["id"]) for p in prods],
                    format_func=lambda x: prod_map.get(int(x), str(x)),
                )
            with col3:
                ratio = st.number_input("Oran (ratio)", min_value=0.000001, value=1.0)
                unit = st.text_input("Birim", value="kg")

            valid_from = st.datetime_input("Geçerlilik başlangıcı (valid_from)", value=_utcnow_naive(), key="bom_valid_from")

            if st.button("Kaydet (BOM)", type="primary"):
                try:
                    payload = BOMEdgeUpsert(
                        parent_product_id=int(parent_id),
                        child_product_id=int(child_id),
                        ratio=float(ratio),
                        unit=unit,
                        valid_from=valid_from,
                    )
                    svc.upsert_bom_edge(payload)
                    s.commit()
                    st.success("BOM kaydedildi ✅")
                    _rerun()
                except Exception as e:
                    s.rollback()
                    _err(e)

    # ----------------------
    # CN codes
    # ----------------------
    with tabs[3]:
        st.subheader("CN Kodları (Global Registry)")
        st.write("Bu tablo CN kodu açıklaması ve sektör bilgisini tutar. (Sonra TARIC dataset ile toplu yükleme eklenebilir.)")

        codes = svc.list_cn_codes(limit=300)
        st.dataframe(pd.DataFrame(codes), use_container_width=True)

        st.divider()
        st.markdown("### CN kodu ekle / güncelle")
        code = st.text_input("CN kodu", value="")
        desc = st.text_area("Açıklama", value="", height=90)
        sector = st.text_input("Sektör", value="", key="cn_sector")
        source = st.text_input("Kaynak (örn: TARIC, company)", value="")
        col1, col2 = st.columns(2)
        with col1:
            vf = st.datetime_input("valid_from (opsiyonel)", value=_utcnow_naive(), key="cn_vf")
        with col2:
            vt = st.datetime_input("valid_to (opsiyonel)", value=_utcnow_naive(), key="cn_vt")

        if st.button("Kaydet (CN Kodu)", type="primary"):
            try:
                payload = CNCodeUpsert(
                    code=code,
                    description=desc,
                    sector=sector,
                    source=source,
                    valid_from=vf,
                    valid_to=vt,
                )
                svc.upsert_cn_code(payload)
                s.commit()
                st.success("CN kodu kaydedildi ✅")
                _rerun()
            except Exception as e:
                s.rollback()
                _err(e)

    # ----------------------
    # Changes
    # ----------------------
    with tabs[4]:
        st.subheader("Değişiklik Kayıtları (Change Log)")
        st.write("Her değişiklik için sistem önceki/sonraki **hash** üretir. Denetimde kanıttır.")
        changes = svc.list_changes(limit=300)
        st.dataframe(pd.DataFrame(changes), use_container_width=True)
