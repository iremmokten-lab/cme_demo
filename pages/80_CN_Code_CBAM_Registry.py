from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.session import db, init_db
from src.services.authz import current_user
from src.db.cbam_registry import CbamCnMapping


def utcnow():
    return datetime.now(timezone.utc)


def _clean_cn(s: str) -> str:
    s = str(s or "").strip()
    s = s.replace(".", "").replace(" ", "")
    return s


def _role_guard():
    u = current_user()
    if not u:
        st.error("Bu sayfayı görmek için giriş yapmalısınız.")
        st.stop()

    role = str(getattr(u, "role", "") or "")
    if not (role.startswith("consultant") or role in ("admin", "consultantadmin")):
        st.error("Bu sayfaya erişim yetkiniz yok. (Sadece danışman/admin)")
        st.stop()


def _goods_options():
    return [
        ("iron_steel", "Demir-Çelik"),
        ("aluminium", "Alüminyum"),
        ("cement", "Çimento"),
        ("fertilizers", "Gübre"),
        ("electricity", "Elektrik"),
        ("hydrogen", "Hidrojen"),
        ("chemicals", "Kimyasallar"),
        ("other", "Diğer"),
    ]


def _load_rows(active_only: bool) -> pd.DataFrame:
    with db() as s:
        q = select(CbamCnMapping)
        if active_only:
            q = q.where(CbamCnMapping.active == True)  # noqa: E712
        q = q.order_by(CbamCnMapping.active.desc(), CbamCnMapping.priority.desc(), CbamCnMapping.cn_pattern.desc())
        rows = s.execute(q).scalars().all()

    out = []
    for r in rows:
        out.append(
            {
                "id": r.id,
                "cn_pattern": r.cn_pattern,
                "match_type": r.match_type,
                "cbam_good_key": r.cbam_good_key,
                "cbam_good_name": r.cbam_good_name,
                "priority": r.priority,
                "active": bool(r.active),
                "notes": r.notes or "",
                "updated_at": (r.updated_at.isoformat(timespec="seconds") if getattr(r, "updated_at", None) else None),
            }
        )
    return pd.DataFrame(out)


def main():
    st.set_page_config(page_title="CN → CBAM Mapping Registry", layout="wide")
    init_db()
    _role_guard()

    st.title("CN → CBAM Goods Mapping Registry")
    st.caption(
        "Bu ekran CN kodlarını (exact/prefix) CBAM goods sınıfına bağlamak için kullanılır. "
        "CBAM motoru önce buraya bakar; eşleşme yoksa fallback prefix kuralları çalışır."
    )

    colA, colB, colC = st.columns([1, 1, 1])
    with colA:
        active_only = st.checkbox("Sadece aktif kuralları göster", value=True)
    with colB:
        st.write("")
    with colC:
        if st.button("Yenile", type="secondary"):
            st.rerun()

    df = _load_rows(active_only=active_only)
    st.subheader("Mevcut Kurallar")
    if df.empty:
        st.info("Henüz kural yok. Aşağıdan yeni kural ekleyin.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Yeni Kural Ekle")

    goods = _goods_options()
    goods_map = {k: v for k, v in goods}

    with st.form("add_rule"):
        c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1])
        with c1:
            cn_pattern = st.text_input("CN Pattern", placeholder="Örn: 72 (prefix) veya 72081000 (exact)")
        with c2:
            match_type = st.selectbox("Match Type", options=["prefix", "exact"], index=0)
        with c3:
            good_key = st.selectbox(
                "CBAM Good",
                options=[k for k, _ in goods],
                format_func=lambda k: f"{goods_map.get(k, k)} ({k})",
            )
        with c4:
            priority = st.number_input("Priority", min_value=0, max_value=10000, value=100, step=10)

        notes = st.text_area("Not", placeholder="Kaynak/yorum/özel notlar (opsiyonel)")
        active = st.checkbox("Aktif", value=True)

        submitted = st.form_submit_button("Kaydet", type="primary")

    if submitted:
        cn_pattern_clean = _clean_cn(cn_pattern)
        if not cn_pattern_clean:
            st.error("CN Pattern boş olamaz.")
            st.stop()

        cbam_name = goods_map.get(good_key, "Diğer")

        with db() as s:
            r = CbamCnMapping(
                cn_pattern=cn_pattern_clean,
                match_type=match_type,
                cbam_good_key=good_key,
                cbam_good_name=cbam_name,
                priority=int(priority),
                active=bool(active),
                notes=notes or "",
                created_at=utcnow(),
                updated_at=utcnow(),
            )
            s.add(r)
            s.commit()

        st.success("Kural eklendi ✅")
        st.rerun()

    st.divider()
    st.subheader("Kural Güncelle / Pasif Et")

    if df.empty:
        st.info("Güncellemek için önce kural ekleyin.")
        return

    ids = df["id"].tolist()
    selected_id = st.selectbox("Düzenlenecek Kural (id)", options=ids)

    with db() as s:
        row = s.get(CbamCnMapping, int(selected_id))

    if not row:
        st.error("Seçilen kural bulunamadı.")
        return

    with st.form("edit_rule"):
        e1, e2, e3, e4 = st.columns([1.2, 1, 1, 1])
        with e1:
            cn_pattern_edit = st.text_input("CN Pattern", value=row.cn_pattern or "")
        with e2:
            match_type_edit = st.selectbox(
                "Match Type",
                options=["prefix", "exact"],
                index=0 if (row.match_type or "prefix") == "prefix" else 1,
            )
        with e3:
            keys = [k for k, _ in goods]
            cur = (row.cbam_good_key or "other").strip().lower()
            idx = keys.index(cur) if cur in keys else keys.index("other")
            good_key_edit = st.selectbox(
                "CBAM Good",
                options=keys,
                index=idx,
                format_func=lambda k: f"{goods_map.get(k, k)} ({k})",
            )
        with e4:
            priority_edit = st.number_input("Priority", min_value=0, max_value=10000, value=int(row.priority or 100), step=10)

        notes_edit = st.text_area("Not", value=row.notes or "")
        active_edit = st.checkbox("Aktif", value=bool(row.active))

        cbtn1, cbtn2, cbtn3 = st.columns([1, 1, 1])
        with cbtn1:
            save_btn = st.form_submit_button("Güncelle", type="primary")
        with cbtn2:
            deactivate_btn = st.form_submit_button("Pasif Et", type="secondary")
        with cbtn3:
            delete_btn = st.form_submit_button("Sil (Hard Delete)", type="secondary")

    if save_btn:
        cn_pattern_clean = _clean_cn(cn_pattern_edit)
        if not cn_pattern_clean:
            st.error("CN Pattern boş olamaz.")
            st.stop()

        with db() as s:
            r = s.get(CbamCnMapping, int(selected_id))
            if not r:
                st.error("Kural bulunamadı.")
                st.stop()
            r.cn_pattern = cn_pattern_clean
            r.match_type = match_type_edit
            r.cbam_good_key = good_key_edit
            r.cbam_good_name = goods_map.get(good_key_edit, "Diğer")
            r.priority = int(priority_edit)
            r.notes = notes_edit or ""
            r.active = bool(active_edit)
            r.updated_at = utcnow()
            s.add(r)
            s.commit()

        st.success("Kural güncellendi ✅")
        st.rerun()

    if deactivate_btn:
        with db() as s:
            r = s.get(CbamCnMapping, int(selected_id))
            if not r:
                st.error("Kural bulunamadı.")
                st.stop()
            r.active = False
            r.updated_at = utcnow()
            s.add(r)
            s.commit()

        st.success("Kural pasif edildi ✅")
        st.rerun()

    if delete_btn:
        st.warning("Hard delete geri alınamaz. Eminseniz tekrar tıklayın.")
        if st.button("Evet, kalıcı olarak sil", type="primary"):
            with db() as s:
                r = s.get(CbamCnMapping, int(selected_id))
                if r:
                    s.delete(r)
                    s.commit()
            st.success("Kural silindi ✅")
            st.rerun()


if __name__ == "__main__":
    main()
