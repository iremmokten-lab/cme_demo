from __future__ import annotations
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.services.projects import list_company_projects_for_user
from src.services.data_governance import list_uploads, submit, review, ensure_approval

st.set_page_config(page_title="Data Governance Onay", layout="wide")
u=current_user()
if not u:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {u.email}")
    st.caption(f"Rol: {u.role}")
    logout_button()

st.title("🧾 Data Governance • Dataset Onay Akışı")
st.caption("ERP/Regülasyon seviyesi için: dataset'ler 'onaylanmadan' rapora girmemeli.")

projects=list_company_projects_for_user(u)
if not projects:
    st.warning("Proje yok."); st.stop()
pmap={f"{p.name} (#{p.id})": int(p.id) for p in projects}
pl=st.selectbox("Proje seç", list(pmap.keys()))
project_id=pmap[pl]

rows=list_uploads(project_id)
if not rows:
    st.info("Henüz upload yok."); st.stop()

data=[]
for up, ap in rows:
    ap = ap or ensure_approval(up.id)
    data.append({
        "upload_id": up.id,
        "dataset_type": up.dataset_type,
        "filename": up.original_filename,
        "uploaded_at": str(up.uploaded_at),
        "approval_status": ap.status,
    })
st.dataframe(data, use_container_width=True)

sel = st.number_input("Upload ID", min_value=1, value=int(data[0]["upload_id"]))
mode = st.selectbox("İşlem", ["Gönder (Submit)","Onayla (Approve)","Reddet (Reject)"])
notes = st.text_area("Not", value="")
if st.button("Uygula", type="primary"):
    try:
        if mode.startswith("Gönder"):
            submit(int(sel), notes, user_id=int(getattr(u,"id",0) or 0) or None)
        elif mode.startswith("Onayla"):
            review(int(sel), True, notes, reviewer_user_id=int(getattr(u,"id",0) or 0) or None)
        else:
            review(int(sel), False, notes, reviewer_user_id=int(getattr(u,"id",0) or 0) or None)
        st.success("Tamamlandı.")
        st.rerun()
    except Exception as e:
        st.error(str(e))
