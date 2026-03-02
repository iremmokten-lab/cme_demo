from __future__ import annotations
import json
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.services.job_queue import enqueue, list_jobs
from src.services.worker import run_once, register

st.set_page_config(page_title="Jobs Monitor", layout="wide")
u=current_user()
if not u:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {u.email}")
    st.caption(f"Rol: {u.role}")
    logout_button()

st.title("🧵 Job Queue + Worker Monitor")
st.caption("Uzun süren işler için kuyruk. Streamlit Cloud için basit worker (manuel tetik).")

# demo handler
def _demo(payload:dict)->dict:
    return {"ok": True, "echo": payload}

register("demo", _demo)

c1,c2=st.columns(2)
with c1:
    payload=st.text_area("Payload JSON", value=json.dumps({"hello":"world"}, ensure_ascii=False, indent=2), height=120)
    if st.button("Demo Job Enqueue", type="primary"):
        try:
            j=enqueue("demo", json.loads(payload))
            st.success(f"Enqueued #{j.id}")
        except Exception as e:
            st.error(str(e))
with c2:
    if st.button("Worker: 1 job çalıştır"):
        did = run_once()
        st.info("Çalıştı" if did else "Kuyruk boş")

st.subheader("Son işler")
jobs=list_jobs(100)
st.dataframe([{"id":j.id,"kind":j.kind,"status":j.status,"error":(j.error or "")[:120], "created_at":str(j.created_at)} for j in jobs], use_container_width=True)
