from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import AuditEvent
from src.db.session import db, init_db
from src.services.authz import current_user, ensure_bootstrap_admin, login_view, logout_button
from src.services import projects as prj

st.set_page_config(page_title="Audit Log", layout="wide")

init_db()
ensure_bootstrap_admin()

user = current_user()
if not user:
    login_view()
    st.stop()

if not str(getattr(user, "role", "") or "").lower().startswith("consultant"):
    st.error("Bu sayfa sadece danışman kullanıcılar içindir.")
    st.stop()

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    st.divider()
    days = st.slider("Gün", 1, 90, 14)
    st.caption("Audit log; snapshot, export, replay ve erişim kayıtlarını gösterir.")
    st.divider()
    logout_button()

st.title("Audit Log")
companies = prj.list_companies_for_user(user)
if not companies:
    st.info("Şirket kaydı yok.")
    st.stop()

label = {c.id: f"{c.name} (#{c.id})" for c in companies}
cid = st.selectbox("Şirket", options=[c.id for c in companies], format_func=lambda x: label.get(x, str(x)))

since = datetime.now(timezone.utc) - timedelta(days=int(days))

with db() as s:
    rows = (
        s.execute(
            select(AuditEvent)
            .where(AuditEvent.company_id == int(cid))
            .where(AuditEvent.at >= since)
            .order_by(AuditEvent.at.desc())
        )
        .scalars()
        .all()
    )

data = []
for r in rows:
    try:
        payload = json.loads(r.meta_json or "{}")
    except Exception:
        payload = {}
    data.append(
        {
            "time": str(r.at),
            "user_id": r.user_id,
            "event": r.action,
            "entity_type": r.entity_type,
            "entity_id": r.entity_id,
            "payload": payload,
        }
    )

df = pd.DataFrame(data)
st.dataframe(df, use_container_width=True)

if st.toggle("Ham JSON", value=False):
    st.code(json.dumps(data, ensure_ascii=False, indent=2), language="json")
