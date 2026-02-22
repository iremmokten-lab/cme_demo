from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import AuditEvent, Company
from src.db.session import db, init_db
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.services.authz import current_user, ensure_bootstrap_admin, login_view, logout_button
from src.services import projects as prj

st.set_page_config(page_title="Audit Log", layout="wide")

init_db()
ensure_bootstrap_admin()

user = current_user()
if not user:
    login_view()
    st.stop()

if not str(user.role).startswith("consultant"):
    st.error("Bu sayfa sadece danışman kullanıcılar içindir.")
    st.stop()

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

append_audit(
    "page_viewed",
    {"page": "audit_log"},
    user_id=getattr(user, "id", None),
    company_id=infer_company_id_for_user(user),
    entity_type="page",
    entity_id=None,
)

st.title("Audit Log")
st.caption("Kim neyi görüntüledi / export etti / hangi aksiyonu aldı? Kurumsal iz için bu ekran kullanılır.")

companies = prj.list_companies_for_user(user)
if not companies:
    st.warning("Şirket bulunamadı.")
    st.stop()

company_map = {c.name: c.id for c in companies}
company_name = st.selectbox("Şirket seçin", list(company_map.keys()), index=0)
company_id = int(company_map[company_name])

col1, col2, col3, col4 = st.columns(4)
with col1:
    days = st.selectbox("Zaman aralığı", [1, 3, 7, 14, 30, 90], index=2)
with col2:
    event_type = st.text_input("Event type filtre (opsiyonel)", value="")
with col3:
    entity_type = st.text_input("Entity type filtre (opsiyonel)", value="")
with col4:
    limit = st.number_input("Kayıt limiti", min_value=50, max_value=5000, value=500, step=50)

time_min = datetime.now(timezone.utc) - timedelta(days=int(days))

with db() as s:
    q = select(AuditEvent).where(AuditEvent.company_id == company_id, AuditEvent.created_at >= time_min).order_by(AuditEvent.created_at.desc())
    if event_type.strip():
        q = q.where(AuditEvent.event_type == event_type.strip())
    if entity_type.strip():
        q = q.where(AuditEvent.entity_type == entity_type.strip())
    evs = s.execute(q.limit(int(limit))).scalars().all()

rows = []
for e in evs:
    try:
        details = json.loads(e.details_json or "{}")
    except Exception:
        details = {}

    rows.append(
        {
            "created_at": e.created_at,
            "event_type": e.event_type,
            "user_id": e.user_id,
            "entity_type": e.entity_type,
            "entity_id": e.entity_id,
            "details": details,
        }
    )

if not rows:
    st.info("Bu filtrelerde audit kaydı bulunamadı.")
    st.stop()

df = pd.DataFrame(rows)
st.dataframe(df, use_container_width=True)

st.divider()
st.subheader("Özet")

# Event type dağılımı
try:
    counts = df["event_type"].value_counts().reset_index()
    counts.columns = ["event_type", "count"]
    st.dataframe(counts, use_container_width=True)
except Exception:
    pass
