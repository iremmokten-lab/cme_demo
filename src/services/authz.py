import streamlit as st
import bcrypt
from sqlalchemy import select

from src.db.session import db
from src.db.models import User, Company

SESSION_KEY = "user_id"

def _hash_pw(pw: str) -> str:
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def _check_pw(pw: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(pw.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False

def ensure_bootstrap_admin():
    # Eğer hiç user yoksa: demo company + consultantadmin yarat
    with db() as s:
        any_user = s.execute(select(User).limit(1)).scalar_one_or_none()
        if any_user:
            return

        c = Company(name="Demo Company")
        s.add(c)
        s.flush()

        admin_email = st.secrets.get("BOOTSTRAP_ADMIN_EMAIL", "admin@demo.com") if hasattr(st, "secrets") else "admin@demo.com"
        admin_pw = st.secrets.get("BOOTSTRAP_ADMIN_PASSWORD", "ChangeMe123!") if hasattr(st, "secrets") else "ChangeMe123!"

        u = User(email=admin_email, password_hash=_hash_pw(admin_pw), role="consultantadmin", company_id=c.id)
        s.add(u)
        s.commit()

def current_user():
    uid = st.session_state.get(SESSION_KEY)
    if not uid:
        return None
    with db() as s:
        return s.get(User, uid)

def login_view():
    st.title("Login")
    email = st.text_input("Email")
    pw = st.text_input("Password", type="password")
    if st.button("Sign in", type="primary"):
        with db() as s:
            u = s.execute(select(User).where(User.email == email)).scalar_one_or_none()
            if not u or not _check_pw(pw, u.password_hash):
                st.error("Hatalı email/şifre")
                st.stop()
            st.session_state[SESSION_KEY] = u.id
            st.rerun()

def logout_button():
    if st.button("Logout"):
        st.session_state.pop(SESSION_KEY, None)
        st.rerun()
