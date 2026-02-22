import streamlit as st
import bcrypt
from sqlalchemy import select

from src.db.session import db
from src.db.models import User, Company

SESSION_KEY = "user_id"


def _hash_pw(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()


def _check_pw(pw: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(pw.encode(), hashed.encode())
    except:
        return False


def ensure_bootstrap_admin():
    with db() as s:
        any_user = s.execute(select(User).limit(1)).scalar_one_or_none()
        if any_user:
            return

        c = Company(name="Demo Company")
        s.add(c)
        s.flush()

        u = User(
            email="admin@demo.com",
            password_hash=_hash_pw("admin123"),
            role="consultantadmin",
            company_id=c.id,
        )

        s.add(u)
        s.commit()


def current_user():
    uid = st.session_state.get(SESSION_KEY)
    if not uid:
        return None
    with db() as s:
        return s.get(User, uid)


def login_view():
    st.title("Giriş")

    email = st.text_input("Email")
    pw = st.text_input("Şifre", type="password")

    if st.button("Giriş yap"):

        with db() as s:
            u = s.execute(select(User).where(User.email == email)).scalar_one_or_none()

        if not u or not _check_pw(pw, u.password_hash):
            st.error("Hatalı giriş")
            return

        st.session_state[SESSION_KEY] = u.id
        st.rerun()


def logout_button():
    if st.button("Çıkış"):
        st.session_state.pop(SESSION_KEY, None)
        st.rerun()
