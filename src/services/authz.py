from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import Optional

import streamlit as st
from sqlalchemy import select

from src.db.models import Company, User
from src.db.session import db


@dataclass
class SessionUser:
    id: int
    email: str
    role: str
    company_id: int | None


def _hash_password(pw: str) -> str:
    salt = os.getenv("AUTH_SALT", "demo_salt")
    raw = (salt + "::" + (pw or "")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _ensure_company() -> Company:
    with db() as s:
        c = s.execute(select(Company).order_by(Company.id).limit(1)).scalars().first()
        if c:
            return c
        c = Company(name="Demo Company")
        s.add(c)
        s.commit()
        s.refresh(c)
        return c


def ensure_bootstrap_admin():
    """Bootstrap users: consultant + verifier + client."""
    admin_email = os.getenv("DEMO_ADMIN_EMAIL", "admin@demo.com")
    admin_pw = os.getenv("DEMO_ADMIN_PASSWORD", "ChangeMe123!")
    verifier_email = os.getenv("DEMO_VERIFIER_EMAIL", "verifier@demo.com")
    verifier_pw = os.getenv("DEMO_VERIFIER_PASSWORD", admin_pw)

    company = _ensure_company()

    with db() as s:
        u = s.execute(select(User).where(User.email == admin_email)).scalars().first()
        if not u:
            u = User(
                email=admin_email,
                password_hash=_hash_password(admin_pw),
                role="consultant_admin",
                company_id=company.id,
                is_active=True,
            )
            s.add(u)

        v = s.execute(select(User).where(User.email == verifier_email)).scalars().first()
        if not v:
            v = User(
                email=verifier_email,
                password_hash=_hash_password(verifier_pw),
                role="verifier",
                company_id=company.id,
                is_active=True,
            )
            s.add(v)

        # default client (opsiyonel)
        client_email = os.getenv("DEMO_CLIENT_EMAIL", "client@demo.com")
        client_pw = os.getenv("DEMO_CLIENT_PASSWORD", admin_pw)
        cusr = s.execute(select(User).where(User.email == client_email)).scalars().first()
        if not cusr:
            cusr = User(
                email=client_email,
                password_hash=_hash_password(client_pw),
                role="client",
                company_id=company.id,
                is_active=True,
            )
            s.add(cusr)

        s.commit()


def current_user() -> Optional[SessionUser]:
    u = st.session_state.get("user")
    if not u:
        return None
    if isinstance(u, SessionUser):
        return u
    # backward compat dict
    try:
        return SessionUser(
            id=int(u.get("id")),
            email=str(u.get("email")),
            role=str(u.get("role")),
            company_id=(int(u.get("company_id")) if u.get("company_id") is not None else None),
        )
    except Exception:
        return None


def authenticate(email: str, password: str) -> Optional[SessionUser]:
    email = (email or "").strip().lower()
    if not email:
        return None

    with db() as s:
        u = s.execute(select(User).where(User.email == email)).scalars().first()
        if not u or not u.is_active:
            return None

        if u.password_hash != _hash_password(password or ""):
            return None

        return SessionUser(id=int(u.id), email=u.email, role=u.role, company_id=u.company_id)


def login_view():
    st.title("🔐 Giriş")
    st.caption("Demo kullanıcılar: admin@demo.com / verifier@demo.com / client@demo.com (şifre: ChangeMe123!)")

    with st.form("login_form"):
        email = st.text_input("E-posta", value="")
        password = st.text_input("Şifre", value="", type="password")
        ok = st.form_submit_button("Giriş yap", type="primary")

    if ok:
        u = authenticate(email, password)
        if not u:
            st.error("Giriş başarısız. E-posta/şifre hatalı.")
        else:
            st.session_state["user"] = u
            st.success("Giriş başarılı.")
            st.rerun()


def logout_button():
    if st.button("Çıkış", use_container_width=True):
        st.session_state.pop("user", None)
        st.rerun()
