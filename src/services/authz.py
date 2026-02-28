from __future__ import annotations

from datetime import datetime, timedelta, timezone

import bcrypt
import streamlit as st
from sqlalchemy import select

from src.db.models import Company, User
from src.db.session import db
from src.mrv.audit import append_audit, infer_company_id_for_user

SESSION_KEY = "user_id"

# Paket C: login güvenliği
MAX_ATTEMPTS = 5
LOCK_MINUTES = 15


def _hash_pw(pw: str) -> str:
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _check_pw(pw: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(pw.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


def _get_secret(key: str, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


def ensure_bootstrap_admin():
    """Bootstrap demo kullanıcıları.

    - Eğer hiç user yoksa: Demo Company + consultantadmin yaratır.
    - Admin zaten varsa: opsiyonel şifre reset (BOOTSTRAP_ADMIN_FORCE_RESET="1")
    - Faz 2: verifier + client demo kullanıcıları da garanti edilir.
    """
    admin_email = _get_secret("BOOTSTRAP_ADMIN_EMAIL", "admin@demo.com")
    admin_pw = _get_secret("BOOTSTRAP_ADMIN_PASSWORD", "ChangeMe123!")
    force_reset = str(_get_secret("BOOTSTRAP_ADMIN_FORCE_RESET", "0")).strip() == "1"

    verifier_email = _get_secret("BOOTSTRAP_VERIFIER_EMAIL", "verifier@demo.com")
    verifier_pw = _get_secret("BOOTSTRAP_VERIFIER_PASSWORD", admin_pw)

    client_email = _get_secret("BOOTSTRAP_CLIENT_EMAIL", "client@demo.com")
    client_pw = _get_secret("BOOTSTRAP_CLIENT_PASSWORD", admin_pw)

    with db() as s:
        any_user = s.execute(select(User).limit(1)).scalars().first()

        # şirket
        c = s.execute(select(Company).order_by(Company.id).limit(1)).scalars().first()
        if not c and not any_user:
            c = Company(name="Demo Company")
            s.add(c)
            s.flush()
        elif not c:
            c = Company(name="Demo Company")
            s.add(c)
            s.flush()

        # admin
        admin_user = s.execute(select(User).where(User.email == admin_email).limit(1)).scalars().first()
        if not admin_user:
            admin_user = User(
                email=admin_email,
                password_hash=_hash_pw(admin_pw),
                role="consultantadmin",
                company_id=c.id,
            )
            s.add(admin_user)

        # verifier
        verifier_user = s.execute(select(User).where(User.email == verifier_email).limit(1)).scalars().first()
        if not verifier_user:
            verifier_user = User(
                email=verifier_email,
                password_hash=_hash_pw(verifier_pw),
                role="verifier",
                company_id=c.id,
            )
            s.add(verifier_user)

        # client
        client_user = s.execute(select(User).where(User.email == client_email).limit(1)).scalars().first()
        if not client_user:
            client_user = User(
                email=client_email,
                password_hash=_hash_pw(client_pw),
                role="client",
                company_id=c.id,
            )
            s.add(client_user)

        s.commit()

        # force reset admin only
        if force_reset:
            admin_user.password_hash = _hash_pw(admin_pw)
            admin_user.failed_login_attempts = 0
            admin_user.locked_until = None
            s.add(admin_user)

            # opsiyonel: verifier/client reset de destekleyelim
            if str(_get_secret("BOOTSTRAP_VERIFIER_FORCE_RESET", "0")).strip() == "1":
                verifier_user.password_hash = _hash_pw(verifier_pw)
                verifier_user.failed_login_attempts = 0
                verifier_user.locked_until = None
                s.add(verifier_user)
            if str(_get_secret("BOOTSTRAP_CLIENT_FORCE_RESET", "0")).strip() == "1":
                client_user.password_hash = _hash_pw(client_pw)
                client_user.failed_login_attempts = 0
                client_user.locked_until = None
                s.add(client_user)

            s.commit()


def get_or_create_demo_user():
    """Geriye dönük uyumluluk (pages/1_Consultant_Panel.py).

    Danışman panelini demo amaçlı hızlı açmak için:
    - ensure_bootstrap_admin() ile demo consultantadmin hesabını garanti eder
    - admin kullanıcıyı session'a yazar ve döndürür

    Not: Bu fonksiyon, ana login/role routing akışını bozmaz; sadece bu sayfa tarafından kullanılır.
    """
    ensure_bootstrap_admin()

    admin_email = _get_secret("BOOTSTRAP_ADMIN_EMAIL", "admin@demo.com")

    with db() as s:
        u = s.execute(select(User).where(User.email == admin_email).limit(1)).scalars().first()
        if not u:
            u = s.execute(select(User).order_by(User.id).limit(1)).scalars().first()
        if not u:
            raise RuntimeError("Demo kullanıcı oluşturulamadı (User tablosu boş).")

        st.session_state[SESSION_KEY] = int(u.id)

    try:
        append_audit(
            "demo_user_auto_login",
            {"email": str(getattr(u, "email", "") or ""), "role": str(getattr(u, "role", "") or "")},
            user_id=getattr(u, "id", None),
            company_id=infer_company_id_for_user(u),
            entity_type="user",
            entity_id=getattr(u, "id", None),
        )
    except Exception:
        pass

    return u


def current_user():
    uid = st.session_state.get(SESSION_KEY)
    if not uid:
        return None
    with db() as s:
        return s.get(User, uid)


def _is_locked(user: User) -> tuple[bool, str]:
    now = datetime.now(timezone.utc)
    try:
        locked_until = getattr(user, "locked_until", None)
        if locked_until and locked_until > now:
            mins = int((locked_until - now).total_seconds() // 60) + 1
            return True, f"Hesap geçici olarak kilitli. {mins} dakika sonra tekrar deneyin."
        return False, ""
    except Exception:
        return False, ""


def _lock_user(user: User):
    now = datetime.now(timezone.utc)
    user.locked_until = now + timedelta(minutes=LOCK_MINUTES)


def _inc_failed_attempts(user: User):
    user.failed_login_attempts = int(getattr(user, "failed_login_attempts", 0) or 0) + 1
    if user.failed_login_attempts >= MAX_ATTEMPTS:
        _lock_user(user)


def login_view():
    st.title("Giriş")
    st.caption("Demo login: admin@demo.com / ChangeMe123!  (client/verifier de aynı şifre ile)")

    email = st.text_input("E-posta", value="", placeholder="admin@demo.com")
    pw = st.text_input("Şifre", value="", type="password")

    if st.button("Giriş Yap", type="primary"):
        email_norm = (email or "").strip().lower()
        if not email_norm or not pw:
            st.warning("E-posta ve şifre zorunlu.")
            return

        with db() as s:
            u = s.execute(select(User).where(User.email == email_norm).limit(1)).scalars().first()
            if not u:
                st.error("Kullanıcı bulunamadı.")
                return

            locked, msg = _is_locked(u)
            if locked:
                st.error(msg)
                return

            if not _check_pw(pw, u.password_hash):
                _inc_failed_attempts(u)
                s.add(u)
                s.commit()
                st.error("Şifre hatalı.")
                return

            # success
            u.failed_login_attempts = 0
            u.locked_until = None
            s.add(u)
            s.commit()

            st.session_state[SESSION_KEY] = int(u.id)

            try:
                append_audit(
                    "login_success",
                    {"email": email_norm, "role": str(getattr(u, "role", "") or "")},
                    user_id=getattr(u, "id", None),
                    company_id=infer_company_id_for_user(u),
                    entity_type="user",
                    entity_id=getattr(u, "id", None),
                )
            except Exception:
                pass

            st.success("Giriş başarılı.")
            st.rerun()


def logout_button():
    if st.button("Çıkış", use_container_width=True):
        st.session_state.pop(SESSION_KEY, None)
        st.rerun()


# ----------------------------
# Authorization helpers (UI-safe)
# ----------------------------
def require_role(user: User, *, allowed: set[str]):
    """UI-level role guard.

    allowed örnek:
      {"consultant", "consultant_admin"} veya {"client"} vb.
    """
    role = str(getattr(user, "role", "") or "").lower().strip()
    norm = role
    if norm.startswith("consultant"):
        norm = "consultant_admin" if norm == "consultant_admin" else "consultant"
    elif norm.startswith("verifier"):
        norm = "verifier_admin" if norm == "verifier_admin" else "verifier"
    elif norm.startswith("client"):
        norm = "client"

    if norm not in allowed and role not in allowed:
        st.error("Bu sayfaya erişim yetkiniz yok.")
        st.stop()


def can_view_client_shared_snapshot(user: User) -> bool:
    role = str(getattr(user, "role", "") or "").lower().strip()
    return role.startswith("client") or role.startswith("verifier")
