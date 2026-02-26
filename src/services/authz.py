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
    """- Eğer hiç user yoksa: demo company + consultantadmin yaratır.
    - Eğer admin email zaten varsa: isteğe bağlı olarak şifreyi resetleyebilir.
      Bunun için Streamlit Secrets'a:
        BOOTSTRAP_ADMIN_FORCE_RESET="1"
      ekleyin.
    """
    admin_email = _get_secret("BOOTSTRAP_ADMIN_EMAIL", "admin@demo.com")
    admin_pw = _get_secret("BOOTSTRAP_ADMIN_PASSWORD", "ChangeMe123!")
    force_reset = str(_get_secret("BOOTSTRAP_ADMIN_FORCE_RESET", "0")).strip() == "1"

    with db() as s:
        any_user = s.execute(select(User).limit(1)).scalars().first()
        admin_user = s.execute(select(User).where(User.email == admin_email).limit(1)).scalars().first()

        if not any_user:
            c = Company(name="Demo Company")
            s.add(c)
            s.flush()
            u = User(
                email=admin_email,
                password_hash=_hash_pw(admin_pw),
                role="consultantadmin",
                company_id=c.id,
            )
            s.add(u)
            s.commit()
            return

        if not admin_user:
            c = s.execute(select(Company).order_by(Company.id).limit(1)).scalars().first()
            if not c:
                c = Company(name="Demo Company")
                s.add(c)
                s.flush()
            u = User(
                email=admin_email,
                password_hash=_hash_pw(admin_pw),
                role="consultantadmin",
                company_id=c.id,
            )
            s.add(u)
            s.commit()
            return

        if force_reset:
            admin_user.password_hash = _hash_pw(admin_pw)
            admin_user.failed_login_attempts = 0
            admin_user.locked_until = None
            s.add(admin_user)
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
            remaining = locked_until - now
            mins = max(1, int(remaining.total_seconds() // 60))
            return True, f"Hesap geçici olarak kilitli. Lütfen {mins} dakika sonra tekrar deneyin."
    except Exception:
        pass
    return False, ""


def login_view():
    st.title("Giriş")

    email = st.text_input("E-posta")
    pw = st.text_input("Şifre", type="password")

    if st.button("Giriş yap", type="primary"):
        with db() as s:
            user = s.execute(select(User).where(User.email == email).limit(1)).scalars().first()

            if not user:
                st.error("Geçersiz e-posta veya şifre.")
                return

            locked, msg = _is_locked(user)
            if locked:
                st.error(msg)
                return

            ok = _check_pw(pw, user.password_hash)
            if not ok:
                # attempt tracking
                try:
                    user.failed_login_attempts = int(getattr(user, "failed_login_attempts", 0) or 0) + 1
                    if user.failed_login_attempts >= MAX_ATTEMPTS:
                        user.locked_until = datetime.now(timezone.utc) + timedelta(minutes=LOCK_MINUTES)
                        s.add(user)
                        s.commit()
                        st.error(f"Çok fazla hatalı deneme. Hesap {LOCK_MINUTES} dakika kilitlendi.")
                        return
                    s.add(user)
                    s.commit()
                except Exception:
                    pass

                st.error("Geçersiz e-posta veya şifre.")
                return

            # successful login
            try:
                user.failed_login_attempts = 0
                user.locked_until = None
                user.last_login_at = datetime.now(timezone.utc)
                s.add(user)
                s.commit()
            except Exception:
                pass

            st.session_state[SESSION_KEY] = user.id

        try:
            append_audit(
                "user_login",
                {"email": email},
                user_id=getattr(user, "id", None),
                company_id=infer_company_id_for_user(user),
                entity_type="user",
                entity_id=getattr(user, "id", None),
            )
        except Exception:
            pass

        st.success("Giriş başarılı.")
        st.rerun()


def logout_button():
    if st.button("Çıkış"):
        uid = st.session_state.get(SESSION_KEY)
        st.session_state.pop(SESSION_KEY, None)
        try:
            append_audit(
                "user_logout",
                {},
                user_id=uid,
                company_id=None,
                entity_type="user",
                entity_id=uid,
            )
        except Exception:
            pass
        st.rerun()
