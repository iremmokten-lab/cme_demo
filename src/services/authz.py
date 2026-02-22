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
        email_norm = (email or "").strip().lower()
        now = datetime.now(timezone.utc)

        with db() as s:
            u = s.execute(select(User).where(User.email == email_norm).limit(1)).scalars().first()

            # Kullanıcı yoksa: generic hata (enumeration önleme)
            if not u:
                st.error("Hatalı e-posta/şifre")
                append_audit("login_failed_unknown_user", {"email": email_norm}, entity_type="user", entity_id=None)
                return

            locked, msg = _is_locked(u)
            if locked:
                st.error(msg)
                append_audit(
                    "login_blocked_locked",
                    {"email": email_norm, "locked_until": getattr(u, "locked_until", None).isoformat() if getattr(u, "locked_until", None) else None},
                    user_id=u.id,
                    company_id=infer_company_id_for_user(u),
                    entity_type="user",
                    entity_id=u.id,
                )
                return

            ok = _check_pw(pw, u.password_hash)
            if not ok:
                # attempt artır
                try:
                    u.failed_login_attempts = int(getattr(u, "failed_login_attempts", 0) or 0) + 1
                except Exception:
                    u.failed_login_attempts = 1

                if u.failed_login_attempts >= MAX_ATTEMPTS:
                    u.locked_until = now + timedelta(minutes=LOCK_MINUTES)

                s.add(u)
                s.commit()

                append_audit(
                    "login_failed",
                    {"email": email_norm, "failed_login_attempts": int(getattr(u, "failed_login_attempts", 0) or 0)},
                    user_id=u.id,
                    company_id=infer_company_id_for_user(u),
                    entity_type="user",
                    entity_id=u.id,
                )

                if getattr(u, "failed_login_attempts", 0) >= MAX_ATTEMPTS:
                    st.error(f"Çok fazla hatalı deneme. Hesap {LOCK_MINUTES} dakika kilitlendi.")
                else:
                    st.error("Hatalı e-posta/şifre")
                return

            # Başarılı login
            u.failed_login_attempts = 0
            u.locked_until = None
            u.last_login_at = now
            s.add(u)
            s.commit()

            st.session_state[SESSION_KEY] = u.id

            append_audit(
                "login_success",
                {"email": email_norm},
                user_id=u.id,
                company_id=infer_company_id_for_user(u),
                entity_type="user",
                entity_id=u.id,
            )

            st.rerun()

    with st.expander("Admin şifresi notu"):
        st.markdown(
            """
Varsayılan bootstrap admin:
- **E-posta:** `admin@demo.com`
- **Şifre:** `ChangeMe123!`

Eğer daha önce farklı şifreyle admin oluştuysa ve giriş yapamıyorsanız:
Streamlit **Secrets** içine geçici olarak şunu ekleyin:
- `BOOTSTRAP_ADMIN_FORCE_RESET = "1"`
ve `BOOTSTRAP_ADMIN_PASSWORD` değerini istediğiniz şifre yapın.

Giriş yaptıktan sonra `BOOTSTRAP_ADMIN_FORCE_RESET` satırını kaldırın.
"""
        )


def logout_button():
    if st.button("Çıkış"):
        u = current_user()
        if u:
            append_audit(
                "logout",
                {"email": getattr(u, "email", "")},
                user_id=getattr(u, "id", None),
                company_id=infer_company_id_for_user(u),
                entity_type="user",
                entity_id=getattr(u, "id", None),
            )
        st.session_state.pop(SESSION_KEY, None)
        st.rerun()
