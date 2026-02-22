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


def _get_secret(key: str, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


def ensure_bootstrap_admin():
    """
    - Eğer hiç user yoksa: demo company + consultantadmin yaratır.
    - Eğer admin email zaten varsa: isteğe bağlı olarak şifreyi resetleyebilir.
      Bunun için Streamlit Secrets'a: BOOTSTRAP_ADMIN_FORCE_RESET="1" ekleyin.
    """
    admin_email = _get_secret("BOOTSTRAP_ADMIN_EMAIL", "admin@demo.com")
    admin_pw = _get_secret("BOOTSTRAP_ADMIN_PASSWORD", "ChangeMe123!")
    force_reset = str(_get_secret("BOOTSTRAP_ADMIN_FORCE_RESET", "0")).strip() == "1"

    with db() as s:
        # DB'de herhangi bir user var mı? (MultipleResultsFound yaşamamak için first)
        any_user = s.execute(select(User).limit(1)).scalars().first()

        # Admin kullanıcı var mı?
        admin_user = s.execute(select(User).where(User.email == admin_email).limit(1)).scalars().first()

        if not any_user:
            # İlk kurulum: demo company + admin
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

        # User var ama admin yoksa: sadece admin'i ekleyelim (company yoksa demo company aç)
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

        # Admin var: istenirse şifre reset
        if force_reset:
            admin_user.password_hash = _hash_pw(admin_pw)
            s.add(admin_user)
            s.commit()


def current_user():
    uid = st.session_state.get(SESSION_KEY)
    if not uid:
        return None
    with db() as s:
        return s.get(User, uid)


def login_view():
    st.title("Giriş")

    email = st.text_input("E-posta")
    pw = st.text_input("Şifre", type="password")

    if st.button("Giriş yap", type="primary"):
        with db() as s:
            u = s.execute(select(User).where(User.email == email).limit(1)).scalars().first()

        if (not u) or (not _check_pw(pw, u.password_hash)):
            st.error("Hatalı e-posta/şifre")
            return

        st.session_state[SESSION_KEY] = u.id
        st.rerun()

    with st.expander("Admin şifresi notu"):
        st.markdown(
            """
Varsayılan bootstrap admin:

- **E-posta:** `admin@demo.com`  
- **Şifre:** `ChangeMe123!`

Eğer daha önce farklı şifreyle admin oluştuysa ve giriş yapamıyorsanız:
Streamlit **Secrets** içine geçici olarak şu satırı ekleyin:

- `BOOTSTRAP_ADMIN_FORCE_RESET = "1"`

ve `BOOTSTRAP_ADMIN_PASSWORD` değerini istediğiniz şifre yapın.
Giriş yaptıktan sonra `BOOTSTRAP_ADMIN_FORCE_RESET` satırını kaldırın.
            """
        )


def logout_button():
    if st.button("Çıkış"):
        st.session_state.pop(SESSION_KEY, None)
        st.rerun()
