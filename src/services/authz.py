from __future__ import annotations

from datetime import datetime, timedelta, timezone

import bcrypt
from sqlalchemy import select

from src.db.models import User
from src.db.session import db
from src.mrv.audit import append_audit, infer_company_id_for_user


MAX_ATTEMPTS = 5
LOCK_MINUTES = 15


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        return False


def authenticate(email: str, password: str) -> User | None:
    email = (email or "").strip().lower()
    if not email or not password:
        return None

    now = datetime.now(timezone.utc)

    with db() as s:
        user = s.execute(select(User).where(User.email == email).limit(1)).scalars().first()
        if not user:
            return None

        # lock check
        if getattr(user, "locked_until", None) is not None:
            try:
                if user.locked_until and user.locked_until > now:
                    append_audit(
                        "login_blocked_locked",
                        {"email": email, "locked_until": user.locked_until.isoformat()},
                        user_id=user.id,
                        company_id=infer_company_id_for_user(user),
                        entity_type="user",
                        entity_id=user.id,
                    )
                    return None
            except Exception:
                pass

        ok = verify_password(password, user.password_hash)
        if ok:
            user.failed_login_attempts = 0
            user.locked_until = None
            user.last_login_at = now
            s.add(user)
            s.commit()

            append_audit(
                "login_success",
                {"email": email},
                user_id=user.id,
                company_id=infer_company_id_for_user(user),
                entity_type="user",
                entity_id=user.id,
            )
            return user

        # failed
        try:
            user.failed_login_attempts = int(user.failed_login_attempts or 0) + 1
        except Exception:
            user.failed_login_attempts = 1

        if user.failed_login_attempts >= MAX_ATTEMPTS:
            user.locked_until = now + timedelta(minutes=LOCK_MINUTES)

        s.add(user)
        s.commit()

        append_audit(
            "login_failed",
            {"email": email, "failed_login_attempts": int(user.failed_login_attempts or 0)},
            user_id=user.id,
            company_id=infer_company_id_for_user(user),
            entity_type="user",
            entity_id=user.id,
        )
        return None
