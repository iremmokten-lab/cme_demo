from __future__ import annotations

import os
import json
from typing import Optional, Dict, Any

from src.services.authz import current_user as _current_user_local
from src.db.session import db
from sqlalchemy import select

try:
    from src.db.production_step1_models import AccessAuditLog
except Exception:
    AccessAuditLog = None  # type: ignore

AUTH_MODE = os.getenv("CME_AUTH_MODE", "local")  # local | header
HEADER_EMAIL = os.getenv("CME_SSO_EMAIL_HEADER", "X-User-Email")
HEADER_ROLE = os.getenv("CME_SSO_ROLE_HEADER", "X-User-Role")
HEADER_COMPANY = os.getenv("CME_SSO_COMPANY_HEADER", "X-Company-Id")

def current_user(request_headers: Optional[Dict[str, str]] = None):
    if AUTH_MODE != "header":
        return _current_user_local()

    headers = request_headers or {}
    email = (headers.get(HEADER_EMAIL) or "").strip()
    if not email:
        # fallback local
        return _current_user_local()

    # Minimal: map header user to existing user by email (must exist)
    from src.db.models import User
    with db() as s:
        u = s.execute(select(User).where(User.email==email)).scalars().first()
        return u

def audit_log(*, user_id:int|None, company_id:int|None, project_id:int|None, action:str, resource:str="", meta:Dict[str,Any]|None=None):
    if AccessAuditLog is None:
        return
    meta = meta or {}
    with db() as s:
        s.add(AccessAuditLog(
            user_id=(int(user_id) if user_id else None),
            company_id=(int(company_id) if company_id else None),
            project_id=(int(project_id) if project_id else None),
            action=str(action),
            resource=str(resource),
            meta_json=json.dumps(meta, ensure_ascii=False),
        ))
        s.commit()
