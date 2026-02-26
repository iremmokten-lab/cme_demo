import uuid
from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from passlib.hash import bcrypt
from sqlalchemy import select, text

from services.api.core.config import settings
from services.api.db.session import get_db
from services.api.db.models import User, UserRole, UserFacilityScope

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

def _create_access_token(payload: dict, minutes: int) -> str:
    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=minutes)
    to_encode = {
        **payload,
        "iss": settings.JWT_ISSUER,
        "aud": settings.JWT_AUDIENCE,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(to_encode, settings.JWT_SECRET, algorithm="HS256")

@router.post("/login")
async def login(data: dict, db=Depends(get_db)):
    tenant_id = data.get("tenant_id")
    email = data.get("email")
    password = data.get("password")

    if not tenant_id or not email or not password:
        raise HTTPException(status_code=400, detail="tenant_id, email, password zorunlu")

    res = await db.execute(select(User).where(User.tenant_id == tenant_id, User.email == email, User.is_active == True))
    user = res.scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=401, detail="Geçersiz giriş")
    if not bcrypt.verify(password, user.password_hash):
        raise HTTPException(status_code=401, detail="Geçersiz giriş")

    # roles
    rr = await db.execute(select(UserRole.role_name).where(UserRole.tenant_id == tenant_id, UserRole.user_id == user.id))
    roles = [r[0] for r in rr.all()]
    roles_str = ",".join(sorted(set(roles))) if roles else ""

    token = _create_access_token(
        payload={
            "tid": str(user.tenant_id),
            "uid": str(user.id),
            "roles": roles,
        },
        minutes=settings.ACCESS_TOKEN_MINUTES,
    )
    return {"access_token": token, "token_type": "bearer", "roles": roles, "roles_str": roles_str}

async def _decode_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=["HS256"], audience=settings.JWT_AUDIENCE, issuer=settings.JWT_ISSUER)
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Token geçersiz veya süresi dolmuş")

async def get_current_context(token: Annotated[str, Depends(oauth2_scheme)]) -> dict:
    p = await _decode_token(token)
    if "tid" not in p or "uid" not in p:
        raise HTTPException(status_code=401, detail="Token eksik")
    return {"tid": p["tid"], "uid": p["uid"], "roles": p.get("roles", [])}

async def get_current_db_with_rls(
    ctx: Annotated[dict, Depends(get_current_context)],
    db=Depends(get_db)
):
    """
    RLS session vars:
      app.tenant_id
      app.user_id
      app.roles
    """
    if settings.RLS_ENABLED:
        await db.execute(text("SELECT set_config('app.tenant_id', :tid, true)"), {"tid": ctx["tid"]})
        await db.execute(text("SELECT set_config('app.user_id', :uid, true)"), {"uid": ctx["uid"]})
        await db.execute(text("SELECT set_config('app.roles', :roles, true)"), {"roles": ",".join(ctx.get("roles", []))})
    return ctx, db

@router.get("/me")
async def me(ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    # facility scopes for UI visibility
    res = await db.execute(
        select(UserFacilityScope.facility_id, UserFacilityScope.scope_role)
        .where(UserFacilityScope.tenant_id == ctx["tid"], UserFacilityScope.user_id == ctx["uid"], UserFacilityScope.is_active == True)
    )
    scopes = [{"facility_id": str(r[0]), "scope_role": r[1]} for r in res.all()]
    return {"tenant_id": ctx["tid"], "user_id": ctx["uid"], "roles": ctx.get("roles", []), "facility_scopes": scopes}
