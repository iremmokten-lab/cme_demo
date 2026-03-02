from __future__ import annotations
import json
from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from src.db.session import db
from src.db.production_step2_models import CacheEntry

def cache_get(key:str):
    now=datetime.now(timezone.utc)
    with db() as s:
        c=s.execute(select(CacheEntry).where(CacheEntry.key==str(key))).scalars().first()
        if not c: return None
        if c.expires_at and now > c.expires_at:
            s.delete(c); s.commit()
            return None
        try:
            return json.loads(c.value_json or "{}")
        except Exception:
            return None

def cache_set(key:str, value:dict, ttl_seconds:int=300):
    exp = datetime.now(timezone.utc) + timedelta(seconds=int(ttl_seconds))
    with db() as s:
        c=s.execute(select(CacheEntry).where(CacheEntry.key==str(key))).scalars().first()
        if not c:
            c=CacheEntry(key=str(key), value_json=json.dumps(value, ensure_ascii=False), expires_at=exp)
            s.add(c); s.commit(); return
        c.value_json=json.dumps(value, ensure_ascii=False)
        c.expires_at=exp
        s.commit()
