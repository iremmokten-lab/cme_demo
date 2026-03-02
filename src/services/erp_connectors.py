from __future__ import annotations

import json
from typing import Any, Dict, List

import httpx
from sqlalchemy import select

from src.db.session import db
from src.db.global_ready_models_step2 import ERPConnection


def create_connection(company_id: int, name: str, kind: str, base_url: str, token_secret: str, config: dict | None = None) -> ERPConnection:
    with db() as s:
        c = ERPConnection(company_id=int(company_id), name=str(name).strip(), kind=str(kind).strip(), base_url=str(base_url).strip(), token_secret=str(token_secret).strip(), config_json=json.dumps(config or {}, ensure_ascii=False))
        s.add(c)
        s.commit()
        s.refresh(c)
        return c


def list_connections(company_id: int) -> list[ERPConnection]:
    with db() as s:
        return (
            s.execute(select(ERPConnection).where(ERPConnection.company_id == int(company_id), ERPConnection.is_active.is_(True)).order_by(ERPConnection.id.desc()))
            .scalars()
            .all()
        )


async def odata_fetch(base_url: str, entity_path: str, token: str = "") -> dict:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(base_url.rstrip("/") + "/" + entity_path.lstrip("/"), headers=headers)
        r.raise_for_status()
        return r.json()
