from __future__ import annotations
import json
from sqlalchemy import select
from src.db.session import db
from src.db.production_step2_models import IntegrationConnection

def create_connection(project_id:int, name:str, kind:str="generic", base_url:str="", auth:dict|None=None, config:dict|None=None)->IntegrationConnection:
    with db() as s:
        c = IntegrationConnection(project_id=int(project_id), name=str(name), kind=str(kind), base_url=str(base_url),
                                  auth_json=json.dumps(auth or {}, ensure_ascii=False), config_json=json.dumps(config or {}, ensure_ascii=False), status="active")
        s.add(c); s.commit(); s.refresh(c); return c

def list_connections(project_id:int)->list[IntegrationConnection]:
    with db() as s:
        return s.execute(select(IntegrationConnection).where(IntegrationConnection.project_id==int(project_id)).order_by(IntegrationConnection.id.desc())).scalars().all()
