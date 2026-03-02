from __future__ import annotations
import csv, io, json
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from sqlalchemy import select

from src.db.session import db
from src.db.models import DatasetUpload
from src.db.erp_automation_models import ERPConnection, ERPMapping, ERPIngestionRun, ERPDeadLetter
from src.services.storage import storage_path_for_project, write_bytes

from src.erp_automation.connectors.generic_rest import GenericRESTConnector
from src.erp_automation.connectors.odata import ODataConnector
from src.erp_automation.connectors.file_drop import FileDropConnector
from src.erp_automation.mapping import apply_mapping
from src.erp_automation.hashing import sha256_json

def _connector_from_row(conn: ERPConnection):
    try:
        auth = json.loads(conn.auth_json or "{}")
    except Exception:
        auth = {}
    try:
        cfg = json.loads(conn.config_json or "{}")
    except Exception:
        cfg = {}
    if conn.kind == "odata":
        return ODataConnector(name=conn.name, base_url=conn.base_url, auth=auth, config=cfg)
    if conn.kind == "file":
        folder = cfg.get("folder") or "./storage/erp_drop"
        return FileDropConnector(name=conn.name, folder=str(folder), config=cfg)
    return GenericRESTConnector(name=conn.name, base_url=conn.base_url, auth=auth, config=cfg)

def get_latest_mapping(project_id:int, dataset_type:str) -> ERPMapping | None:
    with db() as s:
        return s.execute(
            select(ERPMapping).where(
                ERPMapping.project_id==int(project_id),
                ERPMapping.dataset_type==str(dataset_type),
                ERPMapping.status.in_(["approved","locked","draft"]),
            ).order_by(ERPMapping.version.desc())
        ).scalars().first()

def ensure_default_mapping(project_id:int, dataset_type:str) -> ERPMapping:
    with db() as s:
        existing = s.execute(select(ERPMapping).where(ERPMapping.project_id==int(project_id), ERPMapping.dataset_type==str(dataset_type)).order_by(ERPMapping.version.desc())).scalars().first()
        if existing:
            return existing
        # default: identity (user will edit)
        m = ERPMapping(project_id=int(project_id), dataset_type=str(dataset_type), version=1, status="draft", mapping_json=json.dumps({}, ensure_ascii=False))
        s.add(m); s.commit(); s.refresh(m); return m

def upsert_mapping(project_id:int, dataset_type:str, version:int, mapping:dict, *, status:str="draft", notes:str=""):
    with db() as s:
        m = s.execute(select(ERPMapping).where(ERPMapping.project_id==int(project_id), ERPMapping.dataset_type==str(dataset_type), ERPMapping.version==int(version))).scalars().first()
        if not m:
            m = ERPMapping(project_id=int(project_id), dataset_type=str(dataset_type), version=int(version), status=status,
                           mapping_json=json.dumps(mapping or {}, ensure_ascii=False), notes=str(notes or ""))
            s.add(m); s.commit(); s.refresh(m); return m
        m.mapping_json=json.dumps(mapping or {}, ensure_ascii=False)
        m.status=str(status)
        m.notes=str(notes or "")
        s.commit(); s.refresh(m); return m

def approve_mapping(project_id:int, dataset_type:str, version:int):
    with db() as s:
        m = s.execute(select(ERPMapping).where(ERPMapping.project_id==int(project_id), ERPMapping.dataset_type==str(dataset_type), ERPMapping.version==int(version))).scalars().first()
        if not m: raise ValueError("Mapping bulunamadı.")
        m.status="approved"; s.commit()

def create_connection(project_id:int, name:str, kind:str, base_url:str, auth:dict, config:dict):
    with db() as s:
        c = ERPConnection(project_id=int(project_id), name=str(name), kind=str(kind), base_url=str(base_url),
                          auth_json=json.dumps(auth or {}, ensure_ascii=False),
                          config_json=json.dumps(config or {}, ensure_ascii=False),
                          status="active")
        s.add(c); s.commit(); s.refresh(c); return c

def list_connections(project_id:int):
    with db() as s:
        return s.execute(select(ERPConnection).where(ERPConnection.project_id==int(project_id)).order_by(ERPConnection.id.desc())).scalars().all()

def run_ingestion(project_id:int, connection_id:int, dataset_type:str, *, since:str|None=None, until:str|None=None) -> Tuple[int, int, int]:
    # returns: (run_id, upload_id, dlq_count)
    with db() as s:
        conn = s.get(ERPConnection, int(connection_id))
        if not conn: raise ValueError("Connection bulunamadı.")
        m = get_latest_mapping(int(project_id), str(dataset_type))
        if not m: m = ensure_default_mapping(int(project_id), str(dataset_type))
        try:
            mapping = json.loads(m.mapping_json or "{}")
        except Exception:
            mapping = {}

        run = ERPIngestionRun(
            project_id=int(project_id), connection_id=int(connection_id),
            dataset_type=str(dataset_type), mapping_version=int(m.version),
            status="running", started_at=datetime.now(timezone.utc)
        )
        s.add(run); s.commit(); s.refresh(run)
        run_id = int(run.id)

    connector = _connector_from_row(conn)
    raw = connector.fetch(str(dataset_type), params=type("P",(object,),{"since":since,"until":until})())
    raw_hash = sha256_json(raw)

    normalized = apply_mapping(raw, mapping, dataset_type=str(dataset_type))
    norm_hash = sha256_json(normalized)

    # DLQ: rows missing required fields (simple rule)
    required = [k for k,v in (("facility_code",True),("period",True)) if v]
    dlq = []
    ok_rows = []
    for r in normalized:
        if any(r.get(k) in (None,"") for k in required):
            dlq.append(r)
        else:
            ok_rows.append(r)

    # Write normalized CSV to storage and create DatasetUpload
    buf = io.StringIO()
    if ok_rows:
        fieldnames = sorted(ok_rows[0].keys())
    else:
        # still write headers from schema
        fieldnames = sorted(list((normalized[0].keys() if normalized else [])))
    wcsv = csv.DictWriter(buf, fieldnames=fieldnames)
    wcsv.writeheader()
    for r in ok_rows:
        wcsv.writerow(r)

    content = buf.getvalue().encode("utf-8")
    uri_path = storage_path_for_project(int(project_id), f"erp_ingestion/{dataset_type}/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_normalized.csv")
    write_bytes(uri_path, content)

    upload_id = None
    with db() as s:
        up = DatasetUpload(
            project_id=int(project_id),
            dataset_type=str(dataset_type),
            schema_version="v1",
            original_filename=f"erp_{dataset_type}_normalized.csv",
            sha256=sha256_json({"csv": norm_hash})[:64],
            content_hash=norm_hash,
            storage_uri=str(uri_path),
            validated=False,
            data_quality_score=0,
        )
        s.add(up); s.commit(); s.refresh(up)
        upload_id = int(up.id)

        # Store DLQ rows
        for r in dlq[:2000]:
            s.add(ERPDeadLetter(run_id=run_id, dataset_type=str(dataset_type), reason="missing_required_fields", record_json=json.dumps(r, ensure_ascii=False)))
        s.commit()

        run = s.get(ERPIngestionRun, run_id)
        if run:
            run.status = "success"
            run.finished_at = datetime.now(timezone.utc)
            run.raw_count = len(raw)
            run.normalized_count = len(ok_rows)
            run.raw_sha256 = raw_hash
            run.normalized_sha256 = norm_hash
            run.output_upload_id = upload_id
            s.commit()

    return run_id, upload_id, len(dlq)

def list_runs(project_id:int, limit:int=50):
    with db() as s:
        return s.execute(select(ERPIngestionRun).where(ERPIngestionRun.project_id==int(project_id)).order_by(ERPIngestionRun.id.desc()).limit(int(limit))).scalars().all()
