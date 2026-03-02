from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

import requests
from sqlalchemy import select

from src.db.session import db
from src.db.global_ready_models_step2 import RegulationSpecVersion


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def register_spec(spec_name: str, version_label: str, *, source_url: str = "", content_bytes: bytes | None = None, notes: str = "") -> RegulationSpecVersion:
    sha = _sha256_bytes(content_bytes or b"") if content_bytes is not None else ""
    with db() as s:
        obj = RegulationSpecVersion(
            spec_name=str(spec_name),
            version_label=str(version_label),
            sha256=str(sha),
            source_url=str(source_url or ""),
            notes=str(notes or ""),
            is_active=True,
        )
        s.add(obj)
        s.commit()
        s.refresh(obj)
        return obj


def fetch_and_register(spec_name: str, version_label: str, url: str) -> RegulationSpecVersion:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return register_spec(spec_name, version_label, source_url=url, content_bytes=r.content, notes="auto-fetched")


def active_spec(spec_name: str) -> RegulationSpecVersion | None:
    with db() as s:
        return (
            s.execute(select(RegulationSpecVersion).where(RegulationSpecVersion.spec_name == str(spec_name), RegulationSpecVersion.is_active.is_(True)).order_by(RegulationSpecVersion.created_at.desc()))
            .scalars()
            .first()
        )
