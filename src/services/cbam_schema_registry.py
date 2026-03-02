from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Optional, Tuple

import requests

from src.services.storage_backend import get_storage_backend
from src.db.session import db
from src.db.cbam_compliance_models import RegulationSpecVersion

def utcnow():
    return datetime.now(timezone.utc)

DEFAULT_CBAM_XSD_ZIP_URL = "https://taxation-customs.ec.europa.eu/system/files/2025-04/Quarterly%20Report%20structure%20XSD.zip"

@dataclass(frozen=True)
class CBAMSchemaInfo:
    spec_version: str
    spec_hash: str
    source_url: str
    storage_uri: str

def _sha256_bytes(b: bytes) -> str:
    return sha256(b).hexdigest()

def fetch_and_cache_official_cbam_xsd_zip(url: str = DEFAULT_CBAM_XSD_ZIP_URL) -> CBAMSchemaInfo:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.content
    h = _sha256_bytes(data)
    ver = "official:" + h[:12]
    backend = get_storage_backend()
    uri = backend.put_bytes(f"spec/cbam_xsd/{ver}.zip", data)

    with db() as s:
        existing = s.query(RegulationSpecVersion).filter_by(spec_name="CBAM_XSD", spec_version=ver).first()
        if not existing:
            row = RegulationSpecVersion(spec_name="CBAM_XSD", spec_version=ver, spec_hash=h, source=url)
            s.add(row)
            s.commit()
    return CBAMSchemaInfo(spec_version=ver, spec_hash=h, source_url=url, storage_uri=uri)

def get_latest_cbam_xsd() -> Optional[CBAMSchemaInfo]:
    with db() as s:
        row = (
            s.query(RegulationSpecVersion)
            .filter(RegulationSpecVersion.spec_name=="CBAM_XSD")
            .order_by(RegulationSpecVersion.fetched_at.desc())
            .first()
        )
        if not row:
            return None
        backend = get_storage_backend()
        # best effort: rebuild uri from key if possible
        key = f"spec/cbam_xsd/{row.spec_version}.zip"
        uri = backend.uri_for_key(key)
        return CBAMSchemaInfo(spec_version=row.spec_version, spec_hash=row.spec_hash or "", source_url=row.source or "", storage_uri=uri)

def load_xsd_zip_bytes(info: CBAMSchemaInfo) -> bytes:
    backend = get_storage_backend()
    return backend.get_bytes_by_uri(info.storage_uri)

def extract_main_xsd_from_zip(zip_bytes: bytes) -> Tuple[str, bytes]:
    # returns (filename, xsd_bytes)
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    # choose the largest .xsd as root
    xsds = [(n, z.getinfo(n).file_size) for n in z.namelist() if n.lower().endswith(".xsd")]
    if not xsds:
        raise ValueError("XSD zip içinde .xsd bulunamadı.")
    xsds.sort(key=lambda x: x[1], reverse=True)
    name = xsds[0][0]
    return name, z.read(name)
