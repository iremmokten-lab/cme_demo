from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from typing import Optional, List

import httpx
import xmlschema

from src import config as app_config

# Default: DG TAXUD CBAM Registry & Reporting page hosts the latest XSD zip.
# You can override with env CBAM_XSD_ZIP_URL.
DEFAULT_XSD_ZIP_URL = "https://taxation-customs.ec.europa.eu/system/files/2024-10/cbam_xsd_v23_00.zip"


def _schemas_dir() -> Path:
    # repo-relative path
    return Path("schemas") / "cbam"


def _xsd_zip_url() -> str:
    v = os.getenv("CBAM_XSD_ZIP_URL", "").strip()
    if v:
        return v
    return DEFAULT_XSD_ZIP_URL


def ensure_cbam_xsd_present() -> Path:
    """
    Ensures schemas/cbam has at least one .xsd file.
    If missing, downloads a ZIP from DG TAXUD (or override URL) and extracts it.
    Works on Streamlit Cloud (writes to repo working directory).
    """
    d = _schemas_dir()
    d.mkdir(parents=True, exist_ok=True)

    xsds = list(d.rglob("*.xsd"))
    if xsds:
        return d

    url = _xsd_zip_url()
    # Download ZIP
    timeout = httpx.Timeout(30.0)
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
        data = r.content

    # Extract ZIP
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        z.extractall(d)

    return d


def _candidate_xsds(schema_dir: Path) -> List[Path]:
    xsds = list(schema_dir.rglob("*.xsd"))
    xsds.sort(key=lambda p: (len(str(p)), str(p).lower()))
    return xsds


def validate_cbam_xml(xml_string: str, strict: bool = True) -> bool:
    """
    Validates CBAM XML against the official XSD set.

    Strategy:
      - Ensure XSD files are available (download if needed).
      - Try validating against each XSD found; accept the first that validates.
      - If none validate:
          - strict=True => raise ValueError with condensed errors
          - strict=False => return False

    Notes:
      - Different releases may have different "main" XSD entrypoints.
      - This approach is robust to variations as long as an entry XSD exists.
    """
    try:
        schema_dir = ensure_cbam_xsd_present()
    except Exception as e:
        if strict:
            raise
        return False

    xsds = _candidate_xsds(schema_dir)
    if not xsds:
        if strict:
            raise ValueError("CBAM XSD bulunamadı (schemas/cbam altında .xsd yok).")
        return False

    last_err = None
    for xsd in xsds:
        try:
            schema = xmlschema.XMLSchema(str(xsd))
            schema.validate(xml_string)
            return True
        except Exception as e:
            last_err = e
            continue

    if strict:
        raise ValueError(f"CBAM XML XSD doğrulaması başarısız. Son hata: {last_err}")
    return False

