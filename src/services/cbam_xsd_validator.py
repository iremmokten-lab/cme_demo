from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Tuple

import xmlschema

from src.services.cbam_schema_registry import get_latest_cbam_xsd, fetch_and_cache_official_cbam_xsd_zip, load_xsd_zip_bytes, extract_main_xsd_from_zip

class CBAMXSDValidator:
    """Resmi CBAM XSD ile XML doğrulama (Streamlit Cloud uyumlu, pure python)."""

    def __init__(self, xsd_bytes: bytes, xsd_name: str = "root.xsd"):
        self.xsd_name = xsd_name
        self.schema = xmlschema.XMLSchema(io.BytesIO(xsd_bytes))

    @classmethod
    def default_official(cls) -> "CBAMXSDValidator":
        info = get_latest_cbam_xsd()
        if not info:
            info = fetch_and_cache_official_cbam_xsd_zip()
        zbytes = load_xsd_zip_bytes(info)
        name, root_xsd = extract_main_xsd_from_zip(zbytes)
        return cls(root_xsd, xsd_name=name)

    def validate(self, xml_str: str) -> Tuple[bool, str]:
        try:
            self.schema.validate(xml_str.encode("utf-8"))
            return True, ""
        except Exception as e:
            return False, str(e)
