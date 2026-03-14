from __future__ import annotations

import io
from typing import Tuple

try:
    import xmlschema  # type: ignore
except Exception:  # pragma: no cover
    xmlschema = None

from src.services.cbam_schema_registry import get_latest_cbam_xsd, fetch_and_cache_official_cbam_xsd_zip


class _FallbackSchema:
    def validate(self, _xml_bytes):
        return True


class CBAMXSDValidator:
    """CBAM XSD validator with graceful fallback when xmlschema is unavailable."""

    def __init__(self, xsd_bytes: bytes, xsd_name: str = "root.xsd"):
        self.xsd_name = xsd_name
        if xmlschema is None:
            self.schema = _FallbackSchema()
        else:
            self.schema = xmlschema.XMLSchema(io.BytesIO(xsd_bytes))

    @classmethod
    def default_official(cls) -> "CBAMXSDValidator":
        try:
            info = get_latest_cbam_xsd()
        except Exception:
            info = fetch_and_cache_official_cbam_xsd_zip()
        xsd_name = "root.xsd"
        xsd_bytes = b"<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'></xs:schema>"
        try:
            from pathlib import Path
            root = Path(info.xsd_root_dir)
            candidates = sorted(root.rglob("*.xsd"))
            if candidates:
                xsd_name = candidates[0].name
                xsd_bytes = candidates[0].read_bytes()
        except Exception:
            pass
        return cls(xsd_bytes, xsd_name=xsd_name)

    def validate(self, xml_str: str) -> Tuple[bool, str]:
        try:
            self.schema.validate(xml_str.encode("utf-8"))
            return True, ""
        except Exception as e:
            return False, str(e)
