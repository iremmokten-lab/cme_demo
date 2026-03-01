from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional


class CBAMXSDValidator:
    """Official XSD validation (portal-grade) using `xmlschema` (pure python).

    Notes:
      - Streamlit Cloud friendliness: avoids lxml.
      - The official XSD ZIP published by the European Commission typically includes:
          * QReport_verXX.XX.xsd
          * Stypes_verXX.XX.xsd
          * (optional) XML guidance PDF
      - This validator expects a *main* XSD file path (QReport_*.xsd).
    """

    def __init__(self, xsd_main_path: str):
        self.xsd_main_path = Path(xsd_main_path)
        if not self.xsd_main_path.exists():
            raise FileNotFoundError(f"CBAM XSD not found: {self.xsd_main_path}")
        self._schema = None

    @staticmethod
    def sha256_file(path: Path) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                b = f.read(1024 * 1024)
                if not b:
                    break
                h.update(b)
        return h.hexdigest()

    def _load_schema(self):
        if self._schema is not None:
            return self._schema
        try:
            import xmlschema  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "xmlschema dependency is missing. Add `xmlschema` to requirements.txt."
            ) from e

        # xmlschema resolves relative imports/includes based on the XSD file location
        self._schema = xmlschema.XMLSchema(str(self.xsd_main_path))
        return self._schema

    def validate_xml_bytes(self, xml_bytes: bytes) -> Dict[str, Any]:
        schema = self._load_schema()
        errors: List[str] = []
        valid = True
        try:
            # iter_errors yields detailed error objects
            for err in schema.iter_errors(xml_bytes):
                valid = False
                errors.append(str(err))
        except Exception as e:
            valid = False
            errors.append(str(e))

        return {
            "valid": bool(valid),
            "errors": errors[:200],
            "xsd_main": str(self.xsd_main_path),
            "xsd_hash": self.sha256_file(self.xsd_main_path),
        }

    def validate_xml_path(self, xml_path: str) -> Dict[str, Any]:
        p = Path(xml_path)
        return self.validate_xml_bytes(p.read_bytes())
