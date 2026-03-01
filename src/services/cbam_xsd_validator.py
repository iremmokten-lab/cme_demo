import hashlib
from lxml import etree
from pathlib import Path
from typing import Union

from src.services.cbam_schema_assets import ensure_cbam_xsd_assets


class CBAMXSDValidator:
    def __init__(self, xsd_path: str):
        self.xsd_path = Path(xsd_path)
        schema_root = etree.parse(str(self.xsd_path))
        self.schema = etree.XMLSchema(schema_root)

    @staticmethod
    def default_official(version: str = "23.00") -> "CBAMXSDValidator":
        asset = ensure_cbam_xsd_assets(version=version)
        return CBAMXSDValidator(str(asset.qreport_xsd))

    def validate(self, xml_path: str):
        xml_doc = etree.parse(xml_path)
        is_valid = self.schema.validate(xml_doc)
        errors = [str(e) for e in self.schema.error_log]
        return {
            "valid": bool(is_valid),
            "errors": errors,
            "xsd_hash": self._hash_file(self.xsd_path),
        }

    def validate_bytes(self, xml_bytes: Union[bytes, str]):
        if isinstance(xml_bytes, str):
            xml_bytes = xml_bytes.encode("utf-8")
        xml_doc = etree.fromstring(xml_bytes)
        is_valid = self.schema.validate(xml_doc)
        errors = [str(e) for e in self.schema.error_log]
        return {
            "valid": bool(is_valid),
            "errors": errors,
            "xsd_hash": self._hash_file(self.xsd_path),
        }

    def _hash_file(self, p: Path):
        h = hashlib.sha256()
        with open(p, "rb") as f:
            h.update(f.read())
        return h.hexdigest()
