
import hashlib
from lxml import etree
from pathlib import Path

class CBAMXSDValidator:
    def __init__(self, xsd_path: str):
        self.xsd_path = Path(xsd_path)
        schema_root = etree.parse(str(self.xsd_path))
        self.schema = etree.XMLSchema(schema_root)

    def validate(self, xml_path: str):
        xml_doc = etree.parse(xml_path)
        is_valid = self.schema.validate(xml_doc)
        errors = [str(e) for e in self.schema.error_log]
        return {
            "valid": is_valid,
            "errors": errors,
            "xsd_hash": self._hash_file(self.xsd_path),
        }

    def _hash_file(self, p: Path):
        h = hashlib.sha256()
        with open(p, "rb") as f:
            h.update(f.read())
        return h.hexdigest()
