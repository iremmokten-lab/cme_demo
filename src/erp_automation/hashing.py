from __future__ import annotations
import hashlib, json
from typing import Any

def sha256_json(obj: Any) -> str:
    b = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(b).hexdigest()
