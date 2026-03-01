from __future__ import annotations

import datetime
import json
from dataclasses import asdict, is_dataclass
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
from typing import Any


def _normalize_float(x: float) -> str:
    # Deterministik float temsilini güçlendirmek için:
    #  - 12 ondalık basamağa yuvarla
    #  - sondaki sıfırları kırp
    d = Decimal(str(x)).quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_UP)
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def _canonicalize(obj: Any) -> Any:
    if obj is None:
        return None
    if is_dataclass(obj):
        obj = asdict(obj)
    if isinstance(obj, (str, int, bool)):
        return obj
    if isinstance(obj, float):
        return _normalize_float(obj)
    if isinstance(obj, Decimal):
        s = format(obj, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s or "0"
    if isinstance(obj, (datetime.date, datetime.datetime)):
        # ISO format, timezone varsa koru
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    if isinstance(obj, dict):
        return {str(k): _canonicalize(v) for k, v in sorted(obj.items(), key=lambda kv: str(kv[0]))}
    if isinstance(obj, (list, tuple, set)):
        return [_canonicalize(v) for v in obj]
    # fallback: repr
    return str(obj)


def canonical_json_bytes(obj: Any) -> bytes:
    can = _canonicalize(obj)
    return json.dumps(can, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_hex(obj: Any) -> str:
    return sha256(canonical_json_bytes(obj)).hexdigest()
