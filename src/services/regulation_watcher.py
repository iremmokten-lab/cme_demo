from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


@dataclass
class WatchedSpec:
    name: str
    url: str
    last_sha256: str = ""
    last_checked_at: str = ""


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def check_specs(specs: List[WatchedSpec]) -> Dict[str, Any]:
    out = []
    for s in specs:
        status = "OK"
        new_sha = ""
        err = ""
        try:
            r = requests.get(s.url, timeout=60)
            r.raise_for_status()
            new_sha = _sha256(r.content)
            if s.last_sha256 and new_sha != s.last_sha256:
                status = "CHANGED"
            elif not s.last_sha256:
                status = "BASELINED"
        except Exception as e:
            status = "ERROR"
            err = str(e)

        out.append({
            "name": s.name,
            "url": s.url,
            "status": status,
            "sha256": new_sha,
            "previous_sha256": s.last_sha256,
            "error": err,
        })
        s.last_sha256 = new_sha or s.last_sha256
        s.last_checked_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "items": out,
    }
