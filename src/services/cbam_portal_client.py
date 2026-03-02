from __future__ import annotations

import os
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

CBAM_PORTAL_BASE_URL = os.getenv("CME_CBAM_PORTAL_BASE_URL", "")
CBAM_PORTAL_API_KEY = os.getenv("CME_CBAM_PORTAL_API_KEY", "")  # optional token

@dataclass
class PortalSubmitResponse:
    ok: bool
    reference: str
    status: str
    raw: Dict[str, Any]
    error: str = ""

def submit_zip(zip_bytes: bytes, *, filename: str="cbam_submission.zip") -> PortalSubmitResponse:
    if not CBAM_PORTAL_BASE_URL:
        return PortalSubmitResponse(ok=False, reference="", status="NOT_CONFIGURED", raw={}, error="CME_CBAM_PORTAL_BASE_URL ayarlı değil.")
    url = CBAM_PORTAL_BASE_URL.rstrip("/") + "/submit"
    headers = {}
    if CBAM_PORTAL_API_KEY:
        headers["Authorization"] = f"Bearer {CBAM_PORTAL_API_KEY}"
    files = {"file": (filename, zip_bytes, "application/zip")}
    try:
        r = requests.post(url, headers=headers, files=files, timeout=120)
        r.raise_for_status()
        data = r.json() if "application/json" in (r.headers.get("content-type","")) else {"text": r.text}
        ref = str(data.get("reference") or data.get("id") or "")
        st = str(data.get("status") or "submitted")
        return PortalSubmitResponse(ok=True, reference=ref, status=st, raw=data)
    except Exception as e:
        return PortalSubmitResponse(ok=False, reference="", status="ERROR", raw={}, error=str(e))

def check_status(reference: str) -> Dict[str, Any]:
    if not CBAM_PORTAL_BASE_URL:
        return {"ok": False, "status": "NOT_CONFIGURED"}
    url = CBAM_PORTAL_BASE_URL.rstrip("/") + f"/status/{reference}"
    headers = {}
    if CBAM_PORTAL_API_KEY:
        headers["Authorization"] = f"Bearer {CBAM_PORTAL_API_KEY}"
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json() if "application/json" in (r.headers.get("content-type","")) else {"text": r.text}
