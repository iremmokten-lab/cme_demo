# -*- coding: utf-8 -*-
"""ERP Connector (Faz-3)

Bu modülün amacı:
  - ERP'den gelen CSV/JSON dosyalarını deterministik şekilde okumak
  - Kolonları normalize etmek
  - REST/OData gibi HTTP kaynaklarından JSON çekmeye yardımcı olmak

Not:
  - Streamlit Cloud uyumlu olacak şekilde sadece stdlib + pandas + httpx kullanır.
  - Vendor-spesifik "SAP RFC" gibi ağır bağımlılıklar yoktur.
    SAP/Logo/Netsis entegrasyonu pratikte çoğunlukla "export" (CSV/Excel) veya "REST" ile yapılır.
"""

from __future__ import annotations

import io
import json
from typing import Any, Dict, Optional

import httpx
import pandas as pd

from src.connectors.excel_connector import normalize_headers


def read_csv_bytes(csv_bytes: bytes, *, encoding: str = "utf-8") -> pd.DataFrame:
    bio = io.BytesIO(csv_bytes)
    df = pd.read_csv(bio, encoding=encoding)
    return normalize_headers(df)


def read_json_bytes(json_bytes: bytes) -> pd.DataFrame:
    payload = json.loads(json_bytes.decode("utf-8"))
    # payload list-of-dict veya dict{items:[]}
    if isinstance(payload, dict):
        if "items" in payload and isinstance(payload["items"], list):
            payload = payload["items"]
        elif "value" in payload and isinstance(payload["value"], list):
            payload = payload["value"]
        else:
            # tek kayıt
            payload = [payload]
    if not isinstance(payload, list):
        raise ValueError("JSON formatı beklenmiyor (liste veya nesne olmalı).")
    df = pd.DataFrame(payload)
    return normalize_headers(df)


def http_fetch_json(
    *,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout_s: int = 30,
) -> Any:
    """HTTP GET ile JSON çek (REST/OData)."""
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        r = client.get(url, headers=headers or {}, params=params or {})
        r.raise_for_status()
        return r.json()
