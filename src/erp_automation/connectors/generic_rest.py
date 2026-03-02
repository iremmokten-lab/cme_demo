from __future__ import annotations
import json
from typing import Any, Dict, List
import requests

from .base import Connector, FetchParams

class GenericRESTConnector(Connector):
    def __init__(self, *, name: str, base_url: str, auth: Dict[str, Any] | None = None, config: Dict[str, Any] | None = None):
        self.name = name
        self.kind = "rest"
        self.base_url = (base_url or "").rstrip("/")
        self.auth = auth or {}
        self.config = config or {}

    def test(self) -> Dict[str, Any]:
        url = self.base_url + (self.config.get("health_path") or "/health")
        headers = {}
        token = (self.auth.get("token") or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            r = requests.get(url, headers=headers, timeout=15)
            return {"ok": r.status_code < 500, "status_code": r.status_code, "url": url}
        except Exception as e:
            return {"ok": False, "error": str(e), "url": url}

    def fetch(self, dataset_type: str, params: FetchParams) -> List[Dict[str, Any]]:
        endpoints = self.config.get("endpoints") or {}
        path = endpoints.get(dataset_type) or endpoints.get("default") or ""
        if not path:
            raise ValueError(f"Endpoint tanımlı değil: dataset_type={dataset_type}")
        url = self.base_url + path
        headers = {"Accept": "application/json"}
        token = (self.auth.get("token") or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        q = {}
        if params.since: q["since"] = params.since
        if params.until: q["until"] = params.until
        r = requests.get(url, headers=headers, params=q, timeout=60)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "items" in data:
            data = data["items"]
        if not isinstance(data, list):
            raise ValueError("REST response list değil.")
        return [x for x in data if isinstance(x, dict)]
