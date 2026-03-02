from __future__ import annotations
from typing import Any, Dict, List
import requests

from .base import Connector, FetchParams

class ODataConnector(Connector):
    def __init__(self, *, name: str, base_url: str, auth: Dict[str, Any] | None = None, config: Dict[str, Any] | None = None):
        self.name = name
        self.kind = "odata"
        self.base_url = (base_url or "").rstrip("/")
        self.auth = auth or {}
        self.config = config or {}

    def fetch(self, dataset_type: str, params: FetchParams) -> List[Dict[str, Any]]:
        endpoints = self.config.get("endpoints") or {}
        entity = endpoints.get(dataset_type) or endpoints.get("default") or ""
        if not entity:
            raise ValueError(f"OData entity tanımlı değil: dataset_type={dataset_type}")
        url = self.base_url + "/" + entity.lstrip("/")
        headers = {"Accept": "application/json"}
        token = (self.auth.get("token") or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        # Basit filtre şablonu (kurulumda değiştirilebilir)
        params_q = {}
        if params.since and self.config.get("since_filter"):
            params_q["$filter"] = self.config["since_filter"].format(since=params.since)
        r = requests.get(url, headers=headers, params=params_q, timeout=60)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "value" in data:
            data = data["value"]
        if not isinstance(data, list):
            raise ValueError("OData response list değil.")
        return [x for x in data if isinstance(x, dict)]
