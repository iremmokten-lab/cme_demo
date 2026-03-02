from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

@dataclass
class FetchParams:
    since: Optional[str] = None  # ISO date/time string
    until: Optional[str] = None

class Connector:
    name: str
    kind: str

    def test(self) -> Dict[str, Any]:
        return {"ok": True, "kind": self.kind, "name": self.name}

    def fetch(self, dataset_type: str, params: FetchParams) -> List[Dict[str, Any]]:
        raise NotImplementedError
