from __future__ import annotations
import csv
from pathlib import Path
from typing import Any, Dict, List

from .base import Connector, FetchParams

class FileDropConnector(Connector):
    def __init__(self, *, name: str, folder: str, config: Dict[str, Any] | None = None):
        self.name = name
        self.kind = "file"
        self.folder = Path(folder)
        self.config = config or {}

    def fetch(self, dataset_type: str, params: FetchParams) -> List[Dict[str, Any]]:
        # Beklenen dosya: <dataset_type>.csv
        fp = self.folder / f"{dataset_type}.csv"
        if not fp.exists():
            raise ValueError(f"Dosya bulunamadı: {fp}")
        with fp.open("r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            return [dict(row) for row in r]
