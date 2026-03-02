from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Callable, Any

@dataclass(frozen=True)
class RegulationModule:
    code: str
    title: str
    handler: Callable[[dict], dict]

class RegulationRegistry:
    def __init__(self):
        self._mods: Dict[str, RegulationModule] = {}

    def register(self, module: RegulationModule) -> None:
        self._mods[module.code] = module

    def run(self, code: str, payload: dict) -> dict:
        if code not in self._mods:
            raise ValueError(f"Regülasyon modülü yok: {code}")
        return self._mods[code].handler(payload)
