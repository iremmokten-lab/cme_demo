from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class BenchmarkResult:
    intensity: float
    unit: str
    percentile_note: str
    detail: Dict[str, Any]

def simple_benchmark(emissions_tco2: float, production_t: float) -> BenchmarkResult:
    if production_t <= 0:
        return BenchmarkResult(intensity=0.0, unit="tCO2/t", percentile_note="NA", detail={"reason":"production<=0"})
    intensity = float(emissions_tco2) / float(production_t)
    # Placeholder: real benchmark needs sector database
    note = "Sektör verisi bağlanınca yüzdelik hesaplanır."
    return BenchmarkResult(intensity=intensity, unit="tCO2/t", percentile_note=note, detail={"method":"simple"})
