from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class ProcessEmissionResult:
    process_code: str
    direct_co2_t: float
    detail: Dict[str, Any]

def cement_clinker_process(clinker_t: float, ef_tco2_per_t_clinker: float) -> ProcessEmissionResult:
    co2 = float(clinker_t) * float(ef_tco2_per_t_clinker)
    return ProcessEmissionResult("cement_clinker", co2, {"clinker_t": clinker_t, "ef": ef_tco2_per_t_clinker})

def steel_reduction_process(hot_metal_t: float, ef_tco2_per_t_hm: float) -> ProcessEmissionResult:
    co2 = float(hot_metal_t) * float(ef_tco2_per_t_hm)
    return ProcessEmissionResult("steel_reduction", co2, {"hot_metal_t": hot_metal_t, "ef": ef_tco2_per_t_hm})

def aluminium_electrolysis_process(aluminium_t: float, pfc_tco2e_per_t: float) -> ProcessEmissionResult:
    co2e = float(aluminium_t) * float(pfc_tco2e_per_t)
    return ProcessEmissionResult("aluminium_electrolysis", co2e, {"aluminium_t": aluminium_t, "pfc_factor": pfc_tco2e_per_t})
