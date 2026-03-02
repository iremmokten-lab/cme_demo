from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class AbatementOption:
    code: str
    title: str
    typical_reduction_pct: float
    capex_try: float
    opex_delta_try: float
    notes: str

def default_library() -> List[AbatementOption]:
    return [
        AbatementOption("whr","Atık Isı Geri Kazanım", 5.0, 50_000_000, -2_000_000, "Çimento/çelikte yaygın."),
        AbatementOption("fuel_switch","Yakıt Değişimi", 10.0, 10_000_000, 1_000_000, "Kömür->biyokütle/doğalgaz."),
        AbatementOption("efficiency","Enerji Verimliliği", 7.0, 5_000_000, -1_000_000, "Motor, fırın, izolasyon."),
        AbatementOption("electrification","Elektrikleşme", 8.0, 20_000_000, 2_000_000, "Elektrik kaynaklı faktöre bağlı."),
        AbatementOption("ccs","CCS (Karbon Yakalama)", 25.0, 200_000_000, 10_000_000, "Yüksek CAPEX, yüksek azaltım."),
    ]
