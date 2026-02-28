from __future__ import annotations

"""Phase 3 — Benchmark & Outlier Detection

Amaç:
- Facility / product intensity benchmark (tCO2 / output)
- Basit outlier tespiti (z-score + IQR)

Notlar:
- Harici veri kaynağı yok (demo). Benchmarks deterministik fallback tablolar.
- Sonuçlar snapshot.results_json["ai"]["benchmark"] altında saklanır.
"""

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


# Demo benchmark catalog (tCO2 per ton product)
# Değerler: tamamen DEMO / placeholder. Faz 3'te dış veri ile güncellenecek.
_BENCHMARK_TCO2_PER_TON: Dict[str, Dict[str, float]] = {
    # sector -> cbam_good_key -> intensity
    "iron_steel": {
        "iron_steel": 1.9,
        "other": 2.1,
    },
    "aluminium": {
        "aluminium": 8.0,
        "other": 7.5,
    },
    "cement": {
        "cement": 0.75,
        "other": 0.8,
    },
    "fertilizers": {
        "fertilizers": 2.6,
        "other": 2.2,
    },
    "chemicals": {
        "chemicals": 1.2,
        "other": 1.0,
    },
    "default": {
        "other": 1.5,
    },
}


def _pick_benchmark(sector: str, good_key: str) -> Tuple[float, Dict[str, Any]]:
    sec = _norm(sector) or "default"
    gk = _norm(good_key) or "other"

    if sec in _BENCHMARK_TCO2_PER_TON:
        m = _BENCHMARK_TCO2_PER_TON[sec]
        if gk in m:
            return float(m[gk]), {"source": "demo_catalog", "sector": sec, "good_key": gk}
        if "other" in m:
            return float(m["other"]), {"source": "demo_catalog", "sector": sec, "good_key": "other"}

    m = _BENCHMARK_TCO2_PER_TON["default"]
    return float(m["other"]), {"source": "demo_catalog", "sector": "default", "good_key": "other"}


def _zscore(values: List[float]) -> List[float]:
    if not values:
        return []
    mu = sum(values) / len(values)
    var = sum((v - mu) ** 2 for v in values) / len(values)
    sd = math.sqrt(var)
    if sd <= 1e-12:
        return [0.0 for _ in values]
    return [(v - mu) / sd for v in values]


def _iqr_bounds(values: List[float]) -> Tuple[float, float]:
    if not values:
        return (float("-inf"), float("inf"))
    xs = sorted(values)
    n = len(xs)

    def q(p: float) -> float:
        if n == 1:
            return xs[0]
        pos = p * (n - 1)
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return xs[lo]
        w = pos - lo
        return xs[lo] * (1 - w) + xs[hi] * w

    q1 = q(0.25)
    q3 = q(0.75)
    iqr = q3 - q1
    if iqr <= 1e-12:
        return (float("-inf"), float("inf"))
    return (q1 - 3.0 * iqr, q3 + 3.0 * iqr)


@dataclass
class OutlierFlag:
    id: str
    severity: str
    message: str
    meta: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "severity": self.severity,
            "message": self.message,
            "meta": self.meta,
        }


def build_benchmark_report(
    *,
    facility: Dict[str, Any],
    kpis: Dict[str, Any],
    cbam: Dict[str, Any],
) -> Dict[str, Any]:
    """Snapshot sonuçlarından benchmark raporu üretir."""

    sector = str((facility or {}).get("sector", "") or "")
    total_tco2 = _to_float((kpis or {}).get("total_tco2", 0.0), 0.0)

    # Facility intensity: total_tco2 / total production (ton)
    prod_total_ton = 0.0
    try:
        # cbam.product_lines: list of sku lines with quantity_kg or quantity_unit
        lines = (cbam or {}).get("product_lines") or []
        if isinstance(lines, list):
            for ln in lines:
                if not isinstance(ln, dict):
                    continue
                q_ton = _to_float(ln.get("quantity_ton"), 0.0)
                if q_ton > 0:
                    prod_total_ton += q_ton
                else:
                    # fallback: kg
                    q_kg = _to_float(ln.get("quantity_kg"), 0.0)
                    if q_kg > 0:
                        prod_total_ton += q_kg / 1000.0
    except Exception:
        prod_total_ton = 0.0

    facility_intensity = (total_tco2 / prod_total_ton) if prod_total_ton > 0 else None

    # Product intensity benchmark
    product_rows: List[Dict[str, Any]] = []
    outliers: List[OutlierFlag] = []

    lines = (cbam or {}).get("product_lines") or []
    intensities: List[float] = []
    keys: List[str] = []

    if isinstance(lines, list):
        for ln in lines:
            if not isinstance(ln, dict):
                continue
            sku = str(ln.get("sku", "") or "")
            good_key = str(ln.get("cbam_good_key", ln.get("cbam_good", "other")) or "other")
            embedded_tco2 = _to_float(ln.get("embedded_tco2", 0.0), 0.0)
            q_ton = _to_float(ln.get("quantity_ton"), 0.0)
            if q_ton <= 0:
                q_kg = _to_float(ln.get("quantity_kg", 0.0), 0.0)
                q_ton = q_kg / 1000.0 if q_kg > 0 else 0.0
            intensity = (embedded_tco2 / q_ton) if q_ton > 0 else None

            bench, bench_meta = _pick_benchmark(sector, good_key)
            ratio = (float(intensity) / bench) if (intensity is not None and bench > 0) else None

            product_rows.append(
                {
                    "sku": sku,
                    "cbam_good_key": _norm(good_key) or "other",
                    "quantity_ton": q_ton,
                    "embedded_tco2": embedded_tco2,
                    "intensity_tco2_per_ton": intensity,
                    "benchmark_tco2_per_ton": bench,
                    "benchmark_meta": bench_meta,
                    "ratio_to_benchmark": ratio,
                }
            )

            if intensity is not None and q_ton > 0:
                intensities.append(float(intensity))
                keys.append(sku or f"line_{len(keys)+1}")

    # Outlier detection across product intensities
    if intensities:
        z = _zscore(intensities)
        lo, hi = _iqr_bounds(intensities)

        for i, val in enumerate(intensities):
            sku = keys[i]
            zi = z[i]
            if abs(zi) >= 2.8:
                outliers.append(
                    OutlierFlag(
                        id=f"outlier:zscore:{sku}",
                        severity="warn" if abs(zi) < 4.0 else "critical",
                        message=f"Ürün yoğunluğu (tCO2/t) ortalamadan sapıyor (z={zi:.2f}).",
                        meta={"sku": sku, "intensity": val, "zscore": zi},
                    )
                )
            if val < lo or val > hi:
                outliers.append(
                    OutlierFlag(
                        id=f"outlier:iqr:{sku}",
                        severity="warn",
                        message="Ürün yoğunluğu IQR sınırları dışında (potansiyel anomali).",
                        meta={"sku": sku, "intensity": val, "iqr_low": lo, "iqr_high": hi},
                    )
                )

    # Facility benchmark compare (aggregate)
    facility_bench, facility_bench_meta = _pick_benchmark(sector, "other")
    facility_ratio = (float(facility_intensity) / facility_bench) if (facility_intensity is not None and facility_bench > 0) else None

    if facility_ratio is not None and facility_ratio >= 1.6:
        outliers.append(
            OutlierFlag(
                id="facility_intensity_high",
                severity="warn" if facility_ratio < 2.5 else "critical",
                message="Tesis yoğunluğu benchmark'a göre yüksek.",
                meta={"facility_intensity": facility_intensity, "benchmark": facility_bench, "ratio": facility_ratio, "meta": facility_bench_meta},
            )
        )

    out = {
        "facility": {
            "sector": sector,
            "total_tco2": total_tco2,
            "production_total_ton": prod_total_ton,
            "intensity_tco2_per_ton": facility_intensity,
            "benchmark_tco2_per_ton": facility_bench,
            "benchmark_meta": facility_bench_meta,
            "ratio_to_benchmark": facility_ratio,
        },
        "products": product_rows,
        "outliers": [o.to_dict() for o in outliers],
    }
    return out
