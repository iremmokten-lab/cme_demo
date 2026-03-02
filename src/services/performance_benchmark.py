from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Callable


@dataclass
class BenchmarkCase:
    name: str
    fn: Callable[[], Any]


def run_benchmarks(cases: List[BenchmarkCase]) -> Dict[str, Any]:
    results = []
    started = time.time()
    for c in cases:
        t0 = time.time()
        ok = True
        err = ""
        try:
            _ = c.fn()
        except Exception as e:
            ok = False
            err = str(e)
        dt = time.time() - t0
        results.append({"name": c.name, "ok": ok, "seconds": round(dt, 6), "error": err})
    total = time.time() - started
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_seconds": round(total, 6),
        "cases": results,
    }
