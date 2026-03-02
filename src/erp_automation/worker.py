from __future__ import annotations
import json, time, traceback
from typing import Callable, Dict

from src.erp_automation.job_queue import claim_next, finish

_HANDLERS: Dict[str, Callable[[dict], dict]] = {}

def register(kind: str, fn: Callable[[dict], dict]) -> None:
    _HANDLERS[str(kind)] = fn

def run_once() -> bool:
    j = claim_next()
    if not j:
        return False
    try:
        payload = json.loads(j.payload_json or "{}")
    except Exception:
        payload = {}
    try:
        if j.kind not in _HANDLERS:
            raise ValueError(f"Handler yok: {j.kind}")
        res = _HANDLERS[j.kind](payload)
        finish(j.id, True, result=res)
    except Exception as e:
        finish(j.id, False, result={}, error=str(e) + "\n" + traceback.format_exc()[:4000])
    return True

def run_loop(poll_seconds: float = 2.0, max_loops: int = 1000):
    loops = 0
    while loops < max_loops:
        did = run_once()
        if not did:
            time.sleep(poll_seconds)
        loops += 1
