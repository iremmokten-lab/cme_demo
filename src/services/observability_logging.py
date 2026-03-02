from __future__ import annotations
import json, sys, time
from typing import Any, Dict

def log_event(event:str, **fields:Any)->None:
    payload={"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "event": event}
    payload.update(fields)
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()
