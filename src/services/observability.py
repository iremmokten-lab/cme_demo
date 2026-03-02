from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger("carbon_platform")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)

def log_event(event: str, **fields: Any) -> None:
    payload: Dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    logger.info(json.dumps(payload, ensure_ascii=False, sort_keys=True))
