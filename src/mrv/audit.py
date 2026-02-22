from datetime import datetime, timezone
import json
from pathlib import Path

AUDIT_DIR = Path("./audit_logs")
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

def append_audit(event_type: str, payload: dict):
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "payload": payload,
    }
    fp = AUDIT_DIR / "audit.jsonl"
    with fp.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
