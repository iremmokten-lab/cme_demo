from pathlib import Path

UPLOAD_DIR = Path("./storage/uploads")
REPORT_DIR = Path("./storage/reports")
EXPORT_DIR = Path("./storage/exports")

for p in (UPLOAD_DIR, REPORT_DIR, EXPORT_DIR):
    p.mkdir(parents=True, exist_ok=True)

def write_bytes(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
