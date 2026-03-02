from __future__ import annotations

from pathlib import Path

UPLOAD_DIR = Path("./storage/uploads")
REPORT_DIR = Path("./storage/reports")
EXPORT_DIR = Path("./storage/exports")
EVIDENCE_DIR = Path("./storage/evidence_packs")

# Paket B: kurumsal evidence folders
EVIDENCE_DOCS_DIR = Path("./storage/evidence")
EVIDENCE_DOCS_CATEGORIES = ["documents", "meter_readings", "invoices", "contracts"]

for p in (UPLOAD_DIR, REPORT_DIR, EXPORT_DIR, EVIDENCE_DIR, EVIDENCE_DOCS_DIR):
    p.mkdir(parents=True, exist_ok=True)

for cat in EVIDENCE_DOCS_CATEGORIES:
    (EVIDENCE_DOCS_DIR / cat).mkdir(parents=True, exist_ok=True)


def write_bytes(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)

    # Many callers store the returned value as a URI.
    # Returning the string form keeps compatibility across local + Streamlit Cloud.
    return str(path)


def storage_path_for_project(project_id: int, relative_path: str) -> Path:
    """Return a stable, per-project storage location.

    This is used by CBAM portal simulators, ERP ingestion, and report exports.
    """
    pid = int(project_id)
    rel = str(relative_path or "").lstrip("/\\")
    base = Path("./storage/projects") / str(pid)
    return base / rel


def read_bytes(path_or_uri: str | Path) -> bytes:
    """Best-effort read for local file storage."""
    try:
        p = Path(path_or_uri)
        if p.exists():
            return p.read_bytes()
    except Exception:
        pass
    return b""
