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


def ensure_directory(path: Path):
    path.mkdir(parents=True, exist_ok=True)
    return path


def storage_path_for_project(project_id: int) -> str:
    """Proje bazlı storage klasörü döndürür ve yoksa oluşturur."""
    p = Path("./storage") / f"project_{int(project_id)}"
    ensure_directory(p)
    return str(p)
