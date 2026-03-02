from __future__ import annotations

import hashlib
import os
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

DEFAULT_CACHE_DIR = os.getenv("CME_CBAM_XSD_CACHE", "/tmp/cbam_xsd_cache")


@dataclass(frozen=True)
class SchemaRef:
    version_label: str
    zip_path: str
    xsd_root_dir: str
    sha256: str


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_official_cbam_xsd_zip(url: str, version_label: str) -> SchemaRef:
    Path(DEFAULT_CACHE_DIR).mkdir(parents=True, exist_ok=True)
    zip_path = str(Path(DEFAULT_CACHE_DIR) / f"cbam_xsd_{version_label}.zip")

    if not os.path.exists(zip_path):
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(r.content)

    sha = _sha256_file(zip_path)
    extract_dir = str(Path(DEFAULT_CACHE_DIR) / f"cbam_xsd_{version_label}")
    if not os.path.exists(extract_dir):
        Path(extract_dir).mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(extract_dir)

    return SchemaRef(version_label=version_label, zip_path=zip_path, xsd_root_dir=extract_dir, sha256=sha)
