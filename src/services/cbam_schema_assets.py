from __future__ import annotations

import hashlib
import zipfile
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import urllib.request


# Resmi kaynak (EC TAXUD) — CBAM Registry and Reporting sayfasındaki "CBAM Quarterly Report structure XSD and stypes.xsd"
DEFAULT_CBAM_XSD_ZIP_URL = (
    "https://taxation-customs.ec.europa.eu/document/download/"
    "90f33757-731c-4383-acb8-7950dab4438d_en?filename=CBAM+XSD+Version+23.00.zip"
)


@dataclass(frozen=True)
class CBAMSchemaAsset:
    version: str
    xsd_dir: Path
    qreport_xsd: Path
    stypes_xsd: Path
    zip_sha256: str


def _sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def ensure_cbam_xsd_assets(
    *,
    base_dir: str = "./spec/cbam_xsd",
    version: str = "23.00",
    url: str = DEFAULT_CBAM_XSD_ZIP_URL,
    timeout_sec: int = 30,
) -> CBAMSchemaAsset:
    """Resmi CBAM XSD paketini indirir ve cache'ler.

    - Streamlit Cloud uyumlu.
    - Offline ortamda indirilemezse mevcut cache kullanılır.
    - Audit için ZIP SHA256 hash'i döner.
    """
    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)

    ver_dir = base / f"ver_{version}"
    ver_dir.mkdir(parents=True, exist_ok=True)

    qreport = ver_dir / f"QReport_ver{version}.xsd"
    stypes = ver_dir / f"Stypes_ver{version}.xsd"
    zip_hash_file = ver_dir / "xsd_zip.sha256"

    # already cached
    if qreport.exists() and stypes.exists() and zip_hash_file.exists():
        return CBAMSchemaAsset(
            version=version,
            xsd_dir=ver_dir,
            qreport_xsd=qreport,
            stypes_xsd=stypes,
            zip_sha256=zip_hash_file.read_text(encoding="utf-8").strip(),
        )

    # download
    with urllib.request.urlopen(url, timeout=timeout_sec) as resp:
        content = resp.read() or b""
    zhash = _sha256_bytes(content)

    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        # extract only XSDs we expect; fall back to any .xsd
        members = zf.namelist()
        qname = None
        sname = None
        for n in members:
            low = n.lower()
            if low.endswith(".xsd") and "qreport" in low and f"ver{version}".replace(".", "")[:2] in low:
                qname = n
            if low.endswith(".xsd") and "stypes" in low and f"ver{version}".replace(".", "")[:2] in low:
                sname = n
        # permissive matching (exact filenames are common)
        if qname is None:
            for n in members:
                if n.lower().endswith(f"qreport_ver{version}.xsd".lower()):
                    qname = n
                    break
        if sname is None:
            for n in members:
                if n.lower().endswith(f"stypes_ver{version}.xsd".lower()):
                    sname = n
                    break

        # fallback: pick first two xsd files
        xsds = [n for n in members if n.lower().endswith(".xsd")]
        if qname is None and xsds:
            qname = xsds[0]
        if sname is None and len(xsds) > 1:
            sname = xsds[1]

        if qname is None or sname is None:
            raise ValueError("CBAM XSD paketi içinde beklenen XSD dosyaları bulunamadı.")

        qreport.write_bytes(zf.read(qname))
        stypes.write_bytes(zf.read(sname))

    zip_hash_file.write_text(zhash, encoding="utf-8")

    return CBAMSchemaAsset(
        version=version,
        xsd_dir=ver_dir,
        qreport_xsd=qreport,
        stypes_xsd=stypes,
        zip_sha256=zhash,
    )
