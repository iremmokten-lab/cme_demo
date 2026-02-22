from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class StorageLocation:
    uri: str
    backend: str  # local / s3


class StorageBackend:
    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> StorageLocation:
        raise NotImplementedError

    def get_bytes(self, uri: str) -> bytes:
        raise NotImplementedError


class LocalStorageBackend(StorageBackend):
    def __init__(self, base_dir: str = "./storage/blob"):
        self.base = Path(base_dir)
        self.base.mkdir(parents=True, exist_ok=True)

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> StorageLocation:
        # key: "company_1/project_2/file.pdf"
        p = self.base / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return StorageLocation(uri=str(p), backend="local")

    def get_bytes(self, uri: str) -> bytes:
        try:
            p = Path(str(uri))
            if p.exists():
                return p.read_bytes()
        except Exception:
            pass
        return b""


class S3StorageBackend(StorageBackend):
    def __init__(self, bucket: str, prefix: str = "cme"):
        self.bucket = bucket
        self.prefix = prefix.strip("/")

        try:
            import boto3  # type: ignore
        except Exception as e:
            raise RuntimeError("boto3 bulunamadı. S3 backend için boto3 gerekli.") from e

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
        )

    def _key(self, key: str) -> str:
        key = key.lstrip("/")
        if self.prefix:
            return f"{self.prefix}/{key}"
        return key

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> StorageLocation:
        k = self._key(key)
        self.s3.put_object(Bucket=self.bucket, Key=k, Body=data, ContentType=content_type)
        return StorageLocation(uri=f"s3://{self.bucket}/{k}", backend="s3")

    def get_bytes(self, uri: str) -> bytes:
        # uri: s3://bucket/key
        if not uri.startswith("s3://"):
            return b""
        rest = uri[len("s3://") :]
        bucket, _, key = rest.partition("/")
        try:
            obj = self.s3.get_object(Bucket=bucket, Key=key)
            return obj["Body"].read()
        except Exception:
            return b""


def get_storage_backend() -> StorageBackend:
    """Paket C altyapı adaptörü.
    Env:
      - STORAGE_BACKEND=local|s3
      - S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
    """
    backend = (os.getenv("STORAGE_BACKEND") or "local").strip().lower()
    if backend == "s3":
        bucket = os.getenv("S3_BUCKET") or ""
        prefix = os.getenv("S3_PREFIX") or "cme"
        if bucket:
            try:
                return S3StorageBackend(bucket=bucket, prefix=prefix)
            except Exception:
                # S3 yoksa local fallback
                return LocalStorageBackend()
        return LocalStorageBackend()
    return LocalStorageBackend()
