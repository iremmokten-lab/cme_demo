import os
import hashlib
from dataclasses import dataclass
from typing import BinaryIO, Optional
import aiofiles

from services.api.core.config import settings

try:
    import boto3
    from botocore.client import Config as BotoConfig
except Exception:
    boto3 = None
    BotoConfig = None


@dataclass
class StoredObject:
    key: str
    sha256: str
    size: int


class StorageBase:
    async def put_bytes(self, key: str, data: bytes) -> StoredObject:
        raise NotImplementedError

    async def put_fileobj(self, key: str, fileobj: BinaryIO) -> StoredObject:
        raise NotImplementedError

    async def get_bytes(self, key: str) -> bytes:
        raise NotImplementedError

    def presign_get(self, key: str, expires_seconds: int | None = None) -> str:
        raise NotImplementedError


class LocalStorage(StorageBase):
    def __init__(self, root: str | None = None):
        self.root = os.path.abspath(root or settings.LOCAL_S3_ROOT)
        os.makedirs(self.root, exist_ok=True)

    def _full_path(self, key: str) -> str:
        key = key.lstrip("/").replace("..", "_")
        return os.path.join(self.root, key)

    async def put_bytes(self, key: str, data: bytes) -> StoredObject:
        path = self._full_path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        sha = hashlib.sha256(data).hexdigest()
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)
        return StoredObject(key=key, sha256=sha, size=len(data))

    async def put_fileobj(self, key: str, fileobj: BinaryIO) -> StoredObject:
        path = self._full_path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        h = hashlib.sha256()
        size = 0
        async with aiofiles.open(path, "wb") as f:
            while True:
                chunk = fileobj.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                h.update(chunk)
                await f.write(chunk)
        return StoredObject(key=key, sha256=h.hexdigest(), size=size)

    async def get_bytes(self, key: str) -> bytes:
        path = self._full_path(key)
        async with aiofiles.open(path, "rb") as f:
            return await f.read()

    def presign_get(self, key: str, expires_seconds: int | None = None) -> str:
        # Local modda presigned yerine dosya yolunu döndürür (Streamlit indirme için kullanılabilir)
        return self._full_path(key)


class S3Storage(StorageBase):
    def __init__(
        self,
        bucket: str,
        region: str,
        access_key: str,
        secret_key: str,
        prefix: str = "carbon-platform",
    ):
        if boto3 is None:
            raise RuntimeError("boto3 bulunamadı. requirements.txt içinde boto3 olmalı.")
        self.bucket = bucket
        self.prefix = (prefix or "").strip("/")

        self.client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=BotoConfig(signature_version="s3v4"),
        )

    def _k(self, key: str) -> str:
        key = key.lstrip("/").replace("..", "_")
        if self.prefix:
            return f"{self.prefix}/{key}"
        return key

    async def put_bytes(self, key: str, data: bytes) -> StoredObject:
        sha = hashlib.sha256(data).hexdigest()
        self.client.put_object(Bucket=self.bucket, Key=self._k(key), Body=data)
        return StoredObject(key=key, sha256=sha, size=len(data))

    async def put_fileobj(self, key: str, fileobj: BinaryIO) -> StoredObject:
        h = hashlib.sha256()
        chunks = []
        size = 0
        while True:
            chunk = fileobj.read(1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            h.update(chunk)
        data = b"".join(chunks)
        self.client.put_object(Bucket=self.bucket, Key=self._k(key), Body=data)
        return StoredObject(key=key, sha256=h.hexdigest(), size=size)

    async def get_bytes(self, key: str) -> bytes:
        obj = self.client.get_object(Bucket=self.bucket, Key=self._k(key))
        return obj["Body"].read()

    def presign_get(self, key: str, expires_seconds: int | None = None) -> str:
        exp = int(expires_seconds or settings.PRESIGN_EXPIRES_SECONDS)
        return self.client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": self.bucket, "Key": self._k(key)},
            ExpiresIn=exp,
        )


def build_storage() -> StorageBase:
    mode = (settings.STORAGE_MODE or "local").lower().strip()
    if mode == "s3":
        if not settings.AWS_S3_BUCKET or not settings.AWS_REGION or not settings.AWS_ACCESS_KEY_ID or not settings.AWS_SECRET_ACCESS_KEY:
            raise RuntimeError("STORAGE_MODE=s3 için AWS_S3_BUCKET/AWS_REGION/AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY zorunlu.")
        return S3Storage(
            bucket=settings.AWS_S3_BUCKET,
            region=settings.AWS_REGION,
            access_key=settings.AWS_ACCESS_KEY_ID,
            secret_key=settings.AWS_SECRET_ACCESS_KEY,
            prefix=settings.AWS_S3_PREFIX or "carbon-platform",
        )
    return LocalStorage(settings.LOCAL_S3_ROOT)

storage: StorageBase = build_storage()
