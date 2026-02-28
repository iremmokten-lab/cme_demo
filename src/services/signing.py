from __future__ import annotations

import base64
import hmac
import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Dict, Optional

from src.config import (
    get_evidence_pack_ed25519_private_key_b64,
    get_evidence_pack_ed25519_public_key_b64,
    get_evidence_pack_hmac_key,
)


@dataclass
class EvidenceSignature:
    algorithm: str
    signature_b64: str
    public_key_b64: str | None = None
    key_id: str | None = None

    def to_dict(self) -> Dict[str, Any]:
        d = {"algorithm": self.algorithm, "signature_b64": self.signature_b64}
        if self.public_key_b64:
            d["public_key_b64"] = self.public_key_b64
        if self.key_id:
            d["key_id"] = self.key_id
        return d


def _canonical_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_hmac_sha256(payload_obj: Any) -> Optional[EvidenceSignature]:
    key = get_evidence_pack_hmac_key()
    if not key:
        return None
    mac = hmac.new(key.encode("utf-8"), _canonical_json_bytes(payload_obj), sha256).digest()
    return EvidenceSignature(algorithm="HMAC-SHA256", signature_b64=base64.b64encode(mac).decode("utf-8"))


def sign_ed25519(payload_obj: Any) -> Optional[EvidenceSignature]:
    priv_b64 = get_evidence_pack_ed25519_private_key_b64()
    if not priv_b64:
        return None

    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    except Exception as e:
        # cryptography missing
        raise RuntimeError("cryptography paketi gerekli (Ed25519 imzası için).") from e

    priv_raw = base64.b64decode(priv_b64.encode("utf-8"))
    if len(priv_raw) != 32:
        raise ValueError("EVIDENCE_PACK_ED25519_PRIVATE_KEY_B64 32-byte raw private key olmalı (base64).")

    priv = Ed25519PrivateKey.from_private_bytes(priv_raw)
    sig = priv.sign(_canonical_json_bytes(payload_obj))

    pub_b64 = get_evidence_pack_ed25519_public_key_b64()
    if not pub_b64:
        try:
            pub_b64 = base64.b64encode(priv.public_key().public_bytes_raw()).decode("utf-8")
        except Exception:
            pub_b64 = ""

    return EvidenceSignature(
        algorithm="Ed25519",
        signature_b64=base64.b64encode(sig).decode("utf-8"),
        public_key_b64=pub_b64 or None,
    )


def build_signature_block(manifest_obj: Any) -> Dict[str, Any]:
    signatures = []
    h = sign_hmac_sha256(manifest_obj)
    if h:
        signatures.append(h.to_dict())

    e = sign_ed25519(manifest_obj)
    if e:
        signatures.append(e.to_dict())

    return {"signatures": signatures, "signed_payload_hash_sha256": sha256(_canonical_json_bytes(manifest_obj)).hexdigest()}
