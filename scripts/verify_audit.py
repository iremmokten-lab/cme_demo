from __future__ import annotations

import json
import sys
import zipfile
from hashlib import sha256
import hmac

from src.mrv.replay import replay
from src import config as app_config


def sha256_bytes(b: bytes) -> str:
    return sha256(b).hexdigest()


def verify_manifest_and_signature(zip_path: str) -> dict:
    with zipfile.ZipFile(zip_path, "r") as z:
        manifest = json.loads(z.read("manifest.json").decode("utf-8"))
        signature = json.loads(z.read("signature.json").decode("utf-8"))

        errors = []
        for f in manifest.get("files", []):
            path = f["path"]
            expected = f["sha256"]
            data = z.read(path)
            actual = sha256_bytes(data)
            if actual != expected:
                errors.append({"file": path, "expected": expected, "actual": actual})

        key = app_config.get_evidence_pack_hmac_key()
        sig_ok = None
        if key:
            m2 = dict(manifest)
            m2.pop("signature", None)
            msg = json.dumps(m2, ensure_ascii=False, sort_keys=True, default=str, indent=2).encode("utf-8")
            h = hmac.new(key.encode("utf-8"), msg, digestmod="sha256").hexdigest()
            sig_ok = (h == signature.get("signature"))

        return {"hash_errors": errors, "signature_ok": sig_ok}


def main():
    if len(sys.argv) < 2:
        print("Kullanım: python scripts/verify_audit.py <snapshot_id> [evidence_pack_zip_path]")
        sys.exit(2)

    snapshot_id = int(sys.argv[1])
    rep = replay(snapshot_id)
    ok = rep.get("input_hash_match") and rep.get("result_hash_match")

    print("REPLAY:", "PASS" if ok else "FAIL")
    print(json.dumps(rep, ensure_ascii=False, indent=2))

    if len(sys.argv) >= 3:
        v = verify_manifest_and_signature(sys.argv[2])
        print("EVIDENCE PACK VERIFY:")
        print(json.dumps(v, ensure_ascii=False, indent=2))

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
