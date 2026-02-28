from __future__ import annotations

from typing import Any, Dict, Tuple

from src.services.exports import build_evidence_pack
from src.mrv.replay import replay
from src.mrv.snapshot_store import snapshot_payload


def export_evidence_pack_zip(project_id: int, snapshot_id: int) -> Tuple[bytes, Dict[str, Any]]:
    """
    UI/Service için tek giriş noktası:
      - ZIP bytes
      - manifest dict (ZIP içindeki manifest.json)
    """
    zip_bytes = build_evidence_pack(int(snapshot_id))

    # manifest dict'i ZIP içinden oku
    import zipfile, io, json
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as z:
        manifest = json.loads(z.read("manifest.json").decode("utf-8"))
    return zip_bytes, manifest


def audit_replay_check(snapshot_id: int) -> Dict[str, Any]:
    snap = snapshot_payload(int(snapshot_id))
    rep = replay(int(snapshot_id))
    return {
        "snapshot": {
            "snapshot_id": int(snapshot_id),
            "input_hash": snap.get("input_hash"),
            "result_hash": snap.get("result_hash"),
            "locked": snap.get("locked"),
        },
        "replay": rep,
        "status": "PASS" if rep.get("input_hash_match") and rep.get("result_hash_match") else "FAIL",
    }
