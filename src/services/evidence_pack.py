from __future__ import annotations

from typing import Any, Dict, Tuple

from src.services.exports import export_evidence_pack  # exports.py içindeki fonksiyon
from src.mrv.replay import replay
from src.mrv.snapshot_store import snapshot_payload


def build_evidence_pack(
    *,
    project_id: int,
    snapshot_id: int,
) -> Tuple[bytes, Dict[str, Any]]:
    """
    Tek giriş noktası:
      - evidence pack zip bytes
      - manifest dict (hash zinciri)
    """
    zip_bytes, manifest = export_evidence_pack(project_id=int(project_id), snapshot_id=int(snapshot_id))
    return zip_bytes, manifest


def audit_replay_check(snapshot_id: int) -> Dict[str, Any]:
    """
    Denetçi için “replay sonucu”: input_hash/result_hash match.
    """
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
