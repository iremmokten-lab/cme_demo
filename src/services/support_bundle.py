from __future__ import annotations
import io, json, zipfile, time
from typing import Dict, Any, List

from src.services.snapshots import list_snapshots_for_project
from src.services.evidence_pack import build_evidence_pack
from src.services.compliance_reports import build_compliance_report_json

def build_support_bundle(project_id:int)->bytes:
    buf=io.BytesIO()
    z=zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED)

    # Minimal: compliance report + evidence pack manifest + snapshot list
    comp = build_compliance_report_json(project_id=int(project_id))
    z.writestr("support/compliance_report.json", json.dumps(comp, ensure_ascii=False, indent=2))

    snaps = list_snapshots_for_project(int(project_id))
    z.writestr("support/snapshots.json", json.dumps([{"id":s.id,"created_at":str(s.created_at),"result_hash":s.result_hash} for s in snaps], ensure_ascii=False, indent=2))

    # evidence pack (manifest + signature) - build_evidence_pack returns dict with paths sometimes; here we just include JSON representation
    try:
        ep = build_evidence_pack(project_id=int(project_id))
        z.writestr("support/evidence_pack.json", json.dumps(ep, ensure_ascii=False, indent=2))
    except Exception as e:
        z.writestr("support/evidence_pack_error.txt", str(e))

    z.writestr("support/generated_at.txt", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    z.close()
    return buf.getvalue()
