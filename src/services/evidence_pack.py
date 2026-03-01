
import json, hashlib, os
from datetime import datetime

def sha256_file(path):
    h = hashlib.sha256()
    with open(path,'rb') as f:
        h.update(f.read())
    return h.hexdigest()

def generate_evidence_pack(snapshot_id, files, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    manifest = {
        "snapshot_id": snapshot_id,
        "generated_at": datetime.utcnow().isoformat(),
        "files": []
    }

    for f in files:
        if os.path.exists(f):
            manifest["files"].append({
                "path": f,
                "sha256": sha256_file(f)
            })

    manifest_path = os.path.join(out_dir, "manifest.json")
    with open(manifest_path,"w",encoding="utf-8") as fp:
        json.dump(manifest, fp, indent=2)

    signature = {
        "manifest_hash": sha256_file(manifest_path)
    }

    sig_path = os.path.join(out_dir,"signature.json")
    with open(sig_path,"w",encoding="utf-8") as fp:
        json.dump(signature, fp, indent=2)

    return {
        "manifest": manifest_path,
        "signature": sig_path
    }
