
import json
from datetime import datetime

def generate_ets_report(snapshot, output_path):
    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "engine_version": snapshot.get("engine_version"),
        "facility": snapshot.get("facility"),
        "source_streams": snapshot.get("source_streams"),
        "activity_data": snapshot.get("activity_data"),
        "emission_factors": snapshot.get("emission_factors"),
        "tier_logic": snapshot.get("tier_logic"),
        "uncertainty": snapshot.get("uncertainty"),
        "qa_qc": snapshot.get("qa_qc"),
        "annual_emissions": snapshot.get("annual_emissions"),
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    return output_path
