
import yaml
from pathlib import Path

def load_mrr_mapping(path="spec/ets_mrr_2018_2066_mapping.yaml"):
    p = Path(path)
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text())
