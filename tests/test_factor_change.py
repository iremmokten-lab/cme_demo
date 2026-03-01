from src.mrv.lineage import sha256_json


def test_factor_change_changes_factor_hash():
    f1 = {"factor_type": "natural_gas", "region": "TR", "year": 2025, "version": "v1", "value": 2.0}
    f2 = {"factor_type": "natural_gas", "region": "TR", "year": 2025, "version": "v1", "value": 2.1}
    assert sha256_json(f1) != sha256_json(f2)
