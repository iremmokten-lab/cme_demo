from src.mrv.lineage import canonical_json, sha256_json


def test_hash_consistency():
    a = {"b": 1, "a": 2.0, "c": [3.141592653589793, {"z": 0.1 + 0.2}]}
    b = {"c": [{"z": 0.3}, 3.141592653589793], "a": 2.0, "b": 1}
    # ordering differs but normalization should make deterministic JSON stable when lists are intentionally equal-order.
    # Here list order differs, so hashes differ (expected).
    assert sha256_json(a) != sha256_json(b)

    x = {"a": 0.1 + 0.2}
    y = {"a": 0.3}
    assert sha256_json(x) == sha256_json(y)
