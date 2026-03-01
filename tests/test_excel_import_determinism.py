from src.connectors.excel_connector import compute_hash
import pandas as pd


def test_hash_determinism():

    data = [
        {"a": 1, "b": 2},
        {"a": 3, "b": 4}
    ]

    df1 = pd.DataFrame(data)
    df2 = pd.DataFrame(data)

    h1 = compute_hash(df1)
    h2 = compute_hash(df2)

    assert h1 == h2
