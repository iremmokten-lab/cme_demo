import pandas as pd

def read_csv_uploaded(uploaded_file) -> pd.DataFrame:
    return pd.read_csv(uploaded_file)
