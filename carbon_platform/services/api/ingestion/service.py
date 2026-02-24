import pandas as pd
from sqlalchemy.orm import Session


def parse_excel(file_path: str):

    df = pd.read_excel(file_path)

    rows = df.to_dict(orient="records")

    return rows


def parse_csv(file_path: str):

    df = pd.read_csv(file_path)

    rows = df.to_dict(orient="records")

    return rows
