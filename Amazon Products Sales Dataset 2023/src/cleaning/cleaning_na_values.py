import pandas as pd


def drop_na(df: pd.DataFrame, thresh: int) -> None:
    if thresh != 1:
        df.dropna(thresh=(10 - thresh), inplace=True)
    else:
        df.dropna(how='any', inplace=True)


def fill_na(df: pd.DataFrame, column: str, value=0) -> None:
    df[column].fillna(value, inplace=True)
