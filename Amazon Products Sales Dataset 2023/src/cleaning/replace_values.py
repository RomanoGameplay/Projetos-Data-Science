import pandas as pd


def str_in_numeric_columns(df: pd.DataFrame, column: str, re_exp='[a-zA-Z]') -> pd.DataFrame:
    mask = (df[column].str.match(re_exp) == True)
    return df.loc[~mask]


def replace_values(df: pd.DataFrame, column: str, regex: bool, old_value=None, new_value=None) -> None:
    if old_value:
        df[column].replace(to_replace=old_value, value=new_value, regex=regex, inplace=True)
