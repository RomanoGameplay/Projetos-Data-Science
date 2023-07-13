import pandas as pd
from typing import Union


def drop_na(df: pd.DataFrame, thresh: int) -> None:
    '''
    :param df: DataFrame a ser manipulado.
    :param thresh: Número mínimo de nulos numa linha para que a mesma seja excluída.
    :return: None
    '''
    if thresh != 1:
        df.dropna(thresh=(10 - thresh), inplace=True)
    else:
        df.dropna(how='any', inplace=True)
