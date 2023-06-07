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


def fill_na(df: pd.DataFrame, columns: Union[str, list], value=0) -> None:
    '''
    :param df: DataFrame a ser manipulado
    :param columns: coluna específica a ser preenchida.
    :param value: Valor usado para preencher os valores nulos.
    :return: None
    '''
    if isinstance(columns, str):
        df[columns].fillna(value, inplace=True)
