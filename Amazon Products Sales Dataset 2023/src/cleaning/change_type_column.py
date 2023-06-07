import pandas as pd


def change_type_column(df: pd.DataFrame, column: str, type: str) -> pd.DataFrame:
    '''
    :param df: DataFrame a ser manipulado.
    :param column: nome de coluna específica a ser manipulado.
    :param type: Transformar para [string, float, int]
    :return: pandas.DataFrame
    '''
    if type == 'str':
        df[column] = df[column].astype(str)
    elif type == 'int':
        df[column] = df[column].astype(int)
    elif type == 'float':
        df[column] = df[column].astype(float)
    return df
