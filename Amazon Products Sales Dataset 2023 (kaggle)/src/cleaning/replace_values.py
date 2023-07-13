import pandas as pd
from typing import Union
from .change_type_column import change_type_column


def str_in_numeric_columns(df: pd.DataFrame, columns: Union[str, list], re_exp='[a-zA-Z]') -> pd.DataFrame:
    '''
    :param df: DataFrame a ser maniuplado.
    :param column: Nome de coluna a ser manipulada.
    :param re_exp: Padrão a ser usado para eliminar linhas específicas que atendam a expressão regular.
    :return: pandas.DataFrame
    '''
    if isinstance(columns, str):
        columns = [columns]

    # mask = (df[columns].str.match(re_exp) == True)
    mask = df[columns].apply(lambda x: x.str.contains(re_exp, case=False, regex=True)).any(axis=1)
    return df.loc[~mask]


def replace_values(df: pd.DataFrame, column: str, regex: bool, transform_col: bool=False, type: str=None, old_values=None, new_values=None) -> pd.DataFrame:
    '''
    :param df: DataFrame a ser manipulado.
    :param column: Nome de coluna a ser manipulada.
    :param regex: Padrão de expressão regular.
    :param old_value: Valor a ser substituído.
    :param new_value: Valor para substituir.
    :return: pandas.DataFrame
    '''

    if old_values:
        df[column].replace(to_replace=old_values, value=new_values, regex=regex, inplace=True)
        if transform_col and type:
            df = change_type_column(df=df, column=column, type=type)
        return df
    else:
        return df
