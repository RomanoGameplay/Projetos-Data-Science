import os
import time
import numpy as np
from .utils.load_data import load, to_csv, NEW_DF_FILE_NAME
from .cleaning import *
from parallel_pandas import ParallelPandas


def main() -> None:
    '''
    Função principal da qual realiza todas as operações necessárias para a limpeza de dados.
    :return: None
    '''
    ParallelPandas.initialize(n_cpu=os.cpu_count(), split_factor=4, disable_pr_bar=True)
    start = time.time()
    df = load()
    print(df.shape[0])

    if not os.path.isfile(NEW_DF_FILE_NAME):
        drop_na_in_df(df=df)
        fillna_in_df(df=df)
        df = change_in_df(df=df)
        df = replace_in_df(df=df)
        df = to_csv(df=df, get_df=True)
    else:
        pass

    print(df)
    final = time.time()
    print(f'{round((final - start), 2)} segundos')


def drop_na_in_df(df: pd.DataFrame) -> None:
    '''
    Agrupa as operações centradas em eliminar valores nulos dentro do DataFrame.
    :param df: Dataframe a ser manipulado.
    :return: None
    '''

    drop_na(df=df, thresh=4)
    df.dropna(subset='actual_price', inplace=True)


def fillna_in_df(df: pd.DataFrame) -> None:
    '''
    Agrupa as operações centradas em preencher valores nulos dentro do DataFrame.
    :param df: Dataframe a ser manipulado.
    :return: None
    '''

    fill_na(df=df, columns=['ratings', 'no_of_ratings'], value=0)


def change_in_df(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Agrupa operações centradas em realizar mudanças dentro de colunas no DataFrame.
    :param df: Dataframe a ser manipulado.
    :return: pandas.DataFrame
    '''

    df = str_in_numeric_columns(df=df, columns=['no_of_ratings', 'no_of_ratings'], re_exp='[a-zA-Z]')
    return df


def replace_in_df(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Agrupa operações centradas em realizar trocas de valores dentro do dataframe.
    :param df: Dataframe a ser manipulado.
    :return: pandas.DataFrame
    '''

    df = change_type_column(df=df, column='ratings', type='float')

    list_special_characters = detect_special_characters(df, 'no_of_ratings')
    df = replace_values(df=df, column='no_of_ratings', regex=True,
                        old_values=list_special_characters, new_values='',
                        transform_col=True, type='float')

    list_special_characters = detect_special_characters(df, 'discount_price')
    replace_values(df=df, column='discount_price', regex=True,
                   old_values=list_special_characters, new_values='')
    df = replace_values(df=df, column='discount_price', regex=True, old_values=np.nan, new_values=0, transform_col=True,
                        type='float')

    list_special_characters = detect_special_characters(df, 'actual_price')
    df = replace_values(df=df, column='actual_price', regex=True,
                        old_values=list_special_characters, new_values='',
                        transform_col=True, type='float')
    df = df[df['actual_price'] != 0]

    return df
