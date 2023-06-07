import os
import pandas as pd
from .constant import DIR_CSV_NAME, DF_FILE_NAME, NEW_DF_FILE_NAME
from typing import Union


def load() -> pd.DataFrame:
    '''
    :return: pandas.DataFrame
    '''

    cur_dir = os.getcwd()

    if os.path.isfile(f'{cur_dir}/{DF_FILE_NAME}'):
        if os.path.isfile(f'{cur_dir}/{NEW_DF_FILE_NAME}'):
            df = load_new_df()
        else:
            df = load_from_file_csv(cur_dir=cur_dir)
    else:
        df = load_data_from_dir(cur_dir=cur_dir)
    return df


def load_data_from_dir(cur_dir: str) -> pd.DataFrame:
    '''
    :param cur_dir: Representa o caminho para o diretório atual.
    :return: pandas.DataFrame
    '''

    # Lista todos os arquivos csv.
    list_csv = os.listdir(DIR_CSV_NAME)

    # Usa list comprehension para ler os arquivos CSV e criar uma lista de dataframes
    dataframes = [pd.read_csv(f'{cur_dir}/{DIR_CSV_NAME}/{arquivo_csv}') for arquivo_csv in list_csv]

    # Concatena os dataframes em um único dataframe
    df = pd.concat(dataframes, ignore_index=True)
    df.drop(columns=['Unnamed: 0'], inplace=True, errors='ignore')
    df.to_csv('amazon_products_sales_dataset.csv', index=False)

    return df


def load_from_file_csv(cur_dir: str) -> pd.DataFrame:
    '''
    :param cur_dir: Representa o caminho para o diretório atual.
    :return: pandas.DataFrame
    '''

    # Cria um dataframe diretamente do arquivo salvo.
    df = pd.read_csv(f'{cur_dir}/{DF_FILE_NAME}')

    return df


def to_csv(df: pd.DataFrame, get_df: bool = False) -> Union[pd.DataFrame, None]:
    '''
    :param df: Dataframe a ser manipulado.
    :param get_df: Verifica se o usuário deseja que retorne o dataframe.
    :return: None | pandas.DataFrame
    '''

    df.reset_index(inplace=True)
    df.drop(columns='index', inplace=True, errors='ignore')
    df.to_csv(NEW_DF_FILE_NAME, index=False)
    if get_df:
        return df


def load_new_df() -> pd.DataFrame:
    '''
    Carrega o arquivo csv após as operações de limpeza dos dados.
    :return: pandas.DataFrame
    '''

    df = pd.read_csv(NEW_DF_FILE_NAME)
    df.drop('Unnamed: 0', inplace=True, errors='ignore')
    return df
