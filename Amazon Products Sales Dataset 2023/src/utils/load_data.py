import os
import pandas as pd
from .constant import DIR_CSV_NAME, DF_FILE_NAME


def load() -> pd.DataFrame:
    '''
    :param DIR_CSV_NAME: Representa o nome do diretório no qual estão armazenados os arquivos .csv .
    :param DF_FILE_NAME: Representa o nome do arquivo csv após a concatenação de todos os arquivos.
    :return: pandas.DataFrame
    '''

    cur_dir = os.getcwd()

    if os.path.isfile(f'{cur_dir}/{DF_FILE_NAME}'):
        df = load_from_file_csv(cur_dir=cur_dir)
    else:
        df = load_data_from_dir(cur_dir=cur_dir)

    return df


def load_data_from_dir(cur_dir: str) -> pd.DataFrame:
    '''
    :param cur_dir: Representa o caminho para o diretório atual.
    :param DIR_CSV_NAME: Representa o nome do diretório no qual estão localizados os arquivos .csv .
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
    :param DF_FILE_NAME: Representa o nome do arquivo csv após a concatenação de todos os arquivos.
    :return: pandas.DataFrame
    '''
    # Cria um dataframe diretamente do arquivo salvo.
    df = pd.read_csv(f'{cur_dir}/{DF_FILE_NAME}')

    return df
