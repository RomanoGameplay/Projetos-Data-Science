import os
import time
import numpy as np
from .utils.load_data import load, to_csv
from .utils.constant import NEW_DF_FILE_NAME
from .cleaning import *
from parallel_pandas import ParallelPandas


def main() -> None:
    '''
    Função principal da qual realiza todas as operações necessárias para a limpeza de dados.
    :return: None
    '''

    ParallelPandas.initialize(n_cpu=os.cpu_count(), split_factor=4, disable_pr_bar=True)
    start = time.time()
    print('Carregando dados...\n')
    df = load()
    print('Total de linhas:', df.shape[0])
    print('\nRealizando limpeza dos dados...\n')
    if not os.path.isfile(NEW_DF_FILE_NAME):
        drop_na(df=df, thresh=4)
        df.dropna(subset='actual_price', inplace=True)
        df = str_in_numeric_columns(df=df, columns=['ratings', 'no_of_ratings'], re_exp='[a-zA-Z]')

        df = change_type_column(df=df, column='ratings', type='float')

        list_special_characters = detect_special_characters(df=df, column='no_of_ratings')
        df = replace_values(df=df, column='no_of_ratings', regex=True,
                            old_values=list_special_characters, new_values='',
                            transform_col=True, type='float')

        list_special_characters = detect_special_characters(df=df, column='discount_price')
        replace_values(df=df, column='discount_price', regex=True,
                       old_values=list_special_characters, new_values='')
        df = replace_values(df=df, column='discount_price', regex=True, old_values=np.nan, new_values=0,
                            transform_col=True,
                            type='float')

        list_special_characters = detect_special_characters(df=df, column='actual_price')
        df = replace_values(df=df, column='actual_price', regex=True,
                            old_values=list_special_characters, new_values='',
                            transform_col=True, type='float')
        df = df[df['actual_price'] != 0]
        outliers = search_anomalies_by_standard_deviation(df=df, column='actual_price', show_df_values=False)

        df = df.loc[~df['actual_price'].isin(outliers)]

        df.no_of_ratings.fillna(0, inplace=True)
        df.ratings.fillna(0, inplace=True)
        df.actual_price.fillna(0, inplace=True)
        df.discount_price.fillna(0, inplace=True)

        df.reset_index(inplace=True)
        df.drop(columns='index', inplace=True, errors='ignore')

    print('novo número de linhas:', df.shape[0])
    df = to_csv(df=df, get_df=True)
    final = time.time()
    print(f'{round((final - start), 2)} segundos')
