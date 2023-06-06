import os
from utils.load_data import load
from cleaning import *
import time
from parallel_pandas import ParallelPandas


def main():
    ParallelPandas.initialize(n_cpu=os.cpu_count(), split_factor=4, disable_pr_bar=True)

    start = time.time()

    df = load()
    print(df.shape[0])

    drop_na(df=df, thresh=4)
    df = str_in_numeric_columns(df=df, column='no_of_ratings', re_exp='[a-zA-Z]')

    df.reset_index(inplace=True)
    df.drop(columns='index', inplace=True, errors='ignore')

    final = time.time()
    print(f'{final - start} segundos')


if __name__ == '__main__':
    main()
# Inserir os métodos necessários e a documentação de cada arquivo.
