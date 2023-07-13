import pandas as pd
from pathlib import Path


def carrega_csv(caminho: Path, nome: str, **kwargs) -> pd.DataFrame:
    """
    Função responsável por carregar o arquivo csv e transformá-lo num dataframe

    :param caminho: Indica o caminho a percorrer para carregar o arquivo csv
    :param nome: Nome do dataframe
    :param kwargs: Parâmetros adicionais
    :return: DataFrame
    """
    df = pd.DataFrame()
    if '.csv' in nome:
        df = pd.read_csv(caminho / nome, **kwargs)
    return df


def exporta_csv(caminho_saida: Path, tabela: str, dados_saida: dict) -> None:
    """
    Função responsável por exportar o dataframe num arquivo csv

    :param caminho_saida: Indica a saída do arquivo csv
    :param tabela: Nome do dataframe
    :param dados_saida: Dataframe a ser exportado
    :return: DataFrame
    """
    for arq, df in dados_saida.items():
        df.to_csv(caminho_saida / f'{tabela}.csv', index=False)
