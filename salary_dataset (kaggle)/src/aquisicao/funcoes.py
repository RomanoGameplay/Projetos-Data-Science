import pandas as pd
from pathlib import Path


def carrega_csv(caminho: Path, nome: str, **kwargs) -> pd.DataFrame:
    df = pd.DataFrame()
    if '.csv' in nome:
        df = pd.read_csv(caminho / nome, **kwargs)
    return df


def exporta_csv(caminho_saida: Path, nome: str, ) -> None:
    pass
