from pathlib import Path
import pandas as pd
import typing
from tqdm import tqdm
from src.aquisicao._base import BaseMovieETL


class MoviesETL(BaseMovieETL):
    """
    Classe responsável por realizar ETL dos dados do conjunto de dados dos filmes
    """
    _tabela: str

    def __init__(self, entrada: Path, saida: Path, criar_caminho: bool = True, reprocessar: bool = False):
        super().__init__(entrada, saida, criar_caminho, reprocessar)

        self._tabela = 'movies'

    def extract(self) -> None:
        """
        Extrai os dados
        """
        self._logger.info('CARREGANDO DADOS...')
        for tabela in tqdm([self._tabela]):
            self.dados_entrada[tabela] = pd.read_csv(self._cam_entrada / f'{tabela}.csv')
        print(self.dados_entrada)

    def transform(self) -> None:
        """
        Transforma os dados
        """
        pass
